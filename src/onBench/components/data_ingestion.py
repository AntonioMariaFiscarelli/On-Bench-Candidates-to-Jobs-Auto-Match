from src.onBench import logger

from snowflake.snowpark.functions import col, coalesce, lit, regexp_replace, when, length, trim, lower, udf, to_varchar
from snowflake.snowpark.functions import expr, iff, call_function, row_number, substring, count_distinct, avg, min, greatest
from snowflake.snowpark.types import StructType, StructField, StringType, DoubleType, BooleanType

from snowflake.snowpark.functions import abs, round, radians, sin, cos, asin, sqrt

from snowflake.snowpark.window import Window

import pandas as pd
import json
import re

from src.onBench.utils.common import normalize_cap

from src.onBench.entity.config_entity import DataIngestionConfig, DomainConfig


class DataIngestion:
    """
    This class is used to create the candidate and joborder tables.
    It provides methods for:
        - reading the input tables
        - cleanings candidates' CVs and joborder descriptions
        - enriching candidates and joborders with geographical information
    """

    def __init__(self, config: DataIngestionConfig, domain_config = DomainConfig):
        self.config = config
        self.domain_config = domain_config

    def _clean_description(
            self, 
            df, 
            column = "description_cleaned",
            max_tokens = 20000
            ):
        """Clean text fields such as CVs and job descriptions.

        Cleaning steps:
            - Normalize whitespace.
            - Remove HTML tags.
            - Convert text to lowercase.
            - Convert empty strings to NULL.

        Args:
            df (DataFrame): Input dataframe.
            column (str): Column to clean.

        Returns:
            DataFrame: Dataframe with the cleaned column.
        """

        #df = df.filter((col(column).is_not_null()) & (trim(col(column)) != ""))
        
        original_columns = df.columns

        # ------------------------------------------------------------------
        # Remove long meaningless sequences from descriptions, such as:
        # - base64-encoded content
        # - embedded files or images
        # - unusually long random strings
        # ------------------------------------------------------------------

        noise_pattern = r"(/9j/[A-Za-z0-9+/=]{30,}|iVBORw0KGgo[A-Za-z0-9+/=]{30,}|JVBERi0x[A-Za-z0-9+/=]{30,}|[A-Za-z0-9+/=]{80,}|[^A-Za-z0-9 ]{30,}|([A-Za-z]{3,} ){15,})"
        
        df = df.with_column(
            column, 
            regexp_replace(
                col(column), 
                noise_pattern, 
                ""
                )
        )

        # ------------------------------------------------------------------
        # Normalize whitespace:
        # - replace consecutive spaces and tabs
        # - trim leading and trailing spaces
        # ------------------------------------------------------------------

        def build_normalize_whitespace_udf():
            def normalize(text: str) -> str:
                import re
                if text is None:
                    return ''
                return re.sub(r'[ \t]+', ' ', text).strip()

            return udf(normalize, return_type=StringType(), input_types=[StringType()])
        
        normalize_udf = build_normalize_whitespace_udf()

        df = df.with_column(
            column, 
            normalize_udf(df[column])
            )
        

        # ------------------------------------------------------------------
        # Remove HTML tags while preserving textual content.
        # ------------------------------------------------------------------

        def build_clean_html_udf():
            from bs4 import BeautifulSoup
            def clean_html(text: str) -> str:
                if not text:
                    return ""
                return BeautifulSoup(text, "html.parser").get_text()

            return udf(
                clean_html,
                return_type=StringType(),
                input_types=[StringType()],
                packages=["beautifulsoup4"]
            )

        clean_html_udf = build_clean_html_udf()

        df = df.with_column(
            column, 
            clean_html_udf(df[column])
        )

        # ------------------------------------------------------------------
        # Standardize text representation.
        # ------------------------------------------------------------------

        df = df.with_column(
            column, 
            lower(df[column])
        )

        """
        df = df.filter((col(column).is_not_null()) & 
                       (trim(col(column)) != "") &
                       (length(trim(col(column))) > 5) &
                       (~col(column).like("%None%")) &
                       (~col(column).like("%null%"))
                       )
        """
        df = df.with_column(
            column, 
            col(column).cast("STRING")
            )


        # ------------------------------------------------------------------
        # Convert empty strings to NULL.
        # ------------------------------------------------------------------

        df = df.with_column(
            column,
            iff(
                trim(col(column)) == "", 
                None, 
                col(column)
                )
        )

        # ------------------------------------------------------------------
        # Truncate descriptions exceeding the configured size limit
        # ------------------------------------------------------------------

        df = df.with_column(
            "description",
            substring(col("description"), 1, max_tokens)
        )

        logger.info(f"Column {column} successfully cleaned")

        df = df.select(*original_columns)

        return df


    def _build_normalize_city_udf(
            self
            ):
        """
        Build a UDF that converts geographic names into a normalized format
        suitable for matching and lookup operations.

        The normalization logic:
        - converts text to lowercase
        - removes accents and diacritics
        - replaces punctuation and separators with spaces
        - collapses consecutive spaces
        - trims leading and trailing spaces
        - converts empty strings to NULL

        Examples:
            "Milàno"                    -> "milano"
            "Reggio-Emilia"             -> "reggio emilia"
            "L'Aquila"                  -> "l aquila"
            "  Villanova   d'Ardenghi " -> "villanova d ardenghi"

        Returns:
            UserDefinedFunction: Snowpark UDF returning a normalized string.
        """

        def normalize_city(city: str) -> str:

            import unicodedata

            if city is None:
                return None

            # --------------------------------------------------------------
            # Remove leading/trailing spaces and convert empty strings
            # to NULL.
            # --------------------------------------------------------------

            city = city.strip()
            if not city:
                return None

            # --------------------------------------------------------------
            # Normalize Unicode characters and convert to lowercase.
            #
            # Example:
            # "À" -> "à"
            # --------------------------------------------------------------
            city = unicodedata.normalize(
                "NFKD", 
                city.casefold()
                )


            # --------------------------------------------------------------
            # Remove accents and diacritical marks.
            #
            # Example:
            # "milàno" -> "milano"
            # --------------------------------------------------------------

            city = "".join(
                char for char in city
                if not unicodedata.combining(char)
            )


            # --------------------------------------------------------------
            # Replace punctuation and non-alphanumeric characters
            # with spaces.
            #
            # Examples:
            # "l'aquila" -> "l aquila"
            # "reggio-emilia" -> "reggio emilia"
            # -----------

            city = re.sub(
                r"[^a-z0-9]+", 
                " ", 
                city)


            # --------------------------------------------------------------
            # Collapse consecutive spaces and trim the result.
            # --------------------------------------------------------------

            city = re.sub(
                r"\s+", 
                " ", 
                city).strip()

            return city
        
        return udf(
            normalize_city, 
            return_type=StringType(), 
            input_types=[StringType()]
            )

        
    def read_table_candidate(
            self, 
            session
            ):
        """Read and assemble the candidate dataset.

        The dataset is built by combining Bullhorn and PowerBase sources
        and selecting the fields required by the recommendation pipeline.

        Args:
            session (Session): Snowflake session.

        Returns:
            DataFrame: Candidate dataframe.
        """
        
        config = self.config
        database = config.database
        schema = config.input_schema
        input_table = config.candidate.input_table
        input_pb_table = config.candidate.input_pb_table
        days_nomission = config.candidate.days_nomission


        # ------------------------------------------------------------------
        # Build the consolidated candidate dataset:
        # - retrieve candidate status from PowerBase
        # - compute on-bench metrics
        # - compute absenteeism indicators
        # - retrieve candidate profile information
        # - reconstruct job history
        # - merge all sources into a single dataset
        # ------------------------------------------------------------------

        query = f""" 

            WITH dati_sospensione AS (
                SELECT 
                    dbSospCandNo AS db_sosp_cand_no,
                    dbSospDescGiust AS db_sosp_desc_giust,
                    dbSospDataInizio AS db_sosp_data_inizio,
                    dbSospDataFine AS db_sosp_data_fine
                FROM (
                    SELECT *,
                        ROW_NUMBER() OVER (
                            PARTITION BY dbsospcandno
                            ORDER BY load_datetimestamp DESC
                        ) AS rn2
                    FROM IT_DISCOVERY.CONSUMER_INT_MODEL.PB_FTCANDTISOSPENSIONEDISP_CLEANED -- PARAMETRISE
                )
                WHERE rn2 = 1
            ),

            absenteeism_candidate AS (
                SELECT candidato_pb as candidate_id_pb, SUM(ore_malattie_correnti) AS hours_leave, sum(ore_lavorabili) AS hours_worked,
                CASE 
                    WHEN hours_worked > 0 THEN ROUND(hours_leave/hours_worked, 4)
                    ELSE null 
                END AS absenteeism_rate
                FROM aas_it_hist_prod.finance.dati_ore_costi
                GROUP BY candidate_id_pb
            ),

            candidate_pb_src AS (
                SELECT
                    ca.dbcandno AS db_cand_no,
                    ca.dbcandpcode AS db_cand_pcode,
                    ca.dbcandsuburb AS db_cand_suburb,
                    ca.dbcandstate AS db_cand_state,
                    ca.dbAssTINoMission AS db_ass_ti_nomission,
                    ca.dbAssTIDataCessaz AS db_ass_ti_data_cessaz,
                    d.db_sosp_desc_giust,
                    j.dbjobordno AS jo_filled,
                FROM IT_DISCOVERY.CONSUMER_INT_MODEL.PB_FTCANDIDATE_CLEANED ca

                LEFT JOIN dati_sospensione d
                    ON ca.dbcandno = d.db_sosp_cand_no
                    AND (d.db_sosp_data_fine >= CURRENT_DATE OR d.db_sosp_data_fine IS NULL)
                    AND (d.db_sosp_data_inizio <= CURRENT_DATE OR d.db_sosp_data_inizio IS NULL)
                    AND d.db_sosp_desc_giust NOT ILIKE 'mal.no%'

                LEFT JOIN IT_DISCOVERY.CONSUMER_INT_MODEL.PB_FTJOBORDER_CLEANED j
                    ON ca.dbcandno = j.dbjobcandno
                    AND j.dbjobstatus IN (2,4)
                    AND j.dbjobenddate >= j.dbjobdatecommence
                    AND j.dbjobenddate >= CURRENT_DATE AND j.dbjobdatecommence <= CURRENT_DATE
                    --A candidate is ricollocato if either 
                    --1) is onbench atm and has any job that has started/will start 
                    --2) will be onbench in the future and has any job after the onbench date
                    --j.dbjobenddate >
                    --    DATEADD(
                    --        DAY,
                    --        30, --PARAMETRISE: if a candidate is ricollocato but its job ends within N days from today/future no mission day, it is still considered no mission
                    --        GREATEST(CURRENT_DATE, COALESCE(ca.db_ass_ti_nomission, DATE '1900-01-01'))
                    --        )  
                    --AND j.dbjobdatecommence BETWEEN reference_date
                    --    AND DATEADD( DAY, 30, reference_date) --PARAMETRISE   
                    --AND j.dbjobenddate BETWEEN DATEADD( DAY, 31, reference_date) --PARAMETRISE
                    --    AND DATEADD( DAY, 60, reference_date) --PARAMETRISE   
                WHERE ca.dbAssTINoMission IS NOT NULL
                AND ca.dbAssTINoMission <= DATEADD(DAY, 180, CURRENT_DATE)  --PARAMETRISE
            ),

            candidate_pb_status AS (
                SELECT DISTINCT
                    c.db_cand_no AS candidate_id_pb,
                    mc.bhid AS candidate_id_bh,
                    c.db_cand_pcode,
                    c.db_cand_suburb,
                    c.db_cand_state,
                    c.db_ass_ti_nomission,
                    CASE
                        WHEN c.db_ass_ti_data_cessaz <= CURRENT_DATE THEN 'Cessato'
                        WHEN c.db_ass_ti_nomission > CURRENT_DATE THEN 'Futuro no mission'
                        WHEN c.db_ass_ti_nomission <= CURRENT_DATE
                            AND c.jo_filled IS NULL
                            AND c.db_sosp_desc_giust IS NULL
                            THEN 'No mission'
                        WHEN c.db_ass_ti_nomission <= CURRENT_DATE
                            AND c.jo_filled > 0
                            THEN 'Ricollocato'
                        WHEN c.db_ass_ti_nomission < CURRENT_DATE
                            AND c.db_sosp_desc_giust IS NOT NULL
                            THEN 'Indisponibile'
                        ELSE 'da controllare'
                    END AS candidate_status
                FROM candidate_pb_src c
                LEFT JOIN IT_DISCOVERY.CONSUMER_INT_MODEL.PB_MATRIX_CANDIDATE_CLEANED mc -- PARAMETRISE
                    ON c.db_cand_no = mc.MOID
                WHERE candidate_status = 'No mission' OR candidate_status = 'Futuro no mission'
            ),
            
            candidate_pb_daysinfo AS (
                SELECT 
                    c.candidate_id_pb, 
                    c.candidate_id_bh,
                    c.candidate_status, 
                    c.db_cand_pcode,
                    c.db_cand_suburb,
                    c.db_cand_state,
                    c.db_ass_ti_nomission, 
                    MAX(jo.dbjobdatecommence) AS last_onbench_job_start_date, 
                    MAX(jo.dbjobenddate) AS last_onbench_job_end_date, 
                    CASE
                        WHEN c.candidate_status = 'No mission' THEN COALESCE(SUM(DATEDIFF(day, jo.dbjobdatecommence, jo.dbjobenddate) + 1), 0) 
                        ELSE 0 
                    END AS tot_days_job,
                    CASE
                        WHEN c.candidate_status = 'No mission' THEN COUNT(DISTINCT(jo.dbjobdatecommence)) 
                        ELSE 0
                    END AS num_jobs_during_onbench
                FROM candidate_pb_status c
                LEFT JOIN
                    (
                    SELECT
                        dbjobcandno,
                        dbjobdatecommence, 
                        dbjobenddate
                    FROM IT_DISCOVERY.CONSUMER_INT_MODEL.PB_FTJOBORDER_CLEANED 
                    WHERE dbjobdatecommence <= dbjobenddate AND dbjobdatecommence IS NOT NULL and dbjobenddate IS NOT NULL
                    ) jo
                ON c.candidate_id_pb = jo.dbjobcandno
                AND c.db_ass_ti_nomission <= jo.dbjobdatecommence and jo.dbjobenddate <= CURRENT_DATE
                GROUP BY c.candidate_id_pb, c.candidate_id_bh, c.db_ass_ti_nomission, c.candidate_status, c. db_cand_pcode, db_cand_suburb, db_cand_state
            ),
            
            candidate_pb AS (
                SELECT 
                    c.candidate_id_pb, 
                    candidate_id_bh, 
                    candidate_status,
                    db_cand_pcode,
                    db_cand_suburb,
                    db_cand_state,
                    db_ass_ti_nomission, 
                    last_onbench_job_start_date, 
                    last_onbench_job_end_date, 
                    tot_days_job,
                    num_jobs_during_onbench,
                    -- days_since_onbench will be positive for No Mission and negative for Future no mission
                    COALESCE(DATEDIFF(day, db_ass_ti_nomission, CURRENT_DATE)+1, NULL) AS days_since_onbench,
                    CASE
                        WHEN candidate_status = 'No mission' THEN COALESCE(DATEDIFF(day, last_onbench_job_end_date, CURRENT_DATE)+1, NULL) 
                        ELSE 0
                    END AS days_since_last_job,      
                    -- days_onbench_last and days_onbench_tot will be the same for No mission candidates with no jobs during the nomission period, and for Future no mission
                    LEAST(days_since_onbench, days_since_last_job) as days_onbench_last,
                    COALESCE(days_since_onbench - tot_days_job, days_onbench_last) AS days_onbench_tot,  
                    CASE
                        WHEN candidate_status = 'No mission' THEN COALESCE(CAST(100*(days_since_onbench-tot_days_job)/(days_since_onbench) AS INTEGER), NULL) 
                        ELSE 0
                    END AS days_onbench_perc,      
                ca.absenteeism_rate
                FROM candidate_pb_daysinfo c
                --WHERE candidate_status = 'No mission'
                LEFT JOIN absenteeism_candidate ca
                ON c.candidate_id_pb = ca.candidate_id_pb
            ),

            candidate AS (
                SELECT
                    candidateid AS candidate_id_bh,
                    pb_id AS candidate_id_pb,
                    first_name,
                    last_name,
                    cap,
                    city,
                    provincia AS province, 
                    skill_set AS skills,
                    education_degree AS education,
                    degree_list AS certifications, 
                    salary_low AS salary, 
                    date_available,
                    will_relocate,
                    desired_location, 
                    parttime_preferenza_perc, 
                    turno_preferenza,
                    description AS curriculum
                FROM IT_DISCOVERY.CONSUMER_INT_MODEL.CANDIDATE_CLEANED -- PARAMETRISE
                WHERE isdeleted = 0
                AND is_anonymized = 0
            ),

            jobs_ranked AS (
                SELECT
                    dbjobordno AS db_jobord_no,
                    dbjobcandno AS db_job_cand_no,
                    dbjobsite AS db_job_site,
                    dbjobposition AS db_job_position,
                    dbjobdatecommence AS db_job_date_commence,
                    dbjobenddate AS db_job_end_date,
                    dbiparttimepcnt dbi_parttime_pcnt,
                    CASE
                        WHEN ROW_NUMBER() OVER (
                            PARTITION BY dbjobcandno
                            ORDER BY dbjobdatecommence
                            ) = 1
                        THEN 'first'
                        WHEN ROW_NUMBER() OVER (
                            PARTITION BY dbjobcandno
                            ORDER BY dbjobdatecommence DESC
                            ) = 1
                        THEN 'last'
                        ELSE 'intermediate'
                    END AS job_rank  
                FROM IT_DISCOVERY.CONSUMER_INT_MODEL.pb_ftjoborder_cleaned -- PARAMETRISE
            ),
            
            jobs_ranked_enriched AS (
                SELECT
                    jo.db_jobord_no,
                    jo.db_job_cand_no,
                    jo.job_rank,
                    jo.db_job_site,
                    jo.db_job_position,
                    jo.db_job_date_commence,
                    jo.db_job_end_date,
                    jo.dbi_parttime_pcnt,
                    je.dbilevel as ccnl_level,
                    je.dbiqualification as legal_category,
                    pc.ccnl_codice as ccnl_code,
                    pc.ccnl_descrizione as ccnl_description
                FROM jobs_ranked jo
                INNER JOIN IT_DISCOVERY.CONSUMER_INT_MODEL.pb_ftjobext_cleaned je -- PARAMETRISE
                    ON jo.db_jobord_no = je.dbjobordno
                LEFT JOIN IT_DISCOVERY.CONSUMER_INT_MODEL.v_ppccnlsalias pc -- PARAMETRISE
                    ON pc.chiave = je.ccnlid
            ),
            
            jobs_ranked_agg AS (
                SELECT
                    db_job_cand_no,
                    MAX(CASE WHEN job_rank = 'first' THEN db_jobord_no END) AS joborder_id_first,
                    MAX(CASE WHEN job_rank = 'first' THEN db_job_site END) AS job_site,
                    MAX(CASE WHEN job_rank = 'first' THEN db_job_position END) AS job_first,
                    NULLIF(
                        LISTAGG(
                            CASE WHEN job_rank = 'intermediate' THEN db_job_position END,
                            ', '
                        ) WITHIN GROUP (ORDER BY db_job_date_commence),
                        ''
                    ) AS job_intermediate,
                    MAX(CASE WHEN job_rank = 'last' THEN db_job_position END) AS job_last,
                    MIN(db_job_date_commence) AS start_date,
                    MAX(db_job_end_date) AS end_date,
                    MAX(CASE WHEN job_rank = 'first' THEN dbi_parttime_pcnt END) AS parttime_perc,
                    MAX(CASE WHEN job_rank = 'first' THEN ccnl_level END) AS ccnl_level,
                    MAX(CASE WHEN job_rank = 'first' THEN legal_category END) AS legal_category,
                    MAX(CASE WHEN job_rank = 'first' THEN ccnl_code END) AS ccnl_code,
                    MAX(CASE WHEN job_rank = 'first' THEN ccnl_description END) AS ccnl_description

                FROM jobs_ranked_enriched
                GROUP BY db_Job_cand_no
            )
            
            SELECT
                cc.candidate_id_bh,
                cc.candidate_id_pb,
                cc.first_name,
                cc.last_name,
                --cc.cap,
                --cc.city,
                --cc.province,
                cob.db_cand_pcode as cap,
                cob.db_cand_suburb as city,
                cob.db_cand_state as province,
                /*
                cc.skills,
                cc.education,
                cc.certifications,
                cc.salary,
                cc.date_available,
                cc.will_relocate,
                cc.desired_location,
                cc.parttime_preferenza_perc,
                cc.turno_preferenza,
                cc.curriculum,
                */
                IFF(
                    COALESCE(
                        NULLIF(TRIM(cc.curriculum), ''),
                        NULLIF(TRIM(cc.skills), ''),
                        NULLIF(TRIM(cc.education), ''),
                        NULLIF(TRIM(cc.certifications), '')
                    ) IS NULL,
                    NULL,
                    CONCAT(
                        'Curriculum: ', COALESCE(cc.curriculum, ''), ' | ',
                        'Skills: ', COALESCE(cc.skills, ''), ' | ',
                        'Titoli di studio richiesti: ', COALESCE(cc.education, ''), ' | ',
                        'Certificazioni: ', COALESCE(cc.certifications, ''), ' | '
                    )
                ) AS description,
                --jri.joborder_id_first,
                jri.job_site,
                jri.job_first,
                jri.job_intermediate,
                jri.job_last,
                --jri.start_date,
                --jri.end_date,
                jri.parttime_perc,
                jri.ccnl_level,
                jri.legal_category,
                jri.ccnl_code,
                jri.ccnl_description,
                cob.candidate_status,
                cob.days_onbench_last,
                cob.days_onbench_tot,
                cob.days_onbench_perc,
                cob.absenteeism_rate,
            FROM candidate cc
            INNER JOIN candidate_pb cob
                ON cc.candidate_id_pb = cob.candidate_id_pb
            LEFT JOIN jobs_ranked_agg jri
                ON cc.candidate_id_pb = jri.db_job_cand_no
            --order by num_jobs_during_onbench desc
            
        """
        
        df = session.sql(query)

        logger.info(f"Table {input_table} successfully read")
        
        return df


    def read_table_joborder(
            self, 
            session
            ):
        """Read and assemble the joborder dataset.

        The dataset is built by combining Bullhorn and PowerBase sources
        and selecting the fields required by the recommendation pipeline.

        Args:
            session (Session): Snowflake session.

        Returns:
            DataFrame: Joborder dataframe.
        """
    
        config = self.config
        database = config.database
        schema = config.input_schema
        input_table = config.joborder.input_table


        # ------------------------------------------------------------------
        # Build the consolidated joborder dataset:
        # - aggregate CCNL information
        # - retrieve joborder data from PowerBase
        # - retrieve joborder data from Bullhorn
        # - retrieve latest submission activity
        # - retrieve custom joborder attributes
        # - merge all sources into a single dataset
        # ------------------------------------------------------------------

        query = f"""
        WITH 
            ccnls_alias AS(
                SELECT 
                    chiave as key,
                    LISTAGG(DISTINCT ccnl_codice, '||') WITHIN GROUP (ORDER BY ccnl_codice) AS ccnl_code,
                    LISTAGG(DISTINCT ccnl_descrizione, '||') WITHIN GROUP (ORDER BY ccnl_descrizione) AS ccnl_description
                FROM  IT_DISCOVERY.CONSUMER_INT_MODEL.V_PPCCNLSALIAS -- PARAMETRISE
                GROUP BY key
                ),
                
            joborder_pb AS (
                SELECT
                    jo.dbjobordno AS joborder_id_pb,
                    jo.dbjobcandno AS candidate_id_pb,
                    jo.dbjobsite AS job_site,
                    jo.dbjobposition AS job_position,
                    --jo.dbiparttimepcnt,
                    je.dbilevel AS ccnl_level,
                    je.dbiqualification AS legal_category,
                    cc.ccnl_code,
                    cc.ccnl_description
                FROM  IT_DISCOVERY.CONSUMER_INT_MODEL.PB_FTJOBORDER_CLEANED jo -- PARAMETRISE
                LEFT JOIN  IT_DISCOVERY.CONSUMER_INT_MODEL.PB_FTJOBEXT_CLEANED je 
                    ON jo.dbjobordno = je.dbjobordno
                LEFT JOIN ccnls_alias cc
                    ON je.ccnlid = cc.key 
                ),
                
            joborder_bh_src AS (
                SELECT 
                    joborderid AS joborder_id_bh,
                    pb_id AS joborder_id_pb,
                    jobtitle AS job_title,
                    position_esco,
                    citta AS city,
                    cap,
                    regione AS region,
                    salary,
                    data_inizio_validita,
                    skill_list AS skills,
                    part_time_percent AS parttime_percent,
                    titoli_richiesti,
                    descrizione_uso_interno,
                    testo_pubblicazione,
                    richieste_aggiuntive,
                    requisiti,
                    isopen AS is_open, 
                    status, 
                    datelastmodified as date_last_modified
                FROM IT_DISCOVERY.CONSUMER_INT_MODEL.JOBORDER_CLEANED -- PARAMETRISE
                WHERE isopen = 1 AND status = 'Accepting Candidates'
            ),
            
            jobsubmission_status AS (
                SELECT
                    joborderid AS joborder_id_bh,
                    MAX(data_ultimo_cambio_status) AS last_status_change
                FROM  IT_DISCOVERY.CONSUMER_INT_MODEL.JOBSUBMISSION_CLEANED -- PARAMETRISE
                GROUP BY joborderid
            ),

            joborder_custom_object_instance AS (
                SELECT 
                    joborder_id_bh,
                    ruolo_lavorativo_standard_mpg
                FROM 
                (
                    SELECT 
                        joborderid AS joborder_id_bh, 
                        textblock1 AS ruolo_lavorativo_standard_mpg,
                        ROW_NUMBER() OVER (PARTITION BY joborderid ORDER bY LOAD_DATETIMESTAMP DESC) AS row_num
                    FROM aas_it_hist_prod.bullhorn.jobordercustomobjectinstance
                    WHERE instancenumber = 4 AND isdeleted = 0
                ) 
                WHERE row_num = 1),
            
            joborder_bh AS (
                SELECT 
                    jo.joborder_id_bh,
                    jo.joborder_id_pb,
                    jo.job_title,
                    jo.position_esco,
                    joc.ruolo_lavorativo_standard_mpg,
                    jo.city,
                    jo.cap,
                    jo.region,
                    jo.salary,
                    jo.data_inizio_validita,
                    jo.skills,
                    jo.parttime_percent,
                    jo.titoli_richiesti,
                    jo.descrizione_uso_interno,
                    jo.testo_pubblicazione,
                    jo.richieste_aggiuntive,
                    jo.requisiti,
                    jo.is_open, 
                    jo.status, 
                    jo.date_last_modified,
                    js.last_status_change,
                FROM joborder_bh_src jo 
                LEFT JOIN jobsubmission_status js 
                ON jo.joborder_id_bh = js.joborder_id_bh
                LEFT JOIN joborder_custom_object_instance joc on jo.joborder_id_bh = joc.joborder_id_bh
            ),
            
            joborder AS (
            SELECT  
                jo_bh.joborder_id_bh,
                jo_bh.joborder_id_pb,
                jo_bh.job_title,
                jo_bh.position_esco,
                jo_bh.ruolo_lavorativo_standard_mpg,
                jo_bh.city,
                jo_bh.cap,
                jo_bh.region,
                jo_bh.salary,
                jo_bh.data_inizio_validita,
                jo_bh.skills,
                jo_bh.parttime_percent,
                jo_bh.titoli_richiesti,
                jo_bh.descrizione_uso_interno,
                jo_bh.testo_pubblicazione,
                jo_bh.richieste_aggiuntive,
                jo_bh.requisiti,
                jo_bh.is_open, 
                jo_bh.status, 
                jo_bh.date_last_modified,
                jo_bh.last_status_change,
                jo_pb.job_site,
                jo_pb.job_position,
                jo_pb.ccnl_level,
                jo_pb.legal_category,
                jo_pb.ccnl_code,
                jo_pb.ccnl_description
            FROM joborder_bh jo_bh
            LEFT JOIN joborder_pb jo_pb
            ON CAST(jo_bh.joborder_id_pb AS STRING) = CAST(jo_pb.joborder_id_pb AS STRING)
            )
            
            SELECT
                joborder_id_bh,
                joborder_id_pb,
                job_title,
                position_esco,
                ruolo_lavorativo_standard_mpg,
                cap,
                city,
                region,
                /*
                salary,
                data_inizio_validita,
                skills,
                requisiti,
                */
                job_site,
                parttime_percent,
                ccnl_level,
                legal_category,
                ccnl_code,
                ccnl_description,
                /*
                titoli_richiesti,
                descrizione_uso_interno,
                testo_pubblicazione,
                richieste_aggiuntive,
                */
                /*
                CASE 
                    WHEN date_last_modified IS NULL AND last_status_change IS NULL THEN NULL
                    ELSE GREATEST(
                    COALESCE(date_last_modified, TO_TIMESTAMP('0001-01-01 00:00:00')),
                    COALESCE(last_status_change, TO_TIMESTAMP('0001-01-01 00:00:00'))
                    )
                END AS date_last_active,
                */
                IFF(
                    COALESCE(
                        NULLIF(TRIM(skills), ''),
                        NULLIF(TRIM(parttime_percent), ''),
                        NULLIF(TRIM(titoli_richiesti), ''),
                        NULLIF(TRIM(descrizione_uso_interno), ''),
                        NULLIF(TRIM(testo_pubblicazione), ''),
                        NULLIF(TRIM(richieste_aggiuntive), ''),
                        NULLIF(TRIM(requisiti), '')
                    ) IS NULL,
                    NULL,
                    CONCAT(
                        'Skills richieste: ', COALESCE(skills, ''), ' || ',
                        --'Percentuale part time: ', COALESCE(parttime_percent, ''), ' || ',
                        'Titoli di studio richiesti: ', COALESCE(titoli_richiesti, ''), ' || ',
                        'DESCRIZIONE USO INTERNO: ', COALESCE(descrizione_uso_interno, ''), ' || ',
                        'TESTO PUBBLICAZIONE: ', COALESCE(testo_pubblicazione, ''), ' || ',
                        'RICHIESTE AGGIUNTIVE: ', COALESCE(richieste_aggiuntive, ''), ' || ',
                        'REQUISITI: ', COALESCE(requisiti, '')
                    )
                ) AS description
                
            FROM joborder
        """
        
        df = session.sql(query)

        logger.info(f"Table {input_table} successfully read")

        return df

    
    def clean_table_candidate(
            self, 
            session
            ):
        """Clean and standardize the candidate table.

        Cleaning steps:
            - Clean and normalize description text.
            - Remove descriptions exceeding the configured length limit.
            - Standardize part-time percentage values.
            - Trim and lowercase all string columns.
            - Normalize geographic fields used for matching.

        Args:
            session (Session): Snowflake session.

        Returns:
            DataFrame: Cleaned candidate dataframe.
        """

        config = self.config
        domain_config = self.domain_config
        database = config.database
        schema = config.output_schema
        input_table = config.candidate.output_table
        max_tokens = config.llm.max_tokens

        df = session.table(f"{database}.{schema}.{input_table}")  

        original_columns = df.columns

        # ------------------------------------------------------------------
        # Apply description cleaning and normalization.
        # ------------------------------------------------------------------

        df = self._clean_description(df, "description", max_tokens)
        

        # ------------------------------------------------------------------
        # Standardize part-time percentage:
        # - missing values default to 100
        # - 0 is treated as unknown
        # ------------------------------------------------------------------

        df = (
            df
            .with_column(
                "parttime_perc",
                coalesce(col("parttime_perc"), lit(100))
            )
            .with_column(
                "parttime_perc",
                when(col("parttime_perc") == 0, None)
                .otherwise(col("parttime_perc"))
            )
        )

        # ------------------------------------------------------------------
        # Convert CCNL code to string.
        # ------------------------------------------------------------------

        df = df.with_column(
            "ccnl_code", 
            to_varchar(
                col("ccnl_code").cast("int")
                )
        )


        # ------------------------------------------------------------------
        # Standardize all string columns:
        # - trim leading and trailing spaces
        # - convert values to lowercase
        # ------------------------------------------------------------------

        string_cols = [
            f.name
            for f in df.schema.fields
            if f.datatype.__class__.__name__ == "StringType"
        ]
        for c in string_cols:
            df = df.with_column(
                c, 
                lower(
                    trim(
                        col(c)
                    )
                )
            )

        # ------------------------------------------------------------------
        # Normalize geographic fields used for matching.
        # ------------------------------------------------------------------

        normalize_city_udf = self._build_normalize_city_udf()

        df = (
            df 
            .with_column(
                "city",
                normalize_city_udf(col("city"))
            ) 
            .with_column(
                "province",
                normalize_city_udf(col("province"))
            )
            .with_column(
                "job_site_norm",
                normalize_city_udf(col("job_site"))
            )
        )

        logger.info(f"Table {input_table} successfully cleaned")

        df = df.select(
            *original_columns,
            "job_site_norm"
            )

        return df


    def clean_table_joborder(
            self, 
            session
            ):
        """Clean and standardize the joborder table.

        Cleaning steps:
            - Trim and lowercase all string columns.
            - Normalize city and region values.
            - Remove long meaningless character sequences.
            - Clean description text.
            - Remove descriptions exceeding the configured length limit.

        Args:
            session (Session): Snowflake session.

        Returns:
             DataFrame: Cleaned joborder dataframe.
        """

        config = self.config
        database = config.database
        schema = config.output_schema
        input_table = config.joborder.output_table
        max_tokens = config.llm.max_tokens

        df = session.table(f"{database}.{schema}.{input_table}")

        original_columns = df.columns
        
        # ------------------------------------------------------------------
        # Standardize part-time percentage:
        # - missing values default to 100
        # - 0 is treated as unknown
        # ------------------------------------------------------------------

        df = (
            df

            # 1. Normalizzazione
            .with_column(
                "normalized_value",
                regexp_replace(
                    lower(trim(col("parttime_percent"))),
                    ",",
                    "."
                )
            )

            # 2. Estrazione primo numero
            .with_column(
                "number_1",
                call_function(
                    "TRY_TO_DOUBLE",
                    call_function(
                        "REGEXP_SUBSTR",
                        col("normalized_value"),
                        r"[0-9]+([.][0-9]+)?",
                        lit(1),
                        lit(1),
                        lit("e"),
                        lit(0)
                    )
                )
            )

            # 3. Estrazione secondo numero
            .with_column(
                "number_2",
                call_function(
                    "TRY_TO_DOUBLE",
                    call_function(
                        "REGEXP_SUBSTR",
                        col("normalized_value"),
                        r"[0-9]+([.][0-9]+)?",
                        lit(1),
                        lit(2),
                        lit("e"),
                        lit(0)
                    )
                )
            )

            # 4. Estrazione ore
            .with_column(
                "hours_value",
                call_function(
                    "TRY_TO_DOUBLE",
                    call_function(
                        "REGEXP_SUBSTR",
                        col("normalized_value"),
                        r"^[ ]*([0-9]+([.][0-9]+)?)[ ]+ore$",
                        lit(1),
                        lit(1),
                        lit("e"),
                        lit(1)
                    )
                )
            )

            # 5. Conversione finale
            .with_column(
                "parttime_percent",
                when(
                    col("normalized_value").isin("part time", "si"),
                    lit(50.0)
                )
                .when(
                    col("hours_value").is_not_null(),
                    col("hours_value") * lit(2.5)
                )
                .when(
                    col("number_2").is_not_null(),
                    greatest(col("number_1"), col("number_2"))
                )
                .when(
                    col("number_1").is_not_null(),
                    col("number_1")
                )
                .otherwise(lit(100.0))
            )

            # 6. Rimozione colonne temporanee
            .drop(
                "normalized_value",
                "number_1",
                "number_2",
                "hours_value"
            )
        )

        # ------------------------------------------------------------------
        # Standardize all string columns:
        # - trim leading and trailing spaces
        # - convert values to lowercase
        # ------------------------------------------------------------------
        
        string_cols = [
            f.name
            for f in df.schema.fields
            if f.datatype.__class__.__name__ == "StringType"
        ]
        for c in string_cols:
            df = df.with_column(c, lower(trim(col(c))))

 
        # ------------------------------------------------------------------
        # Normalize geographic fields used for matching.
        # ------------------------------------------------------------------
        
        normalize_city_udf = self._build_normalize_city_udf()

        df = (
            df
            .with_column(
                "city",
                normalize_city_udf(col("city"))
            ) 
            .with_column(
                "region",
                normalize_city_udf(col("region"))
            )
        )


        #df = df.with_column("description_raw", col("description"))
        

        # ------------------------------------------------------------------
        # Apply description cleaning:
        # - remove HTML tags
        # - normalize whitespace
        # - perform text standardization
        # ------------------------------------------------------------------
        
        df = self._clean_description(df, "description", max_tokens)
        #df = self.clean_description(df, "descrizione_uso_interno")
        #df = self.clean_description(df, "testo_pubblicazione")

        
        logger.info(f"Table {input_table} successfully cleaned")

        df = df.select(*original_columns)

        return df


    def resolve_location_candidate(
            self,
            session
            ):
        """
        Resolve CAP and city for candidate table.

        Args:
            session (Session): Snowflake session.
            df (DataFrame): dataframe to be enriched

        Returns:
            DataFrame: enriched dataframe.
        """

        config = self.config
        database = config.database
        schema = config.output_schema
        candidate_table = config.candidate.output_cleaned_table

        df_candidate = session.table(
            f"{database}.{schema}.{candidate_table}"
        )

        df = self._resolve_location(session, df_candidate)

        return df


    def resolve_location_joborder(
            self,
            session
            ):
        """
        Resolve CAP and city for joborder table.

        Args:
            session (Session): Snowflake session.

        Returns:
            DataFrame: enriched dataframe.
        """

        config = self.config
        database = config.database
        schema = config.output_schema
        joborder_table = config.joborder.output_cleaned_table

        df_joborder = session.table(
            f"{database}.{schema}.{joborder_table}"
        )

        df = self._resolve_location(session, df_joborder)

        return df


    def _resolve_location(
            self,
            session,
            df
            ):
        """
        Resolve and standardize locations using the ITALIAN_CITIES reference table.
                    
        Output columns:
            - resolved_cap
            - resolved_city

        ==========
        Input data

        The candidate/joborder table must contains:
            - cap
            - city
        The data may contain:
            - correct CAPs and cities
            - incorrect CAPs and cities
            - missing CAPs and cities

        The ITALIAN_CITIES reference table contains one row for each
        unique (cap, city) pair and provides:
            - cap
            - city
            - city_normalized
            - province
            - region
            - lat
            - lng

        ================
        Data assumptions

        A (cap, city) pair uniquely identifies a location and therefore its coordinates.
        The reference data exhibits the following characteristics:
            - A city can be associated with multiple CAPs. This mainly occurs in large cities and therefore affects
            a large proportion of candidates.
            - A CAP can be associated with multiple cities. This mainly occurs in small villages and therefore affects
            a relatively small proportion of candidates.

        Because city ambiguity is more common than CAP ambiguity, CAP is treated as the most reliable location attribute and is
        used as the primary source of truth whenever available.

        ============
        Resolved CAP

        The resolved CAP is determined using the following priority:
        1. VALID CAP
            If the candidate CAP exists in ITALIAN_CITIES:
                resolved_cap = cap
        2. UNIQUE CITY → CAP
            Otherwise, if the candidate city exists in ITALIAN_CITIES and is associated with exactly one CAP:
                resolved_cap = corresponding CAP
        3. UNRESOLVED
            Otherwise:
                resolved_cap = NULL

        Examples:

        1. 
        cap = 20121
        city = milano
            -> resolved_cap = 20121

        cap = NULL
        city = novi ligure
            novi ligure is associated with a single CAP
            -> resolved_cap = 15067

        3. 
        cap = NULL
        city = milano
            roma is associated with many CAPs
            -> resolved_cap = NULL

        cap = 71100
        city = Foggia
            71100 does not exist anymore. Furthermore Foggia is associated with two different caps (71121, 71122)
            -> resolved_cap = NULL

        =============
        Resolved City

        The resolved city is determined using the following priority:
        1. VALID CITY FOR RESOLVED CAP
            If resolved_cap is valid:
                - retrieve all cities associated with resolved_cap
                - if the candidate city belongs to that set, keep the candidate city
        2. UNIQUE CAP → CITY
            Otherwise, if resolved_cap is valid and is associated with exactly one city:
                resolved_city = corresponding city
        3. VALID CITY
            Otherwise, If resolved_cap is NULL and the candidate city exists in ITALIAN_CITIES:
                resolved_city = city
        4. UNRESOLVED
            Otherwise:
                resolved_city = NULL

                
        Examples:

        1. 
        cap = 15067
        city = novi ligure
            15067 -> novi ligure
            -> resolved_city = novi ligure

        cap = 10030
        city = Rondissone
            41049 is associated with:
                - Maglione, Rondissone, Vestignè, Villareggia, e Vische
            more than one possible city, but city matches Ronsissone
            -> resolved_city = Rondissone

        2.
        cap = 15067
        city = Null
            15067 -> novi ligure
            -> resolved_city = novi ligure

        cap = 15067
        city = Genova
            15067 is associated with Novi Ligure, not Genova (probably a mistake)
            -> resolved_city = novi ligure

        3.
        cap = NULL
        city = milano
            milano exists in ITALIAN_CITIES
            -> resolved_city = milano

        4. 
        cap = 10030
        city = invalid_city
            41049 is associated with:
                - Maglione, Rondissone, Vestignè, Villareggia, e Vische
            more than one possible city
            -> resolved_city = NULL

        cap = NULL
        city = invalid_city
            -> resolved_city = NULL

            
        Args:
            session (Session): Snowflake session.
            df_entity (DataFrame): dataframe to be resolved

        Returns:
            DataFrame: resolved dataframe.
        """

        """
        How to check that it works properly:

        select cap, city, resolved_cap, resolved_city
        from MPG_IT_ONBENCH_TEST_CANDIDATE_CLEANED --MPG_IT_ONBENCH_TEST_JOBORDER_CLEANED
        where resolved_cap is null or resolved_city is null
        --where cap != resolved_cap or city != resolved_city
        """

        config = self.config
        database = config.database
        schema = config.output_schema
        italian_cities_table = config.italian_cities.output_table

        original_columns = df.columns

        df_exact = (
            session.table(
                f"{database}.{schema}.{italian_cities_table}"
            )
            .filter(col("location_type") == "EXACT")
        )

        valid_caps = (
            df_exact
            .select(
                col("cap").alias("valid_cap")
            )
            .distinct()
        )

        valid_cities = (
            df_exact
            .select(
                col("city_normalized").alias("valid_city")
            )
            .distinct()
        )

        # ------------------------------------------------------------------
        # Cities associated with exactly one CAP.
        # ------------------------------------------------------------------

        city_unique_cap = (
            df_exact
            .group_by("city_normalized")
            .agg(
                count_distinct("cap").alias("cap_count"),
                min("cap").alias("unique_cap")
            )
            .filter(col("cap_count") == 1)
            .select(
                col("city_normalized").alias("unique_city"),
                col("unique_cap")
            )
        )

        # ------------------------------------------------------------------
        # CAPs associated with exactly one city.
        # ------------------------------------------------------------------

        cap_unique_city = (
            df_exact
            .group_by("cap")
            .agg(
                count_distinct("city_normalized").alias("city_count"),
                min("city_normalized").alias("unique_city")
            )
            .filter(col("city_count") == 1)
            .select(
                col("cap").alias("unique_cap"),
                col("unique_city")
            )
        )

        # ------------------------------------------------------------------
        # Valid CAP/CITY pairs.
        # ------------------------------------------------------------------

        valid_pairs = (
            df_exact
            .select(
                col("cap").alias("pair_cap"),
                col("city_normalized").alias("pair_city")
            )
            .distinct()
            .with_column(
                "valid_pair",
                lit(True)
            )
        )

        # ------------------------------------------------------------------
        # Resolve CAP.
        # ------------------------------------------------------------------

        df = (
            df
            .join(
                valid_caps,
                col("cap") == col("valid_cap"),
                "left"
            )
            .join(
                city_unique_cap,
                col("city") == col("unique_city"),
                "left"
            )
            .with_column(
                "resolved_cap",
                when(
                    col("valid_cap").is_not_null(),
                    col("cap")
                )
                .when(
                    col("unique_cap").is_not_null(),
                    col("unique_cap")
                )
                .otherwise(
                    lit(None)
                )
            )
        )

        # ------------------------------------------------------------------
        # Check whether city belongs to resolved CAP.
        # ------------------------------------------------------------------

        df = (
            df
            .join(
                valid_pairs,
                (
                    col("resolved_cap") == col("pair_cap")
                )
                &
                (
                    col("city") == col("pair_city")
                ),
                "left"
            )
        )

        # ------------------------------------------------------------------
        # Lookup valid cities.
        # ------------------------------------------------------------------

        df = (
            df
            .join(
                valid_cities,
                col("city") == col("valid_city"),
                "left"
            )
        )

        # ------------------------------------------------------------------
        # Resolve CITY.
        # ------------------------------------------------------------------

        cap_unique_city_lookup = (
            cap_unique_city
            .select(
                col("unique_cap").alias("city_cap"),
                col("unique_city").alias("resolved_city_lookup")
            )
        )

        df = (
            df
            .join(
                cap_unique_city_lookup,
                col("resolved_cap") == col("city_cap"),
                "left"
            )
            .with_column(
                "resolved_city",
                when(
                    col("resolved_cap").is_not_null()
                    & col("valid_pair").is_not_null(),
                    col("city")
                )
                .when(
                    col("resolved_cap").is_not_null()
                    & col("resolved_city_lookup").is_not_null(),
                    col("resolved_city_lookup")
                )
                .when(
                    col("resolved_cap").is_null()
                    & col("valid_city").is_not_null(),
                    col("city")
                )
                .otherwise(
                    lit(None)
                )
            )
            .drop(
                "valid_cap",
                "valid_city",
                "unique_city",
                "unique_cap",
                "pair_cap",
                "pair_city",
                "valid_pair",
                "city_cap",
                "resolved_city_lookup"
            )
        )

        df = df.select(
            *original_columns,
            "resolved_cap",
            "resolved_city"
            )

        return df


    def enrich_location_candidate(
            self,
            session
            ):
        """
        Populate coordinates and administrative information for candidate table.

        Args:
            session (Session): Snowflake session.

        Returns:
            DataFrame: enriched dataframe.
        """

        config = self.config
        database = config.database
        schema = config.output_schema
        candidate_table = config.candidate.output_cleaned_table

        df_candidate = session.table(
            f"{database}.{schema}.{candidate_table}"
        )

        df = self._enrich_location(session, df_candidate)

        return df


    def enrich_location_joborder(
            self,
            session
            ):
        """
        Populate coordinates and administrative information for joborder.

        Args:
            session (Session): Snowflake session.

        Returns:
            DataFrame: enriched dataframe.
        """

        config = self.config
        database = config.database
        schema = config.output_schema
        joborder_table = config.joborder.output_cleaned_table

        df_joborder = session.table(
            f"{database}.{schema}.{joborder_table}"
        )

        df = self._enrich_location(session, df_joborder)

        return df


    def _enrich_location(
            self,
            session,
            df):
        """
        Enrich dataframe with geographic coordinates and administrative information using resolved_cap and resolved_city.

        Output columns:
            - resolved_province
            - resolved_region
            - lat
            - lng


        ==============
        Input datasets

        Candidate tables must contain:
            - resolved_cap
            - resolved_city
            - province
        Joborder tables must contain:
            - resolved_cap
            - resolved_city
            - region
        The ITALIAN_CITIES table contains one row for each unique (cap, city) pair and provides:
            - cap
            - city_normalized
            - province
            - province_ext
            - region
            - lat
            - lng

        A CAP may correspond to multiple cities and a city may correspond to multiple CAPs.

        =====================
        Coordinate resolution

        1. EXACT MATCH
            If:
                resolved_cap  = IC.cap
                resolved_city = IC.city_normalized
            then:
                (lat = IC.lat, lng = IC.lng)
        2. CAP FALLBACK
            Otherwise if resolved_cap is available:
                (lat = average(lat), lng = average(lng)) of all IC rows with that CAP
        3. CITY FALLBACK
            Otherwise if resolved_city is available:
                (lat = average(lat), lng = average(lng)) of all IC rows with that CAP
        4. NULL

        ========
        Examples

        1.
        resolved_cap  = 20121
        resolved_city = milano
            -> lat/lng from (20121, milano)

        3.
        resolved_cap  = NUll
        resolved_city = milano
            milano is associated with multiple CAPs
                -> average coordinates of all milano locations

        3.
        resolved_cap = 10030
        resolved_city = Null
            10030 is associated with:
                - Maglione, Rondissone, Vestignè, Villareggia, e Vische
            more than one possible city, but city matches Ronsissone
                -> average coordinates of all locations with CAP 10030

        =========================
        Administrative resolution for candidate table

        1. EXACT MATCH
            If (resolved_cap, resolved_city) matches a row in IC:
                resolved_province = IC.province
                resolved_region   = IC.region
        2. CAP/CITY FALLBACK
            If only one among resolved_cap and resolved_city can be used for the lookup:
                - If all matching IC rows share the same province/region, use that province/region.
                - Otherwise, if the candidate province matches one of the provinces found in IC, use the corresponding IC province and IC.region.
        3. FINAL FALLBACK
            If resolved_province or resolved_region are still NULL:
                - If the input province is a valid province in IC, use it as resolved_province.
                - If the input region is a valid region in IC, use it as resolved_region.

        Administrative resolution for joborder table is equivalent to candidate, just replace province with region.

        ========
        Examples

        1.
            resolved_cap  = 20121
            resolved_city = milano
                -> province = MI
                -> region   = Lombardia
        2.
            resolved_cap  = 10030
            resolved_city = NULL
            10030 is associated with:
                - Maglione, Rondissone, Vestignè, Villareggia, e Vische
                They all share the same province and region
                -> province = TO
                -> region   = Piemonte

            resolved_cap  = 12071
            resolved_city = NULL
            province = SV
            10030 is associated with:
                - Bagnasco, Massimino
                They share different province and region:
                - Bagnasco
                    - province = CN, region = piemonte
                - Massimino
                    - province = SV, region = liguria
                -> province = SV
                -> region   = Null

            resolved_cap  = 12071
            resolved_city = NULL
            province = NULL
            10030 is associated with:
                - Bagnasco, Massimino
                They share different province and region:
                - Bagnasco
                    - province = CN, region = piemonte
                - Massimino
                    - province = SV, region = liguria
                -> province = NULL
                -> region   = NULL

            resolved_cap  = NULL
            resolved_city = milano
                Milano is associated with multiple CAPs, all share the same province and region
                    -> province = MI
                    -> region   = Lombardia
        3.
            Candidate:
            resolved_cap  = NULL
            resolved_city = NULL
            province = MI
                -> province = MI
                -> region   = NULL

            Joborder:
            resolved_cap  = NULL
            resolved_city = NULL
            province = MI
                -> province = MI
                -> region   = NULL       


        Args:
            session (Session): Snowflake session.
            df (DataFrame): dataframe to be enriched

        Returns:
            DataFrame: enriched dataframe.   
        """

        """
        How to check that it works properly:
 
        select region, resolved_cap, resolved_city, resolved_province, resolved_region
        from MPG_IT_ONBENCH_TEST_JOBORDER_CLEANED
        where region != resolved_region and region != city --and resolved_region not in ('trentino alto adige sudtirol', 'valle d aosta vallee d aoste')
        """


        config = self.config
        database = config.database
        schema = config.output_schema
        italian_cities_table = config.italian_cities.output_table

        original_columns = df.columns
        
        ic_df = session.table(
            f"{database}.{schema}.{italian_cities_table}"
        )

        columns_lower = [c.lower() for c in original_columns]

        is_candidate = (
            "province" in columns_lower
            and "region" not in columns_lower
        )

        is_joborder = (
            "region" in columns_lower
            and "province" not in columns_lower
        )

        if not (is_candidate or is_joborder):
            raise ValueError(
                "Input dataframe must contain either province or region"
            )

        #
        # EXACT LOOKUP
        #
        ic_exact = (
            ic_df
            .select(
                col("cap").alias("exact_cap"),
                col("city_normalized").alias("exact_city"),
                col("province").alias("exact_province"),
                col("province_ext").alias("exact_province_ext"),
                col("region").alias("exact_region"),
                col("lat").alias("exact_lat"),
                col("lng").alias("exact_lng")
            )
        )

        #
        # CAP AGGREGATION
        #
        ic_by_cap = (
            ic_df
            .with_column_renamed("cap", "ic_by_cap_cap")
            .group_by("ic_by_cap_cap")
            .agg(
                avg("lat").alias("cap_lat"),
                avg("lng").alias("cap_lng"),

                count_distinct("province").alias("cap_province_cnt"),
                min("province").alias("cap_province"),
                min("province_ext").alias("cap_province_ext"),

                count_distinct("region").alias("cap_region_cnt"),
                min("region").alias("cap_region")
            )
        )

        #
        # CITY AGGREGATION
        #
        ic_by_city = (
            ic_df
            .with_column_renamed("city_normalized", "ic_by_city_city")
            .group_by("ic_by_city_city")
            .agg(
                avg("lat").alias("city_lat"),
                avg("lng").alias("city_lng"),

                count_distinct("province").alias("city_province_cnt"),
                min("province").alias("city_province"),
                min("province_ext").alias("city_province_ext"),

                count_distinct("region").alias("city_region_cnt"),
                min("region").alias("city_region")
            )
        )

        #
        # CAP + PROVINCE DISAMBIGUATION
        #
        ic_cap_province = (
            ic_df
            .select(
                col("cap").alias("cpp_cap"),
                col("province").alias("cpp_province"),
                col("province_ext").alias("cpp_province_ext"),
                col("region").alias("cpp_region")
            )
            .distinct()
        )

        #
        # CITY + PROVINCE DISAMBIGUATION
        #
        ic_city_province = (
            ic_df
            .select(
                col("city_normalized").alias("ctp_city"),
                col("province").alias("ctp_province"),
                col("province_ext").alias("ctp_province_ext"),
                col("region").alias("ctp_region")
            )
            .distinct()
        )

        #
        # CAP + REGION DISAMBIGUATION
        #
        ic_cap_region = (
            ic_df
            .select(
                col("cap").alias("cpr_cap"),
                col("province").alias("cpr_province"),
                col("province_ext").alias("cpr_province_ext"),
                col("region").alias("cpr_region")
            )
            .distinct()
        )

        #
        # CITY + REGION DISAMBIGUATION
        #
        ic_city_region = (
            ic_df
            .select(
                col("city_normalized").alias("ctr_city"),
                col("province").alias("ctr_province"),
                col("province_ext").alias("ctr_province_ext"),
                col("region").alias("ctr_region")
            )
            .distinct()
        )

        #
        # JOINS
        #
        df = (
            df

            .join(
                ic_exact,
                (
                    (col("resolved_cap") == col("exact_cap"))
                    &
                    (col("resolved_city") == col("exact_city"))
                ),
                "left"
            )

            .join(
                ic_by_cap,
                col("resolved_cap") == col("ic_by_cap_cap"),
                "left"
            )

            .join(
                ic_by_city,
                col("resolved_city") == col("ic_by_city_city"),
                "left"
            )
        )

        if is_candidate:

            df = (
                df

                .join(
                    ic_cap_province,
                    (
                        (col("resolved_cap") == col("cpp_cap"))
                        &
                        (col("province") == col("cpp_province"))
                    ),
                    "left"
                )

                .join(
                    ic_city_province,
                    (
                        (col("resolved_city") == col("ctp_city"))
                        &
                        (col("province") == col("ctp_province"))
                    ),
                    "left"
                )
            )

        else:

            df = (
                df

                .join(
                    ic_cap_region,
                    (
                        (col("resolved_cap") == col("cpr_cap"))
                        &
                        (col("region") == col("cpr_region"))
                    ),
                    "left"
                )

                .join(
                    ic_city_region,
                    (
                        (col("resolved_city") == col("ctr_city"))
                        &
                        (col("region") == col("ctr_region"))
                    ),
                    "left"
                )
            )

        #
        # FLAGS
        #
        exact_match = col("exact_lat").is_not_null()

        cap_match = (
            (~exact_match)
            &
            col("resolved_cap").is_not_null()
        )

        city_match = (
            (~exact_match)
            &
            col("resolved_cap").is_null()
            &
            col("resolved_city").is_not_null()
        )

        #
        # LAT / LNG
        #
        df = (
            df

            .with_column(
                "lat",
                when(
                    exact_match,
                    col("exact_lat")
                )
                .when(
                    cap_match,
                    col("cap_lat")
                )
                .when(
                    city_match,
                    col("city_lat")
                )
                .otherwise(lit(None))
            )

            .with_column(
                "lng",
                when(
                    exact_match,
                    col("exact_lng")
                )
                .when(
                    cap_match,
                    col("cap_lng")
                )
                .when(
                    city_match,
                    col("city_lng")
                )
                .otherwise(lit(None))
            )
        )

        if is_candidate:

            province_expr = (
                when(
                    exact_match,
                    col("exact_province")
                )
                .when(
                    cap_match
                    & (col("cap_province_cnt") == 1),
                    col("cap_province")
                )
                .when(
                    cap_match,
                    col("cpp_province")
                )
                .when(
                    city_match
                    & (col("city_province_cnt") == 1),
                    col("city_province")
                )
                .when(
                    city_match,
                    col("ctp_province")
                )
                .otherwise(lit(None))
            )
            """
            province_ext_expr = (
                when(
                    exact_match,
                    col("exact_province_ext")
                )
                .when(
                    cap_match
                    & (col("cap_province_cnt") == 1),
                    col("cap_province_ext")
                )
                .when(
                    cap_match,
                    col("cpp_province_ext")
                )
                .when(
                    city_match
                    & (col("city_province_cnt") == 1),
                    col("city_province_ext")
                )
                .when(
                    city_match,
                    col("ctp_province_ext")
                )
                .otherwise(lit(None))
            )
            """
            
            region_expr = (
                when(
                    exact_match,
                    col("exact_region")
                )
                .when(
                    cap_match
                    & (col("cap_region_cnt") == 1),
                    col("cap_region")
                )
                .when(
                    cap_match,
                    col("cpp_region")
                )
                .when(
                    city_match
                    & (col("city_region_cnt") == 1),
                    col("city_region")
                )
                .when(
                    city_match,
                    col("ctp_region")
                )
                .otherwise(lit(None))
            )

        else:

            province_expr = (
                when(
                    exact_match,
                    col("exact_province")
                )
                .when(
                    cap_match
                    & (col("cap_province_cnt") == 1),
                    col("cap_province")
                )
                .when(
                    cap_match,
                    col("cpr_province")
                )
                .when(
                    city_match
                    & (col("city_province_cnt") == 1),
                    col("city_province")
                )
                .when(
                    city_match,
                    col("ctr_province")
                )
                .otherwise(lit(None))
            )

            """
            province_ext_expr = (
                when(
                    exact_match,
                    col("exact_province_ext")
                )
                .when(
                    cap_match
                    & (col("cap_province_cnt") == 1),
                    col("cap_province_ext")
                )
                .when(
                    cap_match,
                    col("cpr_province_ext")
                )
                .when(
                    city_match
                    & (col("city_province_cnt") == 1),
                    col("city_province_ext")
                )
                .when(
                    city_match,
                    col("ctr_province_ext")
                )
                .otherwise(lit(None))
            )
            """
            region_expr = (
                when(
                    exact_match,
                    col("exact_region")
                )
                .when(
                    cap_match
                    & (col("cap_region_cnt") == 1),
                    col("cap_region")
                )
                .when(
                    cap_match,
                    col("cpr_region")
                )
                .when(
                    city_match
                    & (col("city_region_cnt") == 1),
                    col("city_region")
                )
                .when(
                    city_match,
                    col("ctr_region")
                )
                .otherwise(lit(None))
            )

        df = (
            df
            .with_column("resolved_province", province_expr)
            #.with_column("resolved_province_ext", province_ext_expr)
            .with_column("resolved_region", region_expr)
        )

        df = df.select(
            *original_columns,
            "resolved_province",
            #"resolved_province_ext",
            "resolved_region",
            "lat",
            "lng",
        )

        return df


    def extract_geo_info_job_site(
            self, 
            session
            ):
        """
        Extract geographic information from the candidate job_site field and enrich
        the candidate dataset with the most likely Italian city match.

        Strings contained in job_site typically have the form
        "Via Roma 10, Milano (MI)". They usually contain the province
        abbreviation at the end and do not contain a CAP.

        The function analyzes the job_site value and attempts to identify
        the municipality referenced within the string.

        When a province abbreviation is present (e.g. "Milano (MI)"), it is
        extracted and used to restrict city matching to municipalities
        belonging to that province, reducing ambiguity between cities with
        the same or similar names.

        Matching is performed against the Italian cities reference table using
        the city_normalized column. When multiple city matches are found, the
        best match is selected according to the following priority:
            1. City whose occurrence ends furthest to the right in the job_site string.
            2. Longest city name.
            3. Alphabetical order as a deterministic tie-breaker.

        Examples:

            Single city match:
                - "Via Augusto 10, Milano (MI)"
                -> Milano

            Multiple city matches in the same string:

                - "Via Roma 10, Milano (MI)"
                (both Roma and Milano match. Milano appears furthest to the right)
                    -> Milano

                - "Via Milano 10, Roma (RM)"
                (both Milano and Roma match. Roma appears furthest to the right)
                    -> Roma

                - "Via Augusto, Bolzano Vicentino (VI)"
                (both 'Bolzano' and 'Bolzano Vicentino' match, and one is included in the other. 
                Bolzano Vicentino appears furthest to the right)
                    -> Bolzano Vicentino

                - "Via Augusto 10, Villanova di Camposampiero (PD)" 
                -> Villanova di Camposampiero
                (both 'Villanova' and 'Villanova di Camposampiero' match, and one is included in the other.)
                Villanova di Camposampiero appearts further to the right
                    -> Villanova di Camposampiero

                Province extraction limits candidate cities to the
                corresponding province before ranking.

        Geographic attributes associated with the selected city are then added to the candidate record.
        In particular, CAP is assigned only for cities that have a single unique CAP in the reference table.

        Output columns:
            - job_site_cap
            - job_site_city
            - job_site_province
            - job_site_province_ext
            - job_site_region
            - job_site_lat
            - job_site_lng

        Notes:
            - Province extraction supports formats such as:
                * "Milano (MI)"
                * "Milano ( MI )"
                * "Milano MI"

            - If no province can be extracted, city matching is performed
            across all Italian provinces.

            - CAP information is taken from the matched reference record and
            may not uniquely identify the municipality when multiple CAPs
            exist for the same city.

        Args:
            session (Session): Active Snowpark session.

        Returns:
            DataFrame: Candidate dataframe enriched with geographic information
            extracted from job_site.
        """

        config = self.config
        database = config.database
        schema = config.output_schema
        candidate_table = config.candidate.output_cleaned_table
        italian_cities_table = config.italian_cities.output_table

        df = session.table(
            f"{database}.{schema}.{candidate_table}"
        )

        original_columns = df.columns

        df_cities = session.table(
                f"{database}.{schema}.{italian_cities_table}"
            )

        df_cities = (
            df_cities
            .select(
                col("city").alias("city_lookup"),
                col("city_normalized"),
                col("cap").alias("cap_lookup"),
                col("province").alias("province_lookup"),
                col("province_ext").alias("province_ext_lookup"),
                col("region").alias("region_lookup"),
                col("lat").alias("lat_lookup"),
                col("lng").alias("lng_lookup")
            )
        )


        # ------------------------------------------------------------------
        # Determine whether each city is associated with a unique CAP.
        #
        # Cities having exactly one distinct CAP can safely populate job_site_cap.
        #
        # Cities associated with multiple CAPs will later receive a NULL
        # job_site_cap value to avoid selecting an arbitrary postal code.
        # ------------------------------------------------------------------

        city_caps = (
            df_cities
            .group_by("city_normalized")
            .agg(
                count_distinct("cap_lookup").alias("cap_count"),
                min(col("cap_lookup")).alias("single_cap")
            )
        )


        # ------------------------------------------------------------------
        # Extract the province abbreviation from the end of job_site.
        #
        # Supported examples:
        # - Milano (MI)
        # - Milano ( MI )
        # - Milano MI
        # - Milano MI)
        #
        # If no province can be extracted, the value remains NULL.
        # -----------------------------------------------------------------

        df = (
            df
            .with_column(
                "job_site_province_extracted",
                lower(
                    coalesce(
                        call_function(
                            "REGEXP_SUBSTR",
                            col("job_site"),
                            lit(r"\(\s*([a-zA-Z]{2})\s*\)\s*$"),
                            lit(1),
                            lit(1),
                            lit("e"),
                            lit(1)
                        ),
                        call_function(
                            "REGEXP_SUBSTR",
                            col("job_site"),
                            lit(r"(\(\s*|\s+)([a-zA-Z]{2})(\s*\)|\s*)$"),
                            lit(1),
                            lit(1),
                            lit("e"),
                            lit(2)
                        )
                    )
                )
            )
        )


        # ------------------------------------------------------------------
        # Generate city matches.
        #
        # When a province has been extracted, limit matching to cities
        # belonging to that province.
        #
        # For each valid city match, compute:
        # - city length
        # - position of the city occurrence within job_site
        # - ending position of the matched city
        # ------------------------------------------------------------------

        matches = (
            df
            .join(
                df_cities,
                (col("job_site_province_extracted") == col("province_lookup"))
                | (col("job_site_province_extracted").is_null()), # if unable to extract the province, then don't use it to filter
                "inner"
            )
            .filter(
                expr(
                    "POSITION(city_normalized, job_site_norm) > 0"
                )
            )
            .with_column(
                "city_len",
                length(col("city_normalized"))
            )
            .with_column(
                "last_position",
                expr("""
                    LENGTH(job_site_norm)
                    - POSITION(
                        REVERSE(city_normalized),
                        REVERSE(job_site_norm)
                    )
                    - LENGTH(city_normalized)
                    + 2
                """)
            )
            .with_column(
                "match_end_position",
                col("last_position") + col("city_len")
            )
        )


        # ------------------------------------------------------------------
        # Select the best city match.
        #
        # Preference order:
        # 1. City appearing furthest to the right in job_site.
        # 2. Longest city name.
        # 3. Alphabetical tie-breaker.
        #
        # Examples:
        # - "Via Milano 10, Roma (RM)" -> Roma
        # - "Via Roma 10 Milano (MI)" -> Milano
        # - "Via Roma 10, Villanova di Camposampiero (PD)" -> Villanova di Camposampiero
        # - "Via Roma 10, Bolzano Vicentino (VI)" -> "Bolzano Vicentino"
        # ------------------------------------------------------------------

        ranking_window = (
            Window
            .partition_by(col("candidate_id_bh"))
            .order_by(
                col("match_end_position").desc(),
                col("city_len").desc(),
                col("city_normalized").asc()
            )
        )

        best_match = (
            matches
            .with_column(
                "rn",
                row_number().over(ranking_window)
            )
            .filter(col("rn") == 1)
        )


        # ------------------------------------------------------------------
        # Enrich the selected city match with CAP cardinality information.
        #
        # This allows distinguishing municipalities with a unique CAP from
        # municipalities associated with multiple postal codes.
        # ------------------------------------------------------------------

        best_match = (
            best_match
            .join(
                city_caps,
                "city_normalized",
                "left"
            )
        )


        # ------------------------------------------------------------------
        # Build the enrichment dataset.
        #
        # The matched city is always retained.
        #
        # CAP is populated only when the matched city is associated with a
        # single distinct CAP in the Italian cities reference table.
        #
        # Examples:
        # - Prato   -> 59100      (single CAP)      -> job_site_cap = 59100
        # - Roma    -> many CAPs  (00118, 00119...) -> job_site_cap = NULL
        # - Milano  -> many CAPs  (20121, 20122...) -> job_site_cap = NULL
        #
        # This avoids assigning an arbitrary CAP when a municipality is
        # served by multiple postal codes.
        # ------------------------------------------------------------------

        extracted = (
            best_match
            .select(
                col("candidate_id_bh"),
                when(
                    col("cap_count") == 1,
                    col("single_cap")
                )
                .otherwise(lit(None))
                .alias("job_site_cap"),
                col("city_normalized")
                    .alias("job_site_city"),
                col("province_lookup")
                    .alias("job_site_province"),
                col("province_ext_lookup")
                    .alias("job_site_province_ext"),
                col("region_lookup")
                    .alias("job_site_region"),
                col("lat_lookup")
                    .alias("job_site_lat"),
                col("lng_lookup")
                    .alias("job_site_lng"),
            )
        )


        # ------------------------------------------------------------------
        # Merge extracted geographic information back into the candidate
        # dataset.
        # ------------------------------------------------------------------

        df = (
            df
            .drop("job_site_province_extracted")
            .join(
                extracted,
                "candidate_id_bh",
                "left"
            )
        )

        df = df.select(
            *original_columns,
            "job_site_cap",
            "job_site_city",
            "job_site_province",
            #"job_site_province_ext",
            "job_site_region",
            "job_site_lat",
            "job_site_lng"
            )

        return df


    def create_geo_table(
            self,
            session
            ):
        """
        Build the Italian cities reference table from the source JSON file.

        The output contains:
        - one EXACT row for each valid (cap, city) pair
        - one CITY_ONLY row for each city associated with multiple CAPs
        - one CAP_ONLY row for each CAP shared by multiple cities

        Output columns:
            cap                 STRING
            city                STRING
            city_normalized     STRING
            province            STRING
            province_ext        STRING
            region              STRING
            has_multiple_caps   BOOLEAN
            is_capoluogo        BOOLEAN
            cadastral_code      STRING
            lat                 DOUBLE
            lng                 DOUBLE
            location_type       STRING

        location_type:
            EXACT
            CITY_ONLY
            CAP_ONLY

        Args:
            session (Session): Snowpark session.

        Returns:
            DataFrame: Geographic reference table.
        """

        """
        Three caps are missing for Castegnero, Nanto and Lirio
        Il Codice di Avviamento Postale (CAP) di Castegnero è 36024 (dopo la fusione con il comune di Nanto nel nuovo comune di Castegnero Nanto, istituito il 21 febbraio 2026). 
        In precedenza il CAP era 36020.

        Il CAP di Lirio (frazione del comune di Montalto Pavese in provincia di Pavia) è 27040.
        Nota amministrativa: Dal 31 gennaio 2026, l'ex comune di Lirio è stato ufficialmente incorporato nel comune di Montalto Pavese
        """

        config = self.config
        italian_cities_file = config.italian_cities.input_file

        # ------------------------------------------------------------------
        # Load municipalities from the source JSON file.
        # ------------------------------------------------------------------

        with open(italian_cities_file, "r", encoding="utf-8") as f:
            comuni = json.load(f)

        # ------------------------------------------------------------------
        # Transform source records into a flat structure.
        #
        # A municipality may have multiple CAP values.
        # Generate one row per (cap, city) pair.
        # ------------------------------------------------------------------

        rows = []

        for comune in comuni:

            raw_caps = comune.get("cap") or []

            normalized_caps = list(dict.fromkeys(
                normalize_cap(cap)
                for cap in raw_caps
            ))

            normalized_caps = [
                cap
                for cap in normalized_caps
                if cap is not None
            ]

            coordinates = comune.get("coordinate") or {}
            regione = comune.get("regione") or {}
            provincia = comune.get("provincia") or {}

            for cap in normalized_caps or [None]:

                rows.append({
                    "cap": cap,
                    "city": comune.get("nome"),
                    "province": comune.get("sigla"),
                    "province_ext": provincia.get("nome"),
                    "region": regione.get("nome"),
                    "has_multiple_caps": len(normalized_caps) > 1,
                    "is_capoluogo": comune.get("capoluogo"),
                    "cadastral_code": comune.get("codiceCatastale"),
                    "lat": coordinates.get("lat"),
                    "lng": coordinates.get("lng"),
                    "location_type": "EXACT",
                })

        # ------------------------------------------------------------------
        # Define the Snowpark schema.
        # ------------------------------------------------------------------

        schema = StructType([
            StructField("cap", StringType()),
            StructField("city", StringType()),
            StructField("province", StringType()),
            StructField("province_ext", StringType()),
            StructField("region", StringType()),
            StructField("has_multiple_caps", BooleanType()),
            StructField("is_capoluogo", BooleanType()),
            StructField("cadastral_code", StringType()),
            StructField("lat", DoubleType()),
            StructField("lng", DoubleType()),
            StructField("location_type", StringType()),
        ])

        df = session.create_dataframe(
            rows,
            schema=schema
        )

        # ------------------------------------------------------------------
        # Build normalized lookup fields used during city matching.
        # ------------------------------------------------------------------

        normalize_city_udf = self._build_normalize_city_udf()

        df = (
            df
            .with_column(
                "CITY_NORMALIZED",
                normalize_city_udf(col("CITY"))
            )
            .with_column(
                "PROVINCE",
                lower(col("PROVINCE"))
            )
            .with_column(
                "PROVINCE_EXT",
                normalize_city_udf(col("PROVINCE_EXT"))
            )
            .with_column(
                "REGION",
                normalize_city_udf(col("REGION"))
            )
        )

        # ------------------------------------------------------------------
        # Standardize string fields.
        #
        # - Collapse consecutive spaces.
        # - Trim leading and trailing spaces.
        # - Convert empty strings to NULL.
        # ------------------------------------------------------------------

        string_cols = [
            f.name
            for f in df.schema.fields
            if f.datatype.__class__.__name__ == "StringType"
        ]

        for c in string_cols:

            cleaned = trim(
                regexp_replace(
                    col(c),
                    r"\s+",
                    " "
                )
            )

            df = df.with_column(
                c,
                when(
                    cleaned == "",
                    lit(None)
                ).otherwise(cleaned)
            )

        # ------------------------------------------------------------------
        # Validate geographic coordinates.
        #
        # Latitude range:  [-90, 90]
        # Longitude range: [-180, 180]
        #
        # Missing or invalid values are replaced with NaN.
        # ------------------------------------------------------------------

        df = (
            df
            .with_column(
                "lat",
                when(
                    col("lat").is_null(),
                    lit(float("nan"))
                ).when(
                    (col("lat") >= -90)
                    & (col("lat") <= 90),
                    col("lat")
                ).otherwise(
                    lit(float("nan"))
                )
            )
            .with_column(
                "lng",
                when(
                    col("lng").is_null(),
                    lit(float("nan"))
                ).when(
                    (col("lng") >= -180)
                    & (col("lng") <= 180),
                    col("lng")
                ).otherwise(
                    lit(float("nan"))
                )
            )
        )

        # ------------------------------------------------------------------
        # Add CITY_ONLY rows.
        #
        # For each city associated with multiple CAPs add:
        #
        #     (NULL, city)
        #
        # Coordinates are the average coordinates of all rows
        # associated with the city.
        # Other columns such as province and region are populated only if all caps associated to the city share the same value
        # ------------------------------------------------------------------

        city_only_df = (
            df
            .group_by(
                "city",
                "city_normalized"
            )
            .agg(
                count_distinct("cap").alias("cap_count"),

                count_distinct("province").alias("province_count"),
                min("province").alias("province_value"),

                count_distinct("province_ext").alias("province_ext_count"),
                min("province_ext").alias("province_ext_value"),

                count_distinct("region").alias("region_count"),
                min("region").alias("region_value"),

                count_distinct("cadastral_code").alias("cadastral_count"),
                min("cadastral_code").alias("cadastral_value"),

                count_distinct("is_capoluogo").alias("capoluogo_count"),
                min("is_capoluogo").alias("capoluogo_value"),

                count_distinct("has_multiple_caps").alias("multiple_caps_count"),
                min("has_multiple_caps").alias("multiple_caps_value"),

                avg("lat").alias("lat"),
                avg("lng").alias("lng")
            )
            .filter(col("cap_count") > 1)
            .select(
                lit(None).cast(StringType()).alias("cap"),

                col("city"),
                col("city_normalized"),

                when(
                    col("province_count") == 1,
                    col("province_value")
                ).otherwise(lit(None)).alias("province"),

                when(
                    col("province_ext_count") == 1,
                    col("province_ext_value")
                ).otherwise(lit(None)).alias("province_ext"),

                when(
                    col("region_count") == 1,
                    col("region_value")
                ).otherwise(lit(None)).alias("region"),

                when(
                    col("multiple_caps_count") == 1,
                    col("multiple_caps_value")
                ).otherwise(lit(None)).alias("has_multiple_caps"),

                when(
                    col("capoluogo_count") == 1,
                    col("capoluogo_value")
                ).otherwise(lit(None)).alias("is_capoluogo"),

                when(
                    col("cadastral_count") == 1,
                    col("cadastral_value")
                ).otherwise(lit(None)).alias("cadastral_code"),

                col("lat"),
                col("lng"),

                lit("CITY_ONLY").alias("location_type")
            )
        )

        # ------------------------------------------------------------------
        # Add CAP_ONLY rows.
        #
        # For each CAP associated with multiple cities add:
        #
        #     (cap, NULL)
        #
        # Coordinates are the average coordinates of all rows
        # associated with the CAP.
        # Other columns such as province and region are populated only if all cities associated to the cap share the same value
        # ------------------------------------------------------------------

        cap_only_df = (
            df
            .group_by("cap")
            .agg(
                count_distinct("city").alias("city_count"),

                count_distinct("province").alias("province_count"),
                min("province").alias("province_value"),

                count_distinct("province_ext").alias("province_ext_count"),
                min("province_ext").alias("province_ext_value"),

                count_distinct("region").alias("region_count"),
                min("region").alias("region_value"),

                count_distinct("cadastral_code").alias("cadastral_count"),
                min("cadastral_code").alias("cadastral_value"),

                count_distinct("is_capoluogo").alias("capoluogo_count"),
                min("is_capoluogo").alias("capoluogo_value"),

                count_distinct("has_multiple_caps").alias("multiple_caps_count"),
                min("has_multiple_caps").alias("multiple_caps_value"),

                avg("lat").alias("lat"),
                avg("lng").alias("lng")
            )
            .filter(
                col("cap").is_not_null()
                & (col("city_count") > 1)
            )
            .select(
                col("cap"),

                lit(None).cast(StringType()).alias("city"),
                lit(None).cast(StringType()).alias("city_normalized"),

                when(
                    col("province_count") == 1,
                    col("province_value")
                ).otherwise(lit(None)).alias("province"),

                when(
                    col("province_ext_count") == 1,
                    col("province_ext_value")
                ).otherwise(lit(None)).alias("province_ext"),

                when(
                    col("region_count") == 1,
                    col("region_value")
                ).otherwise(lit(None)).alias("region"),

                when(
                    col("multiple_caps_count") == 1,
                    col("multiple_caps_value")
                ).otherwise(lit(None)).alias("has_multiple_caps"),

                when(
                    col("capoluogo_count") == 1,
                    col("capoluogo_value")
                ).otherwise(lit(None)).alias("is_capoluogo"),

                when(
                    col("cadastral_count") == 1,
                    col("cadastral_value")
                ).otherwise(lit(None)).alias("cadastral_code"),

                col("lat"),
                col("lng"),

                lit("CAP_ONLY").alias("location_type")
            )
        )

        # ------------------------------------------------------------------
        # Put toghether all the rows.
        # ------------------------------------------------------------------

        city_only_df = city_only_df.select(df.columns)
        cap_only_df = cap_only_df.select(df.columns)

        df = (
            df
            .union_by_name(city_only_df)
            .union_by_name(cap_only_df)
        )

        # ------------------------------------------------------------------
        # Reorder columns.
        # ------------------------------------------------------------------

        df = df.select(
            "location_type",
            "cap",
            "city",
            "city_normalized",
            "province",
            "province_ext",
            "region",
            "has_multiple_caps",
            "is_capoluogo",
            "cadastral_code",
            "lat",
            "lng",
        )

        return df


    def _haversine_expr(
            self,
            lat1_column: str,
            lng1_column: str,
            lat2_column: str,
            lng2_column: str
            ):
        """Builds an expression that computes the Haversine distance in km between two coordinate pairs

        Args:
            lat1_column (string): source latitude column name
            lng1_column (string): source longitude column name
            lat2_column (string): destination latitude column name
            lng2_column (string): destination longitude column name

        Returns:
            expr (Snowpark expression): Haversine distance in km
        """

        lat1 = radians(col(lat1_column))
        lng1 = radians(col(lng1_column))
        lat2 = radians(col(lat2_column))
        lng2 = radians(col(lng2_column))

        dlat = lat2 - lat1
        dlng = lng2 - lng1

        a = (
            sin(dlat / 2) * sin(dlat / 2)
            + cos(lat1)
            * cos(lat2)
            * sin(dlng / 2)
            * sin(dlng / 2)
        )

        expr = lit(6371) * (lit(2) * asin(sqrt(a)))

        return expr


    def build_geo_distance_table(
        self,
        session,
        max_distance_km=100
    ):
        """
        Builds a geographic lookup table containing all pairs of locations that
        are within a specified distance radius.

        This table is used during candidate/job matching to quickly retrieve
        nearby locations without recalculating geographic distances at runtime.

        The source ITALIAN_CITIES table contains one row for each available
        location and supports three location granularities:

            (cap, city): Exact location corresponding to a specific CAP/city pair.

            (NULL, city): Generic location representing a city associated with multiple CAPs. 
                Latitude and longitude correspond to the city centroid.

            (cap, NULL): Generic location representing a CAP shared by multiple cities.
                Latitude and longitude correspond to the CAP centroid.

        
        Including the generic rows makes it possible to compute distances even
        when only CAP or city information is available.

        The resulting table contains:
            from_cap
            from_city
            to_cap
            to_city
            distance_km

        Rows are generated for all location pairs whose distance is less than
        or equal to max_distance_km. Both directions are stored:
            A -> B
            B -> A

        This simplifies downstream joins between candidates and job orders by
        eliminating the need to reverse lookup directions.

        To reduce computational cost, location pairs are first filtered using
        a latitude/longitude bounding box and only then evaluated using the
        Haversine formula.

        Args:
            session (Session): Snowpark session.
            max_distance_km (int): Maximum distance allowed between two locations.

        Returns:
            DataFrame: Distance reference table containing all qualifying
                location pairs and their distance in kilometers.
        DataFrame: Geo distance table.

        """

        # Create target table with clustering key

        config = self.config
        database = config.database
        schema = config.output_schema
        italian_cities_table = config.italian_cities.output_table
        distance_table = config.italian_cities.distance_table


        # ------------------------------------------------------------------
        # Create output table and define cluster
        # ------------------------------------------------------------------

        session.sql(f"""
            DROP TABLE IF EXISTS {database}.{schema}.{config.italian_cities.distance_table}
        """).collect()

        session.sql(f"""
            CREATE OR REPLACE TABLE {database}.{schema}.{config.italian_cities.distance_table} (
                from_cap VARCHAR,
                from_city VARCHAR,
                to_cap VARCHAR,
                to_city VARCHAR,
                distance_km FLOAT
            )
            CLUSTER BY (from_cap, from_city)
        """).collect()


        # ------------------------------------------------------------------
        # Loads from/to locations
        # ------------------------------------------------------------------

        ic = (
            session.table(f"{database}.{schema}.{italian_cities_table}")
            .select(
                col("cap"),
                col("city_normalized"),
                col("lat"),
                col("lng")
            )
            .filter(
                col("lat").is_not_null()
                & col("lng").is_not_null()
            )
            .cache_result()
        )

        from_df = (
            ic.select(
                col("cap").alias("from_cap"),
                col("city_normalized").alias("from_city"),
                col("lat").alias("from_lat"),
                col("lng").alias("from_lng")
            )
        )

        to_df = (
            ic.select(
                col("cap").alias("to_cap"),
                col("city_normalized").alias("to_city"),
                col("lat").alias("to_lat"),
                col("lng").alias("to_lng")
            )
        )


        # ------------------------------------------------------------------
        # Build geographic distance expression and spatial filters
        # ------------------------------------------------------------------

        max_lat_delta = max_distance_km / 111.0
        max_lng_delta = max_distance_km / 111.0

        distance_expr = self._haversine_expr(
            "from_lat",
            "from_lng",
            "to_lat",
            "to_lng"
        )


        # ------------------------------------------------------------------
        # Generate location pairs and keep only nearby locations
        # ------------------------------------------------------------------

        result = (
            from_df
            .cross_join(to_df)
            .filter(
                ~(
                    col("from_cap").eqNullSafe(col("to_cap"))
                    &
                    col("from_city").eqNullSafe(col("to_city"))
                )
            )
            .filter(
                abs(col("from_lat") - col("to_lat"))
                <= lit(max_lat_delta)
            )
            .filter(
                abs(col("from_lng") - col("to_lng"))
                <= lit(max_lng_delta)
            )
            .with_column("distance_km", distance_expr)
            .filter(col("distance_km") <= lit(max_distance_km))
            .select(
                col("from_cap"),
                col("from_city"),
                col("to_cap"),
                col("to_city"),
                round(col("distance_km"), 1).alias("distance_km")
            )
            .sort(
                col("from_cap"),
                col("from_city")
            )
        )

        return result

    def write_table(
            self, 
            df, 
            table_name = "output_table",
            mode = "overwrite"
            ):
        """Write a dataframe to a Snowflake table.

        Args:
            df (DataFrame): Snowpark dataframe to write.
            table_name (str): Target Snowflake table name.
        """

        df.write.save_as_table(
            table_name, 
            mode=mode
            )
        
        logger.info(f"Table {table_name} successfully written")



