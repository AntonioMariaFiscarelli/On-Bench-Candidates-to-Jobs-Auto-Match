from src.onBench import logger

from snowflake.snowpark.functions import col, coalesce, lit, regexp_replace, when, greatest, length, trim, lower, udf, to_varchar
from snowflake.snowpark.functions import expr, split, iff, concat, call_function
from snowflake.snowpark.types import StructType, StructField, StringType, FloatType, DoubleType, BooleanType, ArrayType
from snowflake.snowpark import Row

import pandas as pd
import json
import re

from src.onBench.utils.common import normalize_cap#, normalize_city


from src.onBench.entity.config_entity import DataIngestionConfig, DomainConfig


class DataIngestion:
    """
    This class is used to create the candidate and vacancie tables.
    It provides methods for:
        - reading the input tables
        - filtering the tables in order to select only the candidates/vacancies that have been created/modified recently
        - cleanings candidates' CVs and descriptions

    """

    def __init__(self, config: DataIngestionConfig, domain_config = DomainConfig):
        self.config = config
        self.domain_config = domain_config

    def clean_description(self, df, column = "description_cleaned"):
        """Cleans string columns such as parsed CVs and job descriptions:
            - replaces multiple consecutive whitespaces with a single whitespace (preserves newlines)
            - removes all html tags
            - lowercases all text

        Args:
            df (DataFrame): dataframe
            column (string): column to clean

        Returns:
            df (Snowpark Dataframe): dataframe
        """

        #df = df.filter((col(column).is_not_null()) & (trim(col(column)) != ""))
        
        def build_normalize_whitespace_udf():
            def normalize(text: str) -> str:
                import re
                if text is None:
                    return ''
                return re.sub(r'[ \t]+', ' ', text).strip()

            return udf(normalize, return_type=StringType(), input_types=[StringType()])
        normalize_udf = build_normalize_whitespace_udf()
        df = df.with_column(column, normalize_udf(df[column]))
        

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
        df = df.with_column(column, clean_html_udf(df[column]))

        df = df.with_column(column, lower(df[column]))

        """
        df = df.filter((col(column).is_not_null()) & 
                       (trim(col(column)) != "") &
                       (length(trim(col(column))) > 5) &
                       (~col(column).like("%None%")) &
                       (~col(column).like("%null%"))
                       )
        """
        df = df.with_column(column, col(column).cast("STRING"))

        df = df.with_column(
            column,
            iff(trim(col(column)) == "", None, col(column))
        )

        
        logger.info(f"Column {column} successfully cleaned")

        return df

    def read_table_candidate(self, session):
        """Creates candidate table with all the columns needed

        Args:
            session (Session): Snowflake session

        Returns:
            df (Snowpark Dataframe): dataframe
        """
        config = self.config
        database = config.database
        schema = config.input_schema
        input_table = config.candidate.input_table
        input_pb_table = config.candidate.input_pb_table
        days_nomission = config.candidate.days_nomission

        # Create a temporary table with only 100 rows from the view 
        if(False):
            input_table = f"candidate_materialized_100"
            """
            CREATE OR REPLACE TABLE IT_DISCOVERY.CONSUMER_INT_MODEL.candidate_materialized AS 
            SELECT * FROM IT_DISCOVERY.CONSUMER_INT_MODEL.CANDIDATE_CLEANED -- this is your VIEW 
            WHERE date_added >= DATEADD(day, -60, CURRENT_TIMESTAMP())
            LIMIT 1000
            """

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

            candidate_pb_src AS (
                SELECT
                    ca.dbcandno AS db_cand_no,
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
                    c.db_ass_ti_nomission, 
                    c.candidate_status,
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
                GROUP BY c.candidate_id_pb, c.candidate_id_bh, c.db_ass_ti_nomission, c.candidate_status
            ),
            
            candidate_pb AS (
                SELECT 
                    candidate_id_pb, 
                    candidate_id_bh, 
                    candidate_status,
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
                FROM candidate_pb_daysinfo
                --WHERE candidate_status = 'No mission'
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
                cc.cap,
                cc.city,
                cc.province,
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
                jri.joborder_id_first,
                jri.job_site,
                jri.job_first,
                jri.job_intermediate,
                jri.job_last,
                jri.start_date,
                jri.end_date,
                jri.parttime_perc,
                jri.ccnl_level,
                jri.legal_category,
                jri.ccnl_code,
                jri.ccnl_description,
                cob.candidate_status,
                cob.days_onbench_last,
                cob.days_onbench_tot,
                cob.days_onbench_perc
            FROM candidate cc
            INNER JOIN  candidate_pb cob
                ON cc.candidate_id_pb = cob.candidate_id_pb
            LEFT JOIN jobs_ranked_agg jri
                ON cc.candidate_id_pb = jri.db_job_cand_no
            order by num_jobs_during_onbench desc
            
        """
        
        df = session.sql(query)

        #candidate_df = candidate_df.with_column("candidateid", col("candidateid").cast("string"))

        logger.info(f"Table {input_table} successfully read")
        
        return df

    def read_table_joborder(self, session):
        """Creates joborder table with all the columns needed

        Args:
            session (Session): Snowflake session

        Returns:
            df (Snowpark Dataframe): dataframe
        """
        config = self.config
        database = config.database
        schema = config.input_schema
        input_table = config.joborder.input_table

        # Create a temporary table with only 100 rows from the view 
        if(False):
            input_table_joborder = f"joborder_materialized_100"
            """
            CREATE OR REPLACE TABLE IT_DISCOVERY.CONSUMER_INT_MODEL.joborder_materialized AS 
            SELECT * FROM IT_DISCOVERY.CONSUMER_INT_MODEL.JOBORDER_CLEANED -- this is your VIEW 
            WHERE isopen = 1 AND status = 'Accepting Candidates' 
            AND datelastmodified >= DATEADD(day, -60, CURRENT_TIMESTAMP())
            LIMIT 1000

            """

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
                city,
                cap,
                region,
                salary,
                --data_inizio_validita,
                skills,
                parttime_percent,
                titoli_richiesti,
                descrizione_uso_interno,
                testo_pubblicazione,
                richieste_aggiuntive,
                requisiti,
                job_site,
                ccnl_level,
                legal_category,
                ccnl_code,
                ccnl_description,
                CASE 
                    WHEN date_last_modified IS NULL AND last_status_change IS NULL THEN NULL
                    ELSE GREATEST(
                    COALESCE(date_last_modified, TO_TIMESTAMP('0001-01-01 00:00:00')),
                    COALESCE(last_status_change, TO_TIMESTAMP('0001-01-01 00:00:00'))
                    )
                END AS date_last_active,
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


    
    def clean_table_candidate(self, session):
        """Feature engineering and cleaning of the candidate table:
            - PARTTIME_PERC: if missing, default value is 100; if it's 0, it's set to None
            - Removes leading and trailing whitespaces from all string columns
            - Removes very long meaningless sequences of characters
            - General cleaning of the description column:
                - replaces multiple consecutive whitespaces with a single whitespace (preserves newlines)
                - removes all html tags
                - lowercases all text
            - Removes descriptions that are longer than max_tokens characters

        Args:
            session (Session): Snowflake session

        Returns:
            df (Snowpark Dataframe): dataframe
        """
        config = self.config
        domain_config = self.domain_config
        database = config.database
        schema = config.output_schema
        input_table = config.candidate.output_table
        max_tokens = config.llm.max_tokens
        desired_locations = domain_config.desired_locations
        parttime_preferenza_perc = domain_config.parttime_preferenza_perc

        df = session.table(f"{database}.{schema}.{input_table}")  

        df = df.with_column("description_raw", col("description"))
        
        #Desired location is set to its default values: "Locale", "Regionale", "Nazionale", "Internazionale". Only one value allowed
        case_parts = []
        for level in reversed(desired_locations):   # highest first
            case_parts.append(f"WHEN DESIRED_LOCATION ILIKE '%{level}%' THEN '{level}'")

        case_expr = "CASE " + " ".join(case_parts) + " ELSE NULL END"

        #Partime percentage as of bullhorn source. Recruiter is supposed to insert the value with a drow down menu with set values.
        #In reality it's more similar to a free text input where you'll find values such ash "50", "50%", "50, 60" etc
        #Parttime preferenza perc is set to its default values representing percentages: [10, 20, 30, 40, 50, 60, 70, 80, 90, 100]. If missing, default value is 100
        df = df.with_column(
            "PARTTIME_PREFERENZA_PERC",
            coalesce(col("PARTTIME_PREFERENZA_PERC"), lit("100%"))
        )

        #it can contain multiple values such as "80, 100". If so, it will take the maximum value.
        exprs = [
            when(col("PARTTIME_PREFERENZA_PERC").contains(lit(v)), lit(int(v))).otherwise(lit(0))
            for v in parttime_preferenza_perc
        ]
        # Take the maximum across all expressions
        df = df.with_column(
            "PARTTIME_PREFERENZA_PERC",
            greatest(*exprs)
        )
        #If it contained none of the default values, it's set to None
        df = df.with_column(
            "PARTTIME_PREFERENZA_PERC",
            when(col("PARTTIME_PREFERENZA_PERC") == 0, None).otherwise(col("PARTTIME_PREFERENZA_PERC"))
        )

        df = df.with_column(
            "DESIRED_LOCATION",
            expr(case_expr)
        )
        #Turno preferenza is set to its default values: ["Mattina", "Pomeriggio", "Notte", "Fine Settimana"] Multiple values are allowed as a string of comma separated values such as "Mattina, Pomeriggio"
        df = df.with_column(
            "turno_preferenza",
            split(col("turno_preferenza"), lit(","))
        )
        
        
        #Partime percentage as of powerbase source. Integer value between 0 and 100
        #If missing, default value is 100
        df = df.with_column(
            "PARTTIME_PERC",
            coalesce(col("PARTTIME_PERC"), lit(100))
        )
        #If it's 0, it's set to None
        df = df.with_column(
            "PARTTIME_PERC",
            when(col("PARTTIME_PERC") == 0, None).otherwise(col("PARTTIME_PERC"))
        )

        df = df.with_column("CCNL_CODE", to_varchar(col("CCNL_CODE").cast("int")))

        df = df.with_column(
            "JOB_SITE_PROVINCE",
            call_function(
                "REGEXP_SUBSTR",
                col("JOB_SITE"),
                lit(r"\(([A-Za-z]{2})\)$"),
                lit(1),
                lit(1),
                lit("e"),
                lit(1)
            )
        )

        # Trim all leading and trailing whitespaces from all string columns
        string_cols = [
            f.name
            for f in df.schema.fields
            if f.datatype.__class__.__name__ == "StringType"
        ]
        for c in string_cols:
            df = df.with_column(c, lower(trim(col(c))))


        def build_normalize_city_udf():
            def normalize_city(city: str) -> str:

                import unicodedata

                if city is None:
                    return None

                city = city.strip()
                if not city:
                    return None

                # Minuscolo e normalizzazione Unicode
                city = unicodedata.normalize("NFKD", city.casefold())
                # Rimozione degli accenti
                city = "".join(
                    char for char in city
                    if not unicodedata.combining(char)
                )
                # Punteggiatura e separatori diventano spazi
                city = re.sub(r"[^a-z0-9]+", " ", city)
                # Spazi multipli
                city = re.sub(r"\s+", " ", city).strip()

                return city
            return udf(normalize_city, return_type=StringType(), input_types=[StringType()])
        
        normalize_city_udf = build_normalize_city_udf()

        df = df.with_column(
            "CITY",
            normalize_city_udf(col("CITY"))
        ) 
        df = df.with_column(
            "PROVINCE",
            normalize_city_udf(col("PROVINCE"))
        )
        df = df.with_column(
            "JOB_SITE_NORM",
            normalize_city_udf(col("JOB_SITE"))
        )

        #Removes very long meaningless sequences of characters, including character codes for attachments (pdf, jpg etc)
        noise_pattern = r"(/9j/[A-Za-z0-9+/=]{30,}|iVBORw0KGgo[A-Za-z0-9+/=]{30,}|JVBERi0x[A-Za-z0-9+/=]{30,}|[A-Za-z0-9+/=]{80,}|[^A-Za-z0-9 ]{30,}|([A-Za-z]{3,} ){15,})"
        df = df.with_column("description", regexp_replace(col("description"), noise_pattern, "") )

        df = self.clean_description(df, "description")

        #If description is longer than max_tokens, replace it with an empty string
        df = (
            df
            .with_column(
                "description",
                when(
                    length(col("description")) > max_tokens,
                    lit("")                     
                ).otherwise(col("description"))
            )
        )

        logger.info(f"Table {input_table} successfully cleaned")

        return df

    def clean_table_joborder(self, session):
        """Feature engineering and cleaning of the joborder table:
            - Removes leading and trailing whitespaces from all string columns
            - Removes very long meaningless sequences of characters
            - General cleaning of the description column:
                - replaces multiple consecutive whitespaces with a single whitespace (preserves newlines)
                - removes all html tags
                - lowercases all text
            - Removes descriptions that are longer than max_tokens characters

        Args:
            session (Session): Snowflake session

        Returns:
            df (Snowpark Dataframe): dataframe
        """
        config = self.config
        database = config.database
        schema = config.output_schema
        input_table = config.joborder.output_table
        max_tokens = config.llm.max_tokens


        df = session.table(f"{database}.{schema}.{input_table}")

        df = df.with_column("description_raw", col("description"))

        string_cols = [
            f.name
            for f in df.schema.fields
            if f.datatype.__class__.__name__ == "StringType"
        ]
        for c in string_cols:
            df = df.with_column(c, lower(trim(col(c))))

        def build_normalize_city_udf():
            def normalize_city(city: str) -> str:

                import unicodedata

                if city is None:
                    return None

                city = city.strip()
                if not city:
                    return None

                # Minuscolo e normalizzazione Unicode
                city = unicodedata.normalize("NFKD", city.casefold())
                # Rimozione degli accenti
                city = "".join(
                    char for char in city
                    if not unicodedata.combining(char)
                )
                # Punteggiatura e separatori diventano spazi
                city = re.sub(r"[^a-z0-9]+", " ", city)
                # Spazi multipli
                city = re.sub(r"\s+", " ", city).strip()

                return city
            return udf(normalize_city, return_type=StringType(), input_types=[StringType()])
        
        normalize_city_udf = build_normalize_city_udf()

        df = df.with_column(
            "CITY",
            normalize_city_udf(col("CITY"))
        ) 
        df = df.with_column(
            "REGION",
            normalize_city_udf(col("REGION"))
        ) 


        #Removes very long meaningless sequences of characters
        noise_pattern = r"(/9j/[A-Za-z0-9+/=]{30,}|iVBORw0KGgo[A-Za-z0-9+/=]{30,}|JVBERi0x[A-Za-z0-9+/=]{30,}|[A-Za-z0-9+/=]{80,}|[^A-Za-z0-9 ]{30,}|([A-Za-z]{3,} ){15,})"
        df = df.with_column("description", regexp_replace(col("description"), noise_pattern, "") )

        df = self.clean_description(df, "description")
        df = self.clean_description(df, "descrizione_uso_interno")
        df = self.clean_description(df, "testo_pubblicazione")

        df = (
            df
            .with_column(
                "description",
                when(
                    length(col("description")) > max_tokens,
                    lit("")                      # replace with empty string
                ).otherwise(col("description"))   # keep original
            )
        )

        logger.info(f"Table {input_table} successfully cleaned")

        return df


    def create_comuni_dataframe(
        self,
        session,
    ):
        """
        Reads the Italian municipalities JSON file and returns
        a cleaned Snowpark DataFrame.

        Output columns:
            nome        STRING
            sigla       STRING
            cap         STRING
            cap_list    ARRAY<STRING>
            capoluogo   BOOLEAN
            lat         DOUBLE
            lng         DOUBLE
            regione     STRING
            provincia   STRING
        """

        config = self.config
        italian_cities_file = config.italian_cities.input_file

        with open(italian_cities_file, "r", encoding="utf-8") as f:
            comuni = json.load(f)

        rows = []
        for comune in comuni:

            raw_caps = comune.get("cap") or []

            normalized_caps = list(dict.fromkeys(
                normalize_cap(cap)
                for cap in raw_caps
            ))

            normalized_caps = [
                cap for cap in normalized_caps
                if cap is not None
            ]

            first_cap = (
                normalized_caps[0]
                if len(normalized_caps) > 0
                else None
            )

            coordinates = comune.get("coordinate") or {}
            regione = comune.get("regione") or {}
            provincia = comune.get("provincia") or {}

            rows.append({
                "city": comune.get("nome"),
                "province": comune.get("sigla"),
                "cap": first_cap,
                "cap_list": normalized_caps,
                "has_multiple_caps": len(normalized_caps) > 1,
                "lat": coordinates.get("lat"),
                "lng": coordinates.get("lng"),
                "is_capoluogo": comune.get("capoluogo"),
                "region": regione.get("nome"),
                "province_ext": provincia.get("nome"),
            })

        # ------------------------------------------------------------------
        # Schema
        # ------------------------------------------------------------------

        schema = StructType([
            StructField("city", StringType()),
            StructField("province", StringType()),
            StructField("cap", StringType()),
            StructField("cap_list", ArrayType(StringType())),
            StructField("has_multiple_caps", BooleanType()),
            StructField("lat", DoubleType()),
            StructField("lng", DoubleType()),
            StructField("is_capoluogo", BooleanType()),
            StructField("region", StringType()),
            StructField("province_ext", StringType()),
        ])

        df = session.create_dataframe(
            rows,
            schema=schema
        )

        def build_normalize_city_udf():
            def normalize_city(city: str) -> str:

                import unicodedata

                if city is None:
                    return None

                city = city.strip()
                if not city:
                    return None

                # Minuscolo e normalizzazione Unicode
                city = unicodedata.normalize("NFKD", city.casefold())
                # Rimozione degli accenti
                city = "".join(
                    char for char in city
                    if not unicodedata.combining(char)
                )
                # Punteggiatura e separatori diventano spazi
                city = re.sub(r"[^a-z0-9]+", " ", city)
                # Spazi multipli
                city = re.sub(r"\s+", " ", city).strip()

                return city
            return udf(normalize_city, return_type=StringType(), input_types=[StringType()])
        
        normalize_city_udf = build_normalize_city_udf()

        df = df.with_column(
            "CITY_NORMALIZED",
            normalize_city_udf(col("CITY"))
        )

        df = df.with_column(
            "PROVINCE_EXTS",
            normalize_city_udf(col("PROVINCE_EXT"))
        )

        df = df.with_column(
            "REGION",
            normalize_city_udf(col("REGION"))
        )

        string_columns = [
            "city",
            "city_normalized",
            "province",
            "cap",
            "region",
            "province_ext",
        ]

        for c in string_columns:
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
        # Latitude and Longitudevalidation
        # Latitude valid range: [-90, 90]
        # Longitude valid range: [-180, 180]
        # Invalid values -> NaN
        # ------------------------------------------------------------------

        df = df.with_column(
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

        df = df.with_column(
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

        return df


    def add_geo_info(self, session):
        config = self.config

        database = config.database
        schema = config.output_schema
        input_table = config.candidate.output_table
        cities_input_table = config.italian_cities.output_table

        # Snowpark DataFrames
        df = session.table(f"{database}.{schema}.{input_table}")
        df_city = session.table(f"{database}.{schema}.{cities_input_table}")

        import ahocorasick

        # Recupera le circa 8.000 città da Snowflake.
        # Questa è l'unica raccolta di dati sul client.
        cities = [
            str(row["CITY"]).strip()
            for row in (
                df_city
                .select("CITY")
                .where(col("CITY").is_not_null())
                .distinct()
                .collect()
            )
            if str(row["CITY"]).strip()
        ]

        # Costruisce l'automa una sola volta per worker UDF.
        #@lru_cache(maxsize=1)
        def get_automaton():
            import ahocorasick
            automaton = ahocorasick.Automaton()

            for city in cities:
                automaton.add_word(city.lower(), city)

            automaton.make_automaton()
            return automaton

        def estrai_city(indirizzo: str) -> str:
            if indirizzo is None:
                return None

            indirizzo = str(indirizzo).lower()

            # Rimuove la sigla provinciale finale, ad esempio (mn).
            indirizzo = re.sub(
                r"\s*\([a-z]{2}\)\s*$",
                "",
                indirizzo
            )

            automaton = get_automaton()
            matches = []

            for end_index, city in automaton.iter(indirizzo):
                # La città memorizzata mantiene le maiuscole originali.
                city_lower = city.lower()
                start_index = end_index - len(city_lower) + 1

                before = (
                    indirizzo[start_index - 1]
                    if start_index > 0
                    else " "
                )

                after_index = end_index + 1
                after = (
                    indirizzo[after_index]
                    if after_index < len(indirizzo)
                    else " "
                )

                # Evita corrispondenze dentro altre parole.
                if not before.isalnum() and not after.isalnum():
                    matches.append((len(city), city))

            # Preferisce il nome della città più lungo.
            return max(matches, default=(0, None))[1]

        # Registra la funzione come UDF Snowpark.
        estrai_city_udf = udf(
            estrai_city,
            return_type=StringType(),
            input_types=[StringType()],
            packages=["pyahocorasick==2.1.0"]
        )

        # Crea la nuova colonna senza convertire df in pandas.
        df = df.with_column(
            "JOB_SITE_CITY",
            estrai_city_udf(col("JOB_SITE"))
        )

        return df
    
    def write_table(self, df, table_name = 'output_table'):
        """Writes dataframe in Snowflake

        Args:
            df (Snowpark Dataframe): dataframe
            table_name (string): name of the Snowflake table
        """

        df.write.save_as_table(table_name, mode="overwrite")
        logger.info(f"Table {table_name} successfully written")



