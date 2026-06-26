from src.onBench import logger
import pandas as pd
from snowflake.snowpark import Row
from snowflake.snowpark.functions import col, dateadd, current_date, lit, coalesce, when, greatest, expr, split, listagg, regexp_replace, length
from snowflake.snowpark.types import StructType, StructField, StringType, FloatType


from src.onBench.entity.config_entity import DataIngestionConfig

class DataIngestion:
    def __init__(self, config: DataIngestionConfig):
        self.config = config

    def read_table(self, session):
        """
        Reads input table
        Function returns Snowflake dataframe
        """
        database = self.config.database
        schema = self.config.schema
        input_table = self.config.input_table
        columns = self.config.columns
        days_prior = self.config.days_prior
        desired_locations = self.config.desired_locations
        parttime_preferenza_perc = self.config.parttime_preferenza_perc

        
        # Create a temporary table with only 100 rows from the view 
        if(False):
            input_table = f"candidate_materialized"
            """
            CREATE OR REPLACE TABLE IT_DISCOVERY.CONSUMER_INT_MODEL.candidate_materialized AS 
            SELECT * FROM IT_DISCOVERY.CONSUMER_INT_MODEL.CANDIDATE_CLEANED -- this is your VIEW 
            WHERE date_added >= DATEADD(day, -60, CURRENT_TIMESTAMP())
            LIMIT 1000
            """
        '''
        candidate_df = session.sql(f"""
                                   SELECT 
                                    DISTINCT {", cc.".join(columns)} ,
                                    cc.date_last_modified,
                                    jsc.data_ultimo_cambio_status,
                                    --cc.date_added,
                                FROM {database}.{schema}.{input_table} cc
                                LEFT JOIN IT_DISCOVERY.CONSUMER_INT_MODEL.JOBSUBMISSION_CLEANED jsc
                                    ON cc.candidateid = jsc.candidateid AND 
                                    jsc.data_ultimo_cambio_status >= DATEADD(day, -{days_prior}, CURRENT_TIMESTAMP())
                                    --jsc.data_ultimo_cambio_status BETWEEN DATEADD(day, -4, CURRENT_TIMESTAMP()) AND DATEADD(day, -3, CURRENT_TIMESTAMP())
                                WHERE 
                                    cc.date_added >= DATEADD(day, -{days_prior}, CURRENT_TIMESTAMP()) OR 
                                    cc.date_last_modified >= DATEADD(day, -{days_prior}, CURRENT_TIMESTAMP()) OR
                                    --cc.date_added BETWEEN DATEADD(day, -4, CURRENT_TIMESTAMP()) AND DATEADD(day, -3, CURRENT_TIMESTAMP()) OR
                                    --cc.date_last_modified BETWEEN DATEADD(day, -4, CURRENT_TIMESTAMP()) AND DATEADD(day, -3, CURRENT_TIMESTAMP()) OR
                                    jsc.candidateid is not null
                                   """)
        '''
        candidate_df = session.sql(f"""
            WITH jsc_agg AS (
                SELECT 
                    candidateid,
                    MAX(data_ultimo_cambio_status) AS last_status_change
                FROM IT_DISCOVERY.CONSUMER_INT_MODEL.JOBSUBMISSION_CLEANED
                WHERE data_ultimo_cambio_status >= DATEADD(day, -{days_prior}, CURRENT_TIMESTAMP())
                GROUP BY candidateid
            )
            SELECT
                cc.{", cc.".join(columns)},
                cc.date_last_modified,
                jsc_agg.last_status_change,
                GREATEST(
                    cc.date_added,
                    cc.date_last_modified,
                    jsc_agg.last_status_change
                ) AS date_last_active
            FROM {database}.{schema}.{input_table} cc
            LEFT JOIN jsc_agg
                ON cc.candidateid = jsc_agg.candidateid
            WHERE 
                cc.date_added >= DATEADD(day, -{days_prior}, CURRENT_TIMESTAMP())
                OR cc.date_last_modified >= DATEADD(day, -{days_prior}, CURRENT_TIMESTAMP())
                OR jsc_agg.candidateid IS NOT NULL
        """)

        
        
        """
        candidate_df = (
            session.table(f"{database}.{schema}.{input_table}")
            .filter(col("date_added") >= lit(cutoff_date))
            .select([col(c) for c in columns])
            .with_column("candidateid", col("candidateid").cast("string"))
        )
        """
        candidate_df = candidate_df.with_column("candidateid", col("candidateid").cast("string"))
        

        #Removes very long meaningless sequences of characters
        #noise_pattern = r"([A-Za-z0-9+/]{200,}|[0-9A-F]{200,}|[^A-Za-z0-9\s]{30,})"
        noise_pattern = r"(/9j/[A-Za-z0-9+/=]{30,}|iVBORw0KGgo[A-Za-z0-9+/=]{30,}|JVBERi0x[A-Za-z0-9+/=]{30,}|[A-Za-z0-9+/=]{80,}|[^A-Za-z0-9 ]{30,}|([A-Za-z]{3,} ){15,})"

        candidate_df = candidate_df.with_column("description", regexp_replace(col("description"), noise_pattern, "") )

        joboti_df = session.sql("""
        WITH note AS (
            SELECT *
            FROM (
                SELECT personreferenceid, comments, action, load_datetimestamp, isdeleted,
                    ROW_NUMBER() OVER (
                        PARTITION BY noteid
                        ORDER BY load_datetimestamp DESC
                    ) AS row_num
                FROM aas_it_hist_prod.bullhorn.note
            )
            WHERE row_num = 1
            AND isdeleted = 0
        )
        SELECT personreferenceid, comments, load_datetimestamp
        FROM note
        WHERE action ILIKE '%joboti%'
        AND comments ILIKE '%Sei disponibile a lavorare nel fine settimana?%'
        """)
        

        agg_df = (
            joboti_df
            .group_by("PERSONREFERENCEID")
            .agg(
                listagg(col("COMMENTS"), " || ")
                    .within_group(col("LOAD_DATETIMESTAMP"))
                    .alias("CHATWA")
            )
            .with_column_renamed("PERSONREFERENCEID", "CANDIDATEID")
        )


        agg_df = agg_df.with_column( "chatwa", regexp_replace(col("chatwa"), noise_pattern, "") )

        df = (
            candidate_df
            .join(
                agg_df,
                candidate_df["CANDIDATEID"] == agg_df["CANDIDATEID"],
                join_type="left",
                rsuffix="_right"
            )
            .select(candidate_df["*"], agg_df["CHATWA"])
        )
        
        
        max_tokens = 200000

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

        df = (
            df
            .with_column(
                "chatwa",
                when(
                    length(col("chatwa")) > max_tokens,
                    lit("")                      # replace with empty string
                ).otherwise(col("chatwa"))   # keep original
            )
        )
        
        #df = candidate_df
        #df = df.filter(col("CHATWA").is_not_null())
        
        case_parts = []
        for level in reversed(desired_locations):   # highest first
            case_parts.append(f"WHEN DESIRED_LOCATION ILIKE '%{level}%' THEN '{level}'")

        case_expr = "CASE " + " ".join(case_parts) + " ELSE NULL END"

        df = df.with_column(
            "DESIRED_LOCATION",
            expr(case_expr)
        )

        df = df.with_column(
            "turno_preferenza",
            split(col("turno_preferenza"), lit(","))
        )

        # Transform the dataframe
        df = df.with_column(
            "PARTTIME_PREFERENZA_PERC",
            coalesce(col("PARTTIME_PREFERENZA_PERC"), lit("100%"))
        )

        # Build expressions: if STRING_COL contains 'v', return int(v), else 0
        exprs = [
            when(col("PARTTIME_PREFERENZA_PERC").contains(lit(v)), lit(int(v))).otherwise(lit(0))
            for v in parttime_preferenza_perc
        ]

        # Take the maximum across all expressions
        df = df.with_column(
            "PARTTIME_PREFERENZA_PERC",
            greatest(*exprs)
        )

        df = df.with_column(
            "PARTTIME_PREFERENZA_PERC",
            when(col("PARTTIME_PREFERENZA_PERC") == 1, None).otherwise(col("PARTTIME_PREFERENZA_PERC"))
        )

        logger.info(f"Table {input_table} successfully read")
        
        return df
    


    def read_vacancy_table(self, session):
        """
        Reads input table
        Function returns Snowflake dataframe
        """
        database = self.config.database
        schema = self.config.schema
        input_table_vacancy = self.config.input_table_vacancy
        columns_vacancy = self.config.columns_vacancy
        days_prior = self.config.days_prior * 2

        # Create a temporary table with only 100 rows from the view 
        if(False):
            input_table_vacancy = f"joborder_materialized"
            """
            CREATE OR REPLACE TABLE IT_DISCOVERY.CONSUMER_INT_MODEL.joborder_materialized AS 
            SELECT * FROM IT_DISCOVERY.CONSUMER_INT_MODEL.JOBORDER_CLEANED -- this is your VIEW 
            WHERE isopen = 1 AND status = 'Accepting Candidates' 
            AND datelastmodified >= DATEADD(day, -60, CURRENT_TIMESTAMP())
            LIMIT 1000

            """

        '''
        df = session.sql(f"""
                    WITH filtered_jc AS (
                        SELECT {",".join(columns_vacancy)} , isopen, status, datelastmodified
                        FROM {database}.{schema}.{input_table_vacancy} 
                        WHERE isopen = 1 AND status = 'Accepting Candidates'
                    )
                    SELECT 
                        DISTINCT jc.joborderid,
                        jc.status,
                        jc.datelastmodified,
                        jc.dateadded,
                        jc.jobtitle,
                        jc.citta AS location,
                        jc.regione AS region,
                        jc.salary AS salary_low,
                        jc.data_inizio_validita AS date_available,
                        GREATEST(jc.datelastmodified, jsc.data_ultimo_cambio_status) as date_last_active,
                        COALESCE(CAST(jc.part_time_percent AS STRING), '') AS parttime_preferenza_perc,
                        COALESCE(jc.skill_list, '') AS skills,
                        COALESCE(jc.titoli_richiesti, '') AS education,
                        COALESCE(REGEXP_REPLACE(REGEXP_REPLACE(TRIM(jc.DESCRIZIONE_USO_INTERNO), '\\s+', ' '), '[^A-Za-z0-9À-ÖØ-öø-ÿ .,;:!?()_''"-]', ''), '') AS DESCRIZIONE_USO_INTERNO,
                        COALESCE(REGEXP_REPLACE(REGEXP_REPLACE(TRIM(jc.TESTO_PUBBLICAZIONE), '\\s+', ' '), '[^A-Za-z0-9À-ÖØ-öø-ÿ .,;:!?()_''"-]', ''), '') AS TESTO_PUBBLICAZIONE,
                        COALESCE(REGEXP_REPLACE(REGEXP_REPLACE(TRIM(jc.RICHIESTE_AGGIUNTIVE), '\\s+', ' '), '[^A-Za-z0-9À-ÖØ-öø-ÿ .,;:!?()_''"-]', ''), '') AS RICHIESTE_AGGIUNTIVE,
                        COALESCE(REGEXP_REPLACE(REGEXP_REPLACE(TRIM(jc.REQUISITI), '\\s+', ' '), '[^A-Za-z0-9À-ÖØ-öø-ÿ .,;:!?()_''"-]', ''), '') AS REQUISITI
                    FROM filtered_jc jc
                    LEFT JOIN IT_DISCOVERY.CONSUMER_INT_MODEL.JOBSUBMISSION_CLEANED jsc
                        ON jc.joborderid = jsc.joborderid
                    WHERE 
                        jc.datelastmodified >= DATEADD(day, -{days_prior}, CURRENT_TIMESTAMP()) OR
                        jsc.data_ultimo_cambio_status >= DATEADD(day, -{days_prior}, CURRENT_TIMESTAMP())
                        --jc.datelastmodified BETWEEN DATEADD(day, -8, CURRENT_TIMESTAMP()) AND DATEADD(day, -3, CURRENT_TIMESTAMP()) OR
                        --jsc.data_ultimo_cambio_status BETWEEN DATEADD(day, -8, CURRENT_TIMESTAMP()) AND DATEADD(day, -3, CURRENT_TIMESTAMP())
                         """)
        '''
        df = session.sql(f"""
            WITH filtered_jc AS (
                SELECT {",".join(columns_vacancy)}, isopen, status, datelastmodified
                FROM {database}.{schema}.{input_table_vacancy}
                WHERE isopen = 1 AND status = 'Accepting Candidates'
            ),

            jsc_agg AS (
                SELECT
                    joborderid,
                    MAX(data_ultimo_cambio_status) AS last_status_change
                FROM IT_DISCOVERY.CONSUMER_INT_MODEL.JOBSUBMISSION_CLEANED
                WHERE data_ultimo_cambio_status >= DATEADD(day, -{days_prior}, CURRENT_TIMESTAMP())
                GROUP BY joborderid
            )

            SELECT
                jc.joborderid,
                jc.status,
                jc.datelastmodified,
                jc.dateadded,
                jc.jobtitle,
                jc.citta AS location,
                jc.regione AS region,
                jc.salary AS salary_low,
                jc.data_inizio_validita AS date_available,

                -- unified activity timestamp
                GREATEST(
                    jc.datelastmodified,
                    jsc_agg.last_status_change
                ) AS date_last_active,

                COALESCE(CAST(jc.part_time_percent AS STRING), '') AS parttime_preferenza_perc,
                COALESCE(jc.skill_list, '') AS skills,
                COALESCE(jc.titoli_richiesti, '') AS education,

                COALESCE(
                    REGEXP_REPLACE(
                        REGEXP_REPLACE(TRIM(jc.DESCRIZIONE_USO_INTERNO), '\\s+', ' '),
                        '[^A-Za-z0-9À-ÖØ-öø-ÿ .,;:!?()_''"-]',
                        ''
                    ),
                    ''
                ) AS DESCRIZIONE_USO_INTERNO,

                COALESCE(
                    REGEXP_REPLACE(
                        REGEXP_REPLACE(TRIM(jc.TESTO_PUBBLICAZIONE), '\\s+', ' '),
                        '[^A-Za-z0-9À-ÖØ-öø-ÿ .,;:!?()_''"-]',
                        ''
                    ),
                    ''
                ) AS TESTO_PUBBLICAZIONE,

                COALESCE(
                    REGEXP_REPLACE(
                        REGEXP_REPLACE(TRIM(jc.RICHIESTE_AGGIUNTIVE), '\\s+', ' '),
                        '[^A-Za-z0-9À-ÖØ-öø-ÿ .,;:!?()_''"-]',
                        ''
                    ),
                    ''
                ) AS RICHIESTE_AGGIUNTIVE,

                COALESCE(
                    REGEXP_REPLACE(
                        REGEXP_REPLACE(TRIM(jc.REQUISITI), '\\s+', ' '),
                        '[^A-Za-z0-9À-ÖØ-öø-ÿ .,;:!?()_''"-]',
                        ''
                    ),
                    ''
                ) AS REQUISITI

            FROM filtered_jc jc
            LEFT JOIN jsc_agg
                ON jc.joborderid = jsc_agg.joborderid

            WHERE
                jc.datelastmodified >= DATEADD(day, -{days_prior}, CURRENT_TIMESTAMP())
                OR jsc_agg.last_status_change >= DATEADD(day, -{days_prior}, CURRENT_TIMESTAMP())
        """)


        
        logger.info(f"Table {input_table_vacancy} successfully read")

        return df
    
    def read_cities_file(self, session):
        """
        Reads XLSX file containing italian cities
        Function returns Snowflake dataframe
        """
        italian_cities_file = self.config.italian_cities_file
        string_columns = self.config.italian_cities_string_columns
        numeric_columns = self.config.italian_cities_numeric_columns

        df = pd.read_excel(italian_cities_file, header=0)

        # Rename columns for consistency (optional but recommended)
        df.columns = (
            df.columns
            .str.strip()
            .str.replace(" ", "_")
            .str.replace('"', '')
            .str.replace("'", '')
            .str.lower()
        )
        df = df[string_columns + numeric_columns]
        
        # Convert ZIP to string (preserve leading zeros)
        df["zip"] = df["zip"].apply(lambda x: str(int(x)).zfill(5) if pd.notnull(x) else None)
        
        # Convert string columns
        for col in string_columns:
            df[col] = df[col].astype(str).str.strip()

        # Convert latitude and longitude to float, handle NaNs
        for col in numeric_columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        # Remove rows with missing city_name
        df = df[
            df["city_name"].notna() &  # Remove NaN and None
            (df["city_name"].str.strip() != "") &  # Remove empty and whitespace-only strings
            (df["city_name"].str.lower().str.strip() != "null") &  # Remove "NULL" string
            (df["city_name"].str.lower().str.strip() != "nan")  # Remove "nan" string
        ]


        #These are necessary in order to avoid columns names qith quotes (e.g. "city" instead of city)
        rows = [Row(**row) for row in df.to_dict(orient="records")]
        schema = StructType([
            StructField("unique_identifier", StringType()),
            StructField("city_name", StringType()),
            StructField("province", StringType()),
            StructField("province_ext", StringType()), 
            StructField("region", StringType()), 
            StructField("zip", StringType()),
            StructField("latitude", FloatType()),
            StructField("longitude", FloatType())
        ])

        logger.info(f"XLSX file containing italian cities successfully read")

        return session.create_dataframe(rows, schema=schema)
    
    def complete_cities_table(self, session):
        """
        Completes cities table with geocoding info from Cortex
        Function returns Snowflake dataframe
        """

        output_table_italian_cities = self.config.output_table_italian_cities
        string_columns = self.config.italian_cities_string_columns
        numeric_columns = self.config.italian_cities_numeric_columns

        from snowflake.snowpark.functions import call_function, concat, lower

        df=session.table(output_table_italian_cities)

        missing_df = df.filter(
            col("latitude").is_null() |
            col("longitude").is_null() |
            (lower(col("latitude").cast("string")) == lit("nan")) |
            (lower(col("longitude").cast("string")) == lit("nan")) |
            (col("latitude").cast("string") == lit("NULL")) |
            (col("longitude").cast("string") == lit("NULL")) |
            (~col("latitude").cast("string").rlike(r"^-?\d{1,3}\.\d{1,10}$")) |
            (~col("longitude").cast("string").rlike(r"^-?\d{1,3}\.\d{1,10}$"))
        )


        prompt = concat( 
            lit("Restituisci la latitudine per la seguente città italiana:\n"), 

            lit("Città: "), col("city_name"), lit("\n"), lit("Provincia: "), col("province_ext"), lit("\n"), lit("Regione: "), col("region"), lit("\n"), lit("Nazione: Italia\n\n"), 
            lit("Rispondi SOLO con un numero nel formato seguente: 40.1234567)."),
            lit("Se non conosci la latitudine della città, non restituire nulla") 
            )
        missing_df = missing_df.with_column("latitude",
                            call_function(
                                "SNOWFLAKE.CORTEX.COMPLETE",
                                lit("claude-4-sonnet"),
                                prompt
                                )
                            )

        prompt = concat( 
            lit("Restituisci la longitudine per la seguente città italiana:\n"), 
            lit("Città: "), col("city_name"), lit("\n"), lit("Provincia: "), col("province"), lit("\n"), lit("Regione: "), col("region"), lit("\n"), lit("Nazione: Italia\n\n"), 
            lit("Rispondi SOLO con un numero nel formato seguente: 10.1234567)."),
            lit("Se non conosci la longitudine della città, non restituire nulla")  
            )
        missing_df = missing_df.with_column("longitude",
                            call_function(
                                "SNOWFLAKE.CORTEX.COMPLETE",
                                lit("claude-4-sonnet"),
                                prompt
                                )
                            )
        
        
        missing_df = missing_df.with_column(
            "latitude",
            when(
                col("latitude").cast("string").rlike(r"^-?\d{1,3}\.\d{1,10}$"),
                col("latitude")
            ).otherwise(lit(None))
        ).with_column(
            "longitude",
            when(
                col("longitude").cast("string").rlike(r"^-?\d{1,3}\.\d{1,10}$"),
                col("longitude")
            ).otherwise(lit(None))
        )
        

        complete_df = df.filter(
            ~(
                col("latitude").is_null() |
                col("longitude").is_null() |
                (lower(col("latitude").cast("string")) == lit("nan")) |
                (lower(col("longitude").cast("string")) == lit("nan")) |
                (col("latitude").cast("string") == lit("NULL")) |
                (col("longitude").cast("string") == lit("NULL")) |
                (~col("latitude").cast("string").rlike(r"^-?\d{1,3}\.\d{1,10}$")) |
                (~col("longitude").cast("string").rlike(r"^-?\d{1,3}\.\d{1,10}$"))
            )
        )

        final_df = complete_df.union_by_name(missing_df)                   

        return final_df 
    
    def write_table(self, df, table_name = 'output_table'):
        """
        Writes table
        Function returns nothing
        """

        df.write.save_as_table(table_name, mode="overwrite")
        logger.info(f"Table {table_name} successfully written")


