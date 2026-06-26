from src.onBench import logger
import pandas as pd
from snowflake.snowpark import Row
from snowflake.snowpark.functions import col, lit, coalesce, when, greatest, expr, split, listagg, regexp_replace
from snowflake.snowpark.functions import call_function, concat, lower, length, trim, udf
from snowflake.snowpark.types import StructType, StructField, StringType, FloatType


from src.onBench.entity.config_entity import DataIngestionConfig

class DataIngestion:
    """
    This class is used to create the candidate and vacancie tables.
    It provides methods for:
        - reading the input tables
        - filtering the tables in order to select only the candidates/vacancies that have been created/modified recently
        - cleanings candidates' CVs and descriptions

    """

    def __init__(self, config: DataIngestionConfig):
        self.config = config

    def clean_description(self, df, column = "description_cleaned"):
        """Cleans string columns such as parsed CVs and job descriptions:
            - removes rows with empty description
            - replaces multiple consecutive whitespaces with a single whitespace (preserves newlines)
            - removes all html tags
            - lowercases all text

        Args:
            df (DataFrame): dataframe
            column (string): column to clean

        Returns:
            df (Snowpark Dataframe): dataframe
        """

        df = df.filter((col(column).is_not_null()) & (trim(col(column)) != ""))
        
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

        df = df.filter((col(column).is_not_null()) & 
                       (trim(col(column)) != "") &
                       (length(trim(col(column))) > 5) &
                       (~col(column).like("%None%")) &
                       (~col(column).like("%null%"))
                       )

        df = df.with_column(column, col(column).cast("STRING"))
        
        logger.info(f"Table successfully cleaned")

        return df


    def read_table_candidate(self, session):
        """Creates candidate table with all the columns needed

        Args:
            session (Session): Snowflake session

        Returns:
            df (Snowpark Dataframe): dataframe
        """
        database = self.config.database
        schema = self.config.schema
        input_table = self.config.candidate.input_table
        columns = self.config.candidate.columns
        days_prior = self.config.candidate.days_prior
        desired_locations = self.config.desired_locations
        parttime_preferenza_perc = self.config.parttime_preferenza_perc
        
        # Create a temporary table with only 100 rows from the view 
        if(True):
            input_table = f"candidate_materialized_100"
            """
            CREATE OR REPLACE TABLE IT_DISCOVERY.CONSUMER_INT_MODEL.candidate_materialized AS 
            SELECT * FROM IT_DISCOVERY.CONSUMER_INT_MODEL.CANDIDATE_CLEANED -- this is your VIEW 
            WHERE date_added >= DATEADD(day, -60, CURRENT_TIMESTAMP())
            LIMIT 1000
            """

        # Reads only "fresh" candidates, i.e. candidates that have been added or have been modified in the last N days
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
                CASE 
                    WHEN cc.date_added IS NULL
                    AND cc.date_last_modified IS NULL
                    AND jsc_agg.last_status_change IS NULL
                        THEN NULL
                    ELSE GREATEST(
                            COALESCE(cc.date_added, TO_TIMESTAMP('0001-01-01 00:00:00')),
                            COALESCE(cc.date_last_modified, TO_TIMESTAMP('0001-01-01 00:00:00')),
                            COALESCE(jsc_agg.last_status_change, TO_TIMESTAMP('0001-01-01 00:00:00'))
                        )
                END AS date_last_active

            FROM {database}.{schema}.{input_table} cc
            LEFT JOIN jsc_agg
                ON cc.candidateid = jsc_agg.candidateid
            WHERE 
                cc.date_added >= DATEADD(day, -{days_prior}, CURRENT_TIMESTAMP())
                OR cc.date_last_modified >= DATEADD(day, -{days_prior}, CURRENT_TIMESTAMP())
                OR jsc_agg.candidateid IS NOT NULL
        """)

        candidate_df = candidate_df.with_column("description_raw", col("description"))

        candidate_df = candidate_df.with_column("candidateid", col("candidateid").cast("string"))
        

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
        
        df = df.with_column("description_raw", col("description"))
        df = df.with_column("chatwa_raw", col("chatwa"))

        
        #Desired location is set to its default values: "Locale", "Regionale", "Nazionale", "Internazionale". Only one value allowed
        case_parts = []
        for level in reversed(desired_locations):   # highest first
            case_parts.append(f"WHEN DESIRED_LOCATION ILIKE '%{level}%' THEN '{level}'")

        case_expr = "CASE " + " ".join(case_parts) + " ELSE NULL END"

        df = df.with_column(
            "DESIRED_LOCATION",
            expr(case_expr)
        )

        #Turno preferenza is set to its default values: ["Mattina", "Pomeriggio", "Notte", "Fine Settimana"] Multiple values are allowed as a string of comma separated values such as "Mattina, Pomeriggio"
        df = df.with_column(
            "turno_preferenza",
            split(col("turno_preferenza"), lit(","))
        )


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

        logger.info(f"Table {input_table} successfully read")
        
        return df
    

    def read_table_vacancy(self, session):
        """Creates vacancy table with all the columns needed

        Args:
            session (Session): Snowflake session

        Returns:
            df (Snowpark Dataframe): dataframe
        """
        database = self.config.database
        schema = self.config.schema
        input_table_vacancy = self.config.vacancy.input_table
        columns_vacancy = self.config.vacancy.columns
        days_prior = self.config.vacancy.days_prior

        # Create a temporary table with only 100 rows from the view 
        if(True):
            input_table_vacancy = f"joborder_materialized_100"
            """
            CREATE OR REPLACE TABLE IT_DISCOVERY.CONSUMER_INT_MODEL.joborder_materialized AS 
            SELECT * FROM IT_DISCOVERY.CONSUMER_INT_MODEL.JOBORDER_CLEANED -- this is your VIEW 
            WHERE isopen = 1 AND status = 'Accepting Candidates' 
            AND datelastmodified >= DATEADD(day, -60, CURRENT_TIMESTAMP())
            LIMIT 1000

            """

        # Reads only "fresh" vacancies, i.e. vacancies that have been added or have been modified  in the last N days
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
                jc.jobtitle,
                jc.citta AS location,
                jc.regione AS region,
                jc.salary AS salary_low,
                jc.data_inizio_validita AS date_available,

                CASE 
                    WHEN jc.datelastmodified IS NULL
                    AND jsc_agg.last_status_change IS NULL
                        THEN NULL
                    ELSE GREATEST(
                            COALESCE(jc.datelastmodified, TO_TIMESTAMP('0001-01-01 00:00:00')),
                            COALESCE(jsc_agg.last_status_change, TO_TIMESTAMP('0001-01-01 00:00:00'))
                        )
                END AS date_last_active,
                IFF(
                    COALESCE(
                        NULLIF(TRIM(skill_list), ''),
                        NULLIF(TRIM(part_time_percent), ''),
                        NULLIF(TRIM(titoli_richiesti), ''),
                        NULLIF(TRIM(DESCRIZIONE_USO_INTERNO), ''),
                        NULLIF(TRIM(TESTO_PUBBLICAZIONE), ''),
                        NULLIF(TRIM(RICHIESTE_AGGIUNTIVE), ''),
                        NULLIF(TRIM(REQUISITI), '')
                    ) IS NULL,
                    
                    NULL,
                    
                    CONCAT(
                        'Skills richieste: ', COALESCE(skill_list, ''), ' | ',
                        'Percentuale part time: ', COALESCE(part_time_percent, ''), ' | ',
                        'Titoli di studio richiesti: ', COALESCE(titoli_richiesti, ''), ' | ',
                        'DESCRIZIONE USO INTERNO: ', COALESCE(DESCRIZIONE_USO_INTERNO, ''), ' | ',
                        'TESTO PUBBLICAZIONE: ', COALESCE(TESTO_PUBBLICAZIONE, ''), ' | ',
                        'RICHIESTE AGGIUNTIVE: ', COALESCE(RICHIESTE_AGGIUNTIVE, ''), ' | ',
                        'REQUISITI: ', COALESCE(REQUISITI, '')
                    )
                ) AS description

            FROM filtered_jc jc
            LEFT JOIN jsc_agg
                ON jc.joborderid = jsc_agg.joborderid

            WHERE
                jc.datelastmodified >= DATEADD(day, -{days_prior}, CURRENT_TIMESTAMP())
                OR jsc_agg.last_status_change >= DATEADD(day, -{days_prior}, CURRENT_TIMESTAMP())

        """)

        df = df.with_column("description_raw", col("description"))

        logger.info(f"Table {input_table_vacancy} successfully read")

        return df
    

    def clean_description_candidate(self, session):
        """Cleans string columncontaining parsed CVs:
            - removes very long meaningless sequences of characters
            - removes character codes for attachments (pdf, jpg etc)

            - General cleaning:
                - removes rows with empty description
                - replaces multiple consecutive whitespaces with a single whitespace (preserves newlines)
                - removes all html tags
                - lowercases all text
            - Removes parsed CVs that are longer than 200K characters

        Args:
            session (Session): Snowflake session

        Returns:
            df (Snowpark Dataframe): dataframe
        """
        database = self.config.database
        schema = self.config.schema
        input_table = self.config.candidate.output_table
        max_tokens = self.config.max_tokens
        df = session.table(f"{database}.{schema}.{input_table}")

        #Removes very long meaningless sequences of characters
        noise_pattern = r"(/9j/[A-Za-z0-9+/=]{30,}|iVBORw0KGgo[A-Za-z0-9+/=]{30,}|JVBERi0x[A-Za-z0-9+/=]{30,}|[A-Za-z0-9+/=]{80,}|[^A-Za-z0-9 ]{30,}|([A-Za-z]{3,} ){15,})"
        df = df.with_column("description", regexp_replace(col("description"), noise_pattern, "") )
        df = df.with_column("chatwa", regexp_replace(col("chatwa"), noise_pattern, "") )

        df = self.clean_description(df, "description")

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

        logger.info(f"Table {input_table} successfully cleaned")

        return df

    def clean_description_vacancy(self, session):
        """Cleans string columncontaining parsed CVs:
            - removes very long meaningless sequences of characters
            - removes character codes for attachments (pdf, jpg etc)

            - General cleaning:
                - removes rows with empty description
                - replaces multiple consecutive whitespaces with a single whitespace (preserves newlines)
                - removes all html tags
                - lowercases all text
            - Removes parsed CVs that are longer than 200K characters

        Args:
            session (Session): Snowflake session

        Returns:
            df (Snowpark Dataframe): dataframe
        """
        database = self.config.database
        schema = self.config.schema
        input_table = self.config.vacancy.output_table
        max_tokens = self.config.max_tokens


        df = session.table(f"{database}.{schema}.{input_table}")

        #Removes very long meaningless sequences of characters
        noise_pattern = r"(/9j/[A-Za-z0-9+/=]{30,}|iVBORw0KGgo[A-Za-z0-9+/=]{30,}|JVBERi0x[A-Za-z0-9+/=]{30,}|[A-Za-z0-9+/=]{80,}|[^A-Za-z0-9 ]{30,}|([A-Za-z]{3,} ){15,})"
        df = df.with_column("description", regexp_replace(col("description"), noise_pattern, "") )
        df = self.clean_description(df, "description")

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



    def read_cities_file(self, session):
        """Reads XLSX file containing italian cities and writes table in Snowflake

        Args:
            session (Session): Snowflake session

        Returns:
            df (Snowpark Dataframe): dataframe
        """
        italian_cities_file = self.config.italian_cities.input_file
        string_columns = self.config.italian_cities.string_columns
        numeric_columns = self.config.italian_cities.numeric_columns

        df = pd.read_excel(italian_cities_file, header=0)

        #Renames columns for consistency
        df.columns = (
            df.columns
            .str.strip()
            .str.replace(" ", "_")
            .str.replace('"', '')
            .str.replace("'", '')
            .str.lower()
        )
        df = df[string_columns + numeric_columns]
        
        # Converts ZIP to string (preserve leading zeros)
        df["zip"] = df["zip"].apply(lambda x: str(int(x)).zfill(5) if pd.notnull(x) else None)
        
        # Converts string columns
        for col in string_columns:
            df[col] = df[col].astype(str).str.strip()

        # Converts latitude and longitude to float, handle NaNs
        for col in numeric_columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        # Remove rows with missing city_name
        df = df[
            df["city_name"].notna() &  # Remove NaN and None
            (df["city_name"].str.strip() != "") &  # Remove empty and whitespace-only strings
            (df["city_name"].str.lower().str.strip() != "null") &  # Remove "NULL" string
            (df["city_name"].str.lower().str.strip() != "nan")  # Remove "nan" string
        ]

        #These are necessary in order to avoid columns names with quotes (e.g. "city" instead of city)
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

        df = session.create_dataframe(rows, schema=schema)

        return df
    
    def complete_cities_table(self, session):
        """Uses available information to fill latitude and longitude by prompting a LLM 

        Args:
            session (Session): Snowflake session

        Returns:
            df (Snowpark Dataframe): dataframe
        """

        output_table_italian_cities = self.config.italian_cities.output_table

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
        
        
        #Makes sure latitude and longitude respect the correct format
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
        """Writes dataframe in Snowflake

        Args:
            df (Snowpark Dataframe): dataframe
            table_name (string): name of the Snowflake table
        """

        df.write.save_as_table(table_name, mode="overwrite")
        logger.info(f"Table {table_name} successfully written")


