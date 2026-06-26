from src.onBench import logger
from snowflake.snowpark.functions import col, trim, lower, length, when, lit, trim, to_date, to_varchar, coalesce
from snowflake.snowpark.functions import array_append, array_to_string, call_function, array_construct
from snowflake.snowpark.types import StringType
from snowflake.snowpark.functions import udf
from datetime import date

from src.onBench.utils.common import validate_string, validate_json

from src.onBench.entity.config_entity import DataTransformationConfig


class DataTransformation:
    def __init__(self, config: DataTransformationConfig):
        self.config = config

    def read_new_data(self, session):
        database = self.config.database
        schema = self.config.schema

        input_table = self.config.input_table
        #input_table_cleaned = self.config.input_table_cleaned
        output_table = self.config.output_table
        input_table_vacancy = self.config.input_table_vacancy
        output_table_vacancy = self.config.output_table_vacancy

        # Step 1 — Check if T2 exists
        t2_exists = False
        try:
            session.table(f"{database}.{schema}.{output_table}").limit(1).collect()
            t2_exists = True
        except Exception:
            t2_exists = False

        # Step 2 — Build SQL depending on existence
        if t2_exists:
            # T2 exists → compute latest date and filter T1
            query = f"""
                WITH latest_t2 AS (
                    SELECT 
                        MAX(date_last_active) AS latest_date
                    FROM {database}.{schema}.{output_table}
                ),
                safe_latest AS (
                    SELECT 
                        COALESCE(latest_date, '1900-01-01') AS latest_date
                    FROM latest_t2
                )
                SELECT *
                FROM {database}.{schema}.{input_table}
                WHERE date_last_active >= (SELECT latest_date FROM safe_latest) AND
                --date_last_active >= DATEADD(day, -30, CURRENT_TIMESTAMP())
            """
        else:
            # T2 does not exist → return all of T1
            query = f"""
                SELECT *
                FROM {database}.{schema}.{input_table}
            """

        # Step 3 — Execute
        df_candidate = session.sql(query)

        ##############


        return df_candidate
    
    def limit_tables(self, session):

        database = self.config.database
        schema = self.config.schema
        input_table = self.config.input_table
        input_table_vacancy = self.config.input_table_vacancy

        nlimit = 1000
        df = session.table(f"{database}.{schema}.{input_table}")
        df = df.limit(nlimit)
        df.write.save_as_table(input_table, mode="overwrite")

        df = session.table(f"{database}.{schema}.{input_table_vacancy}")
        df = df.limit(nlimit)
        df.write.save_as_table(input_table_vacancy, mode="overwrite")

        logger.info(f"Tables {input_table} and {input_table_vacancy} limited to {nlimit} rows successfully cleaned")



    def clean_description(self, session):
        """
        Reads input table
        Cleans description column:
            - removes rows with empty description
            - replaces multiple consecutive whitespaces with a single whitespace (preserves newlines)
            - removes all html tags
            - lowercases all text
        Function returns Snowflake dataframe
        """
        database = self.config.database
        schema = self.config.schema
        input_table = self.config.input_table

        df = session.table(f"{database}.{schema}.{input_table}")
        df = df.filter((col("description").is_not_null()) & (trim(col("description")) != ""))
        df=df.limit(1000)
        
        def build_normalize_whitespace_udf():
            def normalize(text: str) -> str:
                import re
                if text is None:
                    return ''
                return re.sub(r'[ \t]+', ' ', text).strip()

            return udf(normalize, return_type=StringType(), input_types=[StringType()])
        normalize_udf = build_normalize_whitespace_udf()
        df = df.with_column("description", normalize_udf(df["description"]))
        

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
        df = df.with_column("description", clean_html_udf(df["description"]))

        df = df.with_column("description", lower(df["description"]))

        df = df.filter((col("description").is_not_null()) & 
                       (trim(col("description")) != "") &
                       (length(trim(col("description"))) > 5) &
                       (~col("description").like("%None%")) &
                       (~col("description").like("%null%"))
                       )

        df = df.with_column("description", col("description").cast("STRING"))
        
        logger.info(f"Table {input_table} successfully cleaned")

        return df


    def apply_ner_cortexai(self, session):
        """
        Reads input table cleaned
        Performs Named Entity Recognition:
            - age
            - date_of_birth
            - location
            - zip_code
            - last_job
            - second_last_job
            - third_last_job
            - skills
        Function returns Snowflake dataframe
        """

        database = self.config.database
        schema = self.config.schema
        input_table_cleaned = self.config.input_table_cleaned
        education_levels = self.config.education_levels
        regions = self.config.regions

        today_string = date.today().strftime("%Y-%m-%d")

        query = f"""
            SELECT
                *,
                SNOWFLAKE.CORTEX.COMPLETE(
                    'claude-4-sonnet',
                    CONCAT(
                        'Estrai dal seguente testo i campi: 
                        age (stringa. calcolala basandoti su date_of_birth, considerando che la data di oggi e {today_string};
                            esempio: se date_of_birth = 1990-05-18 e oggi siamo in data 2025-09-15, age = 35 
                            se non è possibile calcolarla, restituisci NaN), 
                        date_of_birth (stringa in formato YYYY-MM-DD, 
                            YYYY deve essere compreso tra 1900 e {today_string},
                            MM deve essere compreso tra 1 e 12,
                            DD deve essere compreso tra: 
                                1 e 30 quando MM uguale a 11, 04, 06, 09;
                                1 e 28 quando MM uguale a 02
                                1 e 31 per i restanti casi
                            ), 
                        location (stringa. il domicilio. solo la citta, non includere province o altro), 
                        region (la regione corrispondente alla citta del domicilio. stringa. possibili valori (solo uno): {", ".join(regions)},
                        country (la nazione corrispondente alla citta del domicilio. stringa, solo in Italiano. Esempio: Italia, Spagna, Iran etc.),
                        zip_code (cap. 5 caratteri numerici), 
                        last_job, 
                        second_last_job, 
                        third_last_job, 
                        skills (skills tecniche, stringa),
                        soft_skills (stringa),
                        languages (stringa, solo in Italiano),
                        certifications (stringa),
                        education (stringa, solo in Italiano, possibili valori (solo uno): {", ".join(education_levels)})',
                        'Rispondi in formato JSON, senza testo extra, non usare virgolette all interno dei campi. attieniti a questo esempio: 
                        {{"age": 30, "date_of_birth": "1993-05-12", "location": "Milano", "zip_code": "20100", 
                        "last_job": "Data Engineer", "second_last_job": "Developer", "third_last_job": "Intern", 
                        "skills": "Python, SQL, Java",
                        "soft_skills": "Precisione, Puntualità, Problem solving, Team Building, Gestione del tempo, Assistenza del cliente",
                        "languages": "Italiano, Inglese",
                        "certifications": "CISSP, EIPASS, ECDL, PEKIT, ESOL, MOS",
                        "education": "Diploma scuola superiore}}. ',
                        'Testo: ', description
                    )
                ) AS ner_json
            FROM 
            (SELECT *
            FROM {database}.{schema}.{input_table_cleaned}
            --LIMIT 100
            )

            """

        df = session.sql(query)

        df = validate_json(df, "ner_json")

        df = df.with_columns(
            ["age", "date_of_birth", "location", "region", "country", "zip_code", "last_job", "second_last_job", "third_last_job", 
             "skills", "soft_skills", "languages", "certifications", "education"],
            [
                col("ner_json")["age"].cast("STRING"),
                col("ner_json")["date_of_birth"].cast("STRING"),
                col("ner_json")["location"].cast("STRING"),
                col("ner_json")["region"].cast("STRING"),
                col("ner_json")["country"].cast("STRING"),
                col("ner_json")["zip_code"].cast("STRING"), 
                col("ner_json")["last_job"].cast("STRING"),
                col("ner_json")["second_last_job"].cast("STRING"),
                col("ner_json")["third_last_job"].cast("STRING"),
                col("ner_json")["skills"].cast("STRING"),
                col("ner_json")["soft_skills"].cast("STRING"),
                col("ner_json")["languages"].cast("STRING"),
                col("ner_json")["certifications"].cast("STRING"),
                col("ner_json")["education"].cast("STRING"),
            ]
        )
        
        
        df = validate_string(df, "location")
        df = validate_string(df, "region")
        df = validate_string(df, "country")
        df = validate_string(df, "last_job")
        df = validate_string(df, "second_last_job")
        df = validate_string(df, "third_last_job")
        df = validate_string(df, "skills")
        df = validate_string(df, "soft_skills")
        df = validate_string(df, "languages")
        df = validate_string(df, "certifications")
        df = validate_string(df, "education")

        # makes sure zip_code is a valid formato
        df = df.with_column(
            "zip_code",
            when(
                col("zip_code").rlike("^[0-9]{5}$"),
                col("zip_code")
            ).otherwise(lit(None))
        )

        # makes sure age is a reasonable value
        df = df.with_column(
            "age",
            when(
                (col("age") != "nan") &
                (col("age").cast("INT").is_not_null()) &
                (col("age").cast("INT") >= 1) &
                (col("age").cast("INT") <= 150),
                col("age").cast("INT")
            ).otherwise(lit(None))
        )

        # makes sure date_of_birth is a valid date
        date_regex = (
            "^(" +
            # Months with 31 days
            f"(19[0-9][0-9]|20[0-9][0-9]|{today_string})-(01|03|05|07|08|10|12)-(0[1-9]|[12][0-9]|3[01])|" +
            # Months with 30 days
            f"(19[0-9][0-9]|20[0-9][0-9]|{today_string})-(04|06|09|11)-(0[1-9]|[12][0-9]|30)|" +
            # February (non-leap year logic: 1–28)
            f"(19[0-9][0-9]|20[0-9][0-9]|{today_string})-02-(0[1-9]|1[0-9]|2[0-8])" +
            ")$"
        )
        df = df.with_column(
            "date_of_birth",
            when(
                col("date_of_birth").rlike(date_regex),
                to_date(to_varchar(col("date_of_birth")), "YYYY-MM-DD"),
            ).otherwise(lit(None))
        )
        
        df = df.drop("ner_json")
        df = df.drop("description")

        logger.info(f"NER on {input_table_cleaned} table successful")

        return df
    
    def compute_embeddings(self, session):

        database = self.config.database
        schema = self.config.schema
        input_table_cleaned = self.config.output_table

        from snowflake.snowpark.functions import call_function

        df = session.table(f"{database}.{schema}.{input_table_cleaned}")
        cols_no_emb = [c for c in df.columns if not c.lower().endswith("_emb")]
        df = df.select([col(c) for c in cols_no_emb])

        # 3. Clean text columns: replace NULL or '' with "Non disponibile"
        df = (
            df
            .with_column("skills_clean",
                when(col("skills").is_null() | (col("skills") == ""), lit("Non disponibile"))
                .otherwise(col("skills"))
            )
            .with_column("last_job_clean",
                when(col("last_job").is_null() | (col("last_job") == ""), lit("Non disponibile"))
                .otherwise(col("last_job"))
            )
            .with_column("second_last_job_clean",
                when(col("second_last_job").is_null() | (col("second_last_job") == ""), lit("Non disponibile"))
                .otherwise(col("second_last_job"))
            )
            .with_column("third_last_job_clean",
                when(col("third_last_job").is_null() | (col("third_last_job") == ""), lit("Non disponibile"))
                .otherwise(col("third_last_job"))
            )
        )
        # 4. Add new embeddings using Cortex
        df = (
            df
            .with_column(
                "skills_emb",
                call_function(
                    "SNOWFLAKE.CORTEX.AI_EMBED_1024",
                    lit("multilingual-e5-large"),
                    col("skills_clean")
                )
            )
            .with_column(
                "last_job_emb",
                call_function(
                    "SNOWFLAKE.CORTEX.AI_EMBED_1024",
                    lit("multilingual-e5-large"),
                    col("last_job_clean")
                )
            )
            .with_column(
                "second_last_job_emb",
                call_function(
                    "SNOWFLAKE.CORTEX.AI_EMBED_1024",
                    lit("multilingual-e5-large"),
                    col("second_last_job_clean")
                )
            )
            .with_column(
                "third_last_job_emb",
                call_function(
                    "SNOWFLAKE.CORTEX.AI_EMBED_1024",
                    lit("multilingual-e5-large"),
                    col("third_last_job_clean")
                )
            )
        )

        clean_cols = [c for c in df.columns if c.lower().endswith("_clean")] 
        df = df.drop(clean_cols)

        logger.info(f"Embeddings on {input_table_cleaned} table successful")

        return df
        

    def apply_ner_vacancy(self, session):
        """
        Reads vacancy table
        Performs Named Entity Recognition:

        Function returns Snowflake dataframe
        """

        database = self.config.database
        schema = self.config.schema
        input_table_vacancy = self.config.input_table_vacancy

        turno_preferenza = self.config.turno_preferenza
        education_levels = self.config.education_levels
        parttime_preferenza_perc = self.config.parttime_preferenza_perc

        
        texts = ["skills", "parttime_preferenza_perc", "education", "DESCRIZIONE_USO_INTERNO", "TESTO_PUBBLICAZIONE", "RICHIESTE_AGGIUNTIVE", "REQUISITI"]
        labels = ["Skills richieste", "Percentuale part time", "Titoli di studio richiesti", "DESCRIZIONE USO INTERNO", "TESTO PUBBLICAZIONE", "RICHIESTE AGGIUNTIVE", "REQUISITI"]
        
        all_texts = []
        for label, text in zip(labels, texts):
            all_texts.append(f"""'{label}: ', {text} """)

        concat_text = f"""CONCAT( {", '| ', ".join(all_texts)}  )"""


        query = f"""
        SELECT
            joborderid,
            dateadded,
            jobtitle,
            location,
            region,
            'Italia' AS country,
            --SNOWFLAKE.CORTEX.COMPLETE(
            --    'claude-4-sonnet',
            --    CONCAT(
            --        'Data la citta e/o regione, estrai la relativa nazione. Rispondi solo con il nome della nazione, in Italiano.',
            --        'Città: ', COALESCE(location, ''),
            --        'Regione: ', COALESCE(region, '')
            --    )
            --) AS country,
            salary_low,
            date_available,
            {concat_text} AS description,
            SNOWFLAKE.CORTEX.COMPLETE(
                'claude-4-sonnet',
                CONCAT(
                    'Stai analizzando  una posizione lavorativa aperta, estrai i seguenti campi: 
                    - turno_preferenza (turni richiesti per la posizione. stringa, possibili valori (anche multipli): {", ".join(turno_preferenza)}), 
                    Se specifica Full Time Giornaliero  allora deve restituire Mattina, Pomeriggio.
                    - parttime_preferenza_perc (percentuale di part time richiesta. stringa, scegli un solo valore tra questi: {", ".join(str(x) for x in parttime_preferenza_perc)}),
                    - skills (skills tecniche richieste (no soft skills). stringa, ad esempio Python, guida muletto, gestione progetti ecc),
                    - languages (lingue richieste, solo se richieste esplicitamente. stringa, ad esempio Inglese, Francese ecc )
                    - education (titolo di studio richiesto. stringa, solo in Italiano. Possibili valori (solo uno): {", ".join(education_levels)}),
                    - certifications (certificazioni richieste. stringa, ad esempio CISSP, EIPASS, ECDL, Patente B)',

                    'Rispondi in formato JSON, senza testo extra, non usare virgolette all interno dei campi. attieniti a questo esempio: 
                    {{"turno_preferenza": "Pomeriggio, Notte" ,
                    "parttime_preferenza_perc": "30",
                    "skills": "Python, SQL",
                    "languages": "Italiano, Inglese",
                    "education": "Diploma scuola superiore",
                    "certifications": "CISSP, EIPASS, ECDL"}}. ',

                    'Testo: ', {concat_text}
                )
            ) AS ner_json
        FROM {database}.{schema}.{input_table_vacancy}
        """

        df = session.sql(query)

        df = validate_json(df, "ner_json")

        df = df.with_columns(
            ["turno_preferenza", "parttime_preferenza_perc", "skills", "languages", "education", "certifications"],
            [
                col("ner_json")["turno_preferenza"].cast("STRING"),
                col("ner_json")["parttime_preferenza_perc"].cast("STRING"),
                col("ner_json")["skills"].cast("STRING"),
                col("ner_json")["languages"].cast("STRING"), 
                col("ner_json")["education"].cast("STRING"),
                col("ner_json")["certifications"].cast("STRING"),
            ]
        )
        
        
        df = validate_string(df, "turno_preferenza")
        df = validate_string(df, "parttime_preferenza_perc")
        df = validate_string(df, "skills")
        df = validate_string(df, "languages")
        df = validate_string(df, "education")
        df = validate_string(df, "certifications")

        df = df.with_column(
            "parttime_preferenza_perc",
            when(
                (col("parttime_preferenza_perc") != "nan") &
                (col("parttime_preferenza_perc").cast("INT").is_not_null()) &
                (col("parttime_preferenza_perc").cast("INT") >= 0) &
                (col("parttime_preferenza_perc").cast("INT") <= 100),
                col("parttime_preferenza_perc").cast("INT")
            ).otherwise(lit(None))
        )

        df = df.drop("ner_json")
        df = df.drop("description")

        logger.info(f"NER on {input_table_vacancy} table successful")

        return df
    

    def compute_embeddings_vacancy(self, session):

        database = self.config.database
        schema = self.config.schema
        input_table_vacancy = self.config.output_table_vacancy

        df = session.table(f"{database}.{schema}.{input_table_vacancy}")
        cols_no_emb = [c for c in df.columns if not c.lower().endswith("_emb")]
        df = df.select([col(c) for c in cols_no_emb])

        # 3. Clean text columns: replace NULL or '' with "Non disponibile"
        df = (
            df
            .with_column(
                "skills_clean",
                when(col("skills").is_null() | (col("skills") == ""), lit("Non disponibile"))
                .otherwise(col("skills"))
            )
            .with_column(
                "jobtitle_clean",
                when(col("jobtitle").is_null() | (col("jobtitle") == ""), lit("Non disponibile"))
                .otherwise(col("jobtitle"))
            )
        )
        # 4. Add new embeddings using Cortex
        df = (
            df
            .with_column(
                "skills_emb",
                call_function(
                    "SNOWFLAKE.CORTEX.AI_EMBED_1024",
                    lit("multilingual-e5-large"),
                    col("skills_clean")
                )
            )
            .with_column(
                "jobtitle_emb",
                call_function(
                    "SNOWFLAKE.CORTEX.AI_EMBED_1024",
                    lit("multilingual-e5-large"),
                    col("jobtitle_clean")
                )
            )
        )

        clean_cols = [c for c in df.columns if c.lower().endswith("_clean")] 
        df = df.drop(clean_cols)

        logger.info(f"Embeddings on {input_table_vacancy} table successful")

        return df
    
    def apply_chatwa_extraction(self, session):
        """
        Reads input table cleaned
        Performs CHATWA extraction to get CHATWA id from CANDIDATEID
        Function returns Snowflake dataframe
        """

        database = self.config.database
        schema = self.config.schema
        input_table_cleaned = self.config.output_table

        query = f"""
            SELECT
                *,
                SNOWFLAKE.CORTEX.COMPLETE(
                    'claude-4-sonnet',
                    CONCAT(
                        'Assegna True se il candidato ha risposto positivamente alla domanda "Sei disponibile a lavorare nel fine settimana?" altrimenti assegna False.
                        ️Rispondi solo con True o False. ',
                        'Testo: ', COALESCE(chatwa, '')
                    )
                ) AS turno_preferenza_weekend
            FROM 
            (SELECT *
            FROM {database}.{schema}.{input_table_cleaned}
            )

            """

        query = f"""
            SELECT
                *,
                CASE 
                    WHEN chatwa IS NULL OR TRIM(chatwa) = '' THEN 'False'
                    ELSE SNOWFLAKE.CORTEX.COMPLETE(
                            'claude-4-sonnet',
                            CONCAT(
                                'Assegna True se il candidato ha risposto positivamente alla domanda "Sei disponibile a lavorare nel fine settimana?" altrimenti assegna False. ',
                                'Rispondi solo con True o False. ',
                                'Testo: ', COALESCE(chatwa, '')
                            )
                        )
                END AS turno_preferenza_weekend
            FROM 
            (
                SELECT *
                FROM {database}.{schema}.{input_table_cleaned}
            )
        """

        df = session.sql(query)

        df = df.with_column(
            "turno_preferenza",
            when(
                (lower(trim(col("turno_preferenza_weekend"))) == 'true') &
                (~array_to_string(col("turno_preferenza"), lit(",")).contains(lit("Fine Settimana"))),
                array_append(col("turno_preferenza"), lit("Fine Settimana"))
            ).otherwise(col("turno_preferenza"))
        )

        df = df.drop("chatwa")
        df = df.drop("turno_preferenza_weekend")

        logger.info(f"CHAT WA NER on {input_table_cleaned} table successful")

        return df

    def apply_chatwa_extraction(self, session):
        """
        Reads input table cleaned
        Performs CHATWA extraction to get CHATWA id from CANDIDATEID
        Function returns Snowflake dataframe
        """

        database = self.config.database
        schema = self.config.schema
        input_table_cleaned = self.config.output_table


        # ------------------------------------------------------------
        # STEP 1 — Extract only rows with non-null chatwa (SQL, fast)
        # ------------------------------------------------------------
        session.sql(f"""
            CREATE OR REPLACE TEMP TABLE t_chatwa AS
            SELECT candidateid, chatwa
            FROM {database}.{schema}.{input_table_cleaned}
            WHERE chatwa IS NOT NULL
            AND TRIM(chatwa) <> '';
        """).collect()

        # ------------------------------------------------------------
        # STEP 2 — Run Cortex ONLY on those rows (SQL, required)
        # ------------------------------------------------------------
        session.sql("""
            CREATE OR REPLACE TEMP TABLE t_chatwa_llm AS
            SELECT
                candidateid,
                SNOWFLAKE.CORTEX.COMPLETE(
                    'claude-4-sonnet',
                    CONCAT(
                        'Assegna True se il candidato ha risposto positivamente alla domanda "Sei disponibile a lavorare nel fine settimana?" altrimenti assegna False. ',
                        'Rispondi solo con True o False. ',
                        'Testo: ', chatwa
                    )
                ) AS turno_preferenza_weekend
            FROM t_chatwa;
        """).collect()

        # ------------------------------------------------------------
        # STEP 3 — Join LLM results back to full dataset (SQL, fast)
        # ------------------------------------------------------------
        session.sql(f"""
            CREATE OR REPLACE TEMP TABLE t_final AS
            SELECT
                a.*,
                COALESCE(b.turno_preferenza_weekend, 'False') AS turno_preferenza_weekend
            FROM {database}.{schema}.{input_table_cleaned} a
            LEFT JOIN t_chatwa_llm b USING (candidateid);
        """).collect()

        # ------------------------------------------------------------
        # STEP 4 — Load into Snowpark and update turno_preferenza
        # ------------------------------------------------------------
        df = session.table("t_final")

        df = df.with_column(
            "turno_preferenza",
            when(
                (lower(trim(col("turno_preferenza_weekend"))) == 'true') &
                (
                    col("turno_preferenza").is_null() |
                    (~array_to_string(col("turno_preferenza"), lit(",")).contains(lit("Fine Settimana")))
                ),
                array_append(
                    coalesce(col("turno_preferenza"), array_construct()),
                    lit("Fine Settimana")
                )
            ).otherwise(col("turno_preferenza"))
        )


        df = df.drop("chatwa")
        df = df.drop("turno_preferenza_weekend")

        # ------------------------------------------------------------
        # STEP 5 — Save final output table
        # ------------------------------------------------------------

        logger.info(f"CHAT WA NER on {input_table_cleaned} table successful")

        return df 
    
    def add_geo_info(self, session):
        """
        Reads candidate dataframe
        Reads italian cities dataframe
        Checks that zip_codes are valid
        Adds geo info (latitude, longitude)
        Function returns Snowflake dataframe
        """
        database = self.config.database
        schema = self.config.schema
        output_table = self.config.output_table
        input_table_italian_cities = self.config.input_table_italian_cities
        
        candidates = session.table(f"{database}.{schema}.{output_table}")
        italian_cities = session.table(f"{database}.{schema}.{input_table_italian_cities}")

        # This avoids issues when the candidates table has already lat and long columns
        candidates = candidates.select(
            *[col(c) for c in candidates.columns if c.lower() not in {"latitude", "longitude", "province_ext"}]
        )
        
        valid_zips = [
            row[key]
            for row in italian_cities.select("zip").distinct().collect()
            for key in row.as_dict()
            if key.lower() == "zip"
        ]
        # Replace invalid ZIPs with None
        candidates = candidates.with_column(
            "zip_code",
            when(col("zip_code").isin(valid_zips), col("zip_code")).otherwise(lit(None))
            )

        # Rename conflicting columns in italian_cities
        italian_cities_geo = italian_cities.select(
            col("city_name"),
            col("zip"),
            col("latitude"),
            col("longitude"),
            col("province_ext")
        )

        # Join with italian_cities to get zip_code, latitude, longitude, and province_ext
        df = candidates.join(
            italian_cities_geo,
            lower(candidates["location"]) == lower(italian_cities_geo["city_name"]),
            "left"
        )
        
        # Update zip_code if it's missing
        df = df.with_column(
            "zip_code",
            when(
                col("zip_code").is_null(), col("zip") 
            ).otherwise(col("zip_code"))
        )
        
        # Select all original candidates columns plus geo data from italian_cities
        df = df.select(
            *[col(c.name) for c in candidates.schema.fields],
            col("latitude"),
            col("longitude"),
            col("province_ext"),
        ).drop("zip")
        
        df = validate_string(df, "province_ext")

        # Regex for latitude: -90 to 90 with optional decimals
        latitude_regex = r"^[-+]?([0-8]?\d(\.\d+)?|90(\.0+)?)$"
        # Regex for longitude: -180 to 180 with optional decimals
        longitude_regex = r"^[-+]?((1[0-7]\d|0?\d{1,2})(\.\d+)?|180(\.0+)?)$"

        # Make sure latitude and longiture are valid
        df = df.with_column(
            "latitude",
            when(
                col("latitude").rlike(latitude_regex),
                col("latitude").cast("FLOAT")
            ).otherwise(lit(None))
        ).with_column(
            "longitude",
            when(
                col("longitude").rlike(longitude_regex),
                col("longitude").cast("FLOAT")
            ).otherwise(lit(None))
        )

        df = df.with_column("distance_km", lit(999999))

        logger.info(f"Geo info on {input_table_italian_cities} table successful")

        return df


    def remove_old_data(self, session):
        database = self.config.database
        schema = self.config.schema

        input_table = self.config.input_table
        #input_table_cleaned = self.config.input_table_cleaned
        output_table = self.config.output_table
        input_table_vacancy = self.config.input_table_vacancy
        output_table_vacancy = self.config.output_table_vacancy
        days_prior = self.config.days_prior

        from snowflake.snowpark.functions import dateadd, current_timestamp

        # Step 1 — Check if T2 exists
        t2_exists = False
        try:
            session.table(f"{database}.{schema}.{output_table}").limit(1).collect()
            t2_exists = True
        except Exception:
            t2_exists = False

        # Step 2 — Build SQL depending on existence
        if t2_exists:
            df = session.table(f"{database}.{schema}.{output_table}")
            # T2 exists → compute latest date and filter T1
            df = df.filter( col("date_last_active") >= dateadd("day", -days_prior, current_timestamp()) )

        return df
    

    def write_table(self, df, table_name = 'output_table'):
        """
        Writes table
        Function returns nothing
        """
        df.write.save_as_table(table_name, mode="overwrite")
        logger.info(f"Table {table_name} successfully written")

  
