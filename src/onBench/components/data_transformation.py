from src.onBench import logger
from snowflake.snowpark.functions import col, trim, lower, when, lit, trim, to_date, to_varchar, coalesce, concat
from snowflake.snowpark.functions import udf, array_append, array_to_string, call_function, array_construct
from datetime import date, datetime

from src.onBench.utils.common import validate_string, validate_json

from src.onBench.entity.config_entity import DataTransformationConfig


class DataTransformation:
    """
    This class is used to manage the enrichment of the candidate and vacancy tables.
    It provides methos for:
        - implementing a mechanism to process only candidate/vacancy whose information have changed
        - performing Named Entity Recognition in order to extract information from candidates' CVs and vacancies' descriptions
        - computing embeddings of the extracted information

    """

    def __init__(self, config: DataTransformationConfig):
        self.config = config

        self.curr_ts = datetime.now()


    def load_incremental_candidate(self, session):
        """Reads table with fresh candidates (created in the data ingestion stage)
        joins table with the previous candidate table (created in the data transformation stage) in order to check whether description changed.
        If so, sets needs_processing to True so that the NER will be reapplied on the description table only for those candidates whose description changed.

        Args:
            session (Session): Snowflake session

        Returns:
            df: dataframe
        """
        database = self.config.database
        schema = self.config.schema
        input_table = self.config.candidate.input_table
        output_table = self.config.candidate.output_table

        t1 = (
            session.table(f"{database}.{schema}.{input_table}")
            .filter(
                (col("description").is_not_null()) & (trim(col("description")) != "")
                )
        )

        #Check if a candidate table exists, i.e. this is not the first time that the data trasfnormation stage is performed
        t2_exists = False
        try:
            session.table(f"{database}.{schema}.{output_table}").limit(1).collect()
            t2_exists = True
        except Exception:
            t2_exists = False

        if t2_exists:
            # If it exists, the new candidate table is joined with the old candidate table
            t2 = session.table(f"{database}.{schema}.{output_table}")

            joined = t1.join(
                t2,
                on="candidateid",
                how="left",
                rsuffix="_old",
            )

            # Checks if the current candidate description differs from the old one and is not null
            joined = joined.with_column(
                "needs_processing",
                (
                    col("description_raw_old").is_null()
                    | (
                        (col("description_raw") != col("description_raw_old"))
                        & (col("description_raw_old") != lit("Description"))
                    )
                )
            )

            joined = joined.with_column(
                "needs_processing_wa",
                coalesce(
                    (
                        col("chatwa").is_not_null()
                        &
                            (
                            (col("chatwa_raw_old").is_null())
                            | (
                                (col("chatwa_raw") != col("chatwa_raw_old"))
                                & (col("chatwa_raw_old") != lit("Chatwa"))
                            )
                        )
                    ),
                    lit(False)
                )
            )
        else:
            # If it does not exist, each row in the new candidate table must be processed
            joined = t1.with_column(
                    "needs_processing",
                    lit(True)
                    )
            joined = joined.with_column(
                    "needs_processing_wa",
                    col("chatwa").is_not_null()
                    )
        joined = joined.with_column("time_processing", lit(None).cast("TIMESTAMP"))

        return joined

    def load_incremental_vacancy(self, session):
        """Reads table with fresh vacancies (created in the data ingestion stage)
        joins table with the previous vacancy table (created in the data transformation stage) in order to check whether description changed.
        If so, sets needs_processing to True so that the NER will be reapplied on the description table only for those vacancies whose description changed.

        Args:
            session (Session): Snowflake session

        Returns:
            df: dataframe
        """
        database = self.config.database
        schema = self.config.schema
        input_table = self.config.vacancy.input_table
        output_table = self.config.vacancy.output_table

        t1 = (
            session.table(f"{database}.{schema}.{input_table}")
            .filter(
                ((col("description").is_not_null()) & (trim(col("description")) != "")) 
                | ((col("jobtitle").is_not_null()) & (trim(col("jobtitle")) != ""))
                )
        )

        #Check if a candidate table exists, i.e. this is not the first time that the data trasfnormation stage is performed
        t2_exists = False
        try:
            session.table(f"{database}.{schema}.{output_table}").limit(1).collect()
            t2_exists = True
        except Exception:
            t2_exists = False

        if t2_exists:
            # If it exists, the new vacancy table is joined with the old dvacancy table
            t2 = session.table(f"{database}.{schema}.{output_table}")

            joined = t1.join(
                t2,
                on="joborderid",
                how="left",
                rsuffix="_old",
            )


            # Checks if the current vacancy description differs from the old one and is not null
            joined = joined.with_column(
                "needs_processing",
                (
                    col("description_raw_old").is_null()
                    | (
                        (col("description_raw") != col("description_raw_old"))
                        & (col("description_raw_old") != lit("Description"))
                    )
                )
            )
        else:
            # If it does not exist, each row in the new vacancy table must be processed
            joined = t1
            joined = joined.with_column(
                    "needs_processing",
                    lit(True)
                    )

        joined = joined.with_column("time_processing", lit(None).cast("TIMESTAMP"))

        return joined

    def log_processed_rows_candidate(self, session, table_name: str, nproc_cv_rows: int, nproc_wa_rows: int, curr_ts):
        """Logs data processing information for candidates

        Args:
            session (Session): Snowflake session
            table_name (string): name of the log table
            nproc_cv_rows (string): number of candidates for which NER is applied on parsed CV during the current stage
            nproc_wa_rows (string): number of candidates for which NER is applied on Whatsapp conversation during the current stage
            curr_ts: timestamp at which NER is performed
        Returns:
        """
        # 1Create table if it doesn't exist
        session.sql(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id INTEGER AUTOINCREMENT START 1 INCREMENT 1,
                nproc_cv_rows INTEGER,
                nproc_wa_rows INTEGER,
                processed_at TIMESTAMP_NTZ
            )
        """).collect()

        # Insert row using parameter binding
        session.sql(
            f"""
            INSERT INTO {table_name} (nproc_cv_rows, nproc_wa_rows, processed_at)
            SELECT ?, ?, ?
            """,
            params=[nproc_cv_rows, nproc_wa_rows, curr_ts]
        ).collect()

    def log_processed_rows_vacancy(self, session, table_name: str, nproc_rows: int, curr_ts):
        """Logs data processing information for vacancies

        Args:
            session (Session): Snowflake session
            table_name (string): name of the log table
            nproc_rows (string): number of vacancies for which NER is applied
            curr_ts: timestamp at which NER is performed
        Returns:
        """
        # Create table if it doesn't exist
        session.sql(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id INTEGER AUTOINCREMENT START 1 INCREMENT 1,
                nproc_rows INTEGER,
                processed_at TIMESTAMP_NTZ
            )
        """).collect()

        # Insert row using parameter binding
        session.sql(
            f"""
            INSERT INTO {table_name} (nproc_rows, processed_at)
            SELECT ?, ?
            """,
            params=[nproc_rows, curr_ts]
        ).collect()      

    def compute_steps_candidate(self, session):
        """Processes the candidates table:
            - performs NER on candidates' CVs
            - compute embeddings on the new columns
            - add geographical information
        Args:
            session (Session): Snowflake session

        Returns:
            df (Snowpark Dataframe): dataframe
        """
        database = self.config.database
        schema = self.config.schema
        input_table = self.config.candidate.input_table_cleaned

        ner_features = self.config.candidate.ner_features
        emb_columns = self.config.candidate.emb_columns
        geo_columns = self.config.candidate.geo_columns
        candidate_log_table = self.config.candidate.candidate_log_table

        df = session.table(f"{database}.{schema}.{input_table}")

        # select columns from the new table
        new_columns = [ c for c in df.columns if not c.lower().endswith("_old")]
        df = df.select(
            *new_columns
        )

        #split rows that needs processing
        processed = df.filter(col("needs_processing")) 
        not_processed = df.filter(~col("needs_processing"))
        
        cv_count = processed.count()

        #if the new table contains already the column with the NER information, it drops them and recompute them
        processed = processed.drop(ner_features + emb_columns + geo_columns)
        
        processed = self.apply_ner_candidate(processed)
        processed = self.compute_embeddings_candidate(processed)
        processed = self.add_geo_info_candidate(session, processed)


        processed = processed.with_column(
            "time_processing",
            lit(self.curr_ts)
            )
        
        is_empty = not_processed.limit(1).count() == 0
        if(is_empty):
            df_cv = processed
        else:
            df_cv = processed.select(*processed.columns).union_all(
                    not_processed.select(*[
                        col(c).cast(processed.schema[c].datatype) for c in processed.columns
                    ])
                )
            

        processed = df_cv.filter(col("needs_processing_wa")) 
        not_processed = df_cv.filter(~col("needs_processing_wa"))
        
        wa_count = processed.count()
        
        processed = self.apply_chatwa_extraction_candidate(processed)

        processed = processed.with_column(
            "time_processing",
            lit(self.curr_ts)
            )
        self.log_processed_rows_candidate(session, candidate_log_table, cv_count, wa_count, self.curr_ts)

        is_empty = not_processed.limit(1).count() == 0
        if(is_empty):
            df_cv_wa = processed
        else:
            df_cv_wa = processed.select(*processed.columns).union_all(
                    not_processed.select(*[
                        col(c).cast(processed.schema[c].datatype) for c in processed.columns
                    ])
                )

        return df_cv_wa

    def compute_steps_vacancy(self, session):
        """Processes the vacancy table:
            - performs NER on vacancies' descriptions
            - compute embeddings on the new columns
        Args:
            session (Session): Snowflake session

        Returns:
            df (Snowpark Dataframe): dataframe
        """
        database = self.config.database
        schema = self.config.schema
        input_table = self.config.vacancy.input_table_cleaned

        ner_features = self.config.vacancy.ner_features
        emb_columns = self.config.vacancy.emb_columns
        vacancy_log_table = self.config.vacancy.vacancy_log_table

        df = session.table(f"{database}.{schema}.{input_table}")

        # select columns from the new table
        new_columns = [ c for c in df.columns if not c.lower().endswith("_old")]
        df = df.select(
            *new_columns
        )

        #split rows that needs processing
        processed = df.filter(col("needs_processing")) 
        not_processed = df.filter(~col("needs_processing"))

        #if the new table contains already the column with the NER information, it drops them and recompute them
        processed = processed.drop(ner_features + emb_columns)
        
        processed = self.apply_ner_vacancy(processed)
        processed = self.compute_embeddings_vacancy(processed)

        processed = processed.with_column(
            "time_processing",
            lit(self.curr_ts)
            )
        self.log_processed_rows_vacancy(session, vacancy_log_table, processed.count(), self.curr_ts)

        is_empty = not_processed.limit(1).count() == 0
        if(is_empty):
            df = processed
        else:
            df = processed.select(*processed.columns).union_all(
                    not_processed.select(*[
                        col(c).cast(processed.schema[c].datatype) for c in processed.columns
                    ])
                )

        return df

    def apply_ner_candidate(self, df):
        """Performs Named Entity Recognition (NER) on candidates' CVs. Then creates new columns with the extracted information.
        Args:
            df (Snowpark Dataframe): dataframe

        Returns:
            df (Snowpark Dataframe): dataframe
        """

        input_table_cleaned = self.config.candidate.input_table_cleaned

        llm_model = self.config.llm_model
        
        education_levels = self.config.education_levels
        regions = self.config.regions
        ner_features = self.config.candidate.ner_features


        today_string = date.today().strftime("%Y-%m-%d")

        system_prompt = f"""
                        Estrai dal seguente testo i campi: 
                        age (stringa. calcolala basandoti su date_of_birth, considerando che la data di oggi e {today_string};
                            esempio: se date_of_birth = 1990-05-18 e oggi siamo in data 2025-09-15, age = 35 
                            Se non è possibile calcolarla, restituisci stringa vuota), 
                        date_of_birth (stringa in formato YYYY-MM-DD, 
                            YYYY deve essere compreso tra 1900 e {today_string},
                            MM deve essere compreso tra 1 e 12,
                            DD deve essere compreso tra: 
                                1 e 30 quando MM uguale a 11, 04, 06, 09;
                                1 e 28 quando MM uguale a 02
                                1 e 31 per i restanti casi
                            ), 
                        location (stringa. il domicilio. solo la citta, non includere province o altro. Esempio: Milano), 
                        region (la regione corrispondente alla citta del domicilio. stringa. possibili valori (solo uno): {", ".join(regions)},
                        country (la nazione corrispondente alla citta del domicilio. stringa, solo in Italiano. Esempio: Italia),
                        zip_code (cap. 5 caratteri numerici. Esempio: 20100), 
                        last_job (Esempio: Data Engineer), 
                        second_last_job (Esempio: Ingegnere del Software), 
                        third_last_job (Esempio: Intern), 
                        skills (skills tecniche, stringa. Esempio: Python, SQL, Java, Programmazione ad oggetti),
                        soft_skills (stringa),
                        languages (stringa, solo in Italiano. Restituisci solo le lingue, non aggiungere livelli o aggettivi. Esempio: Italiano, Inglese, Hindi),
                        certifications (stringa. Esempio: CISSP, EIPASS, ECDL, PEKIT, ESOL, MOS),
                        education (stringa, solo in Italiano, possibili valori (solo uno): {", ".join(education_levels)})',
                        'Rispondi in formato JSON, senza testo extra, non usare virgolette all interno dei campi. attieniti a questo esempio: 
                        {{"age": 30, "date_of_birth": "1993-05-12", "location": "Milano", "region": "Lombardia", "country": "Italia", "zip_code": "20100", 
                        "last_job": "Data Engineer", "second_last_job": "Ingegnere del Software", "third_last_job": "Intern", 
                        "skills": "Python, SQL, Java",
                        "soft_skills": "Precisione, Puntualità, Problem solving, Team Building, Gestione del tempo, Assistenza del cliente",
                        "languages": "Italiano, Inglese, Hindi",
                        "certifications": "CISSP, EIPASS, ECDL, PEKIT, ESOL, MOS",
                        "education": "Diploma scuola superiore}}
                    """

        df  = (
            df 
            .select(
                "*",
                call_function(
                    "AI_COMPLETE",
                    lit(llm_model),
                    concat(
                        lit(system_prompt),
                        lit("\nTesto: "),
                        col("description")
                    )
                ).alias("ner_json")
            )
        )
        

        df = validate_json(df, "ner_json")
        df = df.select(
            "*",
            *[col("ner_json")[field].cast("STRING").alias(field) for field in ner_features]
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
                (col("age") != "") &
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

        logger.info(f"NER on {input_table_cleaned} table successful")

        return df
    
    def compute_embeddings_candidate(self, df):
        """Compute embeddings on the NER columns. These will be used to compute similarity between candidates and vacancies.
        Args:
            df (Snowpark Dataframe): dataframe

        Returns:
            df (Snowpark Dataframe): dataframe
        """
        input_table_cleaned = self.config.candidate.output_table_cleaned

        cols_no_emb = [c for c in df.columns if not c.lower().endswith("_emb")]
        df = df.select([col(c) for c in cols_no_emb])

        # If column value is null or empty, replace it with "Non disponibile". It is necessary to set a default string when computing embedings
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

    def apply_chatwa_extraction_candidate(self, df):
        """
        Reads input table cleaned
        Performs CHATWA extraction to get CHATWA id from CANDIDATEID
        Function returns Snowflake dataframe
        """

        input_table_cleaned = self.config.candidate.output_table_cleaned

        llm_model = self.config.llm_model


        system_prompt = f"""
                       Assegna True se il candidato ha risposto positivamente alla domanda "Sei disponibile a lavorare nel fine settimana?" altrimenti assegna False.
                       Rispondi solo con True o False.
                       """
        
        df  = (
            df 
            .select(
                "*",
                call_function(
                    "AI_COMPLETE",
                    lit(llm_model),
                    concat(
                        lit(system_prompt),
                        lit("\nTesto: "),
                        col("chatwa")
                    )
                ).alias("turno_preferenza_weekend")
            )
        )
        
        #df = df.with_column("turno_preferenza_weekend", lit("False"))

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

        df = df.drop("turno_preferenza_weekend")

        logger.info(f"CHAT WA NER on {input_table_cleaned} table successful")

        return df 
    
    def add_geo_info_candidate(self, session, df):
        """
        Reads candidate dataframe
        Reads italian cities dataframe
        Checks that zip_codes are valid
        Adds geo info (latitude, longitude)
        Function returns Snowflake dataframe
        """
        database = self.config.database
        schema = self.config.schema
        input_table_italian_cities = self.config.input_table_italian_cities
        
        candidates = df
        italian_cities = session.table(
            f"{database}.{schema}.{input_table_italian_cities}"
        )
        italian_cities = italian_cities.select(
            *[col(c) for c in italian_cities.columns if c.lower() != "region"]
        )

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

    def apply_ner_vacancy(self, df):
        """Performs Named Entity Recognition (NER) on vacancies' descriptions. Then creates new columns with the extracted information.
        Args:
            df (Snowpark Dataframe): dataframe

        Returns:
            df (Snowpark Dataframe): dataframe
        """

        input_table_vacancy = self.config.vacancy.input_table_cleaned

        llm_model = self.config.llm_model

        ner_features = self.config.vacancy.ner_features
        turno_preferenza = self.config.turno_preferenza
        education_levels = self.config.education_levels
        parttime_preferenza_perc = self.config.parttime_preferenza_perc

        turno_vals = ", ".join(turno_preferenza)
        parttime_vals = ", ".join(str(x) for x in parttime_preferenza_perc)
        edu_vals = ", ".join(education_levels)

        system_prompt = f"""
            Stai analizzando una posizione lavorativa aperta, estrai i seguenti campi:
            - country (la nazione in cui è sita la posizione lavorativa. stringa, solo in Italiano. Esempio: Italia, Spagna, Iran etc.). 
            Se non è specificata, restituisci di default Italia.
            - turno_preferenza (turni richiesti per la posizione. stringa, possibili valori (anche multipli): {turno_vals}. 
            Se specifica Full Time Giornaliero allora deve restituire Mattina, Pomeriggio.
            - parttime_preferenza_perc (percentuale di part time richiesta. stringa, scegli un solo valore tra questi: {parttime_vals}). 
            Se sono specificate le ore lavorative settimanali, calcolala sulla base di 40 ore. Ad esempio, se la posizione richiede 20 ore settimanali, la percentuale è 50. Se richiede 24h, la percentuale è 60.
            Se non è presente nessun valore, restituisci 100.
            - skills (skills tecniche richieste (no soft skills). stringa, ad esempio Python, guida muletto, gestione progetti ecc).
            - languages (lingue richieste, solo se richieste esplicitamente. stringa, ad esempio Inglese, Francese ecc ).
            - education (titolo di studio richiesto. stringa, solo in Italiano. Possibili valori (solo uno): {edu_vals}).
            - certifications (certificazioni richieste. stringa, ad esempio CISSP, EIPASS, ECDL, Patente B)'.

            Rispondi in formato JSON, senza testo extra, non usare virgolette all interno dei campi. attieniti a questo esempio: 
            Esempio:
            {{
            "country": "Italia",
            "turno_preferenza": "Pomeriggio, Notte",
            "parttime_preferenza_perc": "30",
            "skills": "Python, SQL",
            "languages": "Italiano, Inglese",
            "education": "Diploma scuola superiore",
            "certifications": "CISSP, EIPASS, ECDL, Patente B"}}
        """


        df = (
            df
            .select(
                "*",
                call_function(
                    "AI_COMPLETE",
                    lit(llm_model),
                    concat(
                        lit(system_prompt),
                        lit("Testo:"),
                        col("description")
                    )
                ).alias("ner_json")
            )
        )


        df = validate_json(df, "ner_json")
        df = df.select(
            "*",
            *[col("ner_json")[field].cast("STRING").alias(field) for field in ner_features]
        )
        
        df = validate_string(df, "country")
        df = validate_string(df, "turno_preferenza")
        df = validate_string(df, "parttime_preferenza_perc")
        df = validate_string(df, "skills")
        df = validate_string(df, "languages")
        df = validate_string(df, "education")
        df = validate_string(df, "certifications")

        # makes sure parttime_preferenza_perc is in the correct format
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

        logger.info(f"NER on {input_table_vacancy} table successful")

        return df 

    def compute_embeddings_vacancy(self, df):
        """Compute embeddings on the NER columns. These will be used to compute similarity between candidates and vacancies.
        Args:
            df (Snowpark Dataframe): dataframe

        Returns:
            df (Snowpark Dataframe): dataframe
        """
        input_table_vacancy = self.config.vacancy.output_table_cleaned

        cols_no_emb = [c for c in df.columns if not c.lower().endswith("_emb")]
        df = df.select([col(c) for c in cols_no_emb])

        # If column value is null or empty, replace it with "Non disponibile". It is necessary to set a default string when computing embedings
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
    
    def write_table(self, df, table_name = 'output_table'):
        """Writes dataframe in Snowflake

        Args:
            df (Snowpark Dataframe): dataframe
            table_name (string): name of the Snowflake table
        Returns:
        """

        df.write.save_as_table(table_name, mode="overwrite")

        logger.info(f"Table {table_name} successfully written")

  
