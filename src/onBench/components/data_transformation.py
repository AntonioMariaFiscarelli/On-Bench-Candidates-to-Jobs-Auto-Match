from src.onBench import logger
from snowflake.snowpark.functions import col, when, lit, coalesce, concat, trim, regexp_replace, call_function
from datetime import date, datetime

from src.onBench.utils.common import validate_string, validate_json

from src.onBench.entity.config_entity import DataTransformationConfig, GlobalConfig


class DataTransformation:
    """
    This class is used to manage the enrichment of the candidate and joborder tables.
    It provides methos for:
        - implementing a mechanism to process only candidate/vacancy whose information have changed
        - performing Named Entity Recognition in order to extract information from candidates' CVs and joborders' descriptions
        - computing embeddings of the extracted information

    """

    def __init__(
            self, 
            config: DataTransformationConfig,
            global_config: GlobalConfig
            ):
        self.config = config
        self.global_config = global_config
        self.curr_ts = datetime.now()



    def ai_complete_column_expr(
            self,
            input_column: str,
            system_prompt: str,
            llm_model: str
            ):
        """Builds the expression to call CORTEX.AI_COMPLETE on a given column
        Args:
            input_column (string): name of the column to process
            system_prompt (string): the system prompt for the LLM
            llm_model (string): the LLM model to use
        Returns:
            expr (Snowpark expression): expression to call CORTEX.AI_COMPLETE on the given column
        """

        expr = call_function(
            "SNOWFLAKE.CORTEX.AI_COMPLETE",
            lit(llm_model),
            concat(
                lit(system_prompt),
                lit("\nTesto: "),
                col(input_column)
            )
        )

        return expr


    def embeddings_column_expr(
            self, 
            input_column: str,
            model: str
            ):
        """Builds the expression to compute embeddings for a given column
        Args:
            input_column (string): name of the column to compute embeddings for
            model (string): the embedding model to use
        Returns:
            expr (Snowpark expression): expression to compute embeddings for the given column
        """

        expr = call_function(
            "SNOWFLAKE.CORTEX.AI_EMBED_1024",
            lit(model),
            when(
                col(input_column).is_null() | (col(input_column) == ""),
                lit("Non disponibile")
            ).otherwise(col(input_column))
        )

        return expr


    def embeddings_column_expr(
            self, 
            input_column: str,
            model: str
            ):
        """Builds the expression to compute embeddings for a given column
        Args:
            input_column (string): name of the column to compute embeddings for
            model (string): the embedding model to use
        Returns:
            expr (Snowpark expression): expression to compute embeddings for the given column
        """

        input_expr = (
            when(
                col(input_column).is_not_null(),
                regexp_replace(
                    trim(col(input_column)),
                    r"\s+",
                    " "
                )
            )
            .otherwise(lit(None))
        )

        expr = call_function(
            "SNOWFLAKE.CORTEX.AI_EMBED_1024",
            lit(model),
            when(
                input_expr.is_null() | (input_expr == ""),
                lit("Non disponibile")
            ).otherwise(input_expr)
        )

        return expr


    def compute_features_candidate(
            self,
            session
            ):
        """Extracts relevant information from candidates' CVs and computes embeddings

        Args:
            session (Snowpark session): Snowpark session

        Returns:
            df (Snowpark DataFrame): DataFrame with the computed features
        """

        database = self.config.database
        schema = self.config.schema
        input_table = self.config.candidate.input_table
        output_table = self.config.candidate.output_table
        llm_model = self.config.llm.model
        embeddings_model = self.config.embeddings.model

        df_new = session.table(f"{database}.{schema}.{input_table}")

        table_exists = (
            session.sql(
                f"SHOW TABLES LIKE '{output_table}' IN SCHEMA {database}.{schema}"
            ).count() > 0
        )

        df_old = (
            session.table(f"{database}.{schema}.{output_table}")
            if table_exists
            else None
        )

        prompt_skills = """Estrai dal seguente testo le skills del candidato (Skills tecniche).
        Rispondi solo con l'elenco delle skill, separate da virgola. Senza testo extra, non usare virgolette o apici all interno dei campi.
        Se non ci sono skills rilevanti, rispondi con una stringa vuota.
        Attieniti a questo esempio: Python, SQL, Java, Programmazione ad oggetti
        """

        prompt_certifications = """Estrai dal seguente testo le certificazioni del candidato (Certificazioni tecniche, patenti, certificazioni linguistiche).
        Rispondi solo con l'elenco delle certificazioni, separate da virgola. Senza testo extra, non usare virgolette o apici all interno dei campi.
        Estrai esclusivamente le certificazioni esplicitamente indicate nel testo. Non inferire, dedurre o aggiungere certificazioni non presenti.
        Se non ci sono certificazioni rilevanti, rispondi con una stringa vuota.
        Attieniti a questo esempio: CISSP, EIPASS, ECDL, PEKIT, ESOL, MOS, TOEFL, IELTS
        """

        feature_configs = [
            {
                "input_column": "description",
                "output_column": "skills",
                "expr": self.ai_complete_column_expr(
                    "description",
                    prompt_skills,
                    llm_model,
                )
            },
            {
                "input_column": "description",
                "output_column": "certifications",
                "expr": self.ai_complete_column_expr(
                    "description",
                    prompt_certifications,
                    llm_model,
                )
            },
            {
                "input_column": "skills",
                "output_column": "skills_emb",
                "expr": self.embeddings_column_expr(
                    "skills",
                    embeddings_model,
                ),
            },
            {
                "input_column": "certifications",
                "output_column": "certifications_emb",
                "expr": self.embeddings_column_expr(
                    "certifications",
                    embeddings_model,
                ),
            },
            {
                "input_column": "job_first",
                "output_column": "job_first_emb",
                "expr": self.embeddings_column_expr(
                    "job_first",
                    embeddings_model,
                ),
            },
            {
                "input_column": "job_intermediate",
                "output_column": "job_intermediate_emb",
                "expr": self.embeddings_column_expr(
                    "job_intermediate",
                    embeddings_model,
                ),
            },
            {
                "input_column": "job_last",
                "output_column": "job_last_emb",
                "expr": self.embeddings_column_expr(
                    "job_last",
                    embeddings_model,
                ),
            },
        ]

        df = df_new

        for config in feature_configs:

            input_column = config["input_column"]
            output_column = config["output_column"]
            expr = config["expr"]

            if not table_exists:
                df = (
                    df
                    .with_column(f"proc_{input_column}", lit(True))
                    .with_column(output_column, expr)
                )
            else:
                df = self.update_feature(
                    df,
                    df_old,
                    "candidate_id_bh",
                    input_column,
                    output_column,
                    expr,
                )

        return df


    def compute_features_joborder(
            self, 
            session
            ):
        """Extracts relevant information from joborders' descriptions and computes embeddings
        Args:
            session (Snowpark session): Snowpark session
        Returns:
            df (Snowpark DataFrame): DataFrame with the computed features
        """

        database = self.config.database
        schema = self.config.schema
        input_table = self.config.joborder.input_table
        output_table = self.config.joborder.output_table  
        llm_model = self.config.llm.model  
        embeddings_model = self.config.embeddings.model

        df_new = session.table(f"{database}.{schema}.{input_table}")

        table_exists = (
            session.sql(
                f"SHOW TABLES LIKE '{output_table}' IN SCHEMA {database}.{schema}"
            ).count() > 0
        )

        df_old = (
            session.table(f"{database}.{schema}.{output_table}")
            if table_exists
            else None
        )

        prompt_skills = """Estrai dal seguente testo le skills richieste dalla posizione lavorativa (Skills tecniche).
        Rispondi solo con l'elenco delle skill, separate da virgola. Senza testo extra, non usare virgolette o apici all interno dei campi. 
        Se non ci sono skills rilevanti, rispondi con una stringa vuota.
        Attieniti a questo esempio: Python, SQL, Java, Programmazione ad oggetti
        """
        
        prompt_certifications = """Estrai dal seguente testo le certificazioni richieste dalla posizione lavorativa (Certificazioni tecniche, patenti, certificazioni linguistiche).
        Rispondi solo con l'elenco delle certificazioni, separate da virgola. Senza testo extra, non usare virgolette o apici all interno dei campi. 
        Estrai esclusivamente le certificazioni esplicitamente indicate nel testo. Non inferire, dedurre o aggiungere certificazioni non presenti.
        Se non ci sono certificazioni rilevanti, rispondi con una stringa vuota.
        Attieniti a questo esempio: CISSP, EIPASS, ECDL, PEKIT, ESOL, MOS, TOEFL, IELTS
        """

        feature_configs = [
            {
                "input_column": "description",
                "output_column": "skills",
                "expr": self.ai_complete_column_expr(
                    "description", 
                    prompt_skills, 
                    llm_model
                    )
            },
            {
                "input_column": "description",
                "output_column": "certifications",
                "expr": self.ai_complete_column_expr(
                    "description", 
                    prompt_certifications, 
                    llm_model
                    )
            },
            {
                "input_column": "skills",
                "output_column": "skills_emb",
                "expr": self.embeddings_column_expr(
                    "skills", 
                    embeddings_model
                    )
            },
            {
                "input_column": "certifications",
                "output_column": "certifications_emb",
                "expr": self.embeddings_column_expr(
                    "certifications", 
                    embeddings_model
                    )
            },
            {
                "input_column": "ruolo_lavorativo_standard_mpg",
                "output_column": "ruolo_lavorativo_standard_mpg_emb",
                "expr": self.embeddings_column_expr(
                    "ruolo_lavorativo_standard_mpg", 
                    embeddings_model
                    )
            },
        ]


        df = df_new

        for config in feature_configs:

            input_column = config["input_column"]
            output_column = config["output_column"]
            expr = config["expr"]
            if not table_exists:
                df = (
                    df
                    .with_column(f"proc_{input_column}", lit(True))
                    .with_column(output_column, expr)
                )
            else:
                df = self.update_feature(
                    df,
                    df_old,
                    "joborder_id_bh",
                    input_column,
                    output_column,
                    expr,
                )

        return df
    

    def update_feature(
            self, 
            df_new, 
            df_old, 
            id_column, 
            input_column, 
            input_column_feat, 
            expr
            ):
        """ Updates a given column with by applying a snowpark expression, by comparing the new and old dataframes and computing embeddings only for the rows that have changed
        Args:
            df_new (Snowpark DataFrame): New dataframe
            df_old (Snowpark DataFrame): Old dataframe
            id_column (str): Name of the column used to identify rows
            column_name (str): Name of the column to update 
        Returns:
            df (Snowpark DataFrame): DataFrame with updated column
        """
        
        """
        New candidates/joborders, and for which information has changed, must be processed:

        A      A_old    row_exists_old    Result
        ----------------------------------------
        x      x        True              False     <- existing, same value
        x      y        True              True      <- existing, changed value
        x      NULL     NULL              True      <- new
        x      NULL     True              True      <- existing, changed value (now non null)
        NULL   x        True              True      <- existing, changed value (now null)
        NULL   NULL     NULL              True      <- new (null value)
        NULL   NULL     True              False     <- existing, same value (null)

        Rather than updating the column for specific canditates/joborders by using a simple WHEN clause,
        it is necessary to compute a separate dataframe with the candidates/joborders to be processed, and join it later.
        The reason is that, the WHEN clause does not garantee that the SNOWFLAKE.CORTEX function will be called only for the rows that satisfy the true condition,
        which can result in a function call for all candidates/joborders (regardless of whether they have changed or not)
        which will result in a significant increase in Snowflake credit costs.
        """

        df_to_process = (
            df_new.select(
                id_column,
                input_column
            )
            .join(
                df_old.select(
                    id_column,
                    input_column
                ).with_column("row_exists_old", lit(True)),
                id_column,
                "left",
                rsuffix="_old"
            )
            .filter(
                col("row_exists_old").is_null()
                |
                ~col(input_column).eqNullSafe(col(input_column + "_old"))
            )
            .drop(input_column + "_old")
            .drop("row_exists_old")
        )

        # Compute embeddings for the rows that need to be processed
        df_to_process = (
            df_to_process
            .with_column(input_column_feat, expr)
            .drop(input_column)
        )

        """
        Join the new dataframe with the processed rows and the old dataframe
        From the new dataframe, we get the candidate/joborder information (always present)
        From the processed rows, we get the new embeddings for the changed rows (present for new and old changed candidates/joborders)
        From the old dataframe, we get the old column (present for old candidates/joborders)
        If a candidate/joborder is new, it will be in the processed dataframe, but not in the old dataframe, so we will get the column from the processed rows
        If a candidate/joborder is old changed, it will be in both the processed dataframe and old dataframe, so we will get the column from the processed rows
        If a candidate/joborder is old and unchanged, it will be in the old dataframe, but not in the processed rows, so we will get the column from the old dataframe
        """

        df_result = (
            df_new
            .join(
                df_to_process,
                id_column,
                "left",
            )
            .join(
                df_old.select(
                    id_column,
                    input_column_feat,
                ),
                id_column,
                "left",
                rsuffix="_old"
            )
            .with_column(
                "proc_" + input_column, 
                when(col(input_column_feat).is_not_null(), lit(True))
                .otherwise(lit(False))
                )
            .with_column(
                input_column_feat,
                coalesce(
                    col(input_column_feat),
                    col(input_column_feat + "_old")
                )
            )
            .drop(col(input_column_feat + "_old"))
        )

        
        return df_result


    def write_table(
            self, 
            df, 
            table_name = 'output_table'
            ):
        """Writes dataframe in Snowflake
        Args:
            df (Snowpark Dataframe): dataframe
            table_name (string): name of the Snowflake table
        Returns:
        """

        df.write.save_as_table(table_name, mode="overwrite")

        logger.info(f"Table {table_name} successfully written")

  
