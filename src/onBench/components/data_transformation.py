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



    def _ai_complete_column_expr(
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


    def _embeddings_column_expr(
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
        
        """
        Computes and incrementally updates AI-derived candidate features.

        The function reads candidate records from the configured input table and
        generates structured features from CV content using Snowflake Cortex and
        embedding models. 
        On the first execution, all features are computed. On subsequent executions, 
        only features whose source columns have changed are recomputed, 
        while existing values are reused from the previous output table.

        The input table requires the following columns:
        - description
        - job_first
        - job_intermediate
        - job_last

        Generated features include:
        - skills
        - certifications
        - skills_emb
        - certifications_emb
        - job_first_emb
        - job_intermediate_emb
        - job_last_emb

        For each feature source column, a corresponding ``proc_<column>``
        flag is maintained to indicate whether the feature was processed
        during the current execution.

        Args:
            session (Session): Active Snowpark session.

        Returns:
            DataFrame: Candidate dataframe containing all original columns plus
        """

        database = self.config.database
        schema = self.config.schema
        input_table = self.config.candidate.input_table
        output_table = self.config.candidate.output_table
        llm_model = self.config.llm.model
        embeddings_model = self.config.embeddings.model

        df = session.table(f"{database}.{schema}.{input_table}")

        original_columns = df.columns


        # ------------------------------------------------------------------
        # Check for previous output so unchanged features can be reused.
        # ------------------------------------------------------------------

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

        # ------------------------------------------------------------------
        # Define features in dependency order: embeddings use extracted text.
        # ------------------------------------------------------------------

        feature_configs = [
            {
                "input_column": "description",
                "output_column": "skills",
                "expr": self._ai_complete_column_expr(
                    "description",
                    prompt_skills,
                    llm_model,
                )
            },
            {
                "input_column": "description",
                "output_column": "certifications",
                "expr": self._ai_complete_column_expr(
                    "description",
                    prompt_certifications,
                    llm_model,
                )
            },
            {
                "input_column": "skills",
                "output_column": "skills_emb",
                "expr": self._embeddings_column_expr(
                    "skills",
                    embeddings_model,
                ),
            },
            {
                "input_column": "certifications",
                "output_column": "certifications_emb",
                "expr": self._embeddings_column_expr(
                    "certifications",
                    embeddings_model,
                ),
            },
            {
                "input_column": "job_first",
                "output_column": "job_first_emb",
                "expr": self._embeddings_column_expr(
                    "job_first",
                    embeddings_model,
                ),
            },
            {
                "input_column": "job_intermediate",
                "output_column": "job_intermediate_emb",
                "expr": self._embeddings_column_expr(
                    "job_intermediate",
                    embeddings_model,
                ),
            },
            {
                "input_column": "job_last",
                "output_column": "job_last_emb",
                "expr": self._embeddings_column_expr(
                    "job_last",
                    embeddings_model,
                ),
            },
        ]


        # ------------------------------------------------------------------
        # Compute all features on first run; otherwise update changed inputs.
        # ------------------------------------------------------------------

        output_columns = []
        for config in feature_configs:

            input_column = config["input_column"]
            output_column = config["output_column"]
            flag_colum = f"proc_{input_column}"
            expr = config["expr"]

            if not table_exists:
                df = (
                    df
                    .with_column(flag_colum, lit(True))
                    .with_column(output_column, expr)
                )
            else:
                df = self._update_feature(
                    df,
                    df_old,
                    "candidate_id_bh",
                    input_column,
                    output_column,
                    expr,
                )
            output_columns = output_columns + [flag_colum, output_column]

        #output_columns = [f"proc_{c['input_column']}" for c in feature_configs] + [c["output_column"] for c in feature_configs]
        output_columns = list(dict.fromkeys(output_columns))

        df = df.select(
            *original_columns,
            *output_columns,
        )


        return df


    def compute_features_joborder(
            self, 
            session
            ):
        """
        Computes and incrementally updates AI-derived joborer features.

        The function reads joborder records from the configured input table and
        generates structured features from job description content using 
        Snowflake Cortex and embedding models. 
        On the first execution, all features are computed. On subsequent executions, 
        only features whose source columns have changed are recomputed, while existing values
        are reused from the previous output table.

        The input table requires the following columns:
        - description
        - ruolo_lavorativo_standard_mpg_emb

        Generated features include:
        - skills
        - certifications
        - skills_emb
        - certifications_emb
        - ruolo_lavorativo_standard_mpg_emb

        For each feature source column, a corresponding ``proc_<column>``
        flag is maintained to indicate whether the feature was processed
        during the current execution.

        Args:
            session (Session): Active Snowpark session.

        Returns:
            DataFrame: Candidate dataframe containing all original columns plus
        """

        database = self.config.database
        schema = self.config.schema
        input_table = self.config.joborder.input_table
        output_table = self.config.joborder.output_table  
        llm_model = self.config.llm.model  
        embeddings_model = self.config.embeddings.model

        df = session.table(f"{database}.{schema}.{input_table}")

        # ------------------------------------------------------------------
        # Check for previous output so unchanged features can be reused.
        # ------------------------------------------------------------------

        table_exists = (
            session.sql(
                f"SHOW TABLES LIKE '{output_table}' IN SCHEMA {database}.{schema}"
            ).count() > 0
        )

        original_columns = df.columns

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

        # ------------------------------------------------------------------
        # Define features in dependency order: embeddings use extracted text.
        # ------------------------------------------------------------------

        feature_configs = [
            {
                "input_column": "description",
                "output_column": "skills",
                "expr": self._ai_complete_column_expr(
                    "description", 
                    prompt_skills, 
                    llm_model
                    )
            },
            {
                "input_column": "description",
                "output_column": "certifications",
                "expr": self._ai_complete_column_expr(
                    "description", 
                    prompt_certifications, 
                    llm_model
                    )
            },
            {
                "input_column": "skills",
                "output_column": "skills_emb",
                "expr": self._embeddings_column_expr(
                    "skills", 
                    embeddings_model
                    )
            },
            {
                "input_column": "certifications",
                "output_column": "certifications_emb",
                "expr": self._embeddings_column_expr(
                    "certifications", 
                    embeddings_model
                    )
            },
            {
                "input_column": "ruolo_lavorativo_standard_mpg",
                "output_column": "ruolo_lavorativo_standard_mpg_emb",
                "expr": self._embeddings_column_expr(
                    "ruolo_lavorativo_standard_mpg", 
                    embeddings_model
                    )
            },
        ]


        # ------------------------------------------------------------------
        # Compute all features on first run; otherwise update changed inputs.
        # ------------------------------------------------------------------
        
        output_columns = []

        for config in feature_configs:

            input_column = config["input_column"]
            output_column = config["output_column"]
            flag_colum = f"proc_{input_column}"
            expr = config["expr"]

            if not table_exists:
                df = (
                    df
                    .with_column(flag_colum, lit(True))
                    .with_column(output_column, expr)
                )
            else:
                df = self._update_feature(
                    df,
                    df_old,
                    "joborder_id_bh",
                    input_column,
                    output_column,
                    expr,
                )
            output_columns = output_columns + [flag_colum, output_column]

        #output_columns = [f"proc_{c['input_column']}" for c in feature_configs] + [c["output_column"] for c in feature_configs]
        output_columns = list(dict.fromkeys(output_columns))

        df = df.select(
            *original_columns,
            *output_columns,
        )

        return df
    

    def _update_feature(
            self, 
            df_new, 
            df_old, 
            id_column, 
            input_column, 
            input_column_feat, 
            expr
            ):
        """
        Incrementally updates a feature column by recomputing values only for
        rows whose source value is new or has changed.

        The function compares the current input dataframe with the previously
        processed output table using the specified identifier column.

        A row is marked for reprocessing when:
        - the row does not exist in the previous output table
        - the source column value has changed
        - a previously null value becomes non-null
        - a previously non-null value becomes null

        Rows whose source value has not changed reuse the previously computed
        feature value, avoiding unnecessary calls to Snowflake Cortex functions
        and reducing credit consumption.

        Change detection uses ``eqNullSafe`` so that null values are compared
        correctly.

        Processing logic:

        A      A_old    row_exists_old    Reprocess    Description
        ----------------------------------------------------------
        x      x        True              False        Existing row, unchanged value
        x      y        True              True         Existing row, changed value
        x      NULL     NULL              True         New row
        x      NULL     True              True         Existing row, NULL -> value
        NULL   x        True              True         Existing row, value -> NULL
        NULL   NULL     NULL              True         New row with NULL value
        NULL   NULL     True              False        Existing row, NULL unchanged

        Row_exists_old = True if the row exists in the previous output table.

        Case 1: Existing row, unchanged value
            A      A_old    row_exists_old    Reprocess
            -------------------------------------------
            x      x        True              False

            Example:
                New value:        "Python, SQL"
                Previous value:   "Python, SQL"

            Result:
                Existing feature is reused.

        Case 2: Existing row, changed value
            A      A_old    row_exists_old    Reprocess
            -------------------------------------------
            x      y        True              True

            Example:
                New value:        "Python, SQL, Snowflake"
                Previous value:   "Python, SQL"

            Result:
                Feature is recomputed.

        Case 3: New row
            A      A_old    row_exists_old    Reprocess
            -------------------------------------------
            x      NULL     NULL              True

            Example:
                Candidate did not previously exist.

            Result:
                Feature is computed for the first time.

        Case 4: Existing row, previously null
            A      A_old    row_exists_old    Reprocess
            -------------------------------------------
            x      NULL     True              True

            Example:
                New value:        "Python"
                Previous value:   NULL

            Result:
                Feature is recomputed.

        Case 5: Existing row, value becomes null
            A      A_old    row_exists_old    Reprocess
            -------------------------------------------
            NULL   x        True              True

            Example:
                New value:        NULL
                Previous value:   "Python"

            Result:
                Feature is recomputed.

        Case 6: New row with null value
            A      A_old    row_exists_old    Reprocess
            -------------------------------------------
            NULL   NULL     NULL              True

            Example:
                New candidate with no source data.

            Result:
                Feature is processed according to the feature expression.

        Case 7: Existing row, null remains null
            A      A_old    row_exists_old    Reprocess
            -------------------------------------------
            NULL   NULL     True              False

            Example:
                New value:        NULL
                Previous value:   NULL

            Result:
                Existing feature is reused.

        Implementation overview:
        1. Identify rows that are new or whose source value changed.
        2. Execute the feature expression only on those rows.
        3. Join newly computed features back to the full dataset.
        4. Reuse previously computed values for unchanged rows.
        5. Create a ``proc_<input_column>`` flag indicating whether the feature
           was recomputed during the current execution.

        Args:
            df_new (DataFrame): Current source dataframe.
            df_old (DataFrame): Previously processed output dataframe.
            id_column (str): Unique row identifier used to match records between the current and previous datasets.
            input_column (str): Source column used to generate the feature.
            input_column_feat (str): Feature column to create or update.
            expr: Snowpark expression used to compute the feature value.

        Returns:
            DataFrame: Dataframe containing all original rows with the updated feature column and processing flag.
        """

        # ------------------------------------------------------------------
        # Identify rows whose input value is new or has changed.
        # ------------------------------------------------------------------
        
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


        # ------------------------------------------------------------------
        # Compute the feature only for rows selected for processing.
        # ------------------------------------------------------------------

        df_to_process = (
            df_to_process
            .with_column(input_column_feat, expr)
            .drop(input_column)
        )



        # ------------------------------------------------------------------
        # Combine newly computed and existing feature values.
        #
        # New rows:
        #   - value comes from df_to_process
        #
        # Existing modified rows:
        #   - value comes from df_to_process
        #
        # Existing unchanged rows:
        #   - value comes from df_old
        # ------------------------------------------------------------------

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
  
