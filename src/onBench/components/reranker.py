from src.onBench import logger
from snowflake.snowpark.functions import col, when, lit, coalesce, concat, trim, regexp_replace, call_function

from src.onBench.entity.config_entity import RerankerConfig


class Matching:
    """
    This class is used to manage the reranker implementation.
    It provides methos for:
    """

    def __init__(
            self, 
            config: RerankerConfig,
            ):
        self.config = config


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

  
