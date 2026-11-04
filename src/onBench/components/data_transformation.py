from src.onBench import logger
from snowflake.snowpark.functions import col, trim, lower, when, lit, trim, to_date, to_varchar, coalesce, concat
from snowflake.snowpark.functions import udf, array_append, array_to_string, call_function, array_construct
from datetime import date, datetime

from src.onBench.utils.common import validate_string, validate_json

from src.onBench.entity.config_entity import DataTransformationConfig, GlobalConfig


class DataTransformation:
    """
    This class is used to manage the enrichment of the candidate and vacancy tables.
    It provides methos for:
        - implementing a mechanism to process only candidate/vacancy whose information have changed
        - performing Named Entity Recognition in order to extract information from candidates' CVs and vacancies' descriptions
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


    def embedding_column_expr(self, column_name):
        """Builds the expression to compute embeddings for a given column
        Args:
            column_name (string): name of the column to compute embeddings for
        Returns:
            expr (Snowpark expression): expression to compute embeddings for the given column
        """

        expr = call_function(
            "SNOWFLAKE.CORTEX.AI_EMBED_1024",
            lit("multilingual-e5-large"),
            when(
                col(column_name).is_null() | (col(column_name) == ""),
                lit("Non disponibile")
            ).otherwise(col(column_name))
        )

        return expr
    
    def compute_embeddings_candidate(self, session):
        """Computes all the embeddings for the candidate table, for all the columns that require embeddings
        Args:
            session (Snowpark session): Snowpark session
        Returns:
            df (Snowpark DataFrame): DataFrame with the computed embeddings
        """

        database = self.config.database
        schema = self.config.schema
        input_table = self.config.candidate.input_table
        output_table = self.config.candidate.output_table    

        df_new = session.table(f"{database}.{schema}.{input_table}")

        # Check if the output table exists. 
        table_exists = session.sql(f"""
            SHOW TABLES LIKE '{output_table}' IN SCHEMA {database}.{schema}
        """).count() > 0

        # If it doesn't, compute embeddings for all the rows. 
        if(not table_exists):
            df_new = (
                df_new
                .with_column("was_processed", lit(True))
            )

            df = (
                df_new
                .with_column(
                    'job_first_emb', 
                    self.embedding_column_expr('job_first')
                )
                .with_column(
                    'job_intermediate_emb', 
                    self.embedding_column_expr('job_intermediate')
                )
                .with_column(
                    'job_last_emb', 
                    self.embedding_column_expr('job_last')
                )
            )

            return df

        # If it does, compute embeddings only for the rows that have changed.
        df_old = session.table(f"{database}.{schema}.{output_table}")

        df = self.update_embeddings(df_new, df_old, "candidate_id_bh", 'job_first')    
        df = self.update_embeddings(df, df_old, "candidate_id_bh", 'job_intermediate')    
        df = self.update_embeddings(df, df_old, "candidate_id_bh", 'job_last')    

        return df

    def compute_embeddings_joborder(self, session):
        """Computes all the embeddings for the joborder table, for all the columns that require embeddings
        Args:
            session (Snowpark session): Snowpark session
        Returns:
            df (Snowpark DataFrame): DataFrame with the computed embeddings
        """

        database = self.config.database
        schema = self.config.schema
        input_table = self.config.joborder.input_table
        output_table = self.config.joborder.output_table    

        df_new = session.table(f"{database}.{schema}.{input_table}")

        # Check if the output table exists. 
        table_exists = session.sql(f"""
            SHOW TABLES LIKE '{output_table}' IN SCHEMA {database}.{schema}
        """).count() > 0

        # If it doesn't, compute embeddings for all the rows. 
        if(not table_exists):
            df_new = (
                df_new
                .with_column("was_processed", lit(True))
            )

            df = df_new.with_column(
                'ruolo_lavorativo_standard_mpg_emb', 
                self.embedding_column_expr('ruolo_lavorativo_standard_mpg')
                )

            return df

        # If it does, compute embeddings only for the rows that have changed.
        df_old = session.table(f"{database}.{schema}.{output_table}")

        df = self.update_embeddings(df_new, 
                                    df_old,
                                    'joborder_id_bh', 
                                    'ruolo_lavorativo_standard_mpg'
                                    )    

        return df


    def update_embeddings(self, df_new, df_old, id_column, column_name):
        """ Updates the embeddings for a given column, by comparing the new and old dataframes and computing embeddings only for the rows that have changed
        Args:
            df_new (Snowpark DataFrame): New dataframe
            df_old (Snowpark DataFrame): Old dataframe
            id_column (str): Name of the column used to identify rows
            column_name (str): Name of the column for which to update embeddings
        Returns:
            df (Snowpark DataFrame): DataFrame with updated embeddings
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

        Rather then computing embeddings for specific canditates/joborders by using a simple WHEN clause,
        it is necessary to compute a separate dataframe with the candidates/joborders to be processed, and join it later.
        The reason is that, the WHEN clause does not garantee that the SNOWFLAKE.CORTEX function will be called only for the rows that satisfy the true condition,
        which can result in a function call for all candidates/joborders (regardless of whether they have changed or not)
        which will result in a significant increase in Snowflake credit costs.
        """

        df_to_process = (
            df_new.select(
                id_column,
                column_name
            )
            .join(
                df_old.select(
                    id_column,
                    column_name
                ).with_column("row_exists_old", lit(True)),
                id_column,
                "left",
                rsuffix="_old"
            )
            .filter(
                col("row_exists_old").is_null()
                |
                ~col(column_name).eqNullSafe(col(column_name + "_old"))
            )
            .drop(column_name + "_old")
            .drop("row_exists_old")
        )

        # Compute embeddings for the rows that need to be processed
        df_to_process = (
            df_to_process
            .with_column(column_name + "_emb", self.embedding_column_expr(column_name))
            .drop(column_name)
        )

        """
        Join the new dataframe with the processed rows and the old dataframe
        From the new dataframe, we get the candidate/joborder information (always present)
        From the processed rows, we get the new embeddings for the changed rows (present for new and old changed candidates/joborders)
        From the old dataframe, we get the old embeddings (present for old candidates/joborders)
        If a candidate/joborder is new, it will be in the processed dataframe, but not in the old dataframe, so we will get the embeddings from the processed rows
        If a candidate/joborder is old changed, it will be in both the processed dataframe and old dataframe, so we will get the embeddings from the processed rows
        If a candidate/joborder is old and unchanged, it will be in the old dataframe, but not in the processed rows, so we will get the embeddings from the old dataframe
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
                    column_name + "_emb",
                ),
                id_column,
                "left",
                rsuffix="_old"
            )
            .with_column(
                "proc_" + column_name, 
                when(col(column_name + "_emb").is_not_null(), lit(True))
                .otherwise(lit(False))
                )
            .with_column(
                column_name + "_emb",
                coalesce(
                    col(column_name + "_emb"),
                    col(column_name + "_emb_old")
                )
            )
            .drop(col(column_name + "_emb_old"))
        )

        return df_result


    def write_table(self, df, table_name = 'output_table'):
        """Writes dataframe in Snowflake
        Args:
            df (Snowpark Dataframe): dataframe
            table_name (string): name of the Snowflake table
        Returns:
        """

        df.write.save_as_table(table_name, mode="overwrite")

        logger.info(f"Table {table_name} successfully written")

  
