from src.onBench import logger
from src.onBench.entity.config_entity import SearchEngineConfig
from snowflake.core import Root

import os
from dotenv import load_dotenv

        
class SearchEngine:
    def __init__(self, config: SearchEngineConfig):
        self.config = config

   
    def create_semantic_search_engine(self, session):
        """
        Creates Cortex Search Service on input table

        Function returns nothing
        """

        database = self.config.database
        schema = self.config.schema
        input_table = self.config.input_table
        search_service = self.config.search_service
        search_columns = self.config.search_columns
        attributes_columns = self.config.attributes_columns
        columns = self.config.columns

        load_dotenv()
        warehouse = os.getenv("SNOWFLAKE_WAREHOUSE")

        description_expr = " || '\\n' || ".join([
            f"'{' '.join(col.strip().split('_')).capitalize()}: ' || {col.strip()}"
            for col in search_columns
        ]) 
               
        query = f"""
            CREATE OR REPLACE CORTEX SEARCH SERVICE {search_service}
            ON description
            ATTRIBUTES {", ".join(attributes_columns)}
            WAREHOUSE = {warehouse}
            TARGET_LAG = '1 hour'
            AS (
                SELECT {", ".join(columns)},
                ({description_expr}) as description
                FROM {database}.{schema}.{input_table}
                );
            """
        print(query)

        session.sql(query).collect()
        logger.info(f"Cortex search index {search_service} successfully defined on table {input_table}")

        
    def get_column_specification(self, session):
        """
        Get Search Service columns:
            - search columns
            - attribute columns
            - all columns involved
        Function returns nothin
        """

        database = self.config.database
        schema = self.config.schema
        search_service = self.config.search_service

        search_service_result = session.sql(f"DESC CORTEX SEARCH SERVICE {database}.{schema}.{search_service}").collect()[0]
        attribute_columns = search_service_result.attribute_columns.split(",")
        search_columns = search_service_result.search_column
        columns = search_service_result.columns.split(",")

        logger.info(f"Column specifications: \nSearch columns: {search_columns} \nAttribute columns: {attribute_columns} \nAll columns: {columns}")

        return search_service, attribute_columns, search_columns, columns
    
    def query_cortex_search_service(self, session, query, filter={}, limit=5):
        """
        Queries the cortex search service in the session state and returns a list of results

        Returns query results
        """

        database = self.config.database
        schema = self.config.schema
        search_service = self.config.search_service
        columns = self.config.columns

        #_, _, _, columns = self.get_column_specification(session)

        cortex_search_service = (
            Root(session)
            .databases[database]
            .schemas[schema]
            .cortex_search_services[search_service]
        )
        context_documents = cortex_search_service.search(
            query,
            columns=columns,
            filter=filter,
            limit=limit)
        
        return context_documents.results
    
    def create_filter(self, max_age=None, province=None):
        """
        Create a filter object to include only candidates with:
        - age <= max_age (if provided)
        - AND province_ext == province (if provided)
        """
        filter_clauses = []

        if max_age is not None:
            age_clause = { "@lte": { "age": max_age } }
            filter_clauses.append(age_clause)

        if province:
            province_clause = { "@eq": { "province_ext": province } }
            filter_clauses.append(province_clause)

        # Return combined filter if any clauses exist
        if filter_clauses:
            return { "@and": filter_clauses }
        else:
            return {}


