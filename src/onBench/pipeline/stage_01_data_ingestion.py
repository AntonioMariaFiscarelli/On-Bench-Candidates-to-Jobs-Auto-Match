from src.onBench.config.configuration import ConfigurationManager
from src.onBench.components.data_ingestion import DataIngestion
from src.onBench import logger

import snowflake.snowpark as sp
from src.onBench.utils.snowflake_utils import get_snowpark_session


STAGE_NAME = "Data Ingestion stage"

class DataIngestionTrainingPipeline:
    def __init__(self, deployment = False):
        self.deployment = deployment
        pass


    def main(self, session):
        
        if self.deployment:
            config = ConfigurationManager(session)
        else:
            config = ConfigurationManager()
            
        #config = ConfigurationManager() 
        
        data_ingestion_config = config.get_data_ingestion_config()
        domain_config = config.get_domain_config()
        data_ingestion = DataIngestion(config=data_ingestion_config, domain_config=domain_config)



        #df = data_ingestion.create_comuni_dataframe(session)
        #data_ingestion.write_table(df, data_ingestion.config.italian_cities.output_table)
        
        
        df = data_ingestion.read_table_joborder(session)
        data_ingestion.write_table(df, data_ingestion.config.joborder.output_table)
        df = data_ingestion.clean_table_joborder(session)
        data_ingestion.write_table(df, data_ingestion.config.joborder.output_cleaned_table)
        df = data_ingestion.enrich_geo_info_from_city_joborder(session)
        data_ingestion.write_table(df, data_ingestion.config.joborder.output_cleaned_table)
        

        df = data_ingestion.read_table_candidate(session)
        data_ingestion.write_table(df, data_ingestion.config.candidate.output_table)
        df = data_ingestion.clean_table_candidate(session)
        data_ingestion.write_table(df, data_ingestion.config.candidate.output_cleaned_table)
        df = data_ingestion.enrich_geo_info_from_city_candidate(session)
        data_ingestion.write_table(df, data_ingestion.config.candidate.output_cleaned_table)
        df = data_ingestion.extract_geo_info_job_site(session)
        data_ingestion.write_table(df, data_ingestion.config.candidate.output_cleaned_table)
        


        


        

        """
        df = data_ingestion.read_cities_file(session)
        data_ingestion.write_table(df, data_ingestion.config.italian_cities.output_table)
        df = data_ingestion.complete_cities_table(session)
        data_ingestion.write_table(df, data_ingestion.config.italian_cities.output_table)
        """
    


def run_data_ingestion_pipeline(session: sp.Session) -> None:
    STAGE_NAME = "Data Ingestion stage"
    try:
        logger.info(f">>>>>> stage {STAGE_NAME} started <<<<<<") 
        data_ingestion = DataIngestionTrainingPipeline(deployment=False) #Set to True for deployment
        data_ingestion.main(session)
        logger.info(f">>>>>> stage {STAGE_NAME} completed <<<<<<\n\nx==========x")
    except Exception as e:
        logger.exception(e)
        raise e
    
