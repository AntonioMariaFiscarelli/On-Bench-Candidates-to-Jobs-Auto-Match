from src.onBench.config.configuration import ConfigurationManager
from src.onBench.components.data_transformation import DataTransformation
from src.onBench import logger

import snowflake.snowpark as sp
from src.onBench.utils.snowflake_utils import get_snowpark_session


STAGE_NAME = "Data Transformnation stage"

class DataTransformationTrainingPipeline:
    def __init__(self, deployment = False):
        self.deployment = deployment
        pass

    def main(self, session):

        if self.deployment:
            config = ConfigurationManager(session)
        else:
            config = ConfigurationManager()

        data_transformation_config = config.get_data_transformation_config()
        data_transformation = DataTransformation(config=data_transformation_config)

        
        df = data_transformation.load_incremental_vacancy(session)
        data_transformation.write_table(df, data_transformation.config.vacancy.input_table_cleaned)
                                        
        df = data_transformation.compute_steps_vacancy(session)
        data_transformation.write_table(df, data_transformation.config.vacancy.output_table)
        
        
        df = data_transformation.load_incremental_candidate(session)
        data_transformation.write_table(df, data_transformation.config.candidate.input_table_cleaned)
                              
        df = data_transformation.compute_steps_candidate(session)
        data_transformation.write_table(df, data_transformation.config.candidate.output_table)
        
        


def run_data_transformation_pipeline(session: sp.Session) -> None:
    STAGE_NAME = "Data Transformation stage"
    try:
        logger.info(f">>>>>> stage {STAGE_NAME} started <<<<<<") 
        data_transformation = DataTransformationTrainingPipeline(deployment=True)
        data_transformation.main(session)
        logger.info(f">>>>>> stage {STAGE_NAME} completed <<<<<<\n\nx==========x")
    except Exception as e:
        logger.exception(e)
        raise e   

