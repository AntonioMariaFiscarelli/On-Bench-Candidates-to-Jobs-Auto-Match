from src.autoMatch import logger
from src.autoMatch.utils.snowflake_utils import get_snowpark_session

from src.autoMatch.pipeline.stage_01_data_ingestion import DataIngestionTrainingPipeline
from src.autoMatch.pipeline.stage_02_data_transformation import DataTransformationTrainingPipeline
from src.autoMatch.pipeline.stage_03_automatch import AutomatchTrainingPipeline


from src.autoMatch.deployment.deployment import deploy_pipeline

import logging
logging.getLogger("snowflake").setLevel(logging.WARNING)
logging.getLogger("snowflake.connector").setLevel(logging.WARNING)
logging.getLogger("snowflake.snowpark").setLevel(logging.WARNING)


session = get_snowpark_session()


'''
try:
   deploy_pipeline(session)
except Exception as e:
   logger.exception(e)
   raise e
'''

'''
STAGE_NAME = "Data Ingestion stage"
try:
   logger.info(f">>>>>> stage {STAGE_NAME} started <<<<<<") 
   data_ingestion = DataIngestionTrainingPipeline()
   data_ingestion.main(session)
   logger.info(f">>>>>> stage {STAGE_NAME} completed <<<<<<\n\nx==========x")
except Exception as e:
    logger.exception(e)
    raise e
'''


STAGE_NAME = "Data Transformation stage"
try:
   logger.info(f">>>>>> stage {STAGE_NAME} started <<<<<<")
   obj = DataTransformationTrainingPipeline()
   obj.main(session)
   logger.info(f">>>>>> stage {STAGE_NAME} completed <<<<<<\n\nx==========x")
except Exception as e:
   logger.exception(e)
   raise e



'''
STAGE_NAME = "Automatch stage"
try:
   logger.info(f">>>>>> stage {STAGE_NAME} started <<<<<<")
   obj = AutomatchTrainingPipeline()
   obj.main(session)
   logger.info(f">>>>>> stage {STAGE_NAME} completed <<<<<<\n\nx==========x")
except Exception as e:
   logger.exception(e)
   raise e
'''