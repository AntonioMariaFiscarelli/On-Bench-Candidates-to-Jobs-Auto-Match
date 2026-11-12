from src.onBench.config.configuration import ConfigurationManager
from src.onBench.components.reranker import Reranker
from src.onBench import logger

import snowflake.snowpark as sp
from src.onBench.utils.snowflake_utils import get_snowpark_session


STAGE_NAME = "Feature store stage"

class RerankerPipeline:
    def __init__(self, deployment = False):
        self.deployment = deployment
        pass

    def main(self, session):

        if self.deployment:
            config = ConfigurationManager(session)
        else:
            config = ConfigurationManager()

        reranker_config = config.get_reranker_config()
        global_config = config.get_global_config()
        reranker = Reranker(
            config=reranker_config,
            global_config=global_config
            )

        #df = reranker.compute_features_candidate(session)
        #reranker.write_table(df, reranker.config.candidate.output_table)
        
        #df = reranker.compute_features_joborder(session)
        #reranker.write_table(df, reranker.config.joborder.output_table)
        


def run_reranker_pipeline(session: sp.Session) -> None:
    STAGE_NAME = "Data Transformation stage"
    try:
        logger.info(f">>>>>> stage {STAGE_NAME} started <<<<<<") 
        data_transformation = RerankerPipeline(deployment=True)
        data_transformation.main(session)
        logger.info(f">>>>>> stage {STAGE_NAME} completed <<<<<<\n\nx==========x")
    except Exception as e:
        logger.exception(e)
        raise e   
