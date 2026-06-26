from src.onBench.config.configuration import ConfigurationManager
from src.onBench.components.search_engine import SearchEngine
from src.onBench import logger

from src.onBench.utils.snowflake_utils import get_snowpark_session


STAGE_NAME = "Search Engine stage"

class SearchEngineTrainingPipeline:
    def __init__(self):
        pass

    def main(self, session):
        config = ConfigurationManager()
        search_engine_config = config.get_search_engine_config()
        search_engine = SearchEngine(config=search_engine_config)
        #search_engine.create_semantic_search_engine(session)
        #search_engine.get_column_specification(session)
        results = search_engine.query_cortex_search_service(session, 
                                                            query='Data Scientist con esperienza in Kafka', 
                                                            filter=search_engine.create_filter(40), 
                                                            limit=5)
        for result in results:
            print(result)



if __name__ == '__main__':
    try:
        logger.info(f">>>>>> stage {STAGE_NAME} started <<<<<<")
        session = get_snowpark_session()
        obj = SearchEngineTrainingPipeline(session)
        obj.main()
        logger.info(f">>>>>> stage {STAGE_NAME} completed <<<<<<\n\nx==========x")
    except Exception as e:
        logger.exception(e)
        raise e