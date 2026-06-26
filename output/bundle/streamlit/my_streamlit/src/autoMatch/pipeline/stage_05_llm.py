from src.onBench.config.configuration import ConfigurationManager
from src.autoMatch.components.llm import LLM
from src.onBench import logger

from src.onBench.utils.snowflake_utils import get_snowpark_session


STAGE_NAME = "LLM stage"

class LLMTrainingPipeline:
    def __init__(self):
        pass

    def main(self, session):
        config = ConfigurationManager()
        llm_config = config.get_llm_config()
        llm = LLM(config=llm_config)
        response = llm.create_prompt_vacancy(session)
        print(response)




if __name__ == '__main__':
    try:
        logger.info(f">>>>>> stage {STAGE_NAME} started <<<<<<")
        session = get_snowpark_session()
        obj = LLMTrainingPipeline(session)
        obj.main()
        logger.info(f">>>>>> stage {STAGE_NAME} completed <<<<<<\n\nx==========x")
    except Exception as e:
        logger.exception(e)
        raise e