from src.onBench.config.configuration import ConfigurationManager
from src.onBench.components.automatch import Automatch
from src.onBench import logger

from src.onBench.utils.snowflake_utils import get_snowpark_session


STAGE_NAME = "Automatch stage"

class AutomatchTrainingPipeline:
    def __init__(self):
        pass

    def main(self, session):
        config = ConfigurationManager()
        automatch_config = config.get_automatch_config()
        automatch = Automatch(config=automatch_config)




