from src.onBench.constants import *
from src.onBench.utils.common import read_yaml, create_directories
from src.onBench import logger

from src.onBench.entity.config_entity import DataIngestionConfig, DataValidationConfig, DataTransformationConfig, SearchEngineConfig, LLMConfig, AppConfig

class ConfigurationManager:
    def __init__(
        self,
        config_filepath = CONFIG_FILE_PATH,
        params_filepath = PARAMS_FILE_PATH,
        schema_filepath = SCHEMA_FILE_PATH):

        self.config = read_yaml(config_filepath)
        self.params = read_yaml(params_filepath)
        self.schema = read_yaml(schema_filepath)

        create_directories([self.config.artifacts_root])

    
    def get_data_ingestion_config(self) -> DataIngestionConfig:
        config = self.config.data_ingestion
        schema = self.schema.data_ingestion

        data_ingestion_config = DataIngestionConfig(
            root_dir=config.root_dir,
            database=config.database,
            schema=config.schema,
            input_table=config.input_table,
            input_table_vacancy = config.input_table_vacancy,
            output_table = config.output_table,
            output_table_vacancy = config.output_table_vacancy,
            italian_cities_file = config.italian_cities_file,
            output_table_italian_cities = config.output_table_italian_cities,
            columns = schema.columns,
            columns_vacancy = schema.columns_vacancy,
            days_prior = schema.date_range.days_prior,
            desired_locations = schema.desired_locations,
            parttime_preferenza_perc = schema.parttime_preferenza_perc,
            italian_cities_string_columns = schema.cities_file_columns.string_columns,
            italian_cities_numeric_columns = schema.cities_file_columns.numeric_columns,
        )

        return data_ingestion_config
    

    def get_data_validation_config(self) -> DataValidationConfig:
        config = self.config.data_validation
        schema = self.schema.data_validation

        create_directories([config.root_dir])

        data_validation_config = DataValidationConfig(
            root_dir=config.root_dir,
            database=config.database,
            schema=config.schema,
            input_table=config.input_table,
            table_schema=schema.table_schema,
            status_file = config.STATUS_FILE
        )

        return data_validation_config
    

    def get_data_transformation_config(self) -> DataTransformationConfig:
        config = self.config.data_transformation
        schema = self.schema.data_transformation

        create_directories([config.root_dir])

        data_transformation_config = DataTransformationConfig(
            root_dir=config.root_dir,
            database=config.database,
            schema=config.schema,
            input_table=config.input_table,
            input_table_cleaned=config.input_table_cleaned,
            input_table_vacancy = config.input_table_vacancy,
            input_table_italian_cities = config.input_table_italian_cities,
            output_table = config.output_table,
            output_table_vacancy = config.output_table_vacancy,
            education_levels = schema.education_levels,
            regions = schema.regions,
            turno_preferenza = schema.turno_preferenza,
            parttime_preferenza_perc = schema.parttime_preferenza_perc,
            days_prior = schema.days_prior
        )

        return data_transformation_config
    

    def get_search_engine_config(self) -> SearchEngineConfig:
        config = self.config.search_engine
        schema = self.schema.search_engine

        create_directories([config.root_dir])

        search_engine_config = SearchEngineConfig(
            root_dir=config.root_dir,
            database=config.database,
            schema=config.schema,
            input_table=config.input_table,
            search_service=config.search_service,
            search_columns=schema.search_columns,
            attributes_columns = schema.attributes_columns,
            columns = schema.columns
        )

        return search_engine_config
    

    def get_llm_config(self) -> LLMConfig:
        config = self.config.llm
        schema = self.schema.llm
        params = self.params.llm

        create_directories([config.root_dir])

        llm_config = LLMConfig(
            root_dir=config.root_dir,
            database=config.database,
            schema=config.schema,
            input_table=config.input_table,
            llm_name=params.llm_name,
            columns=schema.columns,
            role_mappings = schema.role_mappings,
            languages = schema.languages,
            education_levels = schema.education_levels,
            desired_locations = schema.desired_locations,
            turno_preferenza = schema.turno_preferenza,
            parttime_preferenza_perc = schema.parttime_preferenza_perc
        )

        return llm_config


    def get_app_config(self) -> AppConfig:
        config = self.config.app
        schema = self.schema.app

        create_directories([config.root_dir])

        app_config = AppConfig(
            root_dir=config.root_dir,
            database=config.database,
            schema=config.schema,
            default_inputs=schema.default_inputs
        )

        return app_config