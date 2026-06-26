from src.onBench.constants import *
from src.onBench.utils.common import read_yaml, read_yaml_sp, create_directories
from src.onBench import logger

from src.onBench.entity.config_entity import DataIngestionConfig, CandidateIngestionConfig, VacancyIngestionConfig, ItalianCitiesIngestionConfig
from src.onBench.entity.config_entity import DataTransformationConfig, CandidateTransformationConfig, VacancyTransformationConfig, DeploymentConfig
from src.onBench.entity.config_entity import AutomatchConfig, AppConfig

class ConfigurationManager:
    def __init__(
        self,
        session = None,
        config_filepath = CONFIG_FILE_PATH,
        params_filepath = PARAMS_FILE_PATH,
        schema_filepath = SCHEMA_FILE_PATH,
        stage = STAGE_NAME):

        if(not session):
            self.config = read_yaml(config_filepath)
            self.params = read_yaml(params_filepath)
            self.schema = read_yaml(schema_filepath)
        else:
            self.config = read_yaml_sp(session, stage, CONFIG_FILE_PATH) #"config_test.yaml")
            self.params = read_yaml_sp(session, stage, PARAMS_FILE_PATH) 
            self.schema = read_yaml_sp(session, stage, SCHEMA_FILE_PATH) 
            

        #create_directories([self.config.artifacts_root])


    def get_data_ingestion_config(self) -> DataIngestionConfig:
        config = self.config.data_ingestion
        schema = self.schema.data_ingestion

        data_ingestion_config = DataIngestionConfig(
            root_dir=config.root_dir,
            database=config.database,
            schema=config.schema,

            candidate=CandidateIngestionConfig(
                input_table=config.candidate_tables.input_table,
                output_table=config.candidate_tables.output_table,
                columns = schema.candidate.columns,
                days_prior = schema.candidate.days_prior
            ),

            vacancy=VacancyIngestionConfig(
                input_table=config.vacancy_tables.input_table,
                output_table=config.vacancy_tables.output_table,
                columns = schema.vacancy.columns,
                days_prior = schema.vacancy.days_prior

            ),
            italian_cities=ItalianCitiesIngestionConfig(
                input_file=config.italian_cities.input_file,
                output_table=config.italian_cities.output_table,
                string_columns = schema.italian_cities.string_columns,
                numeric_columns = schema.italian_cities.numeric_columns,
            ),

            desired_locations = schema.desired_locations,
            parttime_preferenza_perc = schema.parttime_preferenza_perc,
            max_tokens=schema.max_tokens
        )

        return data_ingestion_config


    def get_data_transformation_config(self) -> DataTransformationConfig:
        config = self.config.data_transformation
        schema = self.schema.data_transformation
        params = self.params.data_transformation

        #create_directories([config.root_dir])

        data_transformation_config = DataTransformationConfig(
            root_dir=config.root_dir,
            database=config.database,
            schema=config.schema,

            candidate=CandidateTransformationConfig(
                input_table=config.candidate_tables.input_table,
                input_table_cleaned=config.candidate_tables.input_table_cleaned,
                output_table_cleaned=config.candidate_tables.output_table_cleaned,
                output_table=config.candidate_tables.output_table,
                days_prior=schema.candidate.days_prior,
                ner_columns=schema.candidate.ner_columns,
                ner_features=schema.candidate.ner_features,
                emb_columns=schema.candidate.emb_columns,
                geo_columns=schema.candidate.geo_columns,
                candidate_log_table=self.config.candidate_log_table,
            ),

            vacancy=VacancyTransformationConfig(
                input_table=config.vacancy_tables.input_table,
                input_table_cleaned=config.vacancy_tables.input_table_cleaned,
                output_table_cleaned=config.vacancy_tables.output_table_cleaned,
                output_table=config.vacancy_tables.output_table,
                days_prior=schema.vacancy.days_prior,
                ner_columns=schema.vacancy.ner_columns,
                ner_features=schema.vacancy.ner_features,
                emb_columns=schema.vacancy.emb_columns,
                vacancy_log_table=self.config.vacancy_log_table
            ),

            llm_model=params.llm_model,

            input_table_italian_cities=config.input_table_italian_cities,

            education_levels=schema.education_levels,
            regions=schema.regions,
            turno_preferenza=schema.turno_preferenza,
            parttime_preferenza_perc=schema.parttime_preferenza_perc
        )

        return data_transformation_config

    def get_deployment_config(self) -> DeploymentConfig:
        config = self.config.deployment
        params = self.params.deployment

        #create_directories([config.root_dir])

        deployment_config = DeploymentConfig(
            root_dir=config.root_dir,
            database=config.database,
            schema=config.schema,
            warehouse=config.warehouse,
            config_dir=config.config_dir,
            stage_sp=config.stage_sp,
            data_ingestion_sp=config.data_ingestion_sp,
            data_transformation_sp=config.data_transformation_sp,
            data_ingestion_task=config.data_ingestion_task,
            data_transformation_task=config.data_transformation_task,
            candidate_log_table=self.config.candidate_log_table,
            vacancy_log_table=self.config.vacancy_log_table,
            runtime_version=params.runtime_version,
            data_ingestion_imports=params.data_ingestion_imports,
            data_ingestion_packages=params.data_ingestion_packages,
            data_transformation_imports=params.data_transformation_imports,
            data_transformation_packages=params.data_transformation_packages
        )

        return deployment_config

    def get_automatch_config(self) -> AutomatchConfig:
        config = self.config.automatch
        schema = self.schema.automatch
        params = self.params.automatch

        #create_directories([config.root_dir])

        automatch_config = AutomatchConfig(
            root_dir=config.root_dir,
            database=config.database,
            schema=config.schema,
            input_table=config.input_table,
            vacancy_search_table=config.vacancy_search_table,
            llm_model=params.llm_model,
            columns=schema.columns,
            #role_mappings = schema.role_mappings,
            languages = schema.languages,
            education_levels = schema.education_levels,
            desired_locations = schema.desired_locations,
            turno_preferenza = schema.turno_preferenza,
            parttime_preferenza_perc = schema.parttime_preferenza_perc
        )

        return automatch_config


    def get_app_config(self) -> AppConfig:
        config = self.config.app
        schema = self.schema.app

        #create_directories([config.root_dir])

        app_config = AppConfig(
            root_dir=config.root_dir,
            database=config.database,
            schema=config.schema,
            vacancy_table=config.vacancy_table,
            vacancy_search_table=config.vacancy_search_table,
            default_inputs=schema.default_inputs
        )

        return app_config