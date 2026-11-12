from src.onBench.constants import CONFIG_FILE_PATH, STAGE_NAME
from src.onBench.utils.common import read_yaml, read_yaml_sp
from src.onBench.entity.config_entity import (
    GlobalConfig,
    DomainConfig,
    EmbeddingsConfig,
    LLMConfig,
    CandidateIngestionConfig,
    JoborderIngestionConfig,
    ItalianCitiesIngestionConfig,
    DataIngestionConfig,
    CandidateTransformationConfig,
    JoborderTransformationConfig,
    ItalianCitiesTransformationConfig,
    RerankerConfig,
    CandidateRerankerConfig,
    JoborderRerankerConfig,
    DataTransformationConfig,
    PipelineDeploymentConfig,
    DeploymentConfig,
)


class ConfigurationManager:
    def __init__(
        self,
        session=None,
        config_filepath=CONFIG_FILE_PATH,
        stage=STAGE_NAME,
    ):
        if session is None:
            self.config = read_yaml(config_filepath)
        else:
            self.config = read_yaml_sp(session, stage, CONFIG_FILE_PATH)

    def get_global_config(self) -> GlobalConfig:
        config = self.config

        return GlobalConfig(
            artifacts_root=config.artifacts_root,
            candidate_log_table=config.candidate_log_table,
            joborder_log_table=config.joborder_log_table,
        )

    def get_domain_config(self) -> DomainConfig:
        config = self.config.domain

        return DomainConfig(
            languages=config.languages,
            regions=config.regions,
            education_levels=config.education_levels,
            parttime_preferenza_perc=config.parttime_preferenza_perc,
            desired_locations=config.desired_locations,
            turno_preferenza=config.turno_preferenza,
        )

    def get_data_ingestion_config(self) -> DataIngestionConfig:
        config = self.config.data_ingestion

        return DataIngestionConfig(
            root_dir=config.root_dir,
            database=config.database,
            input_schema=config.input_schema,
            output_schema=config.output_schema,
            candidate=CandidateIngestionConfig(
                days_prior=config.candidate.days_prior,
                days_nomission=config.candidate.days_nomission,
                input_table=config.candidate.input_table,
                input_pb_table=config.candidate.input_pb_table,
                output_table=config.candidate.output_table,
                output_cleaned_table=config.candidate.output_cleaned_table,
            ),
            joborder=JoborderIngestionConfig(
                days_prior=config.joborder.days_prior,
                input_table=config.joborder.input_table,
                input_pb_table=config.joborder.input_pb_table,
                output_table=config.joborder.output_table,
                output_cleaned_table=config.joborder.output_cleaned_table,
            ),
            italian_cities=ItalianCitiesIngestionConfig(
                input_file=config.italian_cities.input_file,
                output_table=config.italian_cities.output_table,
            ),
            llm=LLMConfig(
                model=config.llm.model,
                max_tokens=config.llm.max_tokens,
            ),
        )

    def get_data_transformation_config(self) -> DataTransformationConfig:
        config = self.config.data_transformation

        return DataTransformationConfig(
            root_dir=config.root_dir,
            database=config.database,
            schema=config.schema,
            candidate=CandidateTransformationConfig(
                input_table=config.candidate.input_table,
                output_table=config.candidate.output_table,
            ),
            joborder=JoborderTransformationConfig(
                input_table=config.joborder.input_table,
                output_table=config.joborder.output_table,
            ),
            italian_cities=ItalianCitiesTransformationConfig(
                input_table=config.italian_cities.input_table,
            ),
            embeddings=EmbeddingsConfig(
                model=config.embeddings.model,
            ),
            llm=LLMConfig(
                model=config.llm.model,
                max_tokens=config.llm.max_tokens,
            ),
        )

    def get_reranker_config(self) -> RerankerConfig:
        config = self.config.reranker

        return RerankerConfig(
            root_dir=config.root_dir,
            database=config.database,
            schema=config.schema,
            candidate=CandidateRerankerConfig(
                input_table=config.candidate.input_table,
                output_table=config.candidate.output_table,
            ),
            joborder=JoborderRerankerConfig(
                input_table=config.joborder.input_table,
                output_table=config.joborder.output_table,
            ),
            embeddings=EmbeddingsConfig(
                model=config.embeddings.model,
            ),
            llm=LLMConfig(
                model=config.llm.model,
                max_tokens=config.llm.max_tokens,
            ),
        )

    def get_deployment_config(self) -> DeploymentConfig:
        config = self.config.deployment

        return DeploymentConfig(
            root_dir=config.root_dir,
            database=config.database,
            schema=config.schema,
            warehouse=config.warehouse,
            config_dir=config.config_dir,
            stage=config.stage,
            runtime_version=config.runtime_version,
            data_ingestion=PipelineDeploymentConfig(
                stored_procedure=config.data_ingestion.stored_procedure,
                task=config.data_ingestion.task,
                imports=list(config.data_ingestion.imports),
                packages=list(config.data_ingestion.packages),
            ),
            data_transformation=PipelineDeploymentConfig(
                stored_procedure=config.data_transformation.stored_procedure,
                task=config.data_transformation.task,
                imports=list(config.data_transformation.imports),
                packages=list(config.data_transformation.packages),
            ),
        )