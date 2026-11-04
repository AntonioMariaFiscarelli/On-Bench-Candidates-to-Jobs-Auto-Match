from dataclasses import dataclass


# ============================================================
# GLOBAL CONFIGURATION
# ============================================================
@dataclass(frozen=True)
class GlobalConfig:
    artifacts_root: str
    candidate_log_table: str
    joborder_log_table: str


# ============================================================
# DOMAIN CONFIGURATION
# ============================================================
@dataclass(frozen=True)
class DomainConfig:
    languages: list[str]
    regions: list[str]
    education_levels: list[str]
    parttime_preferenza_perc: list[int]
    desired_locations: list[str]
    turno_preferenza: list[str]


# ============================================================
# LLM CONFIGURATION
# ============================================================
@dataclass(frozen=True)
class LLMConfig:
    model: str
    max_tokens: int | None = None


# ============================================================
# DATA INGESTION
# ============================================================
@dataclass(frozen=True)
class CandidateIngestionConfig:
    days_prior: int
    days_nomission: int
    input_table: str
    input_pb_table: str
    output_table: str
    output_cleaned_table: str

@dataclass(frozen=True)
class JoborderIngestionConfig:
    days_prior: int
    input_table: str
    input_pb_table: str
    output_table: str
    output_cleaned_table: str

@dataclass(frozen=True)
class ItalianCitiesIngestionConfig:
    input_file: str
    output_table: str

@dataclass(frozen=True)
class DataIngestionConfig:
    root_dir: str
    database: str
    input_schema: str
    output_schema: str
    candidate: CandidateIngestionConfig
    joborder: JoborderIngestionConfig
    italian_cities: ItalianCitiesIngestionConfig
    llm: LLMConfig


# ============================================================
# DATA TRANSFORMATION
# ============================================================
@dataclass(frozen=True)
class CandidateTransformationConfig:
    input_table: str
    output_table: str

@dataclass(frozen=True)
class JoborderTransformationConfig:
    input_table: str
    output_table: str

@dataclass(frozen=True)
class ItalianCitiesTransformationConfig:
    input_table: str

@dataclass(frozen=True)
class DataTransformationConfig:
    root_dir: str
    database: str
    schema: str
    candidate: CandidateTransformationConfig
    joborder: JoborderTransformationConfig
    italian_cities: ItalianCitiesTransformationConfig
    llm: LLMConfig


# ============================================================
# DEPLOYMENT
# ============================================================
@dataclass(frozen=True)
class PipelineDeploymentConfig:
    stored_procedure: str
    task: str
    imports: list[str]
    packages: list[str]

@dataclass(frozen=True)
class DeploymentConfig:
    root_dir: str
    database: str
    schema: str
    warehouse: str
    config_dir: str
    stage: str
    runtime_version: str
    data_ingestion: PipelineDeploymentConfig
    data_transformation: PipelineDeploymentConfig