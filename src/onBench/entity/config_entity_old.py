from dataclasses import dataclass
from pathlib import Path
from typing import List, Dict

#####################################
@dataclass(frozen=True)
class CandidateIngestionConfig:
    input_table: str
    input_table_pb: str
    output_table: str
    output_table_cleaned: str
    days_prior: int
    days_nomission: int

@dataclass(frozen=True)
class JoborderIngestionConfig:
    input_table: str
    input_table_pb: str
    output_table: str
    output_table_cleaned: str
    days_prior: int

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
    desired_locations: dict
    parttime_preferenza_perc: list[int]
    max_tokens: int
    llm_model: str

#####################################


#####################################
@dataclass(frozen=True)
class CandidateTransformationConfig:
    input_table: str
    input_table_cleaned: str
    output_table_cleaned: str
    output_table: str
    days_prior: int
    candidate_log_table:str


@dataclass(frozen=True)
class VacancyTransformationConfig:
    input_table: str
    input_table_cleaned: str
    output_table_cleaned: str
    output_table: str
    days_prior: int
    vacancy_log_table:str

@dataclass(frozen=True)
class DataTransformationConfig:
    root_dir: str
    database: str
    schema: str

    candidate: CandidateTransformationConfig
    vacancy: VacancyTransformationConfig

    llm_model: str

    input_table_italian_cities: str

    education_levels: Dict
    regions: List[str]
    turno_preferenza: Dict
    parttime_preferenza_perc: List[int]
#####################################

@dataclass(frozen=True)
class DeploymentConfig:
    root_dir: str
    database: str
    schema: str
    warehouse: str
    config_dir: str
    stage_sp: str
    data_ingestion_sp: str
    data_transformation_sp: str

    
    data_ingestion_task: str
    data_transformation_task: str
    candidate_log_table: str
    joborder_log_table: str
    runtime_version: str
    data_ingestion_imports: list[str]
    data_ingestion_packages: list[str]
    data_transformation_imports: list[str]
    data_transformation_packages: list[str]

