"""Paths of the project."""
import os
from pathlib import Path


ROOT_PATH = Path(__file__).resolve().parent.parent.parent.parent


ENV_PATH = ROOT_PATH / ".env"
SNOWFLAKE_SESSION_PARAMETERS_PATH = ROOT_PATH / "snowflake-session-parameters.json"

#Change this variable to switch between different config files
env = "dev" 
env = "test"

CONFIG_FILE_PATH = ROOT_PATH / "config" / f"config_{env}.yaml"
PARAMS_FILE_PATH = ROOT_PATH / f"config/params.yaml"
SCHEMA_FILE_PATH = ROOT_PATH / f"config/schema.yaml"

STAGE_NAME = "MPG_IT_AUTOMATCH_STAGE_SP"
