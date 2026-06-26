"""Functions to connect Python and Snowflake"""
import os
import json
from typing import cast

import snowflake.snowpark as sp

from dotenv import load_dotenv
from src.onBench.constants import SNOWFLAKE_SESSION_PARAMETERS_PATH

def get_snowpark_session() -> sp.Session:

    """
    Returns a snowpark session
    """
    load_dotenv()

    cfg_params = {
        "user": os.getenv("SNOWFLAKE_USER"),
        "account": os.getenv("SNOWFLAKE_ACCOUNT"),
        "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
        "authenticator": os.getenv("SNOWFLAKE_AUTHENTICATOR"),
        "role": os.getenv("SNOWFLAKE_ROLE"),
    }
    if cfg_params["authenticator"] == "snowflake":
        cfg_params["password"] = os.getenv("SNOWFLAKE_PASSWORD")

    session = sp.Session.builder.configs(cfg_params).create()

    session.use_database(os.getenv("SNOWFLAKE_DATABASE"))
    session.use_schema(os.getenv("SNOWFLAKE_SCHEMA"))

    with open(
        SNOWFLAKE_SESSION_PARAMETERS_PATH, encoding="utf-8"
    ) as snowflake_session_parameters_file:
        snowflake_session_parameters = json.load(snowflake_session_parameters_file)

    for param_key, param_value in snowflake_session_parameters.items():
        session.sql(f"ALTER SESSION SET {param_key} = '{param_value}'")

    return cast(sp.Session, session)


def generate_create_stage_command(
    stage: str,
    *,
    is_permanent: bool = False,
    overwrite = False
) -> str:
    """
    Generate a Snowflake command to create a stage.
    Examples:
    CREATE STAGE MPG_IT_AUTOMATCHCH_STAGE_SP
    CREATE OR REPLACE STAGE MPG_IT_AUTOMATCHCH_STAGE_SP
    CREATE OR REPLACE STAGE IF NOT EXISTS MPG_IT_AUTOMATCHCH_STAGE_SP
    CREATE OR REPLACE TEMPORARY STAGE IF NOT EXISTS MPG_IT_AUTOMATCHCH_STAGE_SP

    """
    return f"""
        CREATE {'OR REPLACE ' if overwrite else ''} {'TEMPORARY ' if not is_permanent else ''} STAGE {'' if overwrite else 'IF NOT EXISTS '} {stage.strip('@')};
    """