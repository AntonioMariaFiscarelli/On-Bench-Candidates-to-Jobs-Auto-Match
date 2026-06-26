import os
import yaml
from src.onBench import logger
import json
#from ensure import ensure_annotations
from src.onBench.utils.box.config_box import ConfigBox
from src.onBench.utils.box.exceptions import BoxValueError
from pathlib import Path
from typing import Any

from snowflake.snowpark import functions as F

from snowflake.snowpark.functions import col, trim, lower, when, lit, trim, udf, parse_json
from snowflake.snowpark.types import StringType, BooleanType

import math

        
#@ensure_annotations
def read_yaml(path_to_yaml: Path) -> ConfigBox:
    """reads yaml file and returns

    Args:
        path_to_yaml (str): path like input

    Raises:
        ValueError: if yaml file is empty
        e: empty file

    Returns:
        ConfigBox: ConfigBox type
    """
    try:
        with open(path_to_yaml) as yaml_file:
            content = yaml.safe_load(yaml_file)
            logger.info(f"yaml file: {path_to_yaml} loaded successfully")
            return ConfigBox(content)
    except BoxValueError:
        raise ValueError("yaml file is empty")
    except Exception as e:
        raise e

    
def read_yaml_sp(session, stage: str, path_to_yaml: Path)-> ConfigBox:

    try:
        cfg = yaml.safe_load(session.file.get_stream(f"@{stage}/{path_to_yaml.name}"))
        """
        df = session.create_dataframe(
            [(path_to_yaml.name, yaml.dump(cfg))],
            schema=["file_name", "yaml_text"]
        )

        df.write.mode("append").save_as_table("CONFIG_RAW")
        """
    except BoxValueError:
        raise ValueError("YAML file is empty")
    except Exception as e:
        raise e

    return ConfigBox(cfg)





#@ensure_annotations
def create_directories(path_to_directories: list, verbose=True):
    """create list of directories

    Args:
        path_to_directories (list): list of path of directories
        ignore_log (bool, optional): ignore if multiple dirs is to be created. Defaults to False.
    """
    for path in path_to_directories:
        os.makedirs(path, exist_ok=True)
        if verbose:
            logger.info(f"created directory at: {path}")


#@ensure_annotations
def save_json(path: Path, data: dict):
    """save json data

    Args:
        path (Path): path to json file
        data (dict): data to be saved in json file
    """
    with open(path, "w") as f:
        json.dump(data, f, indent=4)

    logger.info(f"json file saved at: {path}")




#@ensure_annotations
def load_json(path: Path) -> ConfigBox:
    """load json files data

    Args:
        path (Path): path to json file

    Returns:
        ConfigBox: data as class attributes instead of dict
    """
    with open(path) as f:
        content = json.load(f)

    logger.info(f"json file loaded succesfully from: {path}")
    return ConfigBox(content)


#@ensure_annotations
def save_bin(data: Any, path: Path):
    """save binary file

    Args:
        data (Any): data to be saved as binary
        path (Path): path to binary file
    """
    joblib.dump(value=data, filename=path)
    logger.info(f"binary file saved at: {path}")


#@ensure_annotations
def load_bin(path: Path) -> Any:
    """load binary data

    Args:
        path (Path): path to binary file

    Returns:
        Any: object stored in the file
    """
    data = joblib.load(path)
    logger.info(f"binary file loaded from: {path}")
    return data



#@ensure_annotations
def get_size(path: Path) -> str:
    """get size in KB

    Args:
        path (Path): path of the file

    Returns:
        str: size in KB
    """
    size_in_kb = round(os.path.getsize(path)/1024)
    return f"~ {size_in_kb} KB"

#@ensure_annotations
def validate_string(df, column_name):
    """Validates a string column of a dataframe. Sets it to None if null, empty, contains keyqords such as "null", "none", "nan".

    Args:
        df (DataFrame): dataframe
        column_name (string): column to validate

    Returns:
        df: dataframe
    """
    df = df.with_column(
        column_name,
        when(
            (col(column_name).is_not_null()) &
            (trim(col(column_name)) != "") &
            (~lower(trim(col(column_name))).isin(["null", "none", "nan"])),
            col(column_name)
            ).otherwise(lit(None))
        )
    return df

def validate_json(df, json_column):
    """Validates a column of a dataframe containing a json in string format. Ensures correctness of the string and safely converts the string to a json object

    Args:
        df (DataFrame): dataframe
        json_column (string): column to validate and convert to json

    Returns:
        df: dataframe
    """
    def build_clean_parsing_udf():
        def clean(x: str) -> str:
            if x is None:
                return ''
            x = x.lower().lstrip()
            if x.startswith("```json"):
                x = x[8:].lstrip()
            x = x.replace('\n', ' ').replace('\t', ' ').replace('\\', '').strip()
            x = ' '.join(x.split())
            if x.endswith("```"):
                x = x[:-3].rstrip()
            if x.endswith("'") or x.endswith('"'):
                x = x[:-1].rstrip()
            return x

        return udf(clean, return_type=StringType(), input_types=[StringType()])
    clean_udf = build_clean_parsing_udf()

    df = df.with_column(json_column, clean_udf(df[json_column]))

    def build_is_valid_json_udf():
        import json
        def is_valid(text: str) -> bool:
            if not text:
                return False
            try:
                json.loads(text)
                return True
            except Exception:
                return False

        return udf(is_valid, return_type=BooleanType(), input_types=[StringType()])
    
    is_valid_json_udf = build_is_valid_json_udf()
    df = df.with_column("is_valid_json", is_valid_json_udf(df[json_column]))
    df = df.filter(col("is_valid_json") == True)
    df = df.drop("is_valid_json")

    #df = df.with_column(json_column, parse_json(col(json_column)))
    from snowflake.snowpark.functions import call_function
    df = df.with_column(
        json_column,
        call_function("TRY_PARSE_JSON", col(json_column))
    )
    
    df = df.filter(df[json_column].is_not_null())

    return df

#@ensure_annotations
def is_valid_number(x):
    """Returns true if x is a number, otherwise False

    Args:
        x: variable to validate
    Returns:
        isNumber: boolean value
    """
    isNumber = isinstance(x, (int, float)) and not math.isnan(x)

    return isNumber

def haversine(lat_ref, lon_ref):
    """Given latitude and longitude values, computes haversine distance between these coordinates and all coordinates in the dataframe

    Args:
        lat_ref: latitude
        lon_ref: longitude

    Returns:
        distance_km: distance in kn between the point (lat_ref, lon_ref) and each point (latitude, longitude) of the dataframe
    """
    lat1 = F.radians(F.lit(lat_ref))
    lon1 = F.radians(F.lit(lon_ref))
    lat2 = F.radians(F.col("LATITUDE"))
    lon2 = F.radians(F.col("LONGITUDE"))

    dlat = lat2 - lat1
    dlon = lon2 - lon1

    a = F.sin(dlat / 2) * F.sin(dlat / 2) + F.cos(lat1) * F.cos(lat2) * F.sin(dlon / 2) * F.sin(dlon / 2)
    c = 2 * F.asin(F.sqrt(a))
    distance_km = 6371 * c  # Earth radius in km

    return distance_km