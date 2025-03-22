import argparse
import re
from dataclasses import dataclass
from typing import Optional

import pyarrow.parquet as pq
import s3fs


@dataclass
class S3Config:
    endpoint: Optional[str] = None
    key: Optional[str] = None
    secret: Optional[str] = None

    def create_filesystem(self) -> s3fs.S3FileSystem:
        """Create an S3FileSystem instance using this configuration."""
        return s3fs.S3FileSystem(
            key=self.key,
            secret=self.secret,
            client_kwargs={"endpoint_url": self.endpoint} if self.endpoint else {},
        )


def camel_to_snake(name: str) -> str:
    """
    Convert a CamelCase string to snake_case.

    Args:
        name (str): The CamelCase string to be converted.

    Returns:
        str: The converted snake_case string.
    """
    name = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", name)
    name = re.sub("([a-z0-9])([A-Z])", r"\1_\2", name).lower()
    return name


def get_column_names(
    object_path: str,
    s3_config: Optional[S3Config] = None,
) -> list[str]:
    """
    Extracts column names from a given parquet file

    Args:
        object_path: path of a given parquet file
        s3_config: optional S3 configuration for S3 paths
    """
    if object_path.startswith("s3://"):
        if s3_config is None:
            s3_config = S3Config()
        obj = _handle_s3_file_system(object_path, s3_config)
    else:
        obj = pq.ParquetFile(object_path)

    return obj.schema.names


def generate_sql_column_aliases(column_names: list[str]) -> list[str]:
    """
    Generate SQL column aliases for a list of column names.

    If a column name is in camelCase, it will be converted to snake_case and an alias
    will be created in the format "originalName as snake_case_name".

    If the column name is already in snake_case, it will be returned as is.

    Args:
        column_names: A list of column names.
    Returns:
        A list of SQL selectors with alises where required
    """

    return [
        f"{col} as {camel_to_snake(col)}" if camel_to_snake(col) != col else col
        for col in column_names
    ]


def get_stage_model_string(as_statements: list[str]) -> str:
    return f"select {', '.join(as_statements)} from"


def _handle_s3_file_system(location_path: str, s3_config: S3Config):
    fs = s3_config.create_filesystem()

    return pq.ParquetFile(location_path, filesystem=fs)


def main() -> None:
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "source",
        help="Source parquet file location",
    )
    parser.add_argument(
        "output",
        help="Output file location",
    )
    parser.add_argument("--host", "-H", type=str, help="override s3 host")
    parser.add_argument("--key", "-k", type=str, help="s3 key for auth")
    parser.add_argument("--secret", "-s", type=str, help="s3 secret for auth")

    args = parser.parse_args()

    source_file: str = args.source
    output_file_path: str = args.output

    s3_config = S3Config(
        endpoint=args.host,
        key=args.key,
        secret=args.secret,
    )

    column_names = get_column_names(source_file, s3_config)
    aliases = generate_sql_column_aliases(column_names)
    stage_model_str = get_stage_model_string(aliases)

    with open(output_file_path, "w") as out_file:
        out_file.write(stage_model_str)
