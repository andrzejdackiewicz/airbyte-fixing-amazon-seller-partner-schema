#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

from airbyte_cdk.sources.file_based.config.file_based_stream_config import CsvFormat, ParquetFormat
from jsonschema import validate
import pytest
from jsonschema import ValidationError
from pydantic import BaseModel


@pytest.mark.parametrize(
    "file_format, expected_error",
    [
        pytest.param(ParquetFormat, None, id="test_parquet_format_is_a_valid_parquet_file_type"),
        pytest.param(CsvFormat, ValidationError, id="test_csv_format_is_not_a_valid_parquet_file_type"),
    ]
)
def test_parquet_file_type_is_not_a_valid_csv_file_type(file_format: BaseModel, expected_error) -> None:
    format_config = {
        "parquet": {
            "filetype": "parquet",
            "decimal_as_float": True
        }
    }

    if expected_error:
        with pytest.raises(expected_error):
            validate(instance=format_config["parquet"], schema=file_format.schema())
    else:
        validate(instance=format_config["parquet"], schema=file_format.schema())
