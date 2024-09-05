#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import csv
import json
import logging
from abc import ABC, abstractmethod
from collections import defaultdict
from functools import partial
from io import IOBase
from typing import Any, Callable, Dict, Generator, Iterable, List, Mapping, Optional, Set, Tuple
from uuid import uuid4

from airbyte_cdk.models import FailureType
from airbyte_cdk.sources.file_based.config.csv_format import CsvFormat, CsvHeaderAutogenerated, CsvHeaderUserProvided, InferenceType
from airbyte_cdk.sources.file_based.config.file_based_stream_config import FileBasedStreamConfig
from airbyte_cdk.sources.file_based.exceptions import FileBasedSourceError, RecordParseError
from airbyte_cdk.sources.file_based.file_based_stream_reader import AbstractFileBasedStreamReader, FileReadMode
from airbyte_cdk.sources.file_based.file_types.file_type_parser import FileTypeParser
from airbyte_cdk.sources.file_based.remote_file import RemoteFile
from airbyte_cdk.sources.file_based.schema_helpers import TYPE_PYTHON_MAPPING, SchemaType
from airbyte_cdk.utils.traced_exception import AirbyteTracedException
from orjson import orjson

DIALECT_NAME = "_config_dialect"


class _CsvReader:
    def read_data(
        self,
        config: FileBasedStreamConfig,
        file: RemoteFile,
        stream_reader: AbstractFileBasedStreamReader,
        logger: logging.Logger,
        file_read_mode: FileReadMode,
    ) -> Generator[Dict[str, Any], None, None]:
        config_format = _extract_format(config)
        lineno = 0

        # Formats are configured individually per-stream so a unique dialect should be registered for each stream.
        # We don't unregister the dialect because we are lazily parsing each csv file to generate records
        # Give each stream's dialect a unique name; otherwise, when we are doing a concurrent sync we can end up
        # with a race condition where a thread attempts to use a dialect before a separate thread has finished
        # registering it.
        dialect_name = f"{config.name}_{str(uuid4())}_{DIALECT_NAME}"
        csv.register_dialect(
            dialect_name,
            delimiter=config_format.delimiter,
            quotechar=config_format.quote_char,
            escapechar=config_format.escape_char,
            doublequote=config_format.double_quote,
            quoting=csv.QUOTE_MINIMAL,
        )
        with stream_reader.open_file(file, file_read_mode, config_format.encoding, logger) as fp:
            try:
                headers = self._get_headers(fp, config_format, dialect_name)
            except UnicodeError:
                raise AirbyteTracedException(
                    message=f"{FileBasedSourceError.ENCODING_ERROR.value} Expected encoding: {config_format.encoding}",
                )

            rows_to_skip = (
                config_format.skip_rows_before_header
                + (1 if config_format.header_definition.has_header_row() else 0)
                + config_format.skip_rows_after_header
            )
            self._skip_rows(fp, rows_to_skip)
            lineno += rows_to_skip

            reader = csv.DictReader(fp, dialect=dialect_name, fieldnames=headers)  # type: ignore
            try:
                for row in reader:
                    lineno += 1

                    # The row was not properly parsed if any of the values are None. This will most likely occur if there are more columns
                    # than headers or more headers dans columns
                    if None in row:
                        if config_format.ignore_errors_on_fields_mismatch:
                            logger.error(f"Skipping record in line {lineno} of file {file.uri}; invalid CSV row with missing column.")
                        else:
                            raise RecordParseError(
                                FileBasedSourceError.ERROR_PARSING_RECORD_MISMATCHED_COLUMNS,
                                filename=file.uri,
                                lineno=lineno,
                            )
                    if None in row.values():
                        if config_format.ignore_errors_on_fields_mismatch:
                            logger.error(f"Skipping record in line {lineno} of file {file.uri}; invalid CSV row with extra column.")
                        else:
                            raise RecordParseError(
                                FileBasedSourceError.ERROR_PARSING_RECORD_MISMATCHED_ROWS, filename=file.uri, lineno=lineno
                            )
                    yield row
            finally:
                # due to RecordParseError or GeneratorExit
                csv.unregister_dialect(dialect_name)

    def _get_headers(self, fp: IOBase, config_format: CsvFormat, dialect_name: str) -> List[str]:
        """
        Assumes the fp is pointing to the beginning of the files and will reset it as such
        """
        # Note that this method assumes the dialect has already been registered if we're parsing the headers
        if isinstance(config_format.header_definition, CsvHeaderUserProvided):
            return config_format.header_definition.column_names  # type: ignore  # should be CsvHeaderUserProvided given the type

        if isinstance(config_format.header_definition, CsvHeaderAutogenerated):
            self._skip_rows(fp, config_format.skip_rows_before_header + config_format.skip_rows_after_header)
            headers = self._auto_generate_headers(fp, dialect_name)
        else:
            # Then read the header
            self._skip_rows(fp, config_format.skip_rows_before_header)
            reader = csv.reader(fp, dialect=dialect_name)  # type: ignore
            headers = list(next(reader))

        fp.seek(0)
        return headers

    def _auto_generate_headers(self, fp: IOBase, dialect_name: str) -> List[str]:
        """
        Generates field names as [f0, f1, ...] in the same way as pyarrow's csv reader with autogenerate_column_names=True.
        See https://arrow.apache.org/docs/python/generated/pyarrow.csv.ReadOptions.html
        """
        reader = csv.reader(fp, dialect=dialect_name)  # type: ignore
        number_of_columns = len(next(reader))  # type: ignore
        return [f"f{i}" for i in range(number_of_columns)]

    @staticmethod
    def _skip_rows(fp: IOBase, rows_to_skip: int) -> None:
        """
        Skip rows before the header. This has to be done on the file object itself, not the reader
        """
        for _ in range(rows_to_skip):
            fp.readline()


class CsvParser(FileTypeParser):
    _MAX_BYTES_PER_FILE_FOR_SCHEMA_INFERENCE = 1_000_000

    def __init__(self, csv_reader: Optional[_CsvReader] = None, csv_field_max_bytes: int = 2**31):
        # Increase the maximum length of data that can be parsed in a single CSV field. The default is 128k, which is typically sufficient
        # but given the use of Airbyte in loading a large variety of data it is best to allow for a larger maximum field size to avoid
        # skipping data on load. https://stackoverflow.com/questions/15063936/csv-error-field-larger-than-field-limit-131072
        csv.field_size_limit(csv_field_max_bytes)
        self._csv_reader = csv_reader if csv_reader else _CsvReader()

    def check_config(self, config: FileBasedStreamConfig) -> Tuple[bool, Optional[str]]:
        """
        CsvParser does not require config checks, implicit pydantic validation is enough.
        """
        return True, None

    async def infer_schema(
        self,
        config: FileBasedStreamConfig,
        file: RemoteFile,
        stream_reader: AbstractFileBasedStreamReader,
        logger: logging.Logger,
    ) -> SchemaType:
        input_schema = config.get_input_schema()
        if input_schema:
            return input_schema

        # todo: the existing InMemoryFilesSource.open_file() test source doesn't currently require an encoding, but actual
        #  sources will likely require one. Rather than modify the interface now we can wait until the real use case
        config_format = _extract_format(config)
        type_inferrer_by_field: Dict[str, _TypeInferrer] = defaultdict(
            lambda: (
                _JsonTypeInferrer(config_format.true_values, config_format.false_values, config_format.null_values)
                if config_format.inference_type != InferenceType.NONE
                else _DisabledTypeInferrer()
            )
        )
        data_generator = self._csv_reader.read_data(config, file, stream_reader, logger, self.file_read_mode)
        read_bytes = 0
        for row in data_generator:
            for header, value in row.items():
                type_inferrer_by_field[header].add_value(value)
                # This is not accurate as a representation of how many bytes were read because csv does some processing on the actual value
                # before returning. Given we would like to be more accurate, we could wrap the IO file using a decorator
                read_bytes += len(value)
            read_bytes += len(row) - 1  # for separators
            if read_bytes >= self._MAX_BYTES_PER_FILE_FOR_SCHEMA_INFERENCE:
                break

        if not type_inferrer_by_field:
            raise AirbyteTracedException(
                message=f"Could not infer schema as there are no rows in {file.uri}. If having an empty CSV file is expected, ignore this. "
                f"Else, please contact Airbyte.",
                failure_type=FailureType.config_error,
            )
        schema = {header.strip(): {"type": type_inferred.infer()} for header, type_inferred in type_inferrer_by_field.items()}
        data_generator.close()
        return schema

    def parse_records(
        self,
        config: FileBasedStreamConfig,
        file: RemoteFile,
        stream_reader: AbstractFileBasedStreamReader,
        logger: logging.Logger,
        discovered_schema: Optional[Mapping[str, SchemaType]],
    ) -> Iterable[Dict[str, Any]]:
        line_no = 0
        try:
            config_format = _extract_format(config)
            if discovered_schema:
                property_types = {col: prop["type"] for col, prop in discovered_schema["properties"].items()}  # type: ignore # discovered_schema["properties"] is known to be a mapping
                deduped_property_types = CsvParser._pre_propcess_property_types(property_types)
            else:
                deduped_property_types = {}
            cast_fn = CsvParser._get_cast_function(deduped_property_types, config_format, logger, config.schemaless)
            data_generator = self._csv_reader.read_data(config, file, stream_reader, logger, self.file_read_mode)
            for row in data_generator:
                line_no += 1
                yield CsvParser._to_nullable(
                    cast_fn(row), deduped_property_types, config_format.null_values, config_format.strings_can_be_null
                )
        except RecordParseError as parse_err:
            raise RecordParseError(FileBasedSourceError.ERROR_PARSING_RECORD, filename=file.uri, lineno=line_no) from parse_err
        finally:
            data_generator.close()

    @property
    def file_read_mode(self) -> FileReadMode:
        return FileReadMode.READ

    @staticmethod
    def _get_cast_function(
        deduped_property_types: Mapping[str, str], config_format: CsvFormat, logger: logging.Logger, schemaless: bool
    ) -> Callable[[Mapping[str, str]], Mapping[str, str]]:
        # Only cast values if the schema is provided
        if deduped_property_types and not schemaless:
            return partial(CsvParser._cast_types, deduped_property_types=deduped_property_types, config_format=config_format, logger=logger)
        else:
            # If no schema is provided, yield the rows as they are
            return _no_cast

    @staticmethod
    def _to_nullable(
        row: Mapping[str, str], deduped_property_types: Mapping[str, str], null_values: Set[str], strings_can_be_null: bool
    ) -> Dict[str, Optional[str]]:
        # Localize variables to avoid repeated lookups
        _value_is_none = CsvParser._value_is_none
        get = deduped_property_types.get
        nullable = {}
        for k, v in row.items():
            nullable[k] = None if _value_is_none(v, get(k), null_values, strings_can_be_null) else v
        return nullable

    @staticmethod
    def _value_is_none(value: Any, deduped_property_type: Optional[str], null_values: Set[str], strings_can_be_null: bool) -> bool:
        return value in null_values and (strings_can_be_null or deduped_property_type != "string")

    @staticmethod
    def _pre_propcess_property_types(property_types: Dict[str, Any]) -> Mapping[str, str]:
        """
        Transform the property types to be non-nullable and remove duplicate types if any.
        Sample input:
        {
        "col1": ["string", "null"],
        "col2": ["string", "string", "null"],
        "col3": "integer"
        }

        Sample output:
        {
        "col1": "string",
        "col2": "string",
        "col3": "integer",
        }
        """
        output = {}
        for prop, prop_type in property_types.items():
            if isinstance(prop_type, list):
                prop_type_distinct = {ptype for ptype in prop_type if ptype != "null"}
                if len(prop_type_distinct) != 1:
                    raise ValueError(f"Could not get non nullable type from {prop_type}")
                output[prop] = prop_type_distinct.pop()
            else:
                output[prop] = prop_type
        return output

    @staticmethod
    def _cast_types(
        row: Dict[str, str], deduped_property_types: Mapping[str, str], config_format: CsvFormat, logger: logging.Logger
    ) -> Dict[str, Any]:
        """
        Casts the values in the input 'row' dictionary according to the types defined in the JSON schema.

        Array and object types are only handled if they can be deserialized as JSON.

        If any errors are encountered, the value will be emitted as a string.
        """
        warnings = []
        result = {}

        for key, value in row.items():
            prop_type = deduped_property_types.get(key)
            cast_value: Any = value

            if prop_type in TYPE_PYTHON_MAPPING and prop_type is not None:
                _, python_type = TYPE_PYTHON_MAPPING[prop_type]

                if python_type is None:
                    if value == "":
                        cast_value = None
                    else:
                        warnings.append(_format_warning(key, value, prop_type))

                elif python_type == bool:
                    try:
                        cast_value = _value_to_bool(value, config_format.true_values, config_format.false_values)
                    except ValueError:
                        warnings.append(_format_warning(key, value, prop_type))

                elif python_type == dict:
                    try:
                        # we don't re-use _value_to_object here because we type the column as object as long as there is only one object
                        cast_value = orjson.loads(value)
                    except orjson.JSONDecodeError:
                        warnings.append(_format_warning(key, value, prop_type))

                elif python_type == list:
                    try:
                        cast_value = _value_to_list(value)
                    except (ValueError, json.JSONDecodeError):
                        warnings.append(_format_warning(key, value, prop_type))

                elif python_type:
                    try:
                        cast_value = _value_to_python_type(value, python_type)
                    except ValueError:
                        warnings.append(_format_warning(key, value, prop_type))

                result[key] = cast_value

        if warnings:
            logger.warning(
                f"{FileBasedSourceError.ERROR_CASTING_VALUE.value}: {','.join([w for w in warnings])}",
            )
        return result


class _TypeInferrer(ABC):
    @abstractmethod
    def add_value(self, value: Any) -> None:
        pass

    @abstractmethod
    def infer(self) -> str:
        pass


class _DisabledTypeInferrer(_TypeInferrer):
    def add_value(self, value: Any) -> None:
        pass

    def infer(self) -> str:
        return "string"


class _JsonTypeInferrer(_TypeInferrer):
    _NULL_TYPE = "null"
    _BOOLEAN_TYPE = "boolean"
    _INTEGER_TYPE = "integer"
    _NUMBER_TYPE = "number"
    _STRING_TYPE = "string"

    def __init__(self, boolean_trues: Set[str], boolean_falses: Set[str], null_values: Set[str]) -> None:
        self._boolean_trues = boolean_trues
        self._boolean_falses = boolean_falses
        self._null_values = null_values
        self._values: Set[str] = set()

    def add_value(self, value: Any) -> None:
        self._values.add(value)

    def infer(self) -> str:
        types: Set[str] = set()
        for value in self._values:
            inferred_types = self._infer_type(value)
            if not inferred_types or self._NULL_TYPE in inferred_types:
                continue

            if not types:
                types = inferred_types
            else:
                types &= inferred_types

        if not types:
            return self._STRING_TYPE

        if self._BOOLEAN_TYPE in types:
            return self._BOOLEAN_TYPE
        if self._INTEGER_TYPE in types:
            return self._INTEGER_TYPE
        if self._NUMBER_TYPE in types:
            return self._NUMBER_TYPE

        return self._STRING_TYPE

    def _infer_type(self, value: str) -> Set[str]:
        if value in self._null_values:
            return {self._NULL_TYPE}
        if self._is_boolean(value):
            return {self._BOOLEAN_TYPE, self._STRING_TYPE}
        if self._is_integer(value):
            return {self._INTEGER_TYPE, self._NUMBER_TYPE, self._STRING_TYPE}
        if self._is_number(value):
            return {self._NUMBER_TYPE, self._STRING_TYPE}

        return {self._STRING_TYPE}

    def _is_boolean(self, value: str) -> bool:
        return value in self._boolean_trues or value in self._boolean_falses

    @staticmethod
    def _is_integer(value: str) -> bool:
        try:
            int(value)
            return True
        except ValueError:
            return False

    @staticmethod
    def _is_number(value: str) -> bool:
        try:
            float(value)
            return True
        except ValueError:
            return False


def _value_to_bool(value: str, true_values: Set[str], false_values: Set[str]) -> bool:
    if value in true_values:
        return True
    if value in false_values:
        return False
    raise ValueError(f"Value {value} is not a valid boolean value")


def _value_to_list(value: str) -> List[Any]:
    parsed_value = json.loads(value)
    if type(parsed_value) == list:
        return parsed_value
    raise ValueError("Value is not a valid list value")


def _value_to_python_type(value: str, python_type: type) -> Any:
    return python_type(value)


def _format_warning(key: str, value: str, expected_type: Optional[Any]) -> str:
    return f"{key}: value={value},expected_type={expected_type}"


def _no_cast(row: Mapping[str, str]) -> Mapping[str, str]:
    return row


def _extract_format(config: FileBasedStreamConfig) -> CsvFormat:
    config_format = config.format
    if not isinstance(config_format, CsvFormat):
        raise ValueError(f"Invalid format config: {config_format}")
    return config_format
