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
from typing import Any, Callable, Dict, Generator, Iterable, List, Mapping, Optional, Set

from airbyte_cdk.models import FailureType
from airbyte_cdk.sources.file_based.config.csv_format import CsvFormat, CsvHeaderAutogenerated, CsvHeaderUserProvided, InferenceType
from airbyte_cdk.sources.file_based.config.file_based_stream_config import FileBasedStreamConfig
from airbyte_cdk.sources.file_based.exceptions import FileBasedSourceError, RecordParseError
from airbyte_cdk.sources.file_based.file_based_stream_reader import AbstractFileBasedStreamReader, FileReadMode
from airbyte_cdk.sources.file_based.file_types.file_type_parser import FileTypeParser
from airbyte_cdk.sources.file_based.remote_file import RemoteFile
from airbyte_cdk.sources.file_based.schema_helpers import TYPE_PYTHON_MAPPING, SchemaType
from airbyte_cdk.utils.traced_exception import AirbyteTracedException

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

        # Formats are configured individually per-stream so a unique dialect should be registered for each stream.
        # We don't unregister the dialect because we are lazily parsing each csv file to generate records
        # This will potentially be a problem if we ever process multiple streams concurrently
        dialect_name = config.name + DIALECT_NAME
        csv.register_dialect(
            dialect_name,
            delimiter=config_format.delimiter,
            quotechar=config_format.quote_char,
            escapechar=config_format.escape_char,
            doublequote=config_format.double_quote,
            quoting=csv.QUOTE_MINIMAL,
        )
        with stream_reader.open_file(file, file_read_mode, config_format.encoding, logger) as fp:
            headers = self._get_headers(fp, config_format, dialect_name)

            rows_to_skip = (
                config_format.skip_rows_before_header
                + (1 if config_format.header_definition.has_header_row() else 0)
                + config_format.skip_rows_after_header
            )
            self._skip_rows(fp, rows_to_skip)

            reader = csv.DictReader(fp, dialect=dialect_name, fieldnames=headers)  # type: ignore
            try:
                for row in reader:
                    # The row was not properly parsed if any of the values are None. This will most likely occur if there are more columns
                    # than headers or more headers dans columns
                    if None in row or None in row.values():
                        raise RecordParseError(FileBasedSourceError.ERROR_PARSING_RECORD)
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

        self._skip_rows(fp, config_format.skip_rows_before_header)
        if isinstance(config_format.header_definition, CsvHeaderAutogenerated):
            headers = self._auto_generate_headers(fp, dialect_name)
        else:
            # Then read the header
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

    def __init__(self, csv_reader: Optional[_CsvReader] = None):
        self._csv_reader = csv_reader if csv_reader else _CsvReader()

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
            lambda: _JsonTypeInferrer(config_format.true_values, config_format.false_values, config_format.null_values)
            if config_format.inference_type != InferenceType.NONE
            else _DisabledTypeInferrer()
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
        config_format = _extract_format(config)
        if discovered_schema:
            property_types = {col: prop["type"] for col, prop in discovered_schema["properties"].items()}  # type: ignore # discovered_schema["properties"] is known to be a mapping
            deduped_property_types = CsvParser._pre_propcess_property_types(property_types)
        else:
            deduped_property_types = {}
        cast_fn = CsvParser._get_cast_function(deduped_property_types, config_format, logger, config.schemaless)
        data_generator = self._csv_reader.read_data(config, file, stream_reader, logger, self.file_read_mode)
        for row in data_generator:
            yield CsvParser._to_nullable(cast_fn(row), deduped_property_types, config_format.null_values, config_format.strings_can_be_null)
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
        nullable = row | {
            k: None if CsvParser._value_is_none(v, deduped_property_types.get(k), null_values, strings_can_be_null) else v
            for k, v in row.items()
        }
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
                prop_type_distinct = set(prop_type)
                prop_type_distinct.remove("null")
                if len(prop_type_distinct) != 1:
                    raise ValueError(f"Could not get non nullable type from {prop_type}")
                output[prop] = next(iter(prop_type_distinct))
            else:
                output[prop] = prop_type
        return output

    @staticmethod
    def _cast_types(
        row: Dict[str, str], deduped_property_types: Dict[str, str], config_format: CsvFormat, logger: logging.Logger
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
                        cast_value = json.loads(value)
                    except json.JSONDecodeError:
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
        types_by_value = {value: self._infer_type(value) for value in self._values}
        types_excluding_null_values = [types for types in types_by_value.values() if self._NULL_TYPE not in types]
        if not types_excluding_null_values:
            # this is highly unusual but we will consider the column as a string
            return self._STRING_TYPE

        types = set.intersection(*types_excluding_null_values)
        if self._BOOLEAN_TYPE in types:
            return self._BOOLEAN_TYPE
        elif self._INTEGER_TYPE in types:
            return self._INTEGER_TYPE
        elif self._NUMBER_TYPE in types:
            return self._NUMBER_TYPE
        return self._STRING_TYPE

    def _infer_type(self, value: str) -> Set[str]:
        inferred_types = set()

        if value in self._null_values:
            inferred_types.add(self._NULL_TYPE)
        if self._is_boolean(value):
            inferred_types.add(self._BOOLEAN_TYPE)
        if self._is_integer(value):
            inferred_types.add(self._INTEGER_TYPE)
            inferred_types.add(self._NUMBER_TYPE)
        elif self._is_number(value):
            inferred_types.add(self._NUMBER_TYPE)

        inferred_types.add(self._STRING_TYPE)
        return inferred_types

    def _is_boolean(self, value: str) -> bool:
        try:
            _value_to_bool(value, self._boolean_trues, self._boolean_falses)
            return True
        except ValueError:
            return False

    @staticmethod
    def _is_integer(value: str) -> bool:
        try:
            _value_to_python_type(value, int)
            return True
        except ValueError:
            return False

    @staticmethod
    def _is_number(value: str) -> bool:
        try:
            _value_to_python_type(value, float)
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
    if isinstance(parsed_value, list):
        return parsed_value
    raise ValueError(f"Value {parsed_value} is not a valid list value")


def _value_to_python_type(value: str, python_type: type) -> Any:
    return python_type(value)


def _format_warning(key: str, value: str, expected_type: Optional[Any]) -> str:
    return f"{key}: value={value},expected_type={expected_type}"


def _no_cast(row: Mapping[str, str]) -> Mapping[str, str]:
    return row


def _extract_format(config: FileBasedStreamConfig) -> CsvFormat:
    config_format = config.format or CsvFormat()
    if not isinstance(config_format, CsvFormat):
        raise ValueError(f"Invalid format config: {config_format}")
    return config_format
