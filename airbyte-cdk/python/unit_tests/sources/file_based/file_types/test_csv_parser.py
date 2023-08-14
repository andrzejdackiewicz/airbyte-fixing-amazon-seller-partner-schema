#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import asyncio
import csv
import io
import logging
import unittest
from datetime import datetime
from typing import Any, Dict, Generator, List, Set
from unittest import TestCase, mock
from unittest.mock import Mock

import pytest
from airbyte_cdk.sources.file_based.config.csv_format import DEFAULT_FALSE_VALUES, DEFAULT_TRUE_VALUES, CsvFormat, InferenceType
from airbyte_cdk.sources.file_based.config.file_based_stream_config import FileBasedStreamConfig
from airbyte_cdk.sources.file_based.exceptions import RecordParseError
from airbyte_cdk.sources.file_based.file_based_stream_reader import AbstractFileBasedStreamReader, FileReadMode
from airbyte_cdk.sources.file_based.file_types.csv_parser import CsvParser, _CsvReader
from airbyte_cdk.sources.file_based.remote_file import RemoteFile

PROPERTY_TYPES = {
    "col1": "null",
    "col2": "boolean",
    "col3": "integer",
    "col4": "number",
    "col5": "string",
    "col6": "object",
    "col7": "array",
    "col8": "array",
    "col9": "array",
    "col10": "string",
}

logger = logging.getLogger()


@pytest.mark.parametrize(
    "row, true_values, false_values, expected_output",
    [
        pytest.param(
            {
                "col1": "",
                "col2": "true",
                "col3": "1",
                "col4": "1.1",
                "col5": "asdf",
                "col6": '{"a": "b"}',
                "col7": "[1, 2]",
                "col8": '["1", "2"]',
                "col9": '[{"a": "b"}, {"a": "c"}]',
                "col10": "asdf",
            },
            DEFAULT_TRUE_VALUES,
            DEFAULT_FALSE_VALUES,
            {
                "col1": None,
                "col2": True,
                "col3": 1,
                "col4": 1.1,
                "col5": "asdf",
                "col6": {"a": "b"},
                "col7": [1, 2],
                "col8": ["1", "2"],
                "col9": [{"a": "b"}, {"a": "c"}],
                "col10": "asdf",
            },
            id="cast-all-cols",
        ),
        pytest.param({"col1": "1"}, DEFAULT_TRUE_VALUES, DEFAULT_FALSE_VALUES, {"col1": "1"}, id="cannot-cast-to-null"),
        pytest.param({"col2": "1"}, DEFAULT_TRUE_VALUES, DEFAULT_FALSE_VALUES, {"col2": True}, id="cast-1-to-bool"),
        pytest.param({"col2": "0"}, DEFAULT_TRUE_VALUES, DEFAULT_FALSE_VALUES, {"col2": False}, id="cast-0-to-bool"),
        pytest.param({"col2": "yes"}, DEFAULT_TRUE_VALUES, DEFAULT_FALSE_VALUES, {"col2": True}, id="cast-yes-to-bool"),
        pytest.param(
            {"col2": "this_is_a_true_value"},
            ["this_is_a_true_value"],
            DEFAULT_FALSE_VALUES,
            {"col2": True},
            id="cast-custom-true-value-to-bool",
        ),
        pytest.param(
            {"col2": "this_is_a_false_value"},
            DEFAULT_TRUE_VALUES,
            ["this_is_a_false_value"],
            {"col2": False},
            id="cast-custom-false-value-to-bool",
        ),
        pytest.param({"col2": "no"}, DEFAULT_TRUE_VALUES, DEFAULT_FALSE_VALUES, {"col2": False}, id="cast-no-to-bool"),
        pytest.param({"col2": "10"}, DEFAULT_TRUE_VALUES, DEFAULT_FALSE_VALUES, {"col2": "10"}, id="cannot-cast-to-bool"),
        pytest.param({"col3": "1.1"}, DEFAULT_TRUE_VALUES, DEFAULT_FALSE_VALUES, {"col3": "1.1"}, id="cannot-cast-to-int"),
        pytest.param({"col4": "asdf"}, DEFAULT_TRUE_VALUES, DEFAULT_FALSE_VALUES, {"col4": "asdf"}, id="cannot-cast-to-float"),
        pytest.param({"col6": "{'a': 'b'}"}, DEFAULT_TRUE_VALUES, DEFAULT_FALSE_VALUES, {"col6": "{'a': 'b'}"}, id="cannot-cast-to-dict"),
        pytest.param(
            {"col7": "['a', 'b']"}, DEFAULT_TRUE_VALUES, DEFAULT_FALSE_VALUES, {"col7": "['a', 'b']"}, id="cannot-cast-to-list-of-ints"
        ),
        pytest.param(
            {"col8": "['a', 'b']"}, DEFAULT_TRUE_VALUES, DEFAULT_FALSE_VALUES, {"col8": "['a', 'b']"}, id="cannot-cast-to-list-of-strings"
        ),
        pytest.param(
            {"col9": "['a', 'b']"}, DEFAULT_TRUE_VALUES, DEFAULT_FALSE_VALUES, {"col9": "['a', 'b']"}, id="cannot-cast-to-list-of-objects"
        ),
        pytest.param({"col11": "x"}, DEFAULT_TRUE_VALUES, DEFAULT_FALSE_VALUES, {"col11": "x"}, id="item-not-in-props-doesn't-error"),
    ],
)
def test_cast_to_python_type(row: Dict[str, str], true_values: Set[str], false_values: Set[str], expected_output: Dict[str, Any]) -> None:
    csv_format = CsvFormat(true_values=true_values, false_values=false_values)
    assert CsvParser._cast_types(row, PROPERTY_TYPES, csv_format, logger) == expected_output


@pytest.mark.parametrize(
    "row, strings_can_be_null, expected_output",
    [
        pytest.param(
            {"id": "1", "name": "bob", "age": 10, "is_cool": False},
            False,
            {"id": "1", "name": "bob", "age": 10, "is_cool": False},
            id="test-no-values-are-null",
        ),
        pytest.param(
            {"id": "1", "name": "bob", "age": "null", "is_cool": "null"},
            False,
            {"id": "1", "name": "bob", "age": None, "is_cool": None},
            id="test-non-string-values-are-none-if-in-null-values",
        ),
        pytest.param(
            {"id": "1", "name": "null", "age": 10, "is_cool": False},
            False,
            {"id": "1", "name": "null", "age": 10, "is_cool": False},
            id="test-string-values-are-not-none-if-strings-cannot-be-null",
        ),
        pytest.param(
            {"id": "1", "name": "null", "age": 10, "is_cool": False},
            True,
            {"id": "1", "name": None, "age": 10, "is_cool": False},
            id="test-string-values-none-if-strings-can-be-null",
        ),
    ],
)
def test_to_nullable(row, strings_can_be_null, expected_output):
    property_types = {"id": "string", "name": "string", "age": "integer", "is_cool": "boolean"}
    null_values = {"null"}
    nulled_row = CsvParser._to_nullable(row, property_types, null_values, strings_can_be_null)
    assert nulled_row == expected_output


_DEFAULT_TRUE_VALUES = {"1", "yes", "yeah", "right"}
_DEFAULT_FALSE_VALUES = {"0", "no", "nop", "wrong"}


class SchemaInferenceTestCase(TestCase):
    _A_NULL_VALUE = "null"
    _HEADER_NAME = "header"

    def setUp(self) -> None:
        self._config_format = CsvFormat()
        self._config_format.true_values = _DEFAULT_TRUE_VALUES
        self._config_format.false_values = _DEFAULT_FALSE_VALUES
        self._config_format.null_values = {self._A_NULL_VALUE}
        self._config_format.inference_type = InferenceType.NONE
        self._config = Mock()
        self._config.get_input_schema.return_value = None
        self._config.format = self._config_format

        self._file = Mock(spec=RemoteFile)
        self._stream_reader = Mock(spec=AbstractFileBasedStreamReader)
        self._logger = Mock(spec=logging.Logger)
        self._csv_reader = Mock(spec=_CsvReader)
        self._parser = CsvParser(self._csv_reader)

    def test_given_user_schema_defined_when_infer_schema_then_return_user_schema(self) -> None:
        self._config.get_input_schema.return_value = {self._HEADER_NAME: {"type": "potato"}}
        self._test_infer_schema(list(_DEFAULT_TRUE_VALUES.union(_DEFAULT_FALSE_VALUES)), "potato")

    def test_given_booleans_only_when_infer_schema_then_type_is_boolean(self) -> None:
        self._config_format.inference_type = InferenceType.PRIMITIVE_TYPES_ONLY
        self._test_infer_schema(list(_DEFAULT_TRUE_VALUES.union(_DEFAULT_FALSE_VALUES)), "boolean")

    def test_given_integers_only_when_infer_schema_then_type_is_integer(self) -> None:
        self._config_format.inference_type = InferenceType.PRIMITIVE_TYPES_ONLY
        self._test_infer_schema(["2", "90329", "5645"], "integer")

    def test_given_integer_overlap_with_bool_value_only_when_infer_schema_then_type_is_integer(self) -> None:
        self._config_format.inference_type = InferenceType.PRIMITIVE_TYPES_ONLY
        self._test_infer_schema(["1", "90329", "5645"], "integer")  # here, "1" is also considered a boolean

    def test_given_numbers_and_integers_when_infer_schema_then_type_is_number(self) -> None:
        self._config_format.inference_type = InferenceType.PRIMITIVE_TYPES_ONLY
        self._test_infer_schema(["2", "90329", "2.312"], "number")

    def test_given_arrays_when_infer_schema_then_type_is_string(self) -> None:
        self._config_format.inference_type = InferenceType.PRIMITIVE_TYPES_ONLY
        self._test_infer_schema(['["first_item", "second_item"]', '["first_item_again", "second_item_again"]'], "string")

    def test_given_objects_when_infer_schema_then_type_is_object(self) -> None:
        self._config_format.inference_type = InferenceType.PRIMITIVE_TYPES_ONLY
        self._test_infer_schema(['{"object1_key": 1}', '{"object2_key": 2}'], "string")

    def test_given_strings_only_when_infer_schema_then_type_is_string(self) -> None:
        self._config_format.inference_type = InferenceType.PRIMITIVE_TYPES_ONLY
        self._test_infer_schema(["a string", "another string"], "string")

    def test_given_a_null_value_when_infer_then_ignore_null(self) -> None:
        self._config_format.inference_type = InferenceType.PRIMITIVE_TYPES_ONLY
        self._test_infer_schema(["2", "90329", "5645", self._A_NULL_VALUE], "integer")

    def test_given_only_null_values_when_infer_then_type_is_string(self) -> None:
        self._config_format.inference_type = InferenceType.PRIMITIVE_TYPES_ONLY
        self._test_infer_schema([self._A_NULL_VALUE, self._A_NULL_VALUE, self._A_NULL_VALUE], "string")

    def test_given_big_file_when_infer_schema_then_stop_early(self) -> None:
        self._config_format.inference_type = InferenceType.PRIMITIVE_TYPES_ONLY
        self._csv_reader.read_data.return_value = ({self._HEADER_NAME: row} for row in ["2." + "2" * 1_000_000] + ["this is a string"])
        inferred_schema = self._infer_schema()
        # since the type is number, we know the string at the end was not considered
        assert inferred_schema == {self._HEADER_NAME: {"type": "number"}}

    def _test_infer_schema(self, rows: List[str], expected_type: str) -> None:
        self._csv_reader.read_data.return_value = ({self._HEADER_NAME: row} for row in rows)
        inferred_schema = self._infer_schema()
        assert inferred_schema == {self._HEADER_NAME: {"type": expected_type}}

    def _infer_schema(self):
        loop = asyncio.new_event_loop()
        task = loop.create_task(self._parser.infer_schema(self._config, self._file, self._stream_reader, self._logger))
        loop.run_until_complete(task)
        return task.result()


class CsvFileBuilder:
    def __init__(self) -> None:
        self._prefixed_rows: List[str] = []
        self._data: List[str] = []

    def with_prefixed_rows(self, rows: List[str]) -> "CsvFileBuilder":
        self._prefixed_rows = rows
        return self

    def with_data(self, data: List[str]) -> "CsvFileBuilder":
        self._data = data
        return self

    def build(self) -> io.StringIO:
        return io.StringIO("\n".join(self._prefixed_rows + self._data))


class CsvReaderTest(unittest.TestCase):
    _CONFIG_NAME = "config_name"

    def setUp(self) -> None:
        self._config_format = CsvFormat()
        self._config = Mock()
        self._config.name = self._CONFIG_NAME
        self._config.format = self._config_format

        self._file = Mock(spec=RemoteFile)
        self._stream_reader = Mock(spec=AbstractFileBasedStreamReader)
        self._logger = Mock(spec=logging.Logger)
        self._csv_reader = _CsvReader()

    def test_given_skip_rows_when_read_data_then_do_not_considered_prefixed_rows(self) -> None:
        self._config_format.skip_rows_before_header = 2
        self._stream_reader.open_file.return_value = (
            CsvFileBuilder()
            .with_prefixed_rows(["first line", "second line"])
            .with_data(
                [
                    "header",
                    "a value",
                    "another value",
                ]
            )
            .build()
        )

        data_generator = self._read_data()

        assert list(data_generator) == [{"header": "a value"}, {"header": "another value"}]

    def test_given_autogenerated_headers_when_read_data_then_generate_headers_with_format_fX(self) -> None:
        self._config_format.autogenerate_column_names = True
        self._stream_reader.open_file.return_value = CsvFileBuilder().with_data(["0,1,2,3,4,5,6"]).build()

        data_generator = self._read_data()

        assert list(data_generator) == [{"f0": "0", "f1": "1", "f2": "2", "f3": "3", "f4": "4", "f5": "5", "f6": "6"}]

    def test_given_skip_rows_after_header_when_read_data_then_do_not_parse_skipped_rows(self) -> None:
        self._config_format.skip_rows_after_header = 1
        self._stream_reader.open_file.return_value = (
            CsvFileBuilder()
            .with_data(
                [
                    "header1,header2",
                    "skipped row: important that the is no comma in this string to test if columns do not match in skipped rows",
                    "a value 1,a value 2",
                    "another value 1,another value 2",
                ]
            )
            .build()
        )

        data_generator = self._read_data()

        assert list(data_generator) == [
            {"header1": "a value 1", "header2": "a value 2"},
            {"header1": "another value 1", "header2": "another value 2"},
        ]

    def test_given_quote_delimiter_when_read_data_then_parse_properly(self) -> None:
        self._config_format.delimiter = "|"
        self._stream_reader.open_file.return_value = (
            CsvFileBuilder()
            .with_data(
                [
                    "header1|header2",
                    "a value 1|a value 2",
                ]
            )
            .build()
        )

        data_generator = self._read_data()

        assert list(data_generator) == [{"header1": "a value 1", "header2": "a value 2"}]

    def test_given_quote_char_when_read_data_then_parse_properly(self) -> None:
        self._config_format.quote_char = "|"
        self._stream_reader.open_file.return_value = (
            CsvFileBuilder()
            .with_data(
                [
                    "header1,header2",
                    "|a,value,1|,|a,value,2|",
                ]
            )
            .build()
        )

        data_generator = self._read_data()

        assert list(data_generator) == [{"header1": "a,value,1", "header2": "a,value,2"}]

    def test_given_escape_char_when_read_data_then_parse_properly(self) -> None:
        self._config_format.escape_char = "|"
        self._stream_reader.open_file.return_value = (
            CsvFileBuilder()
            .with_data(
                [
                    "header1,header2",
                    '"a |"value|", 1",a value 2',
                ]
            )
            .build()
        )

        data_generator = self._read_data()

        assert list(data_generator) == [{"header1": 'a "value", 1', "header2": "a value 2"}]

    def test_given_double_quote_on_when_read_data_then_parse_properly(self) -> None:
        self._config_format.double_quote = True
        self._stream_reader.open_file.return_value = (
            CsvFileBuilder()
            .with_data(
                [
                    "header1,header2",
                    '1,"Text with doublequote: ""This is a text."""',
                ]
            )
            .build()
        )

        data_generator = self._read_data()

        assert list(data_generator) == [{"header1": "1", "header2": 'Text with doublequote: "This is a text."'}]

    def test_given_double_quote_off_when_read_data_then_parse_properly(self) -> None:
        self._config_format.double_quote = False
        self._stream_reader.open_file.return_value = (
            CsvFileBuilder()
            .with_data(
                [
                    "header1,header2",
                    '1,"Text with doublequote: ""This is a text."""',
                ]
            )
            .build()
        )

        data_generator = self._read_data()

        assert list(data_generator) == [{"header1": "1", "header2": 'Text with doublequote: "This is a text."""'}]

    def test_given_generator_closed_when_read_data_then_unregister_dialect(self) -> None:
        self._stream_reader.open_file.return_value = (
            CsvFileBuilder()
            .with_data(
                [
                    "header",
                    "a value",
                    "another value",
                ]
            )
            .build()
        )

        data_generator = self._read_data()
        next(data_generator)
        assert f"{self._CONFIG_NAME}_config_dialect" in csv.list_dialects()
        data_generator.close()
        assert f"{self._CONFIG_NAME}_config_dialect" not in csv.list_dialects()

    def test_given_too_many_values_for_columns_when_read_data_then_raise_exception_and_unregister_dialect(self) -> None:
        data_generator = self._read_data()
        next(data_generator)
        assert f"{self._CONFIG_NAME}_config_dialect" in csv.list_dialects()

        with pytest.raises(RecordParseError):
            next(data_generator)
        assert f"{self._CONFIG_NAME}_config_dialect" not in csv.list_dialects()

    def test_given_too_few_values_for_columns_when_read_data_then_raise_exception_and_unregister_dialect(self) -> None:
        self._stream_reader.open_file.return_value = (
            CsvFileBuilder()
            .with_data(
                [
                    "header1,header2,header3",
                    "value1,value2,value3",
                    "a value",
                ]
            )
            .build()
        )

        data_generator = self._read_data()
        next(data_generator)
        assert f"{self._CONFIG_NAME}_config_dialect" in csv.list_dialects()

        with pytest.raises(RecordParseError):
            next(data_generator)
        assert f"{self._CONFIG_NAME}_config_dialect" not in csv.list_dialects()

    def _read_data(self) -> Generator[Dict[str, str], None, None]:
        data_generator = self._csv_reader.read_data(
            self._config,
            self._file,
            self._stream_reader,
            self._logger,
            FileReadMode.READ,
        )
        return data_generator


def test_encoding_is_passed_to_stream_reader() -> None:
    parser = CsvParser()
    encoding = "ascii"
    stream_reader = Mock()
    mock_obj = stream_reader.open_file.return_value
    mock_obj.__enter__ = Mock(return_value=io.StringIO("c1,c2\nv1,v2"))
    mock_obj.__exit__ = Mock(return_value=None)
    file = RemoteFile(uri="s3://bucket/key.csv", last_modified=datetime.now())
    config = FileBasedStreamConfig(name="test", validation_policy="Emit Record", file_type="csv", format=CsvFormat(encoding=encoding))
    list(parser.parse_records(config, file, stream_reader, logger, {"properties": {"c1": {"type": "string"}, "c2": {"type": "string"}}}))
    stream_reader.open_file.assert_has_calls(
        [
            mock.call(file, FileReadMode.READ, encoding, logger),
            mock.call().__enter__(),
            mock.call().__exit__(None, None, None),
        ]
    )

    mock_obj.__enter__ = Mock(return_value=io.StringIO("c1,c2\nv1,v2"))
    loop = asyncio.get_event_loop()
    loop.run_until_complete(parser.infer_schema(config, file, stream_reader, logger))
    stream_reader.open_file.assert_called_with(file, FileReadMode.READ, encoding, logger)
    stream_reader.open_file.assert_has_calls(
        [
            mock.call(file, FileReadMode.READ, encoding, logger),
            mock.call().__enter__(),
            mock.call().__exit__(None, None, None),
            mock.call(file, FileReadMode.READ, encoding, logger),
            mock.call().__enter__(),
            mock.call().__exit__(None, None, None),
        ]
    )
