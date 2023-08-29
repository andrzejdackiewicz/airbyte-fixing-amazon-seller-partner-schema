#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import unittest

import pytest
from airbyte_cdk.sources.file_based.config.csv_format import CsvHeaderAutogenerated, CsvHeaderFromCsv, CsvHeaderUserProvided
from pydantic import ValidationError


class CsvHeaderDefinitionTest(unittest.TestCase):
    def test_given_user_provided_and_not_column_names_provided_then_raise_exception(self) -> None:
        with pytest.raises(ValidationError):
            CsvHeaderUserProvided(column_names=[])

    def test_given_user_provided_and_column_names_then_config_is_valid(self) -> None:
        # no error means that this test succeeds
        CsvHeaderUserProvided(column_names=["1", "2", "3"])

    def test_given_user_provided_then_csv_does_not_have_header_row(self) -> None:
        assert not CsvHeaderUserProvided(column_names=["1", "2", "3"]).has_header_row()

    def test_given_autogenerated_then_csv_does_not_have_header_row(self) -> None:
        assert not CsvHeaderAutogenerated().has_header_row()

    def test_given_from_csv_then_csv_has_header_row(self) -> None:
        assert CsvHeaderFromCsv().has_header_row()
