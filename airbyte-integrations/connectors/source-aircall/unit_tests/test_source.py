#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

from unittest.mock import MagicMock

from source_aircall.source import SourceAircall


def test_check_connection(mocker):
    source = SourceAircall()
    logger_mock, username_mock, password_mock = MagicMock(), MagicMock(), MagicMock()
    assert source.check_connection(logger_mock, {"username": str(username_mock), "password": str(password_mock)}) == (True, None)


def test_streams(mocker):
    source = SourceAircall()
    config_mock = MagicMock()
    streams = source.streams(config_mock)
    expected_streams_number = 1
    assert len(streams) == expected_streams_number
