#
# Copyright (c) 2021 Airbyte, Inc., all rights reserved.
#

from unittest.mock import MagicMock

from source_freshsales.source import SourceFreshsales


def test_check_connection(mocker):
    source = SourceFreshsales()
    logger_mock, config_mock = MagicMock(), MagicMock()
    assert source.check_connection(logger_mock, config_mock) == (True, None)


def test_streams(mocker):
    source = SourceFreshsales()
    config_mock = MagicMock()
    streams = source.streams(config_mock)
    expected_streams_number = 10
    assert len(streams) == expected_streams_number
