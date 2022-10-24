#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#

from http import HTTPStatus
from unittest.mock import MagicMock

import pytest
from source_nasa_apod.source import NasaApodStream

api_key_value = "foobar"
config = {"api_key": api_key_value}


@pytest.fixture
def patch_base_class(mocker):
    # Mock abstract methods to enable instantiating abstract class
    mocker.patch.object(NasaApodStream, "path", "v0/example_endpoint")
    mocker.patch.object(NasaApodStream, "primary_key", "test_primary_key")
    mocker.patch.object(NasaApodStream, "__abstractmethods__", set())


def test_request_params(patch_base_class):
    stream = NasaApodStream(config=config)
    inputs = {"stream_slice": None, "stream_state": None, "next_page_token": None}
    expected_params = {"api_key": api_key_value}
    assert stream.request_params(**inputs) == expected_params


def test_next_page_token(patch_base_class):
    stream = NasaApodStream(config=config)
    inputs = {"response": MagicMock()}
    expected_token = None
    assert stream.next_page_token(**inputs) == expected_token


def test_parse_response(patch_base_class):
    stream = NasaApodStream(config=config)
    response_object = [{"foo": "bar", "baz": ["qux"]}]
    response_mock = MagicMock()
    response_mock.configure_mock(**{"json.return_value": response_object})
    inputs = {"response": response_mock}
    assert next(stream.parse_response(**inputs)) == response_object


def test_request_headers(patch_base_class):
    stream = NasaApodStream(config=config)
    inputs = {"stream_slice": None, "stream_state": None, "next_page_token": None}
    expected_headers = {}
    assert stream.request_headers(**inputs) == expected_headers


def test_http_method(patch_base_class):
    stream = NasaApodStream(config=config)
    expected_method = "GET"
    assert stream.http_method == expected_method


@pytest.mark.parametrize(
    ("http_status", "should_retry"),
    [
        (HTTPStatus.OK, False),
        (HTTPStatus.BAD_REQUEST, False),
        (HTTPStatus.TOO_MANY_REQUESTS, True),
        (HTTPStatus.INTERNAL_SERVER_ERROR, True),
    ],
)
def test_should_retry(patch_base_class, http_status, should_retry):
    response_mock = MagicMock()
    response_mock.status_code = http_status
    stream = NasaApodStream(config=config)
    assert stream.should_retry(response_mock) == should_retry


def test_backoff_time(patch_base_class):
    response_mock = MagicMock()
    stream = NasaApodStream(config=config)
    expected_backoff_time = None
    assert stream.backoff_time(response_mock) == expected_backoff_time
