#
# MIT License
#
# Copyright (c) 2020 Airbyte
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
#

from http import HTTPStatus
from unittest.mock import MagicMock
import pytest

from source_salesloft.source import SalesloftStream


@pytest.fixture
def patch_base_class(mocker):
    # Mock abstract methods to enable instantiating abstract class
    mocker.patch.object(SalesloftStream, "path", "v0/example_endpoint")
    mocker.patch.object(SalesloftStream, "primary_key", "test_primary_key")
    mocker.patch.object(SalesloftStream, "__abstractmethods__", set())


def test_request_params(patch_base_class):
    stream = SalesloftStream(authenticator=MagicMock())
    inputs = {"stream_slice": None, "stream_state": None, "next_page_token": None}
    expected_params = {'page': 1, 'per_page': 100}
    assert stream.request_params(**inputs) == expected_params


def test_next_page_token(patch_base_class):
    stream = SalesloftStream(authenticator=MagicMock())
    response = MagicMock()
    response.json.return_value = {'metadata': {'paging': {'next_page': 2}}}
    inputs = {"response": response}
    expected_token = {'page': 2}
    assert stream.next_page_token(**inputs) == expected_token


def test_parse_response(patch_base_class):
    stream = SalesloftStream(authenticator=MagicMock())
    response = MagicMock()
    response.json.return_value = {'data': [{'id': 123, 'name': 'John Doe'}]}
    inputs = {"response": response}
    # TODO: replace this with your expected parced object
    expected_parsed_object = {'id': 123, 'name': 'John Doe'}
    assert next(stream.parse_response(**inputs)) == expected_parsed_object


def test_request_headers(patch_base_class):
    stream = SalesloftStream(authenticator=MagicMock())
    # TODO: replace this with your input parameters
    inputs = {"stream_slice": None, "stream_state": None, "next_page_token": None}
    # TODO: replace this with your expected request headers
    expected_headers = {}
    assert stream.request_headers(**inputs) == {}


def test_http_method(patch_base_class):
    stream = SalesloftStream(authenticator=MagicMock())
    # TODO: replace this with your expected http request method
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
    stream = SalesloftStream(authenticator=MagicMock())
    assert stream.should_retry(response_mock) == should_retry


def test_backoff_time(patch_base_class):
    response_mock = MagicMock()
    stream = SalesloftStream(authenticator=MagicMock())
    expected_backoff_time = None
    assert stream.backoff_time(response_mock) == expected_backoff_time
