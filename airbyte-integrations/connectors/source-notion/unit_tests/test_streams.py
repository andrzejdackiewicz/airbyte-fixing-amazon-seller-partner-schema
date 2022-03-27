#
# Copyright (c) 2021 Airbyte, Inc., all rights reserved.
#

from http import HTTPStatus
from unittest.mock import MagicMock

import pytest
import requests
from airbyte_cdk.models import SyncMode
from source_notion.streams import NotionStream, Users


@pytest.fixture
def patch_base_class(mocker):
    # Mock abstract methods to enable instantiating abstract class
    mocker.patch.object(NotionStream, "path", "v0/example_endpoint")
    mocker.patch.object(NotionStream, "primary_key", "test_primary_key")
    mocker.patch.object(NotionStream, "__abstractmethods__", set())


def test_request_params(patch_base_class):
    stream = NotionStream(config=MagicMock())
    inputs = {"stream_slice": None, "stream_state": None, "next_page_token": None}
    expected_params = {}
    assert stream.request_params(**inputs) == expected_params


def test_next_page_token(patch_base_class, requests_mock):
    stream = NotionStream(config=MagicMock())
    requests_mock.get("https://dummy", json={"next_cursor": "aaa"})
    inputs = {"response": requests.get("https://dummy")}
    expected_token = {"next_cursor": "aaa"}
    assert stream.next_page_token(**inputs) == expected_token


def test_parse_response(patch_base_class, requests_mock):
    stream = NotionStream(config=MagicMock())
    requests_mock.get("https://dummy", json={"results": [{"a": 123}, {"b": "xx"}]})
    resp = requests.get("https://dummy")
    inputs = {"response": resp, "stream_state": MagicMock()}
    expected_parsed_object = [{"a": 123}, {"b": "xx"}]
    assert list(stream.parse_response(**inputs)) == expected_parsed_object


def test_request_headers(patch_base_class):
    stream = NotionStream(config=MagicMock())
    inputs = {"stream_slice": None, "stream_state": None, "next_page_token": None}
    expected_headers = {"Notion-Version": "2021-08-16"}
    assert stream.request_headers(**inputs) == expected_headers


def test_http_method(patch_base_class):
    stream = NotionStream(config=MagicMock())
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
    stream = NotionStream(config=MagicMock())
    assert stream.should_retry(response_mock) == should_retry


def test_backoff_time(patch_base_class):
    response_mock = MagicMock()
    stream = NotionStream(config=MagicMock())
    expected_backoff_time = None
    assert stream.backoff_time(response_mock) == expected_backoff_time


def test_users_request_params(patch_base_class):
    stream = Users(config=MagicMock())

    # No next_page_token. First pull
    inputs = {"stream_slice": None, "stream_state": None, "next_page_token": None}
    expected_params = {"page_size": 100}
    assert stream.request_params(**inputs) == expected_params

    # When getting pages after the first pull.
    inputs = {"stream_slice": None, "stream_state": None, "next_page_token": {"next_cursor": "123"}}
    expected_params = {"start_cursor": "123", "page_size": 100}
    assert stream.request_params(**inputs) == expected_params


def test_user_stream_handles_pagination_correclty(requests_mock):
    """
    Test shows that Users stream uses pagination as per Notion API docs.
    """

    response_body = {
        "object": "list",
        "results": [{"id": f"{x}"} for x in range(100)],
        "next_cursor": "test_cursor_1",
        "has_more": True,
        "type": "user"
    }
    url = "https://api.notion.com/v1/users?page_size=100"
    requests_mock.get(url, json=response_body)

    response_body = {
        "object": "list",
        "results": [{"id": f"{x}"} for x in range(100, 200)],
        "next_cursor": "test_cursor_2",
        "has_more": True,
        "type": "user"
    }
    url = "https://api.notion.com/v1/users?page_size=100&start_cursor=test_cursor_1"
    requests_mock.get(url, json=response_body)

    response_body = {
        "object": "list",
        "results": [{"id": f"{x}"} for x in range(200, 220)],
        "next_cursor": None,
        "has_more": False,
        "type": "user"
    }
    url = "https://api.notion.com/v1/users?page_size=100&start_cursor=test_cursor_2"
    requests_mock.get(url, json=response_body)

    stream = Users(config=MagicMock())

    user_ids = set()
    for record in stream.read_records(sync_mode=SyncMode.full_refresh):
        user_ids.add(record["id"])

    assert len(user_ids) == 220
