#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import json
from typing import Iterator
from unittest.mock import MagicMock

import pytest
from airbyte_cdk.models import AirbyteLogMessage, AirbyteMessage, AirbyteRecordMessage, Level
from airbyte_cdk.models import Type as MessageType
from airbyte_cdk.sources.declarative.manifest_declarative_source import ManifestDeclarativeSource
from connector_builder.message_grouper import MessageGrouper
from connector_builder.models import HttpRequest, HttpResponse, LogMessage, StreamRead, StreamReadPages
from unit_tests.connector_builder.utils import create_configured_catalog

MAX_PAGES_PER_SLICE = 4
MAX_SLICES = 3

MANIFEST = {
    "version": "0.30.0",
    "type": "DeclarativeSource",
    "definitions": {
        "selector": {"extractor": {"field_path": ["items"], "type": "DpathExtractor"}, "type": "RecordSelector"},
        "requester": {"url_base": "https://demonslayers.com/api/v1/", "http_method": "GET", "type": "DeclarativeSource"},
        "retriever": {
            "type": "DeclarativeSource",
            "record_selector": {"extractor": {"field_path": ["items"], "type": "DpathExtractor"}, "type": "RecordSelector"},
            "paginator": {"type": "NoPagination"},
            "requester": {"url_base": "https://demonslayers.com/api/v1/", "http_method": "GET", "type": "HttpRequester"},
        },
        "hashiras_stream": {
            "retriever": {
                "type": "DeclarativeSource",
                "record_selector": {"extractor": {"field_path": ["items"], "type": "DpathExtractor"}, "type": "RecordSelector"},
                "paginator": {"type": "NoPagination"},
                "requester": {"url_base": "https://demonslayers.com/api/v1/", "http_method": "GET", "type": "HttpRequester"},
            },
            "$parameters": {"name": "hashiras", "path": "/hashiras"},
        },
        "breathing_techniques_stream": {
            "retriever": {
                "type": "DeclarativeSource",
                "record_selector": {"extractor": {"field_path": ["items"], "type": "DpathExtractor"}, "type": "RecordSelector"},
                "paginator": {"type": "NoPagination"},
                "requester": {"url_base": "https://demonslayers.com/api/v1/", "http_method": "GET", "type": "HttpRequester"},
            },
            "$parameters": {"name": "breathing-techniques", "path": "/breathing_techniques"},
        },
    },
    "streams": [
        {
            "type": "DeclarativeStream",
            "retriever": {
                "type": "SimpleRetriever",
                "record_selector": {"extractor": {"field_path": ["items"], "type": "DpathExtractor"}, "type": "RecordSelector"},
                "paginator": {"type": "NoPagination"},
                "requester": {"url_base": "https://demonslayers.com/api/v1/", "http_method": "GET", "type": "HttpRequester"},
            },
            "$parameters": {"name": "hashiras", "path": "/hashiras"},
        },
        {
            "type": "DeclarativeStream",
            "retriever": {
                "type": "SimpleRetriever",
                "record_selector": {"extractor": {"field_path": ["items"], "type": "DpathExtractor"}, "type": "RecordSelector"},
                "paginator": {"type": "NoPagination"},
                "requester": {"url_base": "https://demonslayers.com/api/v1/", "http_method": "GET", "type": "HttpRequester"},
            },
            "$parameters": {"name": "breathing-techniques", "path": "/breathing_techniques"},
        },
    ],
    "check": {"stream_names": ["hashiras"], "type": "CheckStream"},
}

CONFIG = {"rank": "upper-six"}


def test_get_grouped_messages():
    request = {
        "url": "https://demonslayers.com/api/v1/hashiras?era=taisho",
        "headers": {"Content-Type": "application/json"},
        "http_method": "GET",
        "body": {"custom": "field"},
    }
    response = {"status_code": 200, "headers": {"field": "value"}, "body": '{"name": "field"}', "http_method": "GET"}
    expected_schema = {"$schema": "http://json-schema.org/schema#", "properties": {"name": {"type": "string"}}, "type": "object"}
    expected_pages = [
        StreamReadPages(
            request=HttpRequest(
                url="https://demonslayers.com/api/v1/hashiras",
                parameters={"era": ["taisho"]},
                headers={"Content-Type": "application/json"},
                body={"custom": "field"},
                http_method="GET",
            ),
            response=HttpResponse(status=200, headers={"field": "value"}, body='{"name": "field"}'),
            records=[{"name": "Shinobu Kocho"}, {"name": "Muichiro Tokito"}],
        ),
        StreamReadPages(
            request=HttpRequest(
                url="https://demonslayers.com/api/v1/hashiras",
                parameters={"era": ["taisho"]},
                headers={"Content-Type": "application/json"},
                body={"custom": "field"},
                http_method="GET",
            ),
            response=HttpResponse(status=200, headers={"field": "value"}, body='{"name": "field"}'),
            records=[{"name": "Mitsuri Kanroji"}],
        ),
    ]

    mock_source = make_mock_source(
        iter(
            [
                request_log_message(request),
                response_log_message(response),
                record_message("hashiras", {"name": "Shinobu Kocho"}),
                record_message("hashiras", {"name": "Muichiro Tokito"}),
                request_log_message(request),
                response_log_message(response),
                record_message("hashiras", {"name": "Mitsuri Kanroji"}),
            ]
        )
    )

    connector_builder_handler = MessageGrouper(MAX_PAGES_PER_SLICE, MAX_SLICES)
    actual_response: StreamRead = connector_builder_handler.get_message_groups(source=mock_source, config=CONFIG,
                                                                               configured_catalog=create_configured_catalog("hashiras"))
    assert actual_response.inferred_schema == expected_schema

    single_slice = actual_response.slices[0]
    for i, actual_page in enumerate(single_slice.pages):
        assert actual_page == expected_pages[i]


def test_get_grouped_messages_with_logs():
    request = {
        "url": "https://demonslayers.com/api/v1/hashiras?era=taisho",
        "headers": {"Content-Type": "application/json"},
        "body": {"custom": "field"},
        "http_method": "GET",
    }
    response = {"status_code": 200, "headers": {"field": "value"}, "body": '{"name": "field"}'}
    expected_pages = [
        StreamReadPages(
            request=HttpRequest(
                url="https://demonslayers.com/api/v1/hashiras",
                parameters={"era": ["taisho"]},
                headers={"Content-Type": "application/json"},
                body={"custom": "field"},
                http_method="GET",
            ),
            response=HttpResponse(status=200, headers={"field": "value"}, body='{"name": "field"}'),
            records=[{"name": "Shinobu Kocho"}, {"name": "Muichiro Tokito"}],
        ),
        StreamReadPages(
            request=HttpRequest(
                url="https://demonslayers.com/api/v1/hashiras",
                parameters={"era": ["taisho"]},
                headers={"Content-Type": "application/json"},
                body={"custom": "field"},
                http_method="GET",
            ),
            response=HttpResponse(status=200, headers={"field": "value"}, body='{"name": "field"}'),
            records=[{"name": "Mitsuri Kanroji"}],
        ),
    ]
    expected_logs = [
        LogMessage(**{"message": "log message before the request", "level": "INFO"}),
        LogMessage(**{"message": "log message during the page", "level": "INFO"}),
        LogMessage(**{"message": "log message after the response", "level": "INFO"}),
    ]

    mock_source = make_mock_source(
        iter(
            [
                AirbyteMessage(type=MessageType.LOG, log=AirbyteLogMessage(level=Level.INFO, message="log message before the request")),
                request_log_message(request),
                response_log_message(response),
                record_message("hashiras", {"name": "Shinobu Kocho"}),
                AirbyteMessage(type=MessageType.LOG, log=AirbyteLogMessage(level=Level.INFO, message="log message during the page")),
                record_message("hashiras", {"name": "Muichiro Tokito"}),
                AirbyteMessage(type=MessageType.LOG, log=AirbyteLogMessage(level=Level.INFO, message="log message after the response")),
            ]
        )
    )

    connector_builder_handler = MessageGrouper(MAX_PAGES_PER_SLICE, MAX_SLICES)

    actual_response: StreamRead = connector_builder_handler.get_message_groups(source=mock_source, config=CONFIG,
                                                                               configured_catalog=create_configured_catalog("hashiras"))
    single_slice = actual_response.slices[0]
    for i, actual_page in enumerate(single_slice.pages):
        assert actual_page == expected_pages[i]

    for i, actual_log in enumerate(actual_response.logs):
        assert actual_log == expected_logs[i]


@pytest.mark.parametrize(
    "request_record_limit, max_record_limit",
    [
        pytest.param(1, 3, id="test_create_request_with_record_limit"),
        pytest.param(3, 1, id="test_create_request_record_limit_exceeds_max"),
    ],
)
def test_get_grouped_messages_record_limit(request_record_limit, max_record_limit):
    request = {
        "url": "https://demonslayers.com/api/v1/hashiras?era=taisho",
        "headers": {"Content-Type": "application/json"},
        "body": {"custom": "field"},
    }
    response = {"status_code": 200, "headers": {"field": "value"}, "body": '{"name": "field"}'}
    mock_source = make_mock_source(
        iter(
            [
                request_log_message(request),
                response_log_message(response),
                record_message("hashiras", {"name": "Shinobu Kocho"}),
                record_message("hashiras", {"name": "Muichiro Tokito"}),
                request_log_message(request),
                response_log_message(response),
                record_message("hashiras", {"name": "Mitsuri Kanroji"}),
                response_log_message(response),
            ]
        )
    )
    n_records = 2
    record_limit = min(request_record_limit, max_record_limit)

    api = MessageGrouper(MAX_PAGES_PER_SLICE, MAX_SLICES, max_record_limit=max_record_limit)
    actual_response: StreamRead = api.get_message_groups(mock_source, config=CONFIG,
                                                         configured_catalog=create_configured_catalog("hashiras"),
                                                         record_limit=request_record_limit)
    single_slice = actual_response.slices[0]
    total_records = 0
    for i, actual_page in enumerate(single_slice.pages):
        total_records += len(actual_page.records)
    assert total_records == min([record_limit, n_records])


@pytest.mark.parametrize(
    "max_record_limit",
    [
        pytest.param(2, id="test_create_request_no_record_limit"),
        pytest.param(1, id="test_create_request_no_record_limit_n_records_exceed_max"),
    ],
)
def test_get_grouped_messages_default_record_limit(max_record_limit):
    request = {
        "url": "https://demonslayers.com/api/v1/hashiras?era=taisho",
        "headers": {"Content-Type": "application/json"},
        "body": {"custom": "field"},
    }
    response = {"status_code": 200, "headers": {"field": "value"}, "body": '{"name": "field"}'}
    mock_source = make_mock_source(
        iter(
            [
                request_log_message(request),
                response_log_message(response),
                record_message("hashiras", {"name": "Shinobu Kocho"}),
                record_message("hashiras", {"name": "Muichiro Tokito"}),
                request_log_message(request),
                response_log_message(response),
                record_message("hashiras", {"name": "Mitsuri Kanroji"}),
                response_log_message(response),
            ]
        )
    )
    n_records = 2

    api = MessageGrouper(MAX_PAGES_PER_SLICE, MAX_SLICES, max_record_limit=max_record_limit)
    actual_response: StreamRead = api.get_message_groups(source=mock_source, config=CONFIG,
                                                         configured_catalog=create_configured_catalog("hashiras"))
    single_slice = actual_response.slices[0]
    total_records = 0
    for i, actual_page in enumerate(single_slice.pages):
        total_records += len(actual_page.records)
    assert total_records == min([max_record_limit, n_records])


def test_get_grouped_messages_limit_0():
    request = {
        "url": "https://demonslayers.com/api/v1/hashiras?era=taisho",
        "headers": {"Content-Type": "application/json"},
        "body": {"custom": "field"},
    }
    response = {"status_code": 200, "headers": {"field": "value"}, "body": '{"name": "field"}'}
    mock_source = make_mock_source(
        iter(
            [
                request_log_message(request),
                response_log_message(response),
                record_message("hashiras", {"name": "Shinobu Kocho"}),
                record_message("hashiras", {"name": "Muichiro Tokito"}),
                request_log_message(request),
                response_log_message(response),
                record_message("hashiras", {"name": "Mitsuri Kanroji"}),
                response_log_message(response),
            ]
        )
    )
    api = MessageGrouper(MAX_PAGES_PER_SLICE, MAX_SLICES)

    with pytest.raises(ValueError):
        api.get_message_groups(source=mock_source, config=CONFIG, configured_catalog=create_configured_catalog("hashiras"), record_limit=0)


def test_get_grouped_messages_no_records():
    request = {
        "url": "https://demonslayers.com/api/v1/hashiras?era=taisho",
        "headers": {"Content-Type": "application/json"},
        "body": {"custom": "field"},
        "http_method": "GET",
    }
    response = {"status_code": 200, "headers": {"field": "value"}, "body": '{"name": "field"}'}
    expected_pages = [
        StreamReadPages(
            request=HttpRequest(
                url="https://demonslayers.com/api/v1/hashiras",
                parameters={"era": ["taisho"]},
                headers={"Content-Type": "application/json"},
                body={"custom": "field"},
                http_method="GET",
            ),
            response=HttpResponse(status=200, headers={"field": "value"}, body='{"name": "field"}'),
            records=[],
        ),
        StreamReadPages(
            request=HttpRequest(
                url="https://demonslayers.com/api/v1/hashiras",
                parameters={"era": ["taisho"]},
                headers={"Content-Type": "application/json"},
                body={"custom": "field"},
                http_method="GET",
            ),
            response=HttpResponse(status=200, headers={"field": "value"}, body='{"name": "field"}'),
            records=[],
        ),
    ]

    mock_source = make_mock_source(
        iter(
            [
                request_log_message(request),
                response_log_message(response),
                request_log_message(request),
                response_log_message(response),
            ]
        )
    )

    message_grouper = MessageGrouper(MAX_PAGES_PER_SLICE, MAX_SLICES)

    actual_response: StreamRead = message_grouper.get_message_groups(source=mock_source, config=CONFIG,
                                                                     configured_catalog=create_configured_catalog("hashiras"))

    single_slice = actual_response.slices[0]
    for i, actual_page in enumerate(single_slice.pages):
        assert actual_page == expected_pages[i]


def test_get_grouped_messages_invalid_group_format():
    response = {"status_code": 200, "headers": {"field": "value"}, "body": '{"name": "field"}'}

    mock_source = make_mock_source(
        iter(
            [
                response_log_message(response),
                record_message("hashiras", {"name": "Shinobu Kocho"}),
                record_message("hashiras", {"name": "Muichiro Tokito"}),
            ]
        )
    )

    api = MessageGrouper(MAX_PAGES_PER_SLICE, MAX_SLICES)

    with pytest.raises(ValueError):
        api.get_message_groups(source=mock_source, config=CONFIG, configured_catalog=create_configured_catalog("hashiras"))


@pytest.mark.parametrize(
    "log_message, expected_response",
    [
        pytest.param(
            {"status_code": 200, "headers": {"field": "name"}, "body": '{"id": "fire", "owner": "kyojuro_rengoku"}'},
            HttpResponse(status=200, headers={"field": "name"}, body='{"id": "fire", "owner": "kyojuro_rengoku"}'),
            id="test_create_response_with_all_fields",
        ),
        pytest.param(
            {"status_code": 200, "headers": {"field": "name"}},
            HttpResponse(status=200, headers={"field": "name"}, body="{}"),
            id="test_create_response_with_no_body",
        ),
        pytest.param(
            {"status_code": 200, "body": '{"id": "fire", "owner": "kyojuro_rengoku"}'},
            HttpResponse(status=200, body='{"id": "fire", "owner": "kyojuro_rengoku"}'),
            id="test_create_response_with_no_headers",
        ),
        pytest.param(
            {
                "status_code": 200,
                "headers": {"field": "name"},
                "body": '[{"id": "fire", "owner": "kyojuro_rengoku"}, {"id": "mist", "owner": "muichiro_tokito"}]',
            },
            HttpResponse(
                status=200,
                headers={"field": "name"},
                body='[{"id": "fire", "owner": "kyojuro_rengoku"}, {"id": "mist", "owner": "muichiro_tokito"}]',
            ),
            id="test_create_response_with_array",
        ),
        pytest.param(
            {"status_code": 200, "body": "tomioka"},
            HttpResponse(status=200, body="tomioka"),
            id="test_create_response_with_string",
        ),
        pytest.param("request:{invalid_json: }", None, id="test_invalid_json_still_does_not_crash"),
        pytest.param("just a regular log message", None, id="test_no_response:_prefix_does_not_crash"),
    ],
)
def test_create_response_from_log_message(log_message, expected_response):
    if isinstance(log_message, str):
        response_message = log_message
    else:
        response_message = f"response:{json.dumps(log_message)}"

    airbyte_log_message = AirbyteLogMessage(level=Level.INFO, message=response_message)
    connector_builder_handler = MessageGrouper(MAX_PAGES_PER_SLICE, MAX_SLICES)
    actual_response = connector_builder_handler._create_response_from_log_message(airbyte_log_message)

    assert actual_response == expected_response


def test_get_grouped_messages_with_many_slices():
    request = {}
    response = {"status_code": 200}

    mock_source = make_mock_source(
        iter(
            [
                slice_message(),
                request_log_message(request),
                response_log_message(response),
                record_message("hashiras", {"name": "Muichiro Tokito"}),
                slice_message(),
                request_log_message(request),
                response_log_message(response),
                record_message("hashiras", {"name": "Shinobu Kocho"}),
                record_message("hashiras", {"name": "Mitsuri Kanroji"}),
                request_log_message(request),
                response_log_message(response),
                record_message("hashiras", {"name": "Obanai Iguro"}),
                request_log_message(request),
                response_log_message(response),
            ]
        )
    )

    connecto_builder_handler = MessageGrouper(MAX_PAGES_PER_SLICE, MAX_SLICES)

    stream_read: StreamRead = connecto_builder_handler.get_message_groups(source=mock_source, config=CONFIG,
                                                                          configured_catalog=create_configured_catalog("hashiras"))

    assert not stream_read.test_read_limit_reached
    assert len(stream_read.slices) == 2

    assert len(stream_read.slices[0].pages) == 1
    assert len(stream_read.slices[0].pages[0].records) == 1

    assert len(stream_read.slices[1].pages) == 3
    assert len(stream_read.slices[1].pages[0].records) == 2
    assert len(stream_read.slices[1].pages[1].records) == 1
    assert len(stream_read.slices[1].pages[2].records) == 0


def test_get_grouped_messages_given_maximum_number_of_slices_then_test_read_limit_reached():
    maximum_number_of_slices = 5
    request = {}
    response = {"status_code": 200}
    mock_source = make_mock_source(
        iter([slice_message(), request_log_message(request), response_log_message(response)] * maximum_number_of_slices)
    )

    api = MessageGrouper(MAX_PAGES_PER_SLICE, MAX_SLICES)

    stream_read: StreamRead = api.get_message_groups(source=mock_source, config=CONFIG,
                                                     configured_catalog=create_configured_catalog("hashiras"))

    assert stream_read.test_read_limit_reached


def test_get_grouped_messages_given_maximum_number_of_pages_then_test_read_limit_reached():
    maximum_number_of_pages_per_slice = 5
    request = {}
    response = {"status_code": 200}
    mock_source = make_mock_source(
        iter([slice_message()] + [request_log_message(request), response_log_message(response)] * maximum_number_of_pages_per_slice)
    )

    api = MessageGrouper(MAX_PAGES_PER_SLICE, MAX_SLICES)

    stream_read: StreamRead = api.get_message_groups(source=mock_source, config=CONFIG,
                                                     configured_catalog=create_configured_catalog("hashiras"))

    assert stream_read.test_read_limit_reached


def test_read_stream_returns_error_if_stream_does_not_exist():
    mock_source = MagicMock()
    mock_source.read.side_effect = ValueError("error")

    full_config = {**CONFIG, **{"__injected_declarative_manifest": MANIFEST}}

    message_grouper = MessageGrouper(MAX_PAGES_PER_SLICE, MAX_SLICES)
    actual_response = message_grouper.get_message_groups(source=mock_source, config=full_config,
                                                         configured_catalog=create_configured_catalog("not_in_manifest"))

    assert 1 == len(actual_response.logs)
    assert "Traceback" in actual_response.logs[0].message
    assert "ERROR" in actual_response.logs[0].level


def make_mock_source(return_value: Iterator) -> MagicMock:
    mock_source = MagicMock()
    mock_source.read.return_value = return_value
    return mock_source


def request_log_message(request: dict) -> AirbyteMessage:
    return AirbyteMessage(type=MessageType.LOG, log=AirbyteLogMessage(level=Level.INFO, message=f"request:{json.dumps(request)}"))


def response_log_message(response: dict) -> AirbyteMessage:
    return AirbyteMessage(type=MessageType.LOG, log=AirbyteLogMessage(level=Level.INFO, message=f"response:{json.dumps(response)}"))


def record_message(stream: str, data: dict) -> AirbyteMessage:
    return AirbyteMessage(type=MessageType.RECORD, record=AirbyteRecordMessage(stream=stream, data=data, emitted_at=1234))


def slice_message() -> AirbyteMessage:
    return AirbyteMessage(type=MessageType.LOG, log=AirbyteLogMessage(level=Level.INFO, message='slice:{"key": "value"}'))
