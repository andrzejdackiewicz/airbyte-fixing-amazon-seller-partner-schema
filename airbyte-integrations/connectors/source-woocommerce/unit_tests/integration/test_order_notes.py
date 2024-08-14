# Copyright (c) 2024 Airbyte, Inc., all rights reserved.

from unittest import TestCase

from airbyte_cdk.test.entrypoint_wrapper import EntrypointOutput
from airbyte_cdk.test.mock_http import HttpMocker
from airbyte_protocol.models import SyncMode
from freezegun import freeze_time

from .config import ConfigBuilder
from .request_builder import get_order_notes_request, get_orders_request
from .utils import config, get_json_http_response, read_output

_STREAM_NAME = "order_notes"


class TestFullRefresh(TestCase):

    @staticmethod
    def _read(config_: ConfigBuilder, expecting_exception: bool = False) -> EntrypointOutput:
        return read_output(config_, _STREAM_NAME, SyncMode.full_refresh)

    @HttpMocker()
    @freeze_time("2017-01-29T00:00:00Z")
    def test_read_records(self, http_mocker: HttpMocker) -> None:
        # Register mock response
        http_mocker.get(
            get_orders_request()
            .with_param("orderby", "id")
            .with_param("order", "asc")
            .with_param("dates_are_gmt", "true")
            .with_param("per_page", "100")
            .with_param("modified_after", "2017-01-01T00:00:00")
            .with_param("modified_before", "2017-01-29T00:00:00")
            .build(),
            get_json_http_response("orders.json", 200),
            )

        for order_id in ["727", "723"]:
            http_mocker.get(
                get_order_notes_request(order_id)
                .with_param("orderby", "id")
                .with_param("order", "asc")
                .with_param("dates_are_gmt", "true")
                .with_param("per_page", "100")
                .build(),
                get_json_http_response("order_notes.json", 200),
                )

        # Read records
        output = self._read(config())

        # Check record count: 2 orders, 3 notes per order.
        assert len(output.records) == 6
