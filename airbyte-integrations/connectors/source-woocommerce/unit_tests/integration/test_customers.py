# Copyright (c) 2024 Airbyte, Inc., all rights reserved.

from unittest import TestCase

from airbyte_cdk.test.entrypoint_wrapper import EntrypointOutput
from airbyte_cdk.test.mock_http import HttpMocker
from airbyte_protocol.models import SyncMode

from .config import ConfigBuilder
from .request_builder import get_customers_request
from .utils import config, get_json_http_response, read_output

_STREAM_NAME = "customers"


class TestFullRefresh(TestCase):

    @staticmethod
    def _read(config_: ConfigBuilder, expecting_exception: bool = False) -> EntrypointOutput:
        return read_output(config_, _STREAM_NAME, SyncMode.full_refresh)

    @HttpMocker()
    def test_read_records(self, http_mocker: HttpMocker) -> None:
        # Register mock response
        http_mocker.get(
            get_customers_request()
            .with_param("orderby", "id")
            .with_param("order", "asc")
            .with_param("dates_are_gmt", "true")
            .with_param("per_page", "100")
            .build(),
            get_json_http_response("customers.json",200),
        )

        # Read records
        output = self._read(config())

        # Assert records
        assert len(output.records) == 2
