#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


import json
import traceback
from datetime import datetime, timedelta
from abc import ABC
from typing import Any, Dict, Generator, Iterable, List, Mapping, MutableMapping, Optional, Tuple
from base64 import b64encode

import requests
from airbyte_cdk.logger import AirbyteLogger
from airbyte_cdk.models import (
    AirbyteConnectionStatus,
    AirbyteCatalog,
    AirbyteStream,
    AirbyteMessage,
    AirbyteRecordMessage,
    ConfiguredAirbyteCatalog,
    Status,
    Type,
)
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources import Source
# from airbyte_cdk.sources.streams.http import HttpStream
# from airbyte_cdk.sources.streams.http.auth import BasicHttpAuthenticator

from prometheus_api_client import PrometheusConnect

from .utils import remove_prefix


# # Basic full refresh stream
# class PrometheusStream(HttpStream, ABC):
#     """
#     TODO remove this comment

#     This class represents a stream output by the connector.
#     This is an abstract base class meant to contain all the common functionality at the API level e.g: the API base URL, pagination strategy,
#     parsing responses etc..

#     Each stream should extend this class (or another abstract subclass of it) to specify behavior unique to that stream.

#     Typically for REST APIs each stream corresponds to a resource in the API. For example if the API
#     contains the endpoints
#         - GET v1/customers
#         - GET v1/employees

#     then you should have three classes:
#     `class PrometheusStream(HttpStream, ABC)` which is the current class
#     `class Customers(PrometheusStream)` contains behavior to pull data for customers using v1/customers
#     `class Employees(PrometheusStream)` contains behavior to pull data for employees using v1/employees

#     If some streams implement incremental sync, it is typical to create another class
#     `class IncrementalPrometheusStream((PrometheusStream), ABC)` then have concrete stream implementations extend it. An example
#     is provided below.

#     See the reference docs for the full list of configurable options.
#     """

#     # TODO: Fill in the url base. Required.
#     url_base = "https://example-api.com/v1/"

#     def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
#         """
#         TODO: Override this method to define a pagination strategy. If you will not be using pagination, no action is required - just return None.

#         This method should return a Mapping (e.g: dict) containing whatever information required to make paginated requests. This dict is passed
#         to most other methods in this class to help you form headers, request bodies, query params, etc..

#         For example, if the API accepts a 'page' parameter to determine which page of the result to return, and a response from the API contains a
#         'page' number, then this method should probably return a dict {'page': response.json()['page'] + 1} to increment the page count by 1.
#         The request_params method should then read the input next_page_token and set the 'page' param to next_page_token['page'].

#         :param response: the most recent response from the API
#         :return If there is another page in the result, a mapping (e.g: dict) containing information needed to query the next page in the response.
#                 If there are no more pages in the result, return None.
#         """
#         return None

#     def request_params(
#         self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
#     ) -> MutableMapping[str, Any]:
#         """
#         TODO: Override this method to define any query parameters to be set. Remove this method if you don't need to define request params.
#         Usually contains common params e.g. pagination size etc.
#         """
#         return {}

#     def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
#         """
#         TODO: Override this method to define how a response is parsed.
#         :return an iterable containing each record in the response
#         """
#         yield {}


# class Customers(PrometheusStream):
#     """
#     TODO: Change class name to match the table/data source this stream corresponds to.
#     """

#     # TODO: Fill in the primary key. Required. This is usually a unique field in the stream, like an ID or a timestamp.
#     primary_key = "customer_id"

#     def path(
#         self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
#     ) -> str:
#         """
#         TODO: Override this method to define the path this stream corresponds to. E.g. if the url is https://example-api.com/v1/customers then this
#         should return "customers". Required.
#         """
#         return "customers"


# # Basic incremental stream
# class IncrementalPrometheusStream(PrometheusStream, ABC):
#     """
#     TODO fill in details of this class to implement functionality related to incremental syncs for your connector.
#          if you do not need to implement incremental sync for any streams, remove this class.
#     """

#     # TODO: Fill in to checkpoint stream reads after N records. This prevents re-reading of data if the stream fails for any reason.
#     state_checkpoint_interval = None

#     @property
#     def cursor_field(self) -> str:
#         """
#         TODO
#         Override to return the cursor field used by this stream e.g: an API entity might always use created_at as the cursor field. This is
#         usually id or date based. This field's presence tells the framework this in an incremental stream. Required for incremental.

#         :return str: The name of the cursor field.
#         """
#         return []

#     def get_updated_state(self, current_stream_state: MutableMapping[str, Any], latest_record: Mapping[str, Any]) -> Mapping[str, Any]:
#         """
#         Override to determine the latest state after reading the latest record. This typically compared the cursor_field from the latest record and
#         the current state and picks the 'most' recent cursor. This is how a stream's state is determined. Required for incremental.
#         """
#         return {}


# class Employees(IncrementalPrometheusStream):
#     """
#     TODO: Change class name to match the table/data source this stream corresponds to.
#     """

#     # TODO: Fill in the cursor_field. Required.
#     cursor_field = "start_date"

#     # TODO: Fill in the primary key. Required. This is usually a unique field in the stream, like an ID or a timestamp.
#     primary_key = "employee_id"

#     def path(self, **kwargs) -> str:
#         """
#         TODO: Override this method to define the path this stream corresponds to. E.g. if the url is https://example-api.com/v1/employees then this should
#         return "single". Required.
#         """
#         return "employees"

#     def stream_slices(self, stream_state: Mapping[str, Any] = None, **kwargs) -> Iterable[Optional[Mapping[str, any]]]:
#         """
#         TODO: Optionally override this method to define this stream's slices. If slicing is not needed, delete this method.

#         Slices control when state is saved. Specifically, state is saved after a slice has been fully read.
#         This is useful if the API offers reads by groups or filters, and can be paired with the state object to make reads efficient. See the "concepts"
#         section of the docs for more information.

#         The function is called before reading any records in a stream. It returns an Iterable of dicts, each containing the
#         necessary data to craft a request for a slice. The stream state is usually referenced to determine what slices need to be created.
#         This means that data in a slice is usually closely related to a stream's cursor_field and stream_state.

#         An HTTP request is made for each returned slice. The same slice can be accessed in the path, request_params and request_header functions to help
#         craft that specific request.

#         For example, if https://example-api.com/v1/employees offers a date query params that returns data for that particular day, one way to implement
#         this would be to consult the stream state object for the last synced date, then return a slice containing each date from the last synced date
#         till now. The request_params function would then grab the date from the stream_slice and make it part of the request by injecting it into
#         the date query param.
#         """
#         raise NotImplementedError("Implement stream slices or delete this method!")


class SourcePrometheus(Source):
    def _get_client(self, config: Mapping[str, Any]):
        base_url = config["base_url"]
        basic_auth_user = config["basic_auth_user"]
        basic_auth_pass = config["basic_auth_pass"]

        headers = {}
        if basic_auth_user and basic_auth_pass:
            token = b64encode(f"{basic_auth_user}:{basic_auth_pass}".encode('utf-8')).decode("ascii")
            headers["Authorization"] = f"Basic {token}"

        return PrometheusConnect(url=base_url, headers=headers)

    def _build_stream(self, client: PrometheusConnect, metric: str) -> AirbyteStream:
        labels = list(client.get_label_names(params={'match[]': metric}))
        labels.remove('__name__')

        stream_name = f"metric_{metric}"
        json_schema = json_schema = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": [
                {
                    "type": "date-time",
                    "name": "time",
                    "description": "The time of the data point",
                },
                {
                    "type": "number",
                    "name": "value",
                    "description": "The value of the data point",
                },
            ],
        }

        for label in labels:
            json_schema["properties"].append({
                "type": ["string", "null"],
                "name": label,
                "description": f"The value of the label {label}",
            })

        return AirbyteStream(
            name=stream_name,
            json_schema=json_schema,
            supported_sync_modes=["full_refresh", "incremental"],
            sync_mode="full_refresh",
            destination_sync_mode="overwrite",
        )

    def _read_metric(self, client: PrometheusConnect, stream, metric: str, start_date: str, step: int) -> Generator[AirbyteRecordMessage, None, None]:
        cursor = datetime.strptime(start_date, '%Y-%m-%d')

        while True:
            now = datetime.now()
            # Prometheus allows up to 11k data points per response.
            # But since a lot of series can be returned, we limit to 1k data points.
            current_end = cursor + timedelta(seconds=step*1000)
            if current_end > now:
                current_end = now

            data = client.custom_query_range(query=metric, start_time=cursor, end_time=current_end, step=step)
            for serie in data:
                columns = {}
                if 'metric' in serie:
                    columns = serie['metric']
                    del columns['__name__']

                if 'values' in serie:
                    for value in serie['values']:
                        item = {
                            'time': value[0],
                            'value': value[1],
                        }
                        record = columns | item
                        yield AirbyteRecordMessage(stream=stream.name, data=record, emitted_at=int(datetime.now().timestamp()) * 1000)

            cursor = current_end + timedelta(seconds=1)  # end date is inclusive
            if cursor > now:
                break

        # for item in record:
        #     now = int(datetime.now().timestamp()) * 1000
        #     yield AirbyteRecordMessage(stream=stream, data=item, emitted_at=now)

    def check(self, logger, config: Mapping) -> AirbyteConnectionStatus:
        """
        Check involves verifying that the specified file is reachable with
        our credentials.
        """
        try:
            prom = self._get_client(config)
            # prom.get_current_metric_value(metric_name='up')
            if prom.check_prometheus_connection() is False:
                raise Exception("Failed to connect to Prometheus")
            return AirbyteConnectionStatus(status=Status.SUCCEEDED)
        except Exception as err:
            reason = f"Failed to load {config['base_url']}. Please check URL and credentials are set correctly."
            logger.error(f"{reason}\n{repr(err)}")
            return AirbyteConnectionStatus(status=Status.FAILED, message=reason)

    def discover(self, logger: AirbyteLogger, config: Mapping) -> AirbyteCatalog:
        """
        Returns an AirbyteCatalog representing the available streams and fields in this integration. For example, given valid credentials to a
        Prometheus instance, returns an Airbyte catalog where each metric is a stream, and each column is a label.
        """
        client = self._get_client(config)

        try:
            metrics = client.all_metrics()
            streams = list([self._build_stream(client, metric) for metric in metrics])
            return AirbyteCatalog(streams=streams)
        except Exception as err:
            reason = f"Failed to discover schemas of Prometheus at {config['base_url']}: {repr(err)}\n{traceback.format_exc()}"
            logger.error(reason)
            raise err

    def read(
        self, logger: AirbyteLogger, config: json, catalog: ConfiguredAirbyteCatalog, state: Dict[str, any]
    ) -> Generator[AirbyteMessage, None, None]:
        start_date = config['start_date']
        step = config['step']

        client = self._get_client(config)

        logger.info(f"Starting syncing {self.__class__.__name__}")

        for configured_stream in catalog.streams:
            stream = configured_stream.stream
            logger.info(f"Syncing {stream.name} stream")
            metric_name = remove_prefix(stream.name, "metric_")
            for record in self._read_metric(client, stream, metric_name, start_date, step):
                yield AirbyteMessage(type=Type.RECORD, record=record)

        logger.info(f"Finished syncing {self.__class__.__name__}")
