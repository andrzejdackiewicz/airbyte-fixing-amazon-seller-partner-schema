#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#


from abc import ABC
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple, Generator

import requests
import io
import csv
import gzip
import json
import backoff
import pendulum
from .utils import Utils
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.models import SyncMode
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream
import logging
import time


# Basic full refresh stream
class DoubleverifyStream(HttpStream, ABC):

    url_base = "https://data-api.doubleverify.com"
    request_type = None
    logger = logging.getLogger('airbyte')

    def __init__(self, config: Mapping[str, Any], catalog_stream: Mapping[str, Any]):
        super().__init__()
        self.config = config
        if "end_date" not in self.config:
            self.config["end_date"] = str(pendulum.yesterday())
        self.catalog_stream = catalog_stream

    def request_headers(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> Mapping[str, Any]:
        """
        Override to return any non-auth headers. Authentication headers will overwrite any overlapping headers returned from this method.
        """
        return Utils.get_request_header(config=self.config, accept='text/csv', accept_encoding='gzip')
        
    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        """
        This method should return a Mapping (e.g: dict) containing whatever information required to make paginated requests. This dict is passed
        to most other methods in this class to help you form headers, request bodies, query params, etc..

        For example, if the API accepts a 'page' parameter to determine which page of the result to return, and a response from the API contains a
        'page' number, then this method should probably return a dict {'page': response.json()['page'] + 1} to increment the page count by 1.
        The request_params method should then read the input next_page_token and set the 'page' param to next_page_token['page'].

        :param response: the most recent response from the API
        :return If there is another page in the result, a mapping (e.g: dict) containing information needed to query the next page in the response.
                If there are no more pages in the result, return None.
        """
        return None

    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        return {}

    def path(self, **kwargs) -> str:
        request_id = self.post_create_request(self.config, self.catalog_stream)
        #check if the report is successfully created
        if request_id[0]:
            # Get the report status
            report_status = self.poll_report_status(request_id[1], self.config)
            # If the report status is success, then return the correct URL
            if report_status == "Success":
                # Introduce a small delay to wait for the csv report to be generated on the server side.
                time.sleep(5)
                return DoubleverifyStream.url_base + "/requests/" + request_id[1] + "/data"
        self.logger.error("Report creation was not successful, reason: " + request_id[1])

    @property
    def http_method(self) -> str:
        return "GET"

    def build_request_body(self, config: Mapping[str, any], catalog_stream: Mapping[str, any]) -> Mapping[str, any]:
        request_body = {}
        request_body["requestType"] = self.request_type
        request_body["dimensions"] = self.get_dimensions(catalog_stream)
        request_body["metrics"] = self.get_metrics(catalog_stream)
        request_body["dateRange"] = {
                                        "from": config.get("start_date"),
                                        "to": config.get("end_date")
                                    }
        return request_body


    def get_dimensions(self, catalog_stream: Mapping[str, any]):
        # Get the fields object from the stream, then the dimensions based on the fields type
        fields = catalog_stream.get("properties")
        dim_keys = [k for k in fields.keys() if fields.get(k).get("field_type")=="dimension"]
        dimensions = {k:v for k,v in fields.items() if k in dim_keys}
        # Loop over all the elements in dimensions and get a list of their ids and
        # return a list of mapping
        return [{"id": dimensions.get(dimension).get("id")} for dimension in dimensions.keys()]


    def get_metrics(self, catalog_stream: Mapping[str, any]):
        # Get the fields object from the stream, then the dimensions based on the fields type
        fields = catalog_stream.get("properties")
        metric_keys = [k for k in fields.keys() if fields.get(k).get("field_type")=="metric"]
        metrics = {k:v for k,v in fields.items() if k in metric_keys}
        # Loop over all the elements in metrics and get a list of their ids and
        # return a list of mapping
        return [{"id": metrics.get(metric).get("id")} for metric in metrics.keys()]

    def post_create_request(self, config: Mapping[str, any], catalog_stream: Mapping[str, any]):
        # Make a data request via POST method, this will then generate the report with the specified fields.
        url = self.url_base + "/requests"
        try:
            response = requests.post(
                url = url
                ,json = self.build_request_body(config, catalog_stream)
                ,headers = Utils.get_request_header(config=config)
            )
            if response.status_code == 200:
                return True, response.json().get('id')
            return False, response.reason
        except Exception as e:
            return False, repr(e)

    @backoff.on_predicate(backoff.expo, lambda x: x != 'Success', max_tries=10)
    def poll_report_status(self, request_id: str, config: Mapping[str, any],):
        # Poll report status until Success status is returned, or max retries reached
        url = self.url_base + "/requests/" + request_id + "/status"
        try:
            response = requests.get(
                url = url
                ,headers = Utils.get_request_header(config=config)
            )
            if response.status_code == 200:
                return response.json().get("status")
            return response.reason
        except Exception as e:
            return e
    
    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        # Decompress the response content and convert it to string
        data = gzip.decompress(response.content)
        if data:
            data = str(data, response.encoding)
            # open the response with text buffer
            with io.StringIO(data) as csv_response:
                # Convert csv data into dict rows to be yielded by the generator
                reader = csv.DictReader(csv_response)
                # Sanitize field names
                reader.fieldnames = [Utils.sanitize(col) for col in reader.fieldnames]
                for row in reader :
                    yield row


class IncrementalDoubleverifyStream(DoubleverifyStream, ABC):
    #state_checkpoint_interval = None
    window_in_days = 5

    @property
    def cursor_field(self) -> str:
        """
        The cursor field used by this stream e.g: an API entity might always use created_at as the cursor field. This is
        usually id or date based. This field's presence tells the framework this in an incremental stream. Required for incremental.

        :return str: The name of the cursor field.
        """
        return "date"

    def get_updated_state(self, current_stream_state: MutableMapping[str, Any], latest_record: Mapping[str, Any]) -> Mapping[str, Any]:
        default_value = self.config.get("start_date") # Take start_date as the default value
        latest_state = latest_record.get(self.cursor_field, default_value) # If no cursor field in the latest record, then take default_value
        current_state = current_stream_state.get(self.cursor_field, default_value) # If no cursor field in the current stream state, then take default_value
        
        state = {self.cursor_field: max(pendulum.parse(latest_state), pendulum.parse(current_state)).strftime("%Y-%m-%d")}
        print(state)
        return state

    def stream_slices(
        self, sync_mode: SyncMode, cursor_field: List[str] = None, stream_state: Mapping[str, Any] = None
    ) -> Iterable[Optional[Mapping[str, any]]]:
        """
        Override default stream_slices CDK method to provide date_slices as page chunks for data fetch.
        Returns list of dict
        """
        start_date = pendulum.parse(self.config.get("start_date"))
        end_date = pendulum.parse(self.config.get("end_date"))

        # determine stream_state, if no stream_state we use start_date
        if stream_state:
            state = stream_state.get(self.cursor_field)

            if state:
                state = pendulum.parse(state)

            start_date = state

        # use the lowest date between start_date and end_date, otherwise API fails if start_date is in future
        start_date = min(start_date, end_date)
        date_slices = []

        while start_date < end_date:
            # the amount of days for each data-chunk beginning from start_date
            end_date_slice = start_date.add(days=self.window_in_days)
            date_slices.append({"start_date": start_date.strftime("%Y-%m-%d"), "end_date": end_date_slice.strftime("%Y-%m-%d")})

            # add 1 day for start next slice from next day and not duplicate data from previous slice end date.
            start_date = end_date_slice.add(days=1)

        return date_slices


class Youtube(IncrementalDoubleverifyStream):
    primary_key = ""
    request_type = 2
    catalog = json.load(open("./source_doubleverify/schemas/youtube.json"))
    def __init__(self, config: Mapping[str, Any]):
        super().__init__(config=config, catalog_stream=self.catalog)


class Pinterest(IncrementalDoubleverifyStream):
    primary_key = ""
    request_type = 3
    catalog = json.load(open("./source_doubleverify/schemas/pinterest.json"))
    def __init__(self, config: Mapping[str, Any]):
        super().__init__(config=config, catalog_stream=self.catalog)


class Facebook(IncrementalDoubleverifyStream):
    primary_key = ""
    request_type = 4
    catalog = json.load(open("./source_doubleverify/schemas/facebook.json"))
    def __init__(self, config: Mapping[str, Any]):
        super().__init__(config=config, catalog_stream=self.catalog)


class Twitter(IncrementalDoubleverifyStream):
    primary_key = ""
    request_type = 6
    catalog = json.load(open("./source_doubleverify/schemas/twitter.json"))
    def __init__(self, config: Mapping[str, Any]):
        super().__init__(config=config, catalog_stream=self.catalog)


class Snapchat(IncrementalDoubleverifyStream):
    primary_key = ""
    request_type = 9
    catalog = json.load(open("./source_doubleverify/schemas/snapchat.json"))
    def __init__(self, config: Mapping[str, Any]):
        super().__init__(config=config, catalog_stream=self.catalog)
        
class BrandSafety(IncrementalDoubleverifyStream):
    primary_key = ""
    request_type = 1
    catalog = json.load(open("./source_doubleverify/schemas/brand_safety.json"))
    def __init__(self, config: Mapping[str, Any]):
        super().__init__(config=config, catalog_stream=self.catalog)
        
class Fraud(IncrementalDoubleverifyStream):
    primary_key = ""
    request_type = 1
    catalog = json.load(open("./source_doubleverify/schemas/fraud.json"))
    def __init__(self, config: Mapping[str, Any]):
        super().__init__(config=config, catalog_stream=self.catalog)
        
class GeoReport(IncrementalDoubleverifyStream):
    primary_key = ""
    request_type = 1
    catalog = json.load(open("./source_doubleverify/schemas/geo_report.json"))
    def __init__(self, config: Mapping[str, Any]):
        super().__init__(config=config, catalog_stream=self.catalog)
        
class Viewability(IncrementalDoubleverifyStream):
    primary_key = ""
    request_type = 1
    catalog = json.load(open("./source_doubleverify/schemas/viewability.json"))
    def __init__(self, config: Mapping[str, Any]):
        super().__init__(config=config, catalog_stream=self.catalog)
    

# Source
class SourceDoubleverify(AbstractSource):

    def check_connection(self, logger, config) -> Tuple[bool, any]:
        """
        Check connection to validate that the user-provided config can be used to connect to the DV Data API
        :param config:  the user-input config object conforming to the connector's spec.yaml
        :param logger:  logger object
        :return Tuple[bool, any]: (True, None) if the input config can be used to connect to the API successfully, (False, error) otherwise.
        """
        relative_path = "/requestTypes"
        url = DoubleverifyStream.url_base + relative_path
        request_header = Utils.get_request_header(config=config)
        try:
            req = requests.get(url=url, headers=request_header)
            if req.status_code == 200:
                return True, None
            return False, req.reason
        except Exception as e:
            return False, e


    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        """
        Available streams from the connector

        :param config: A Mapping of the user input configuration as defined in the connector spec.
        """
        return [
            Facebook(config),
            Pinterest(config),
            Snapchat(config),
            Twitter(config),
            Youtube(config),
            BrandSafety(config),
            Fraud(config),
            GeoReport(config),
            Viewability(config)
        ]
