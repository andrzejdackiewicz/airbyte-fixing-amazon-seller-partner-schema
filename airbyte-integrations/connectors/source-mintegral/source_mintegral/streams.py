import logging
import time
import csv
import io
from abc import ABC
from datetime import datetime, timedelta
from typing import Any, Iterable, Mapping, Optional, Union, List, Dict

import requests
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.sources.streams import IncrementalMixin
from airbyte_cdk.sources.streams.core import StreamData
from airbyte_cdk.sources.streams.http.auth import TokenAuthenticator
from airbyte_protocol.models import SyncMode


# Basic full refresh stream
class MintegralStream(HttpStream, ABC):
    page_size = 50
    url_base = "https://ss-api.mintegral.com/api/open/v1/"

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        response_json = response.json()["data"]
        if response_json["limit"] * response_json["page"] < response_json["total"]:
            return {"limit": self.page_size, "ext_fields": "creatives", "page": response_json["page"] + 1}
        return None

    def request_params(
            self,
            stream_state: Optional[Mapping[str, Any]],
            stream_slice: Optional[Mapping[str, Any]] = None,
            next_page_token: Optional[Mapping[str, Any]] = None,
    ):
        if next_page_token:
            return next_page_token
        else:
            return {
                "limit": self.page_size,
                "ext_fields": "creatives"
            }

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        if response.text == "":
            return '{}'
        yield from response.json()["data"]["list"]


class Offers(MintegralStream):
    primary_key = "campaign_id"

    def path(self, **kwargs) -> str:
        return "offers"


class Campaigns(MintegralStream):
    primary_key = "campaign_id"

    def path(self, **kwargs) -> str:
        return "campaign"


class MintegralReportingStream(HttpStream, IncrementalMixin):
    page_size = 500
    backfill_days = 2
    retry_delay = 10
    async_retries = 20
    url_base = "https://ss-api.mintegral.com/api/v2/reports/data"

    def __init__(self, authenticator: TokenAuthenticator, **kwargs):
        self._state = {}
        self.type = 1
        super().__init__(authenticator=authenticator)

    def log(self, message):
        print(message)  # oddly enough logger.debug() os not printed in airbyte logs, but prints are

    @property
    def cursor_field(self) -> Union[str, List[str]]:
        return "date"

    @property
    def state(self):
        return self._state

    @state.setter
    def state(self, value):
        self._state[self.cursor_field] = value[self.cursor_field]

    def next_page_token(self, response: requests.Response) -> Optional[Dict[str, Any]]:
        return None

    def request_params(
            self,
            stream_state: Optional[Mapping[str, Any]] = None,
            stream_slice: Optional[Mapping[str, Any]] = None,
            next_page_token: Optional[Mapping[str, Any]] = None,
    ):
        # Doc: https://adv-new.mintegral.com/doc/en/guide/report/advancedPerformanceReport.html
        request_params = {
            "start_time": '2024-06-12',
            "end_time": '2024-06-12',
            "timezone": "+0",
            "dimension_option": "Creative",
            "type": self.type
        }
        self.log(f"Request params: {request_params}")
        return request_params

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Dict]:
        try:
            json_response = response.json()
            print(f"Received json: {str(json_response)}")
            if json_response.get("code") in [200, 201, 202]:
                self.type = 2
                return None
        except ValueError:
            # Assume it's TSV data if not JSON
            tsv_data = response.text
            reader = csv.DictReader(io.StringIO(tsv_data), delimiter='\t')
            return reader
        return None
    """

    def stream_slices(
            self, *, sync_mode: SyncMode, cursor_field: Optional[List[str]] = None, stream_state: Optional[Mapping[str, Any]] = None
    ) -> Iterable[Optional[Mapping[str, Any]]]:
        end_date = datetime.now()
        state_date = end_date
        if sync_mode == SyncMode.incremental:
            if self.state and self.state.get(self.cursor_field):
                state_date = datetime.strptime(self.state[self.cursor_field], '%Y-%m-%dT%H:%M:%S.%f')

            self.state = {self.cursor_field: end_date}

        start_date = state_date - timedelta(days=self.backfill_days)

        num_days = (end_date - start_date).days
        dates = [(start_date + timedelta(days=i)).strftime('%Y-%m-%d') for i in range(num_days + 1)]

        self.log(f"Slices: {str(dates)}")
        for date in dates:
            yield {self.cursor_field: date}
            
    """

    def read_records(self, sync_mode: SyncMode, cursor_field=None, stream_slice=None, stream_state=None):
        retries = 0
        while retries < self.async_retries:
            response = requests.get(self.url_base, params=self.request_params(), headers=self.authenticator.get_auth_header())
            if response.status_code == 200:
                parsed_response = self.parse_response(response)
                if parsed_response:
                    for row in parsed_response:
                        yield row
                    break
                else:
                    time.sleep(self.retry_delay)
                    retries += 1
            else:
                raise Exception(f"Failed to fetch TSV data. HTTP Status Code: {response.status_code}")
        if retries == self.async_retries:
            raise Exception("Max retries exceeded. Could not fetch TSV data.")


class Reports(MintegralReportingStream):
    primary_key = ["uuid", "date"]

    def path(self, **kwargs) -> str:
        return ""
