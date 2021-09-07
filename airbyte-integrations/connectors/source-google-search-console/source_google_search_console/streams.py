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

import json
from abc import ABC
from typing import Any, Dict, Iterable, List, Mapping, MutableMapping, Optional, Union
from urllib.parse import quote_plus, unquote_plus

import pendulum
import requests
from airbyte_cdk.models import SyncMode
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.sources.streams.http.auth import HttpAuthenticator
from airbyte_cdk.sources.streams.http.auth.oauth import Oauth2Authenticator
from source_google_search_console.service_account_authenticator import ServiceAccountAuthenticator

BASE_URL = "https://www.googleapis.com/webmasters/v3"
GOOGLE_TOKEN_URI = "https://oauth2.googleapis.com/token"
ROW_LIMIT = 25000
CLIENT_AUTH = "Client"


class GoogleSearchConsole(HttpStream, ABC):
    url_base = BASE_URL
    primary_key = None
    data_field = ""

    def __init__(
        self,
        auth_type: str,
        site_urls: list,
        start_date: str,
        end_date: str,
        client_id: str = None,
        client_secret: str = None,
        refresh_token: str = None,
        service_account_info: str = None,
    ):
        super().__init__()
        self._auth_type = auth_type
        self._site_urls = self.sanitize_urls_list(site_urls)
        self._start_date = start_date
        self._end_date = end_date

        self._client_id = client_id
        self._client_secret = client_secret
        self._refresh_token = refresh_token

        self._service_account_info = service_account_info

    @staticmethod
    def sanitize_urls_list(site_urls: list) -> List[str]:
        return list(map(quote_plus, site_urls))

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        return None

    def stream_slices(
        self, sync_mode: SyncMode, cursor_field: List[str] = None, stream_state: Mapping[str, Any] = None
    ) -> Iterable[Optional[Mapping[str, Any]]]:
        for site_url in self._site_urls:
            yield {"site_url": site_url}

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        if not self.data_field:
            yield response.json()

        else:
            records = response.json().get(self.data_field) or []
            for record in records:
                yield record

    @property
    def authenticator(self) -> Union[HttpAuthenticator, None]:
        if self._auth_type == CLIENT_AUTH:
            return Oauth2Authenticator(
                token_refresh_endpoint=GOOGLE_TOKEN_URI,
                client_secret=self._client_secret,
                client_id=self._client_id,
                refresh_token=self._refresh_token,
            )

        else:
            info = json.loads(self._service_account_info)
            return ServiceAccountAuthenticator(service_account_info=info)


class Sites(GoogleSearchConsole):
    """
    API docs: https://developers.google.com/webmaster-tools/search-console-api-original/v3/sites
    """

    def path(
        self,
        stream_state: Mapping[str, Any] = None,
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> str:
        return f"/sites/{stream_slice.get('site_url')}"


class Sitemaps(GoogleSearchConsole):
    """
    API docs: https://developers.google.com/webmaster-tools/search-console-api-original/v3/sitemaps
    """

    data_field = "sitemap"

    def path(
        self,
        stream_state: Mapping[str, Any] = None,
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> str:
        return f"/sites/{stream_slice.get('site_url')}/sitemaps"


class SearchAnalytics(GoogleSearchConsole, ABC):
    """
    API docs: https://developers.google.com/webmaster-tools/search-console-api-original/v3/searchanalytics
    """

    data_field = "rows"
    start_row = 0
    dimensions = []
    search_types = ["web", "news", "image", "video"]

    def path(
        self,
        stream_state: Mapping[str, Any] = None,
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> str:
        return f"/sites/{stream_slice.get('site_url')}/searchAnalytics/query"

    @property
    def cursor_field(self) -> Union[str, List[str]]:
        return "date"

    @property
    def http_method(self) -> str:
        return "POST"

    def stream_slices(
        self, sync_mode: SyncMode, cursor_field: List[str] = None, stream_state: Mapping[str, Any] = None
    ) -> Iterable[Optional[Mapping[str, Any]]]:
        for site_url in self._site_urls:
            for search_type in self.search_types:
                yield {"site_url": site_url, "search_type": search_type}

    def next_page_token(self, response: requests.Response) -> Optional[bool]:
        if len(response.json().get(self.data_field, [])) == ROW_LIMIT:
            self.start_row += ROW_LIMIT
            return True

        self.start_row = 0

    def request_headers(self, **kwargs) -> Mapping[str, Any]:
        return {"Content-Type": "application/json"}

    def request_body_json(
        self,
        stream_state: Mapping[str, Any] = None,
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> Optional[Union[Dict[str, Any], str]]:

        start_date = self._get_start_data(stream_state, stream_slice)

        data = {
            "startDate": start_date,
            "endDate": self._end_date,
            "dimensions": self.dimensions,
            "searchType": stream_slice.get("search_type"),
            "aggregationType": "auto",
            "startRow": self.start_row,
            "rowLimit": ROW_LIMIT,
        }
        return data

    def _get_start_data(
        self,
        stream_state: Mapping[str, Any] = None,
        stream_slice: Mapping[str, Any] = None,
    ) -> str:
        start_date = self._start_date

        if start_date and stream_state:
            if stream_state.get(unquote_plus(stream_slice["site_url"]), {}).get(stream_slice["search_type"]):
                stream_state_value = stream_state.get(unquote_plus(stream_slice["site_url"]), {}).get(stream_slice["search_type"])

                start_date = max(
                    pendulum.parse(stream_state_value[self.cursor_field]),
                    pendulum.parse(start_date),
                ).to_date_string()

        return start_date

    def parse_response(
        self,
        response: requests.Response,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> Iterable[Mapping]:
        records = response.json().get(self.data_field) or []

        for record in records:
            record["site_url"] = unquote_plus(stream_slice.get("site_url"))
            record["search_type"] = stream_slice.get("search_type")

            for dimension in self.dimensions:
                record[dimension] = record["keys"].pop(0)

            # remove unnecessary empty field
            record.pop("keys")

            yield record

    def get_updated_state(
        self,
        current_stream_state: MutableMapping[str, Any],
        latest_record: Mapping[str, Any],
    ) -> Mapping[str, Any]:
        latest_benchmark = latest_record[self.cursor_field]

        site_url = latest_record.get("site_url")
        search_type = latest_record.get("search_type")

        if current_stream_state.get(site_url, {}).get(search_type):
            current_stream_state[site_url][search_type] = {self.cursor_field: max(latest_benchmark, current_stream_state[site_url][search_type][self.cursor_field])}

        elif current_stream_state.get(site_url):
            current_stream_state[site_url][search_type] = {self.cursor_field: latest_benchmark}

        else:
            current_stream_state = {site_url: {search_type: {self.cursor_field: latest_benchmark}}}

        return current_stream_state


class SearchAnalyticsByDate(SearchAnalytics):
    dimensions = ["date"]


class SearchAnalyticsByCountry(SearchAnalytics):
    dimensions = ["date", "country"]


class SearchAnalyticsByDevice(SearchAnalytics):
    dimensions = ["date", "device"]


class SearchAnalyticsByPage(SearchAnalytics):
    dimensions = ["date", "page"]


class SearchAnalyticsByQuery(SearchAnalytics):
    dimensions = ["date", "query"]


class SearchAnalyticsAllFields(SearchAnalytics):
    dimensions = ["date", "country", "device", "page", "query"]
