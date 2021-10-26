from datetime import datetime, timedelta
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple
from abc import abstractmethod

import base64, math

import requests
from airbyte_cdk.models import SyncMode
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.sources.streams.http.auth import HttpAuthenticator, TokenAuthenticator

class SourceRetently(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, any]:
        try:
            api_key = config["api_key"]
            auth_method = f"api_key={api_key}"
            auth = TokenAuthenticator(token="", auth_method=auth_method)
            stream = Companies(auth)
            records = stream.read_records(sync_mode=SyncMode.full_refresh)
            next(records)
            return True, None
        except Exception as e:
            return False, repr(e)

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        api_key = config["api_key"]
        auth_method = f"api_key={api_key}"
        auth = TokenAuthenticator(token="", auth_method=auth_method)
        return [ Customers(auth), Companies(auth), Reports(auth) ]

class RetentlyStream(HttpStream):
    primary_key = None

    @property
    def url_base(self) -> str:
        return "https://app.retently.com/api/v2/"

    @property
    @abstractmethod
    def json_path(self):
        pass

    def parse_response(
            self,
            response: requests.Response,
            stream_state: Mapping[str, Any],
            stream_slice: Mapping[str, Any] = None,
            next_page_token: Mapping[str, Any] = None,
    ) -> Iterable[Mapping]:
        data = response.json().get("data")
        if data:
            stream_data = data.get(self.json_path) if self.json_path else data
            if stream_data:
                for d in stream_data:
                    yield d

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        json = response.json().get("data", dict())
        total = json.get("total")
        limit = json.get("limit")
        page = json.get("page")
        if total and limit and page:
            pages = math.ceil(total / limit)
            if page < pages:
                return { "page": page+1 }
        return None
    
    def request_params(
            self,
            stream_state: Mapping[str, Any],
            stream_slice: Mapping[str, Any] = None,
            next_page_token: Mapping[str, Any] = None,
    ) -> MutableMapping[str, Any]:
        return next_page_token

class Customers(RetentlyStream):
    json_path = "subscribers"
    def path(
            self,
            stream_state: Mapping[str, Any] = None,
            stream_slice: Mapping[str, Any] = None,
            next_page_token: Mapping[str, Any] = None
    ) -> str:
        return "nps/customers"

class Companies(RetentlyStream):
    json_path = "companies"
    def path(
            self,
            stream_state: Mapping[str, Any] = None,
            stream_slice: Mapping[str, Any] = None,
            next_page_token: Mapping[str, Any] = None
    ) -> str:
        return "companies"

class Reports(RetentlyStream):
    json_path = None
    def path(
            self,
            stream_state: Mapping[str, Any] = None,
            stream_slice: Mapping[str, Any] = None,
            next_page_token: Mapping[str, Any] = None
    ) -> str:
        return "reports"

    ### does not support pagination
    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        return None
