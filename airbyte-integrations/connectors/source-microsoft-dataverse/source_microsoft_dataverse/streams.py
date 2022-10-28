#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#

from abc import ABC
from datetime import datetime
from typing import Any, Iterable, Mapping, MutableMapping, Optional
from urllib import parse

import requests
from airbyte_cdk.sources.streams import IncrementalMixin
from airbyte_cdk.sources.streams.http import HttpStream


# Basic full refresh stream
class MicrosoftDataverseStream(HttpStream, ABC):

    # Base url will be set by init(), using information provided by the user through config input
    url_base = ""

    def __init__(self, url, **kwargs):
        super().__init__(**kwargs)
        self.url_base = url + "/api/data/v9.2/"

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        """
        :param response: the most recent response from the API
        :return If there is another page in the result, a mapping (e.g: dict) containing information needed to query the next page in the response.
                If there are no more pages in the result, return None.
        """
        return None

    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        return {}

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        """
        :return an iterable containing each record in the response
        """
        for result in response.json()["value"]:
            yield result

    def request_headers(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> Mapping[str, Any]:
        return {"Cache-Control": "no-cache", "OData-Version": "4.0", "Content-Type": "application/json"}


# Basic incremental stream
class IncrementalMicrosoftDataverseStream(MicrosoftDataverseStream, IncrementalMixin, ABC):

    maxNumPages = 0
    numPagesRetrieved = 0
    odata_maxpagesize = 5000
    delta_token_field = "$deltatoken"
    update_field = "modifiedon"
    today_date = None
    primary_key = ""

    def __init__(self, url, stream_name, stream_path, schema, primary_key, max_num_pages, odata_maxpagesize, **kwargs):
        super().__init__(url, **kwargs)
        self._cursor_value = None
        self.stream_name = stream_name
        self.stream_path = stream_path
        self.primary_key = primary_key
        self.schema = schema
        self.maxNumPages = max_num_pages
        self.odata_maxpagesize = odata_maxpagesize

    state_checkpoint_interval = None

    @property
    def name(self) -> str:
        """Source name"""
        return self.stream_name

    def get_json_schema(self) -> Mapping[str, Any]:
        return self.schema

    @property
    def supports_incremental(self):
        return True

    @property
    def state(self) -> Mapping[str, Any]:
        return {self.delta_token_field: str(self._cursor_value)}

    @property
    def cursor_field(self) -> str:
        return "_ab_cdc_updated_at"  # Defaulting to the cdc field so normalization gets it

    # Sets the state got by state getter. "value" is the return of state getter -> dict
    @state.setter
    def state(self, value: Mapping[str, Any]):
        self._cursor_value = value[self.delta_token_field]

    def request_headers(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> Mapping[str, Any]:
        """
        Override to return any non-auth headers. Authentication headers will overwrite any overlapping headers returned from this method.
        """
        request_headers = super().request_headers(stream_state=stream_state)
        request_headers.update(
            {"Prefer": "odata.track-changes,odata.maxpagesize=" + str(self.odata_maxpagesize)}
        )  # odata.track-changes -> Header that enables change tracking
        return request_headers

    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        """
        :return a dict containing the parameters to be used in the request
        """
        request_params = super().request_params(stream_state)
        # If there is not a nextLink(contains "next_page_token") in the response, means it is the last page.
        # In this case, the deltatoken is passed instead.
        if next_page_token is None:
            request_params.update(stream_state)
            return request_params
        elif next_page_token is not None:
            request_params.update(next_page_token)
            return request_params

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        response_json = response.json()
        if "@odata.deltaLink" in response_json:
            delta_link = response_json["@odata.deltaLink"]
            delta_link_params = dict(parse.parse_qsl(parse.urlsplit(delta_link).query))
            self._cursor_value = delta_link_params[self.delta_token_field]
        for result in response_json["value"]:
            if "@odata.context" in result and result["reason"] == "deleted":
                result.update({self.primary_key[0][0]: result["id"]})
                result.pop("@odata.context", None)
                result.pop("id", None)
                result.pop("reason", None)
                result.update({"_ab_cdc_deleted_at": datetime.now().isoformat()})
            else:
                result.update({"_ab_cdc_updated_at": result[self.update_field]})

            yield result

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        """
        :param response: the most recent response from the API
        :return If there is another page in the result, a mapping (e.g: dict) containing information needed to query the next page in the response.
                If there are no more pages in the result, return None.
        """

        response_json = response.json()

        if "@odata.nextLink" in response_json:
            next_link = response_json["@odata.nextLink"]
            next_link_params = dict(parse.parse_qsl(parse.urlsplit(next_link).query))
            self.numPagesRetrieved += 1
            return next_link_params
        else:
            return None

    def path(
        self,
        *,
        stream_state: Mapping[str, Any] = None,
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> str:
        return self.stream_path
