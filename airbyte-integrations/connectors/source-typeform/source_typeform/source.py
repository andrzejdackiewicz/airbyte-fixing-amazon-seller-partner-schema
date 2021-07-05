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


import urllib.parse as urlparse
from abc import ABC, abstractmethod
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple
from urllib.parse import parse_qs

import pendulum
import requests
from airbyte_cdk import AirbyteLogger
from airbyte_cdk.models import SyncMode
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.sources.streams.http.auth import TokenAuthenticator
from pendulum.datetime import DateTime


class TypeformStream(HttpStream, ABC):
    url_base = "https://api.typeform.com"
    limit: int = 200
    date_format: str = "YYYY-MM-DDTHH:mm:ss[Z]"

    def __init__(self, **kwargs: Mapping[str, Any]):
        super().__init__(authenticator=kwargs["authenticator"])
        self.config: Mapping[str, Any] = kwargs
        self.start_date: DateTime = pendulum.from_format(kwargs["start_date"], self.date_format)

        # changes page limit, this param is using for development and debugging
        if kwargs.get("page_size"):
            self.limit = kwargs.get("page_size")

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        return None

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        for item in response.json()["items"]:
            yield item


class TrimForms(TypeformStream):
    """
    This stream is responsible for fetching list of from_id(s) which required to process data from Forms and Responses.
    API doc: https://developer.typeform.com/create/reference/retrieve-forms/
    """

    primary_key = "id"
    limit: int = 200

    def path(
        self,
        stream_state: Mapping[str, Any] = None,
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> str:
        return "/forms"

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        page = self.get_current_page_token(response.url)
        # stop pagination if current page equals to total pages
        return None if response.json()["page_count"] <= page else page + 1

    def get_current_page_token(self, url: str) -> Optional[int]:
        """
        Fetches page query parameter from URL
        """
        parsed = urlparse.urlparse(url)
        page = parse_qs(parsed.query).get("page")
        return int(page[0]) if page else None

    def request_params(
        self,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> MutableMapping[str, Any]:
        params = {"page_size": self.limit}
        params["page"] = next_page_token if next_page_token else 1
        return params


class StreamMixin:
    def stream_slices(self, **kwargs) -> Iterable[Optional[Mapping[str, any]]]:
        for item in TrimForms(**self.config).read_records(sync_mode=SyncMode.full_refresh):
            yield {"form_id": item["id"]}

        yield from []


class Forms(StreamMixin, TypeformStream):
    """
    This stream is responsible for detailed information about Form.
    API doc: https://developer.typeform.com/create/reference/retrieve-form/
    """

    primary_key = "id"

    def path(
        self,
        stream_state: Mapping[str, Any] = None,
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> str:
        return f"/forms/{stream_slice['form_id']}"

    def request_params(
        self,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> MutableMapping[str, Any]:
        return {}

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        yield response.json()


class IncrementalTypeformStream(TypeformStream, ABC):
    cursor_field: str = "submitted_at"
    token_field: str = "token"

    @property
    def limit(self):
        return super().limit

    state_checkpoint_interval = limit

    @abstractmethod
    def get_updated_state(
        self,
        current_stream_state: MutableMapping[str, Any],
        latest_record: Mapping[str, Any],
    ) -> Mapping[str, Any]:
        pass

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        items = response.json()["items"]
        if items and len(items) == self.limit:
            return items[-1][self.token_field]
        return None


class Responses(StreamMixin, IncrementalTypeformStream):
    """
    This stream is responsible for fetching responses for particulat form_id.
    API doc: https://developer.typeform.com/responses/reference/retrieve-responses/
    """

    primary_key = "response_id"
    limit: int = 1000

    def path(self, stream_slice: Optional[Mapping[str, Any]] = None, **kwargs) -> str:
        return f"/forms/{stream_slice['form_id']}/responses"

    def get_form_id(self, record: Mapping[str, Any]) -> Optional[str]:
        """
        Fetches form id to which current record belongs.
        """
        referer = record.get("metadata", {}).get("referer")
        return referer.rsplit("/")[-1] if referer else None

    def get_updated_state(
        self,
        current_stream_state: MutableMapping[str, Any],
        latest_record: Mapping[str, Any],
    ) -> Mapping[str, Any]:
        form_id = self.get_form_id(latest_record)
        if not form_id or not latest_record.get(self.cursor_field):
            return current_stream_state

        current_stream_state[form_id] = current_stream_state.get(form_id, {})
        current_stream_state[form_id][self.cursor_field] = max(
            pendulum.from_format(latest_record[self.cursor_field], self.date_format).int_timestamp,
            current_stream_state[form_id].get(self.cursor_field, 1),
        )
        return current_stream_state

    def request_params(
        self,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> MutableMapping[str, Any]:
        params = {"page_size": self.limit}
        stream_state = stream_state or {}

        if not next_page_token:
            # use state for first request in incremental sync
            params["sort"] = "submitted_at,asc"
            # start from last state or from start date
            since = max(self.start_date.int_timestamp, stream_state.get(stream_slice["form_id"], {}).get(self.cursor_field, 1))
            if since:
                params["since"] = pendulum.from_timestamp(since).format(self.date_format)
        else:
            # use response token for pagination after first request
            # this approach allow to avoid data duplication whithin single sync
            params["after"] = next_page_token

        return params


class SourceTypeform(AbstractSource):
    def check_connection(self, logger: AirbyteLogger, config: Mapping[str, Any]) -> Tuple[bool, any]:
        try:
            url = f"{TypeformStream.url_base}/forms"
            auth_headers = {"Authorization": f"Bearer {config['token']}"}
            session = requests.get(url, headers=auth_headers)
            session.raise_for_status()
            return True, None
        except requests.exceptions.RequestException as e:
            return False, e

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        auth = TokenAuthenticator(token=config["token"])
        return [Forms(authenticator=auth, **config), Responses(authenticator=auth, **config)]
