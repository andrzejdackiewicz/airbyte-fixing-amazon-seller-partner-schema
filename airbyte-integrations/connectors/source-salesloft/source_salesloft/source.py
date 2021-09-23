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


from abc import ABC
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple

import requests
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.sources.streams.http.auth import TokenAuthenticator
from airbyte_cdk.sources.streams.http.auth.core import HttpAuthenticator


# Basic full refresh stream
class SalesloftStream(HttpStream, ABC):

    url_base = "https://api.salesloft.com/v2/"

    def __init__(
        self,
        authenticator: HttpAuthenticator,
        start_date: str = None,
        **kwargs,
    ):
        self.start_date = start_date
        super().__init__(authenticator=authenticator)

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        next_page = response.json()['metadata']['paging'].get('next_page')
        return None if not next_page else {'page': next_page}

    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        the_params = {'per_page': 100, 'page': 1}
        if next_page_token and 'page' in next_page_token:
            the_params['page'] = next_page_token['page']
        return the_params

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        data = response.json().get('data')
        if not data:
            return
        for element in data:
            yield element


class Users(SalesloftStream):

    primary_key = 'id'

    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        return 'users.json'


# Basic incremental stream
class IncrementalSalesloftStream(SalesloftStream, ABC):

    @property
    def cursor_field(self) -> str:
        return 'updated_at'

    def get_updated_state(self, current_stream_state: MutableMapping[str, Any], latest_record: Mapping[str, Any]) -> Mapping[str, Any]:
        current_stream_state = current_stream_state or {}

        current_stream_state_date = current_stream_state.get(self.cursor_field, self.start_date)
        latest_record_date = latest_record.get(self.cursor_field, self.start_date)

        return {self.cursor_field: max(current_stream_state_date, latest_record_date)}
    
    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        the_params = super().request_params(stream_state=stream_state, stream_slice=stream_slice, next_page_token=next_page_token)
        if self.cursor_field in stream_state:
            the_params['updated_at[gte]'] = stream_state[self.cursor_field]
        return the_params


class People(IncrementalSalesloftStream):

    cursor_field = "updated_at"

    primary_key = "id"

    def path(self, **kwargs) -> str:
        return "people.json"


class Cadences(IncrementalSalesloftStream):

    cursor_field = "updated_at"

    primary_key = "id"

    def path(self, **kwargs) -> str:
        return "cadences.json"


class CadenceMemberships(IncrementalSalesloftStream):

    cursor_field = "updated_at"

    primary_key = "id"

    def path(self, **kwargs) -> str:
        return "cadence_memberships.json"


# Source
class SourceSalesloft(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, any]:
        try:
            api_key = config['api_key']
            response = requests.get('https://api.salesloft.com/v2/me.json', headers={'Authorization': f'Bearer {api_key}'})
            response.raise_for_status()
            return True, None
        except Exception as e:
            return False, e

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        auth = TokenAuthenticator(token=config['api_key'])  # Oauth2Authenticator is also available if you need oauth support
        return [
            Cadences(authenticator=auth, **config),
            CadenceMemberships(authenticator=auth, **config),
            People(authenticator=auth, **config),
            Users(authenticator=auth, **config)
        ]
