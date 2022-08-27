import time
import requests
from datetime import datetime
from typing import Any, Mapping, Iterable, Optional, MutableMapping

from airbyte_cdk.sources.streams.http import HttpStream


class QueueWaitingRoomsStream(HttpStream):
    primary_key = "id"
    cursor_field = "eventDate"
    url_base = "https://prod-main-net-dashboard-api.azurewebsites.net"
    
    def __init__(self, config: Mapping[str, Any], **_):
        super().__init__()
        self.company_id = config["company_id"]
        self.latest_stream_timestamp = config["start_datetime"]

    def request_headers(self, **_) -> Mapping[str, Any]:
        return {}

    def request_params(self, stream_state: Mapping[str, Any], **_) -> MutableMapping[str, Any]:
        if stream_state:
            return {"startDate": self._cursor_value}
        return {"startDate": self.latest_stream_timestamp}

    def parse_response(self, response: requests.Response, **_) -> Iterable[Mapping]:

        self._cursor_value = response.json()[0]["eventDate"][:26]

        if response.status_code != 200:
            return []
        yield from response.json()

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        return None

    @property
    def state(self) -> Mapping[str, Any]:
        return {self.cursor_field: self._cursor_value}

    @state.setter
    def state(self, value: Mapping[str, Any]):
        self._cursor_value = value["eventDate"]


class Events(QueueWaitingRoomsStream):
    def path(self, **_) -> str:
        return f"api/company/{self.company_id}/search"
