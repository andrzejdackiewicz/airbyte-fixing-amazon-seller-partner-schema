#
# Copyright (c) 2020 Airbyte, Inc., all rights reserved.
#


from abc import ABC
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple

import requests
from airbyte_cdk.models import SyncMode
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.sources.streams.http.auth import HttpAuthenticator


class BigcommerceStream(HttpStream, ABC):
    # Latest Stable Release
    api_version = "v3"
    # Page size
    limit = 10
    # Define primary key as sort key for full_refresh, or very first sync for incremental_refresh
    primary_key = "id"
    order_field = "date_created:asc"
    filter_field = "date_modified:min"
    data = "data"

    def __init__(self, start_date: str, store_hash: str, access_token: str, **kwargs):
        super().__init__(**kwargs)
        self.start_date = start_date
        self.store_hash = store_hash
        self.access_token = access_token

    @property
    def url_base(self) -> str:
        return f"https://api.bigcommerce.com/stores/{self.store_hash}/{self.api_version}/"

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        json_response = response.json()
        meta = json_response.get("meta", None)
        if meta:
            pagination = meta.get("pagination", None)
            if pagination and pagination.get("current_page") < pagination.get("total_pages"):
                return dict(page=pagination.get("current_page") + 1)
            else:
                return None

    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        params = {"limit": self.limit}
        if next_page_token:
            params.update(**next_page_token)
        else:
            params.update({"sort": self.order_field})
        return params

    def request_headers(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> Mapping[str, Any]:
        headers = super().request_headers(stream_state=stream_state, stream_slice=stream_slice, next_page_token=next_page_token)
        headers.update({"Accept": "application/json", "Content-Type": "application/json"})
        return headers

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        json_response = response.json()
        records = json_response.get(self.data, []) if self.data is not None else json_response
        yield from records


class IncrementalBigcommerceStream(BigcommerceStream, ABC):
    # Getting page size as 'limit' from parent class
    @property
    def limit(self):
        return super().limit

    # Setting the check point interval to the limit of the records output
    state_checkpoint_interval = limit
    # Setting the default cursor field for all streams
    cursor_field = "date_modified"

    def get_updated_state(self, current_stream_state: MutableMapping[str, Any], latest_record: Mapping[str, Any]) -> Mapping[str, Any]:
        return {self.cursor_field: max(latest_record.get(self.cursor_field, ""), current_stream_state.get(self.cursor_field, ""))}

    def request_params(self, stream_state: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None, **kwargs):
        params = super().request_params(stream_state=stream_state, next_page_token=next_page_token, **kwargs)
        # If there is a next page token then we should only send pagination-related parameters.
        if not next_page_token:
            params["sort"] = f"{self.order_field}"
            params["limit"] = f"{self.limit}"
            if stream_state:
                params[self.filter_field] = stream_state.get(self.cursor_field)
        return params

    def filter_records_newer_than_state(self, stream_state: Mapping[str, Any] = None, records_slice: Mapping[str, Any] = None) -> Iterable:
        if stream_state:
            for record in records_slice:
                if record[self.cursor_field] >= stream_state.get(self.cursor_field):
                    yield record
        else:
            yield from records_slice


class OrderSubstream(IncrementalBigcommerceStream):
    def read_records(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Optional[Mapping[str, Any]] = None, **kwargs
    ) -> Iterable[Mapping[str, Any]]:
        orders_stream = Orders(
            authenticator=self.authenticator, start_date=self.start_date, store_hash=self.store_hash, access_token=self.access_token
        )
        for data in orders_stream.read_records(sync_mode=SyncMode.full_refresh):
            slice = super().read_records(stream_slice={"order_id": data["id"]}, **kwargs)
            yield from self.filter_records_newer_than_state(stream_state=stream_state, records_slice=slice)


class Customers(IncrementalBigcommerceStream):
    data_field = "customers"

    def path(self, **kwargs) -> str:
        return f"{self.data_field}"


class Orders(IncrementalBigcommerceStream):
    data_field = "orders"
    api_version = "v2"
    order_field = "id:asc"
    filter_field = "min_date_modified"
    page = 1

    def path(self, **kwargs) -> str:
        return f"{self.data_field}"

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        return response.json() if len(response.content) > 0 else []

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        if len(response.content) > 0 and len(response.json()) == self.limit:
            self.page = self.page + 1
            return dict(page=self.page)
        else:
            return None


class Pages(IncrementalBigcommerceStream):
    data_field = "pages"
    cursor_field = "id"

    def path(self, **kwargs) -> str:
        return f"content/{self.data_field}"

    def get_updated_state(self, current_stream_state: MutableMapping[str, Any], latest_record: Mapping[str, Any]) -> Mapping[str, Any]:
        return {self.cursor_field: max(latest_record.get(self.cursor_field, 0), current_stream_state.get(self.cursor_field, 0))}

    def request_params(
        self, stream_state: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None, **kwargs
    ) -> MutableMapping[str, Any]:
        params = {"limit": self.limit}
        return params

    def read_records(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Optional[Mapping[str, Any]] = None, **kwargs
    ) -> Iterable[Mapping[str, Any]]:
        slice = super().read_records(sync_mode=SyncMode.full_refresh, stream_slice=stream_slice, stream_state=stream_state)
        yield from self.filter_records_newer_than_state(stream_state=stream_state, records_slice=slice)


class Transactions(OrderSubstream):
    data_field = "transactions"
    cursor_field = "id"

    def path(self, stream_slice: Mapping[str, Any] = None, **kwargs) -> str:
        order_id = stream_slice["order_id"]
        return f"orders/{order_id}/{self.data_field}"

    def get_updated_state(self, current_stream_state: MutableMapping[str, Any], latest_record: Mapping[str, Any]) -> Mapping[str, Any]:
        return {self.cursor_field: max(latest_record.get(self.cursor_field, 0), current_stream_state.get(self.cursor_field, 0))}

    def request_params(
        self, stream_state: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None, **kwargs
    ) -> MutableMapping[str, Any]:
        params = {"limit": self.limit}
        return params


class BigcommerceAuthenticator(HttpAuthenticator):
    def __init__(self, token: str):
        self.token = token

    def get_auth_header(self) -> Mapping[str, Any]:
        return {"X-Auth-Token": f"{self.token}"}


class SourceBigcommerce(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, any]:
        store_hash = config["store_hash"]
        access_token = config["access_token"]
        api_version = "v3"

        headers = {"X-Auth-Token": access_token, "Accept": "application/json", "Content-Type": "application/json"}
        url = f"https://api.bigcommerce.com/stores/{store_hash}/{api_version}/channels"

        try:
            session = requests.get(url, headers=headers)
            session.raise_for_status()
            return True, None
        except requests.exceptions.RequestException as e:
            return False, e

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:

        auth = BigcommerceAuthenticator(token=config["access_token"])
        args = {
            "authenticator": auth,
            "start_date": config["start_date"],
            "store_hash": config["store_hash"],
            "access_token": config["access_token"],
        }
        return [
            Customers(**args),
            Pages(**args),
            Orders(**args),
            Transactions(**args),
        ]
