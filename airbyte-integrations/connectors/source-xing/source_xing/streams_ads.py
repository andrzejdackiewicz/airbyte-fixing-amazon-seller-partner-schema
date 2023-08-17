#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

from typing import Any, Iterable, List, Mapping, MutableMapping, Optional

from airbyte_cdk.models import SyncMode
from airbyte_cdk.sources.streams.http import HttpSubStream

from .streams import XingStream
from .streams_customers import Customers

DEFAULT_COLS = ["name", "type", "status", "start_at", "end_at", "bid", "bidding_mode", "daily_budget", "total_budget", "target_url"]


class Ads(XingStream, HttpSubStream):
    parent = Customers
    primary_key = "id"

    def __init__(self, authenticator, config: Mapping[str, Any], **kwargs):
        super().__init__(config=config, authenticator=authenticator, parent=self.parent)

    def stream_slices(
        self, sync_mode: SyncMode.incremental, cursor_field: List[str] = None, stream_state: Mapping[str, Any] = None
    ) -> Iterable[Optional[Mapping[str, Any]]]:
        parent = self.parent(self._authenticator, self.config)
        parent_stream_slices = parent.stream_slices(sync_mode=SyncMode.incremental, cursor_field=cursor_field, stream_state=stream_state)
        for stream_slice in parent_stream_slices:
            parent_records = parent.read_records(
                sync_mode=SyncMode.incremental, cursor_field=cursor_field, stream_slice=stream_slice, stream_state=stream_state
            )
            for record in parent_records:
                yield {"customer_id": record.get("id")}

    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        params = {}
        if self.config.get("ads"):
            ads = self.config.get("ads")
            if ads.get("fields"):
                params["fields"] = ",".join(ads.get("fields"))
            else:
                params["fields"] = ",".join(DEFAULT_COLS)
            if ads.get("ad_ids"):
                params["filtering.id"] = ",".join(ads.get("ad_ids"))
            return params
        else:
            params["fields"] = ",".join(DEFAULT_COLS)
        return params

    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        return f"customers/{stream_slice['customer_id']}/ads"
