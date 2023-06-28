#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import datetime
from dataclasses import InitVar, dataclass
from typing import Any, ClassVar, Iterable, Mapping, MutableMapping, Optional, Union

from airbyte_cdk.models import SyncMode
from airbyte_cdk.sources.declarative.incremental import Cursor
from airbyte_cdk.sources.declarative.types import Record, StreamSlice, StreamState
from airbyte_cdk.sources.streams.core import Stream


@dataclass
class GreenHouseSlicer(Cursor):
    parameters: InitVar[Mapping[str, Any]]
    cursor_field: str
    request_cursor_field: str

    START_DATETIME: ClassVar[str] = "1970-01-01T00:00:00.000Z"
    DATETIME_FORMAT: ClassVar[str] = "%Y-%m-%dT%H:%M:%S.%fZ"

    def __post_init__(self, parameters: Mapping[str, Any]):
        self._state = {}

    def stream_slices(self) -> Iterable[StreamSlice]:
        yield {self.request_cursor_field: self._state.get(self.cursor_field, self.START_DATETIME)}

    def _max_dt_str(self, *args: str) -> Optional[str]:
        new_state_candidates = list(map(lambda x: datetime.datetime.strptime(x, self.DATETIME_FORMAT), filter(None, args)))
        if not new_state_candidates:
            return
        max_dt = max(new_state_candidates)
        # `.%f` gives us microseconds, we need milliseconds
        (dt, micro) = max_dt.strftime(self.DATETIME_FORMAT).split(".")
        return "%s.%03dZ" % (dt, int(micro[:-1:]) / 1000)

    def set_initial_state(self, stream_state: StreamState) -> None:
        cursor_value = stream_state.get(self.cursor_field)
        if cursor_value:
            self._state[self.cursor_field] = cursor_value

    def close_slice(self, stream_slice: StreamSlice, last_record: Optional[Record]) -> None:
        current_state = self._state.get(self.cursor_field)
        last_cursor = last_record and last_record[self.cursor_field]
        max_dt = self._max_dt_str(current_state, last_cursor)
        if not max_dt:
            return
        self._state[self.cursor_field] = max_dt

    def should_be_synced(self, record: Record) -> bool:
        return self._parse_to_datetime(record[self.cursor_field]) >= self._parse_to_datetime(self._state.get(self.cursor_field, self.START_DATETIME))

    def _parse_to_datetime(self, datetime_str: str) -> datetime.datetime:
        return datetime.datetime.strptime(datetime_str, self.DATETIME_FORMAT)

    def get_stream_state(self) -> StreamState:
        return self._state

    def get_request_params(
        self,
        *,
        stream_state: Optional[StreamState] = None,
        stream_slice: Optional[StreamSlice] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> MutableMapping[str, Any]:
        return stream_slice or {}

    def get_request_headers(self, *args, **kwargs) -> Mapping[str, Any]:
        return {}

    def get_request_body_data(self, *args, **kwargs) -> Optional[Union[Mapping, str]]:
        return {}

    def get_request_body_json(self, *args, **kwargs) -> Optional[Mapping]:
        return {}


@dataclass
class GreenHouseSubstreamSlicer(GreenHouseSlicer):
    parent_stream: Stream
    stream_slice_field: str
    parent_key: str

    def stream_slices(self) -> Iterable[StreamSlice]:
        for parent_stream_slice in self.parent_stream.stream_slices(
            sync_mode=SyncMode.full_refresh, cursor_field=None, stream_state=self.get_stream_state()
        ):
            for parent_record in self.parent_stream.read_records(
                sync_mode=SyncMode.full_refresh, cursor_field=None, stream_slice=parent_stream_slice, stream_state=None
            ):
                parent_state_value = parent_record.get(self.parent_key)
                yield {
                    self.stream_slice_field: parent_state_value,
                    self.request_cursor_field: self._state.get(str(parent_state_value), {}).get(self.cursor_field, self.START_DATETIME),
                }

    def set_initial_state(self, stream_state: StreamState) -> None:
        if self.stream_slice_field in stream_state:
            return
        substream_ids = map(lambda x: str(x), set(stream_state.keys()) | set(self._state.keys()))
        for id_ in substream_ids:
            self._state[id_] = {
                self.cursor_field: self._max_dt_str(
                    stream_state.get(id_, {}).get(self.cursor_field), self._state.get(id_, {}).get(self.cursor_field)
                )
            }

    def close_slice(self, stream_slice: StreamSlice, last_record: Optional[Record]) -> None:
        if last_record:
            substream_id = str(stream_slice[self.stream_slice_field])
            current_state = self._state.get(substream_id, {}).get(self.cursor_field)
            last_state = last_record[self.cursor_field]
            max_dt = self._max_dt_str(last_state, current_state)
            self._state[substream_id] = {self.cursor_field: max_dt}
            return

    def should_be_synced(self, record: Record) -> bool:
        state_cursor = self._state.get(record.associated_slice[self.stream_slice_field], {}).get(self.cursor_field)
        return not state_cursor or self._parse_to_datetime(state_cursor) > self._parse_to_datetime(record[self.cursor_field])

    def get_request_params(
        self,
        *,
        stream_state: Optional[StreamState] = None,
        stream_slice: Optional[StreamSlice] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> MutableMapping[str, Any]:
        # ignore other fields in a slice
        return {self.request_cursor_field: stream_slice[self.request_cursor_field]}
