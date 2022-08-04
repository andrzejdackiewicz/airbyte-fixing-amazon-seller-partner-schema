#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#

import datetime
import re
from dataclasses import InitVar, dataclass, field
from typing import Any, Iterable, Mapping, Optional

import dateutil
from airbyte_cdk.models import SyncMode
from airbyte_cdk.sources.declarative.datetime.min_max_datetime import MinMaxDatetime
from airbyte_cdk.sources.declarative.interpolation.interpolated_string import InterpolatedString
from airbyte_cdk.sources.declarative.interpolation.jinja import JinjaInterpolation
from airbyte_cdk.sources.declarative.requesters.request_option import RequestOption, RequestOptionType
from airbyte_cdk.sources.declarative.stream_slicers.stream_slicer import StreamSlicer
from airbyte_cdk.sources.declarative.types import Config, Record, StreamSlice, StreamState
from dataclasses_jsonschema import JsonSchemaMixin


@dataclass
class DatetimeStreamSlicer(StreamSlicer, JsonSchemaMixin):
    """
    Slices the stream over a datetime range.

    Given a start time, end time, a step function, and an optional lookback window,
    the stream slicer will partition the date range from start time - lookback window to end time.

    The step function is defined as a string of the form:
    `"<number><unit>"`

    where unit can be one of
    - weeks, w
    - days, d

    For example, "1d" will produce windows of 1 day, and 2weeks windows of 2 weeks.

    Attributes:
        start_datetime (MinMaxDatetime): the datetime that determines the earliest record that should be synced
        end_datetime (MinMaxDatetime): the datetime that determines the last record that should be synced
        step (str): size of the timewindow
        cursor_field (InterpolatedString): record's cursor field
        datetime_format (str): format of the datetime
        config (Config): connection config
        start_time_option (Optional[RequestOption]): request option for start time
        end_time_option (Optional[RequestOption]): request option for end time
        stream_state_field_start (Optional[str]): stream slice start time field
        stream_state_field_end (Optional[str]): stream slice end time field
        lookback_window (Optional[InterpolatedString]): how many days before start_datetime to read data for
    """

    start_datetime: MinMaxDatetime
    end_datetime: MinMaxDatetime
    step: str
    cursor_field: InterpolatedString
    datetime_format: str
    config: Config
    options: InitVar[Mapping[str, Any]]
    _cursor: dict = field(repr=False, default=None)  # tracks current datetime
    _cursor_end: dict = field(repr=False, default=None)  # tracks end of current stream slice
    start_time_option: Optional[RequestOption] = None
    end_time_option: Optional[RequestOption] = None
    stream_state_field_start: Optional[str] = None
    stream_state_field_end: Optional[str] = None
    lookback_window: Optional[InterpolatedString] = None

    timedelta_regex = re.compile(r"((?P<weeks>[\.\d]+?)w)?" r"((?P<days>[\.\d]+?)d)?$")

    def __post_init__(self, options: Mapping[str, Any]):
        self._timezone = datetime.timezone.utc
        self._interpolation = JinjaInterpolation()

        self._step = self._parse_timedelta(self.step)
        self.cursor_field = InterpolatedString.create(self.cursor_field, options=options)
        self.stream_slice_field_start = InterpolatedString.create(self.stream_state_field_start or "start_date", options=options)
        self.stream_slice_field_end = InterpolatedString.create(self.stream_state_field_end or "end_date", options=options)

        # If datetime format is not specified then start/end datetime should inherit it from the stream slicer
        if not self.start_datetime.datetime_format:
            self.start_datetime.datetime_format = self.datetime_format
        if not self.end_datetime.datetime_format:
            self.end_datetime.datetime_format = self.datetime_format

        if self.start_time_option and self.start_time_option.inject_into == RequestOptionType.path:
            raise ValueError("Start time cannot be passed by path")
        if self.end_time_option and self.end_time_option.inject_into == RequestOptionType.path:
            raise ValueError("End time cannot be passed by path")

    def get_stream_state(self) -> StreamState:
        return {self.cursor_field.eval(self.config): self._cursor} if self._cursor else {}

    def update_cursor(self, stream_slice: StreamSlice, last_record: Optional[Record] = None):
        """
        Update the cursor value to the max datetime between the last record, the start of the stream_slice, and the current cursor value.
        Update the cursor_end value with the stream_slice's end time.

        :param stream_slice: current stream slice
        :param last_record: last record read
        :return: None
        """
        stream_slice_value = stream_slice.get(self.cursor_field.eval(self.config))
        stream_slice_value_end = stream_slice.get(self.stream_slice_field_end.eval(self.config))
        last_record_value = last_record.get(self.cursor_field.eval(self.config)) if last_record else None
        cursor = None
        if stream_slice_value and last_record_value:
            cursor = max(stream_slice_value, last_record_value)
        elif stream_slice_value:
            cursor = stream_slice_value
        else:
            cursor = last_record_value
        if self._cursor and cursor:
            self._cursor = max(cursor, self._cursor)
        elif cursor:
            self._cursor = cursor
        if self.stream_slice_field_end:
            self._cursor_end = stream_slice_value_end

    def stream_slices(self, sync_mode: SyncMode, stream_state: Mapping[str, Any]) -> Iterable[Mapping[str, Any]]:
        """
        Partition the daterange into slices of size = step.

        The start of the window is the minimum datetime between start_datetime - looback_window and the stream_state's datetime
        The end of the window is the minimum datetime between the start of the window and end_datetime.

        :param sync_mode:
        :param stream_state: current stream state. If set, the start_date will be the day following the stream_state.
        :return:
        """
        stream_state = stream_state or {}
        kwargs = {"stream_state": stream_state}
        end_datetime = min(self.end_datetime.get_datetime(self.config, **kwargs), datetime.datetime.now(tz=datetime.timezone.utc))
        lookback_delta = self._parse_timedelta(self.lookback_window.eval(self.config, **kwargs) if self.lookback_window else "0d")
        start_datetime = self.start_datetime.get_datetime(self.config, **kwargs) - lookback_delta
        start_datetime = min(start_datetime, end_datetime)
        if self.cursor_field.eval(self.config, stream_state=stream_state) in stream_state:
            cursor_datetime = self.parse_date(stream_state[self.cursor_field.eval(self.config)])
        else:
            cursor_datetime = start_datetime

        start_datetime = max(cursor_datetime, start_datetime)

        state_date = self.parse_date(stream_state.get(self.cursor_field.eval(self.config, stream_state=stream_state)))
        if state_date:
            # If the input_state's date is greater than start_datetime, the start of the time window is the state's next day
            next_date = state_date + datetime.timedelta(days=1)
            start_datetime = max(start_datetime, next_date)
        dates = self._partition_daterange(start_datetime, end_datetime, self._step)
        return dates

    def _format_datetime(self, dt: datetime.datetime):
        if self.datetime_format == "timestamp":
            return dt.timestamp()
        else:
            return dt.strftime(self.datetime_format)

    def _partition_daterange(self, start, end, step: datetime.timedelta):
        start_field = self.stream_slice_field_start.eval(self.config)
        end_field = self.stream_slice_field_end.eval(self.config)
        dates = []
        while start <= end:
            end_date = self._get_date(start + step - datetime.timedelta(days=1), end, min)
            dates.append({start_field: self._format_datetime(start), end_field: self._format_datetime(end_date)})
            start += step
        return dates

    def _get_date(self, cursor_value, default_date: datetime.datetime, comparator) -> datetime.datetime:
        cursor_date = self.parse_date(cursor_value or default_date)
        return comparator(cursor_date, default_date)

    def parse_date(self, date: Any) -> datetime:
        if date and isinstance(date, str):
            if self.is_int(date):
                return datetime.datetime.fromtimestamp(int(date)).replace(tzinfo=self._timezone)
            else:
                return dateutil.parser.parse(date).replace(tzinfo=self._timezone)
        elif isinstance(date, int):
            return datetime.datetime.fromtimestamp(int(date)).replace(tzinfo=self._timezone)
        return date

    def is_int(self, s) -> bool:
        try:
            int(s)
            return True
        except ValueError:
            return False

    @classmethod
    def _parse_timedelta(cls, time_str):
        """
        Parse a time string e.g. (2h13m) into a timedelta object.
        Modified from virhilo's answer at https://stackoverflow.com/a/4628148/851699
        :param time_str: A string identifying a duration. (eg. 2h13m)
        :return datetime.timedelta: A datetime.timedelta object
        """
        parts = cls.timedelta_regex.match(time_str)

        assert parts is not None

        time_params = {name: float(param) for name, param in parts.groupdict().items() if param}
        return datetime.timedelta(**time_params)

    def get_request_params(self) -> Mapping[str, Any]:
        return self._get_request_options(RequestOptionType.request_parameter)

    def get_request_headers(self) -> Mapping[str, Any]:
        return self._get_request_options(RequestOptionType.header)

    def get_request_body_data(self) -> Mapping[str, Any]:
        return self._get_request_options(RequestOptionType.body_data)

    def get_request_body_json(self) -> Mapping[str, Any]:
        return self._get_request_options(RequestOptionType.body_json)

    def request_kwargs(self) -> Mapping[str, Any]:
        # Never update kwargs
        return {}

    def _get_request_options(self, option_type):
        options = {}
        if self.start_time_option and self.start_time_option.inject_into == option_type:
            if self._cursor:
                options[self.start_time_option.field_name] = self._cursor
        if self.end_time_option and self.end_time_option.inject_into == option_type:
            options[self.end_time_option.field_name] = self._cursor_end
        return options
