#
# Copyright (c) 2021 Airbyte, Inc., all rights reserved.
#
from datetime import timezone
import pendulum
import pytest
from pytest import fixture, raises
from unittest.mock import MagicMock
from pendulum.parsing.exceptions import ParserError

from airbyte_cdk.models import SyncMode
from source_appsflyer.source import IncrementalAppsflyerStream
from source_appsflyer.source import InAppEvents
from source_appsflyer.source import UninstallEvents
from source_appsflyer.source import Installs
from source_appsflyer.source import RetargetingInAppEvents
from source_appsflyer.source import RetargetingConversions
from source_appsflyer.source import PartnersReport
from source_appsflyer.source import DailyReport
from source_appsflyer.source import GeoReport
from source_appsflyer.source import RetargetingPartnersReport
from source_appsflyer.source import RetargetingDailyReport
from source_appsflyer.source import RetargetingGeoReport
from source_appsflyer import fields


@fixture
def patch_incremental_base_class(mocker):
    # Mock abstract methods to enable instantiating abstract class
    mocker.patch.object(IncrementalAppsflyerStream, "path", "v0/example_endpoint")
    mocker.patch.object(IncrementalAppsflyerStream, "primary_key", "test_primary_key")
    mocker.patch.object(IncrementalAppsflyerStream, "__abstractmethods__", set())


@pytest.mark.parametrize(
    ("class_", "expected_cursor_field"),
    [
        (IncrementalAppsflyerStream, []),
        (InAppEvents, "event_time"),
        (RetargetingInAppEvents, "event_time"),
        (UninstallEvents, "event_time"),
        (Installs, "install_time"),
        (RetargetingConversions, "install_time"),
        (PartnersReport, "date"),
        (DailyReport, "date"),
        (GeoReport, "date"),
        (RetargetingPartnersReport, "date"),
        (RetargetingDailyReport, "date"),
        (RetargetingGeoReport, "date"),
    ],
)
def test_cursor_field(patch_incremental_base_class, mocker, class_, expected_cursor_field):
    mocker.patch.object(class_, "__init__", lambda x: None)
    stream = class_()
    assert stream.cursor_field == expected_cursor_field


def test_request_params_in_app_events(mocker):
    timezone = "UTC"
    def __init__(self):
        self.api_token = "secret"
        self.timezone = pendulum.timezone("UTC")
        self.start_date = pendulum.yesterday(timezone)
        self.end_date = pendulum.today(timezone)
    mocker.patch.object(InAppEvents, "__init__", __init__)
    mocker.patch.object(InAppEvents, "cursor_field", "date")
    stream = InAppEvents()
    inputs = {
        "stream_slice": {
            "date": pendulum.yesterday(timezone),
            "date_end": pendulum.today(timezone)
        },
        "next_page_token": None,
        "stream_state": None
    }
    expected_params = {
        "additional_fields": (",").join(fields.raw_data.additional_fields),
        "api_token": "secret",
        "timezone": timezone,
        "maximum_rows": 1_000_000,
        "from": pendulum.yesterday(timezone).to_datetime_string(),
        "to": pendulum.today(timezone).to_datetime_string(),
    }
    assert stream.request_params(**inputs) == expected_params


def test_request_params_installs(mocker):
    timezone = "UTC"
    def __init__(self):
        self.api_token = "secret"
        self.timezone = pendulum.timezone("UTC")
        self.start_date = pendulum.yesterday(timezone)
        self.end_date = pendulum.today(timezone)
    mocker.patch.object(Installs, "__init__", __init__)
    mocker.patch.object(Installs, "cursor_field", "date")
    stream = Installs()
    inputs = {
        "stream_slice": {
            "date": pendulum.yesterday(timezone),
            "date_end": pendulum.today(timezone)
        },
        "next_page_token": None,
        "stream_state": None
    }
    expected_params = {
        "additional_fields": (",").join(fields.raw_data.additional_fields),
        "api_token": "secret",
        "timezone": timezone,
        "maximum_rows": 1_000_000,
        "from": pendulum.yesterday(timezone).to_datetime_string(),
        "to": pendulum.today(timezone).to_datetime_string(),
    }
    assert stream.request_params(**inputs) == expected_params


def test_request_params_retargeting_in_app_events(mocker):
    timezone = "UTC"
    def __init__(self):
        self.api_token = "secret"
        self.timezone = pendulum.timezone("UTC")
        self.start_date = pendulum.yesterday(timezone)
        self.end_date = pendulum.today(timezone)
    mocker.patch.object(RetargetingInAppEvents, "__init__", __init__)
    mocker.patch.object(RetargetingInAppEvents, "cursor_field", "date")
    stream = RetargetingInAppEvents()
    inputs = {
        "stream_slice": {
            "date": pendulum.yesterday(timezone),
            "date_end": pendulum.today(timezone)
        },
        "next_page_token": None,
        "stream_state": None
    }
    expected_params = {
        "additional_fields": (",").join(fields.raw_data.additional_fields),
        "api_token": "secret",
        "timezone": timezone,
        "maximum_rows": 1_000_000,
        "from": pendulum.yesterday(timezone).to_datetime_string(),
        "to": pendulum.today(timezone).to_datetime_string(),
        "reattr": True
    }
    assert stream.request_params(**inputs) == expected_params


def test_request_params_retargeting_conversions(mocker):
    timezone = "UTC"
    def __init__(self):
        self.api_token = "secret"
        self.timezone = pendulum.timezone("UTC")
        self.start_date = pendulum.yesterday(timezone)
        self.end_date = pendulum.today(timezone)
    mocker.patch.object(RetargetingConversions, "__init__", __init__)
    mocker.patch.object(RetargetingConversions, "cursor_field", "date")
    stream = RetargetingConversions()
    inputs = {
        "stream_slice": {
            "date": pendulum.yesterday(timezone),
            "date_end": pendulum.today(timezone)
        },
        "next_page_token": None,
        "stream_state": None
    }
    expected_params = {
        "additional_fields": (",").join(fields.raw_data.additional_fields),
        "api_token": "secret",
        "timezone": timezone,
        "maximum_rows": 1_000_000,
        "from": pendulum.yesterday(timezone).to_datetime_string(),
        "to": pendulum.today(timezone).to_datetime_string(),
        "reattr": True
    }
    assert stream.request_params(**inputs) == expected_params


def test_request_params_uninstall_events(mocker):
    timezone = "UTC"
    def __init__(self):
        self.api_token = "secret"
        self.timezone = pendulum.timezone("UTC")
        self.start_date = pendulum.yesterday(timezone)
        self.end_date = pendulum.today(timezone)
    mocker.patch.object(UninstallEvents, "__init__", __init__)
    mocker.patch.object(UninstallEvents, "cursor_field", "date")
    stream = UninstallEvents()
    inputs = {
        "stream_slice": {
            "date": pendulum.yesterday(timezone),
            "date_end": pendulum.today(timezone)
        },
        "next_page_token": None,
        "stream_state": None
    }
    expected_params = {
        "additional_fields": (",").join(fields.uninstall_events.additional_fields),
        "api_token": "secret",
        "timezone": timezone,
        "maximum_rows": 1_000_000,
        "from": pendulum.yesterday(timezone).to_datetime_string(),
        "to": pendulum.today(timezone).to_datetime_string(),
    }
    assert stream.request_params(**inputs) == expected_params


def test_request_params_partners_report(mocker):
    timezone = "UTC"
    def __init__(self):
        self.api_token = "secret"
        self.timezone = pendulum.timezone("UTC")
        self.start_date = pendulum.yesterday(timezone)
        self.end_date = pendulum.today(timezone)
    mocker.patch.object(PartnersReport, "__init__", __init__)
    mocker.patch.object(PartnersReport, "cursor_field", "date")
    stream = PartnersReport()
    inputs = {
        "stream_slice": {
            "date": pendulum.yesterday(timezone),
            "date_end": pendulum.today(timezone)
        },
        "next_page_token": None,
        "stream_state": None
    }
    expected_params = {
        "api_token": "secret",
        "timezone": timezone,
        "maximum_rows": 1_000_000,
        "from": pendulum.yesterday(timezone).to_date_string(),
        "to": pendulum.today(timezone).to_date_string(),
    }
    assert stream.request_params(**inputs) == expected_params


def test_request_params_daily_report(mocker):
    timezone = "UTC"
    def __init__(self):
        self.api_token = "secret"
        self.timezone = pendulum.timezone("UTC")
        self.start_date = pendulum.yesterday(timezone)
        self.end_date = pendulum.today(timezone)
    mocker.patch.object(DailyReport, "__init__", __init__)
    mocker.patch.object(DailyReport, "cursor_field", "date")
    stream = DailyReport()
    inputs = {
        "stream_slice": {
            "date": pendulum.yesterday(timezone),
            "date_end": pendulum.today(timezone)
        },
        "next_page_token": None,
        "stream_state": None
    }
    expected_params = {
        "api_token": "secret",
        "timezone": timezone,
        "maximum_rows": 1_000_000,
        "from": pendulum.yesterday(timezone).to_date_string(),
        "to": pendulum.today(timezone).to_date_string(),
    }
    assert stream.request_params(**inputs) == expected_params


def test_request_params_geo_report(mocker):
    timezone = "UTC"
    def __init__(self):
        self.api_token = "secret"
        self.timezone = pendulum.timezone("UTC")
        self.start_date = pendulum.yesterday(timezone)
        self.end_date = pendulum.today(timezone)
    mocker.patch.object(GeoReport, "__init__", __init__)
    mocker.patch.object(GeoReport, "cursor_field", "date")
    stream = GeoReport()
    inputs = {
        "stream_slice": {
            "date": pendulum.yesterday(timezone),
            "date_end": pendulum.today(timezone)
        },
        "next_page_token": None,
        "stream_state": None
    }
    expected_params = {
        "api_token": "secret",
        "timezone": timezone,
        "maximum_rows": 1_000_000,
        "from": pendulum.yesterday(timezone).to_date_string(),
        "to": pendulum.today(timezone).to_date_string(),
    }
    assert stream.request_params(**inputs) == expected_params


def test_request_params_retargeting_partners_report(mocker):
    timezone = "UTC"
    def __init__(self):
        self.api_token = "secret"
        self.timezone = pendulum.timezone("UTC")
        self.start_date = pendulum.yesterday(timezone)
        self.end_date = pendulum.today(timezone)
    mocker.patch.object(RetargetingPartnersReport, "__init__", __init__)
    mocker.patch.object(RetargetingPartnersReport, "cursor_field", "date")
    stream = RetargetingPartnersReport()
    inputs = {
        "stream_slice": {
            "date": pendulum.yesterday(timezone),
            "date_end": pendulum.today(timezone)
        },
        "next_page_token": None,
        "stream_state": None
    }
    expected_params = {
        "api_token": "secret",
        "timezone": timezone,
        "maximum_rows": 1_000_000,
        "from": pendulum.yesterday(timezone).to_date_string(),
        "to": pendulum.today(timezone).to_date_string(),
        "reattr":True
    }
    assert stream.request_params(**inputs) == expected_params


def test_request_params_retargeting_daily_report(mocker):
    timezone = "UTC"
    def __init__(self):
        self.api_token = "secret"
        self.timezone = pendulum.timezone("UTC")
        self.start_date = pendulum.yesterday(timezone)
        self.end_date = pendulum.today(timezone)
    mocker.patch.object(RetargetingDailyReport, "__init__", __init__)
    mocker.patch.object(RetargetingDailyReport, "cursor_field", "date")
    stream = RetargetingDailyReport()
    inputs = {
        "stream_slice": {
            "date": pendulum.yesterday(timezone),
            "date_end": pendulum.today(timezone)
        },
        "next_page_token": None,
        "stream_state": None
    }
    expected_params = {
        "api_token": "secret",
        "timezone": timezone,
        "maximum_rows": 1_000_000,
        "from": pendulum.yesterday(timezone).to_date_string(),
        "to": pendulum.today(timezone).to_date_string(),
        "reattr": True
    }
    assert stream.request_params(**inputs) == expected_params


def test_request_params_retargeting_geo_report(mocker):
    timezone = "UTC"
    def __init__(self):
        self.api_token = "secret"
        self.timezone = pendulum.timezone("UTC")
        self.start_date = pendulum.yesterday(timezone)
        self.end_date = pendulum.today(timezone)
    mocker.patch.object(RetargetingGeoReport, "__init__", __init__)
    mocker.patch.object(RetargetingGeoReport, "cursor_field", "date")
    stream = RetargetingGeoReport()
    inputs = {
        "stream_slice": {
            "date": pendulum.yesterday(timezone),
            "date_end": pendulum.today(timezone)
        },
        "next_page_token": None,
        "stream_state": None
    }
    expected_params = {
        "api_token": "secret",
        "timezone": timezone,
        "maximum_rows": 1_000_000,
        "from": pendulum.yesterday(timezone).to_date_string(),
        "to": pendulum.today(timezone).to_date_string(),
        "reattr": True
    }
    assert stream.request_params(**inputs) == expected_params


def test_get_updated_state_all_exists(patch_incremental_base_class, mocker):
    def __init__(self): self.timezone= pendulum.timezone("UTC")
    mocker.patch.object(IncrementalAppsflyerStream, "__init__", __init__)
    mocker.patch.object(IncrementalAppsflyerStream, "cursor_field", "event_time")
    stream = IncrementalAppsflyerStream()
    inputs = {"current_stream_state": dict(event_time="2021-09-09"), "latest_record": dict(event_time="2021-09-09")}
    expected_state = dict(event_time="2021-09-09")
    assert stream.get_updated_state(**inputs) == expected_state


def test_get_updated_state_empty_current_stream_and_empty_latest_record(patch_incremental_base_class, mocker):
    def __init__(self): self.timezone= pendulum.timezone("UTC")
    mocker.patch.object(IncrementalAppsflyerStream, "__init__", __init__)
    mocker.patch.object(IncrementalAppsflyerStream, "cursor_field", "event_time")
    stream = IncrementalAppsflyerStream()
    inputs = {"current_stream_state": {}, "latest_record": {}}
    expected_state = {}
    assert stream.get_updated_state(**inputs) == expected_state


def test_get_updated_state_empty_current_stream_and_exists_latest_record(patch_incremental_base_class, mocker):
    def __init__(self): self.timezone= pendulum.timezone("UTC")
    mocker.patch.object(IncrementalAppsflyerStream, "__init__", __init__)
    mocker.patch.object(IncrementalAppsflyerStream, "cursor_field", "event_time")
    stream = IncrementalAppsflyerStream()
    inputs = {"current_stream_state": {}, "latest_record": dict(event_time="2021-09-09")}
    expected_state = dict(event_time="2021-09-09")
    assert stream.get_updated_state(**inputs) == expected_state


def test_get_updated_state_exists_current_stream_and_empty_latest_record(patch_incremental_base_class, mocker):
    with raises(TypeError, match=r"Expected (.*) type '(.*)' but returned type '(.*)'."):
        def __init__(self): self.timezone= pendulum.timezone("UTC")
        mocker.patch.object(IncrementalAppsflyerStream, "__init__", __init__)
        mocker.patch.object(IncrementalAppsflyerStream, "cursor_field", "event_time")
        stream = IncrementalAppsflyerStream()
        inputs = {"current_stream_state": dict(event_time="2021-09-09"), "latest_record": {"event_time":None}}
        stream.get_updated_state(**inputs)


def test_stream_slices(patch_incremental_base_class, mocker):
    timezone = "UTC"
    def __init__(self):
        self.api_token= "secret"
        self.timezone= pendulum.timezone("UTC")
        self.start_date= pendulum.yesterday("UTC")
        self.end_date= pendulum.today("UTC")

    mocker.patch.object(IncrementalAppsflyerStream, "__init__", __init__)
    mocker.patch.object(IncrementalAppsflyerStream, "cursor_field", "date")
    stream = IncrementalAppsflyerStream()
    inputs = {
        "sync_mode": SyncMode.incremental,
        "cursor_field": [],
        "stream_state": dict(date=pendulum.yesterday(timezone))
    }
    expected_stream_slice = [{
        "date":pendulum.yesterday("UTC"),
        "date_end":pendulum.today("UTC")
    }]
    assert stream.stream_slices(**inputs) == expected_stream_slice


def test_supports_incremental(patch_incremental_base_class, mocker):
    def __init__(self): pass
    mocker.patch.object(IncrementalAppsflyerStream, "__init__", __init__)
    mocker.patch.object(IncrementalAppsflyerStream, "cursor_field", "dummy_field")
    stream = IncrementalAppsflyerStream()
    assert stream.supports_incremental


def test_source_defined_cursor(patch_incremental_base_class, mocker):
    def __init__(self): pass
    mocker.patch.object(IncrementalAppsflyerStream, "__init__", __init__)
    stream = IncrementalAppsflyerStream()
    assert stream.source_defined_cursor


def test_stream_checkpoint_interval(patch_incremental_base_class, mocker):
    def __init__(self): pass
    mocker.patch.object(IncrementalAppsflyerStream, "__init__", __init__)
    stream = IncrementalAppsflyerStream()
    expected_checkpoint_interval = None
    assert stream.state_checkpoint_interval == expected_checkpoint_interval
