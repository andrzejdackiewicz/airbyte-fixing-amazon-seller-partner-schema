#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

from unittest.mock import Mock

import pytest
from airbyte_cdk.models import SyncMode
from airbyte_cdk.utils import AirbyteTracedException
from google.ads.googleads.errors import GoogleAdsException
from google.ads.googleads.v11.errors.types.errors import ErrorCode, GoogleAdsError, GoogleAdsFailure
from google.ads.googleads.v11.errors.types.request_error import RequestErrorEnum
from google.api_core.exceptions import DataLoss, InternalServerError, ResourceExhausted, TooManyRequests
from grpc import RpcError
from source_google_ads.google_ads import GoogleAds
from source_google_ads.streams import Accounts, ClickView

# EXPIRED_PAGE_TOKEN exception will be raised when page token has expired.
exception = GoogleAdsException(
    error=RpcError(),
    failure=GoogleAdsFailure(errors=[GoogleAdsError(error_code=ErrorCode(request_error=RequestErrorEnum.RequestError.EXPIRED_PAGE_TOKEN))]),
    call=RpcError(),
    request_id="test",
)


def mock_response_1():
    yield [
        {"segments.date": "2021-01-01", "click_view.gclid": "1"},
        {"segments.date": "2021-01-02", "click_view.gclid": "2"},
        {"segments.date": "2021-01-03", "click_view.gclid": "3"},
        {"segments.date": "2021-01-03", "click_view.gclid": "4"},
    ]
    raise exception


def mock_response_2():
    yield [
        {"segments.date": "2021-01-03", "click_view.gclid": "3"},
        {"segments.date": "2021-01-03", "click_view.gclid": "4"},
        {"segments.date": "2021-01-03", "click_view.gclid": "5"},
        {"segments.date": "2021-01-04", "click_view.gclid": "6"},
        {"segments.date": "2021-01-05", "click_view.gclid": "7"},
    ]


class MockGoogleAds(GoogleAds):
    count = 0

    def parse_single_result(self, schema, result):
        return result

    def send_request(self, query: str, customer_id: str):
        self.count += 1
        if self.count == 1:
            return mock_response_1()
        else:
            return mock_response_2()


def test_page_token_expired_retry_succeeds(config, customers):
    """
    Page token expired while reading records on date 2021-01-03
    The latest read record is {"segments.date": "2021-01-03", "click_view.gclid": "4"}
    It should retry reading starting from 2021-01-03, already read records will be reread again from that date.
    It shouldn't read records on 2021-01-01, 2021-01-02
    """
    customer_id = next(iter(customers)).id
    stream_slice = {"customer_id": customer_id, "start_date": "2021-01-01", "end_date": "2021-01-15"}

    google_api = MockGoogleAds(credentials=config["credentials"])
    incremental_stream_config = dict(
        api=google_api,
        conversion_window_days=config["conversion_window_days"],
        start_date=config["start_date"],
        customers=customers,
        end_date="2021-04-04",
    )
    stream = ClickView(**incremental_stream_config)
    stream.get_query = Mock()
    stream.get_query.return_value = "query"

    result = list(stream.read_records(sync_mode=SyncMode.incremental, cursor_field=["segments.date"], stream_slice=stream_slice))
    assert len(result) == 9
    assert stream.get_query.call_count == 2
    stream.get_query.assert_called_with({"customer_id": customer_id, "start_date": "2021-01-03", "end_date": "2021-01-15"})


def mock_response_fails_1():
    yield [
        {"segments.date": "2021-01-01", "click_view.gclid": "1"},
        {"segments.date": "2021-01-02", "click_view.gclid": "2"},
        {"segments.date": "2021-01-03", "click_view.gclid": "3"},
        {"segments.date": "2021-01-03", "click_view.gclid": "4"},
    ]

    raise exception


def mock_response_fails_2():
    yield [
        {"segments.date": "2021-01-03", "click_view.gclid": "3"},
        {"segments.date": "2021-01-03", "click_view.gclid": "4"},
        {"segments.date": "2021-01-03", "click_view.gclid": "5"},
        {"segments.date": "2021-01-03", "click_view.gclid": "6"},
    ]

    raise exception


class MockGoogleAdsFails(MockGoogleAds):
    def send_request(self, query: str, customer_id: str):
        self.count += 1
        if self.count == 1:
            return mock_response_fails_1()
        else:
            return mock_response_fails_2()


def test_page_token_expired_retry_fails(config, customers):
    """
    Page token has expired while reading records within date "2021-01-03", it should raise error,
    because Google Ads API doesn't allow filter by datetime.
    """
    customer_id = next(iter(customers)).id
    stream_slice = {"customer_id": customer_id, "start_date": "2021-01-01", "end_date": "2021-01-15"}

    google_api = MockGoogleAdsFails(credentials=config["credentials"])
    incremental_stream_config = dict(
        api=google_api,
        conversion_window_days=config["conversion_window_days"],
        start_date=config["start_date"],
        end_date="2021-04-04",
        customers=customers,
    )
    stream = ClickView(**incremental_stream_config)
    stream.get_query = Mock()
    stream.get_query.return_value = "query"

    with pytest.raises(AirbyteTracedException) as exception:
        list(stream.read_records(sync_mode=SyncMode.incremental, cursor_field=["segments.date"], stream_slice=stream_slice))
    assert exception.value.message == "Page token has expired during processing response. Please contact support for assistance."

    stream.get_query.assert_called_with({"customer_id": customer_id, "start_date": "2021-01-03", "end_date": "2021-01-15"})
    assert stream.get_query.call_count == 2


def mock_response_fails_one_date():
    yield [
        {"segments.date": "2021-01-03", "click_view.gclid": "3"},
        {"segments.date": "2021-01-03", "click_view.gclid": "4"},
        {"segments.date": "2021-01-03", "click_view.gclid": "5"},
        {"segments.date": "2021-01-03", "click_view.gclid": "6"},
    ]

    raise exception


class MockGoogleAdsFailsOneDate(MockGoogleAds):
    def send_request(self, query: str, customer_id: str):
        return mock_response_fails_one_date()


def test_page_token_expired_it_should_fail_date_range_1_day(config, customers):
    """
    Page token has expired while reading records within date "2021-01-03",
    it should raise error, because Google Ads API doesn't allow filter by datetime.
    Minimum date range is 1 day.
    """
    customer_id = next(iter(customers)).id
    stream_slice = {"customer_id": customer_id, "start_date": "2021-01-03", "end_date": "2021-01-04"}

    google_api = MockGoogleAdsFailsOneDate(credentials=config["credentials"])
    incremental_stream_config = dict(
        api=google_api,
        conversion_window_days=config["conversion_window_days"],
        start_date=config["start_date"],
        end_date="2021-04-04",
        customers=customers,
    )
    stream = ClickView(**incremental_stream_config)
    stream.get_query = Mock()
    stream.get_query.return_value = "query"

    with pytest.raises(AirbyteTracedException) as exception:
        list(stream.read_records(sync_mode=SyncMode.incremental, cursor_field=["segments.date"], stream_slice=stream_slice))
    assert exception.value.message == "Page token has expired during processing response. Please contact support for assistance."
    stream.get_query.assert_called_with({"customer_id": customer_id, "start_date": "2021-01-03", "end_date": "2021-01-04"})
    assert stream.get_query.call_count == 1


@pytest.mark.parametrize("error_cls", (ResourceExhausted, TooManyRequests, InternalServerError, DataLoss))
def test_retry_transient_errors(mocker, config, customers, error_cls):
    mocker.patch("time.sleep")
    credentials = config["credentials"]
    credentials.update(use_proto_plus=True)
    api = GoogleAds(credentials=credentials)
    mocked_search = mocker.patch.object(api.ga_service, "search", side_effect=error_cls("Error message"))
    incremental_stream_config = dict(
        api=api,
        conversion_window_days=config["conversion_window_days"],
        start_date=config["start_date"],
        end_date="2021-04-04",
        customers=customers,
    )
    stream = ClickView(**incremental_stream_config)
    customer_id = next(iter(customers)).id
    stream_slice = {"customer_id": customer_id, "start_date": "2021-01-03", "end_date": "2021-01-04"}
    records = []
    with pytest.raises(error_cls) as exception:
        records = list(stream.read_records(sync_mode=SyncMode.incremental, cursor_field=["segments.date"], stream_slice=stream_slice))
    assert exception.value.message == "Error message"

    assert mocked_search.call_count == 5
    assert records == []


def test_parse_response(mocker, customers, config):
    """
    Tests the `parse_response` method of the `Accounts` class.
    The test checks if the optimization_score_weight of type int is converted to float.
    """

    # Prepare sample input data
    response = [
        {"customer.id": "1", "segments.date": "2023-09-19", "customer.optimization_score_weight": 80},
        {"customer.id": "2", "segments.date": "2023-09-20", "customer.optimization_score_weight": 80.0},
        {"customer.id": "3", "segments.date": "2023-09-21"},
    ]
    mocker.patch("source_google_ads.streams.GoogleAdsStream.parse_response", Mock(return_value=response))

    credentials = config["credentials"]
    api = GoogleAds(credentials=credentials)

    incremental_stream_config = dict(
        api=api,
        conversion_window_days=config["conversion_window_days"],
        start_date=config["start_date"],
        end_date="2021-04-04",
        customers=customers,
    )

    # Create an instance of the Accounts class
    accounts = Accounts(**incremental_stream_config)

    # Use the parse_response method and get the output
    output = list(accounts.parse_response(response))

    # Expected output after the method's logic
    expected_output = [
        {"customer.id": "1", "segments.date": "2023-09-19", "customer.optimization_score_weight": 80.0},
        {"customer.id": "2", "segments.date": "2023-09-20", "customer.optimization_score_weight": 80.0},
        {"customer.id": "3", "segments.date": "2023-09-21"},
    ]

    assert output == expected_output
