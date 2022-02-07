#
# Copyright (c) 2021 Airbyte, Inc., all rights reserved.
#

from abc import ABC
from typing import Any, Iterable, Mapping, MutableMapping, Optional

import pendulum
from airbyte_cdk.sources.streams import Stream
from google.ads.googleads.v8.services.services.google_ads_service.pagers import SearchPager

from .google_ads import GoogleAds


def chunk_date_range(
    start_date: str,
    conversion_window: int,
    field: str,
    end_date: str = None,
    days_of_data_storage: int = None,
    range_days: int = None,
) -> Iterable[Mapping[str, any]]:
    """
    Passing optional parameter end_date for testing
    Returns a list of the beginning and ending timestamps of each `range_days` between the start date and now.
    The return value is a list of dicts {'date': str} which can be used directly with the Slack API
    """
    intervals = []
    end_date = pendulum.parse(end_date) if end_date else pendulum.now()
    start_date = pendulum.parse(start_date)

    # For some metrics we can only get data not older than N days, it is Google Ads policy
    if days_of_data_storage:
        start_date = max(start_date, pendulum.now().subtract(days=days_of_data_storage - conversion_window))

    # As in to return some state when state in abnormal
    if start_date > end_date:
        return [{field: start_date.to_date_string()}]

    # applying conversion window
    start_date = start_date.subtract(days=conversion_window)

    # Each stream_slice contains the beginning and ending timestamp for a 24 hour period
    while start_date < end_date:
        intervals.append({field: start_date.to_date_string()})
        start_date = start_date.add(days=range_days)

    return intervals


class GoogleAdsStream(Stream, ABC):
    def __init__(self, api: GoogleAds):
        self.google_ads_client = api
        self._customer_id = None

    def get_query(self, stream_slice: Mapping[str, Any]) -> str:
        query = GoogleAds.convert_schema_into_query(schema=self.get_json_schema(), report_name=self.name)
        return query

    def parse_response(self, response: SearchPager) -> Iterable[Mapping]:
        for result in response:
            yield self.google_ads_client.parse_single_result(self.get_json_schema(), result)

    def read_records(self, sync_mode, stream_slice: Mapping[str, Any] = None, **kwargs) -> Iterable[Mapping[str, Any]]:
        stream_slice = stream_slice or {}
        account_responses = self.google_ads_client.send_request(self.get_query(stream_slice), customer_id=self._customer_id)
        for response in account_responses:
            yield from self.parse_response(response)


class IncrementalGoogleAdsStream(GoogleAdsStream, ABC):
    days_of_data_storage = None
    cursor_field = "segments.date"
    primary_key = None
    range_days = 15  # date range is set to 15 days, because for conversion_window_days default value is 14. Range less than 15 days will break the integration tests.

    def __init__(self, start_date: str, conversion_window_days: int, time_zone: [pendulum.timezone, str], end_date: str = None, **kwargs):
        self.conversion_window_days = conversion_window_days
        self._start_date = start_date
        self.time_zone = time_zone
        self._end_date = end_date
        super().__init__(**kwargs)

    def stream_slices(self, stream_state: Mapping[str, Any] = None, **kwargs) -> Iterable[Optional[Mapping[str, any]]]:
        for customer_id in self.google_ads_client.customer_ids:
            self.customer_id = customer_id
            stream_state = stream_state or {}
            if stream_state.get(customer_id):
                start_date = stream_state[customer_id].get(self.cursor_field) or self._start_date

            # We should keep backward compatibility with the previous version
            elif stream_state.get(self.cursor_field) and len(self.google_ads_client.customer_ids) == 1:
                start_date = stream_state.get(self.cursor_field) or self._start_date
            else:
                start_date = self._start_date

            end_date = self._end_date

            for chunk in chunk_date_range(
                start_date=start_date,
                end_date=end_date,
                conversion_window=self.conversion_window_days,
                field=self.cursor_field,
                days_of_data_storage=self.days_of_data_storage,
                range_days=self.range_days,
            ):
                yield chunk

    def get_date_params(self, stream_slice: Mapping[str, Any], cursor_field: str, end_date: pendulum.datetime = None):
        """
        Returns `start_date` and `end_date` for the given stream_slice.
        If (end_date - start_date) is a big date range (>= 1 month), it can take more than 2 hours to process all the records from the given slice.
        After 2 hours next page tokens will be expired, finally resulting in page token expired error
        Currently this method returns `start_date` and `end_date` with 15 days difference.
        """

        end_date = end_date or pendulum.yesterday(tz=self.time_zone)
        start_date = pendulum.parse(stream_slice.get(cursor_field))
        if start_date > pendulum.now():
            return start_date.to_date_string(), start_date.add(days=1).to_date_string()

        end_date = min(end_date, pendulum.parse(stream_slice.get(cursor_field)).add(days=self.range_days))

        # Fix issue #4806, start date should always be lower than end date.
        if start_date.add(days=1).date() >= end_date.date():
            return start_date.add(days=1).to_date_string(), start_date.add(days=2).to_date_string()
        return start_date.add(days=1).to_date_string(), end_date.to_date_string()

    def get_updated_state(self, current_stream_state: MutableMapping[str, Any], latest_record: Mapping[str, Any]) -> Mapping[str, Any]:
        current_stream_state = current_stream_state or {}

        if current_stream_state.get(self.cursor_field):
            current_stream = current_stream_state.pop(self.cursor_field)
        elif current_stream_state.get(self.customer_id) and current_stream_state[self.customer_id].get(self.cursor_field):
            current_stream = current_stream_state[self.customer_id][self.cursor_field]
        else:
            current_stream_state.update({self.customer_id: {self.cursor_field: latest_record[self.cursor_field]}})
            return current_stream_state

        date_in_current_stream = pendulum.parse(current_stream)
        date_in_latest_record = pendulum.parse(latest_record[self.cursor_field])

        current_stream_state.update(
            {self.customer_id: {self.cursor_field: (max(date_in_current_stream, date_in_latest_record)).to_date_string()}}
        )

        return current_stream_state

    def get_query(self, stream_slice: Mapping[str, Any] = None) -> str:
        start_date, end_date = self.get_date_params(stream_slice, self.cursor_field)
        query = GoogleAds.convert_schema_into_query(
            schema=self.get_json_schema(), report_name=self.name, from_date=start_date, to_date=end_date, cursor_field=self.cursor_field
        )
        return query


class Accounts(GoogleAdsStream):
    """
    Accounts stream: https://developers.google.com/google-ads/api/fields/v8/customer
    """

    primary_key = "customer.id"


class Campaigns(GoogleAdsStream):
    """
    Campaigns stream: https://developers.google.com/google-ads/api/fields/v8/campaign
    """

    primary_key = "campaign.id"


class AdGroups(GoogleAdsStream):
    """
    AdGroups stream: https://developers.google.com/google-ads/api/fields/v8/ad_group
    """

    primary_key = "ad_group.id"


class AdGroupAds(GoogleAdsStream):
    """
    AdGroups stream: https://developers.google.com/google-ads/api/fields/v8/ad_group_ad
    """

    primary_key = "ad_group_ad.ad.id"


class AccountPerformanceReport(IncrementalGoogleAdsStream):
    """
    AccountPerformanceReport stream: https://developers.google.com/google-ads/api/fields/v8/customer
    Google Ads API field mapping: https://developers.google.com/google-ads/api/docs/migration/mapping#account_performance
    """


class AdGroupAdReport(IncrementalGoogleAdsStream):
    """
    AdGroupAdReport stream: https://developers.google.com/google-ads/api/fields/v8/ad_group_ad
    Google Ads API field mapping: https://developers.google.com/google-ads/api/docs/migration/mapping#ad_performance
    """


class DisplayKeywordPerformanceReport(IncrementalGoogleAdsStream):
    """
    DisplayKeywordPerformanceReport stream: https://developers.google.com/google-ads/api/fields/v8/display_keyword_view
    Google Ads API field mapping: https://developers.google.com/google-ads/api/docs/migration/mapping#display_keyword_performance
    """


class DisplayTopicsPerformanceReport(IncrementalGoogleAdsStream):
    """
    DisplayTopicsPerformanceReport stream: https://developers.google.com/google-ads/api/fields/v8/topic_view
    Google Ads API field mapping: https://developers.google.com/google-ads/api/docs/migration/mapping#display_topics_performance
    """


class ShoppingPerformanceReport(IncrementalGoogleAdsStream):
    """
    ShoppingPerformanceReport stream: https://developers.google.com/google-ads/api/fields/v8/shopping_performance_view
    Google Ads API field mapping: https://developers.google.com/google-ads/api/docs/migration/mapping#shopping_performance
    """


class UserLocationReport(IncrementalGoogleAdsStream):
    """
    UserLocationReport stream: https://developers.google.com/google-ads/api/fields/v8/user_location_view
    Google Ads API field mapping: https://developers.google.com/google-ads/api/docs/migration/mapping#geo_performance
    """


class GeographicReport(IncrementalGoogleAdsStream):
    """
    UserLocationReport stream: https://developers.google.com/google-ads/api/fields/v8/geographic_view
    """


class KeywordReport(IncrementalGoogleAdsStream):
    """
    UserLocationReport stream: https://developers.google.com/google-ads/api/fields/v8/keyword_view
    """


class ClickView(IncrementalGoogleAdsStream):
    """
    ClickView stream: https://developers.google.com/google-ads/api/reference/rpc/v8/ClickView
    """

    days_of_data_storage = 90
    range_days = 1
