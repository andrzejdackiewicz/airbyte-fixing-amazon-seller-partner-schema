#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#
from enum import Enum

from abc import ABC, abstractproperty, abstractmethod
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple
from urllib.parse import parse_qsl, urlparse

import pendulum
import requests
import airbyte_cdk.sources.utils.casing as casing
from airbyte_cdk.models import SyncMode
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.sources.streams.http.auth import Oauth2Authenticator
from airbyte_cdk.sources.streams.core import package_name_from_class, IncrementalMixin
from airbyte_cdk.sources.utils.schema_helpers import ResourceSchemaLoader

# https://marketingapi.snapchat.com/docs/#core-metrics
METRICS_CORE = [
    'impressions',
    'swipes',
    'view_time_millis',
    'screen_time_millis',
    'quartile_1',
    'quartile_2',
    'quartile_3',
    'view_completion',
    'spend',
    'video_views',
]

# https://marketingapi.snapchat.com/docs/#metrics-and-supported-granularities
METRICS = [
    'android_installs',
    'attachment_avg_view_time_millis',
    'attachment_impressions',
    'attachment_quartile_1',
    'attachment_quartile_2',
    'attachment_quartile_3',
    'attachment_total_view_time_millis',
    'attachment_view_completion',
    'avg_screen_time_millis',
    'avg_view_time_millis',
    'impressions',
    'ios_installs',
    'quartile_1',
    'quartile_2',
    'quartile_3',
    'screen_time_millis',
    'spend',
    'swipe_up_percent',
    'swipes',
    'total_installs',
    'video_views',
    'video_views_time_based',
    'video_views_15s',
    'view_completion',
    'view_time_millis',
    'paid_impressions',
    'earned_impressions',
    'total_impressions',
    'play_time_millis',
    'shares',
    'saves',
    'native_leads',
    'conversion_purchases',
    'conversion_purchases_value',
    'conversion_save',
    'conversion_start_checkout',
    'conversion_add_cart',
    'conversion_view_content',
    'conversion_add_billing',
    # 'conversion_signups',  # Invalid query parameters in request URL
    'conversion_searches',
    'conversion_level_completes',
    'conversion_app_opens',
    'conversion_page_views',
    'conversion_subscribe',
    'conversion_ad_click',
    'conversion_ad_view',
    'conversion_complete_tutorial',
    'conversion_invite',
    'conversion_login',
    'conversion_share',
    'conversion_reserve',
    'conversion_achievement_unlocked',
    'conversion_add_to_wishlist',
    'conversion_spend_credits',
    'conversion_rate',
    'conversion_start_trial',
    'conversion_list_view',
    'custom_event_1',
    'custom_event_2',
    'custom_event_3',
    'custom_event_4',
    'custom_event_5',
]

METRICS_NOT_HOURLY = [
    'attachment_frequency',
    'attachment_uniques',
    'frequency',
    'uniques',
    'total_reach',
    'earned_reach',
]


class GranularityType(Enum):
    HOUR = 'HOUR'
    DAY = 'DAY'
    LIFETIME = 'LIFETIME'

# See the get_parent_ids function explanation
# Long story short - used as cache for ids of higher level streams, that is used
# as path variables in other streams. And to avoid requesting those ids for every stream
# we put them to a dict and check if they exist on stream calling
# The structure be like:
# {
#   'Organizations': [
#       {'organization_id': '7f064d90-52a1-some-uuid'}
#   ],
#   'Adaccounts': [
#       {'ad_account_id': '04214c00-3aa5-some-uuid'},
#       {'ad_account_id': 'e4cd371b-8de8-some-uuid'}
#   ]
# }
#
auxiliary_id_map = {}

# The default value that is returned by stream_slices if there is no slice found: [None]
default_stream_slices_return_value = [None]


class SnapchatMarketingException(Exception):
    """Just for formatting the exception as SnapchatMarketing"""


def get_parent_ids(parent) -> List:
    """This auxiliary function is to help retrieving the ids from another stream

    :param parent: instance of stream class from what we need to retrieve ids
    :param logger: Logger to log the messages
    :returns: empty list in case no ids of the stream was found or list with {'id': id}
    """

    # The trick with this code is that some streams are chained:
    # For example Creatives -> AdAccounts -> Organizations.
    # Creatives need the ad_account_id as path variable
    # AdAccounts need the organization_id as path variable
    # So organization_ids from Organizations goes as slices to AdAccounts
    # and after that ad_account_ids from AdAccounts goes as slices to Creatives for path variables
    # So first we must get the AdAccounts, then do the slicing for them
    # and then call the read_records for each slice

    # This auxiliary_id_map is used to prevent the extracting of ids that are used in most streams
    # Instead of running the request to get (for example) AdAccounts for each stream as slices we put them in the dict and
    # return if the same ids are requested in the stream. This saves us a lot of time and requests
    if parent.name in auxiliary_id_map:
        return auxiliary_id_map[parent.name]

    # Some damn logic a?
    # Relax, that has some meaning:
    # if we want to get just 1 level of parent ids (For example AdAccounts need the organization_ids from Organizations, but
    # Organizations do not have slices and returns [None] from stream_slices method) this switch goes for else clause and get all the
    # organization_ids from Organizations and return them as slices
    # But in case we want to retrieve 2 levels of parent ids (For example we run Creatives stream - it needs the ad_account_ids from AdAccount
    # and AdAccount need organization_ids from Organizations and first we must get all organization_ids
    # and for each of them get the ad_account_ids) so switch goes to if claus to get all the nested ids.
    # Let me visualize this for you:
    #
    #           organization_id_1                      organization_id_2
    #                 / \                                    / \
    #                /   \                                  /   \
    # ad_account_id_1     ad_account_id_2    ad_account_id_3     ad_account_id_4
    #
    # So for the AdAccount slices will be [{'organization_id': organization_id_1}, {'organization_id': organization_id_2}]
    # And for the Creatives (Media, Ad, AdSquad, etc...) the slices will be
    # [{'ad_account_id': ad_account_id_1}, {'ad_account_id': ad_account_id_2},
    #  {'ad_account_id': ad_account_id_3},{'ad_account_id': ad_account_id_4}]
    #
    # After getting all the account_ids, they go as slices to Creatives (Media, Ad, AdSquad, etc...)
    # and are used in the path function as a path variables according to the API docs

    parent_ids = []

    parent_stream_slices = parent.stream_slices(sync_mode=SyncMode.full_refresh)
    for parent_stream_slice in parent_stream_slices:
        records = parent.read_records(sync_mode=SyncMode.full_refresh, stream_slice=parent_stream_slice)
        for record in records:
            parent_ids.append({'id': record["id"]})

    if not parent_ids:
        return []

    auxiliary_id_map[parent.name] = parent_ids
    return parent_ids


class SnapchatMarketingStream(HttpStream, ABC):
    url_base = "https://adsapi.snapchat.com/v1/"
    primary_key = "id"

    def __init__(self, start_date, end_date, **kwargs):
        super().__init__(**kwargs)
        self.start_date = start_date
        self.end_date = end_date

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        next_page_cursor = response.json().get("paging", False)
        if next_page_cursor:
            return {"cursor": dict(parse_qsl(urlparse(next_page_cursor["next_link"]).query))["cursor"]}

    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        return next_page_token or {}

    @property
    def response_root_name(self):
        """Using the class name in lower to set the root node for response parsing"""
        return self.name

    @property
    def response_item_name(self):
        """Remove last 's' from response_root_name, see example in parse_response function"""
        return self.response_root_name[:-1]

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        """Response json came like
        {
            "organizations": [                                  <-- response_root_name
                {
                    "organization": {                           <-- response_item_name
                        "id": "some uuid",
                        "updated_at": "2020-12-15T22:35:17.819Z",
                        "created_at": "2020-12-15T11:13:03.910Z",
                        ... some_other_json_fields ...
                    }
                }
            ]
        }
        So the response_root_name will be "organizations", and the response_item_name will be "organization"
        Also, the client side filtering for incremental sync is used
        """
        json_response = response.json().get(self.response_root_name)
        for resp in json_response:
            if self.response_item_name not in resp:
                error_text = f"stream {self.name}: field named '{self.response_item_name}' is absent in the response: {resp}"
                self.logger.error(error_text)
                raise SnapchatMarketingException(error_text)
            yield resp.get(self.response_item_name)


class IncrementalSnapchatMarketingStream(SnapchatMarketingStream, ABC):
    cursor_field = "updated_at"
    parent: SnapchatMarketingStream = None

    last_slice = None
    current_slice = None
    first_run = True
    initial_state = None
    max_state = None

    def path(self, stream_slice: Mapping[str, Any] = None, **kwargs) -> str:
        return f"adaccounts/{stream_slice['id']}/{self.response_root_name}"

    def stream_slices(self, **kwargs) -> Iterable[Optional[Mapping[str, Any]]]:

        stream_state = kwargs.get("stream_state")
        self.initial_state = stream_state.get(self.cursor_field) if stream_state else self.start_date
        self.max_state = self.initial_state

        stream_slices = default_stream_slices_return_value

        if self.parent:
            parent_stream = self.parent(
                authenticator=self.authenticator,
                start_date=self.start_date,
                end_date=self.end_date
            )
            stream_slices = get_parent_ids(parent_stream)

        if not stream_slices:
            self.logger.error(f"No {'id'}s found. {self.name} cannot be extracted without {'id'}.")
            yield from []

        self.last_slice = stream_slices[-1]

        self.logger.info(f"{self.name}: stream_slices:{stream_slices}")

        yield from stream_slices

    def get_updated_state(self, current_stream_state: MutableMapping[str, Any], latest_record: Mapping[str, Any]) -> Mapping[str, Any]:
        """
        I see you have a lot of questions to this function. I will try to explain.
        The problem that it solves is next: records from the streams that used nested ids logic (see the get_parent_ids function comments below)
        can have different, non ordered timestamp in update_at cursor_field and because of they are extracted with slices
        it is a messy task to make the stream works as incremental. To understand it better the read of nested stream data can be next:

        # Reading the data subset for the ad_account_id_1 - first slice
        {"updated_at": "2021-07-22T10:32:05.719Z", other_fields}
        {"updated_at": "2021-07-22T10:47:05.780Z", other_fields}
        {"updated_at": "2021-07-22T10:42:03.830Z", other_fields}
        {"updated_at": "2021-07-21T12:20:34.927Z", other_fields}
        # Reading the data subset for the ad_account_id_2 - second slice
        {"updated_at": "2021-07-07T07:40:09.531Z", other_fields}
        {"updated_at": "2021-06-11T08:04:42.202Z", other_fields}
        {"updated_at": "2021-06-09T13:12:56.350Z", other_fields}

        As you can see the cursor_field (updated_at) values are not ordered and even more - they are descending in some kind
        The standard logic for incremental cannot be done, because in this case after the first slice
        the stream_state will be 2021-07-22T10:42:03.830Z, but the second slice data is less then this value, so it will not be yield

        So the next approach was implemented: Until the last slice is processed the stream state remains initial (whenever it is a start_date
        or the saved stream_state from the state.json), but the maximum value is calculated and saved in class max_state value.
        When the last slice is processed (we write the class last_slice value while getting the slices) the max_state value is written to stream_state
        Thus all the slices data are compared to the initial state, but only on the last one we write it to the stream state.
        This approach gives us the maximum state value of all the records and we exclude the state updates between slice processing
        """
        if self.first_run:
            self.first_run = False
            return {self.cursor_field: self.initial_state}
        else:
            self.max_state = max(self.max_state, latest_record[self.cursor_field])
            return {self.cursor_field: self.max_state if self.current_slice == self.last_slice else self.initial_state}

    def read_records(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, **kwargs
    ) -> Iterable[Mapping[str, Any]]:
        """
        This structure is used to set the class variable current_slice to the current stream slice for the
        purposes described above.
        Then all the retrieved records if the stream_state is present are filtered by the cursor_field value compared to the stream state
        This makes the incremental magic works
        """
        self.current_slice = stream_slice
        records = super().read_records(stream_slice=stream_slice, stream_state=stream_state, **kwargs)
        if stream_state:
            for record in records:
                if record[self.cursor_field] > stream_state.get(self.cursor_field):
                    yield record
        else:
            yield from records


class Organizations(SnapchatMarketingStream):
    """Docs: https://marketingapi.snapchat.com/docs/#organizations"""

    def path(self, **kwargs) -> str:
        return "me/organizations"


class Adaccounts(IncrementalSnapchatMarketingStream):
    """Docs: https://marketingapi.snapchat.com/docs/#get-all-ad-accounts"""

    parent = Organizations

    def path(self, stream_slice: Mapping[str, Any] = None, **kwargs) -> str:
        return f"organizations/{stream_slice['id']}/adaccounts"


class Creatives(IncrementalSnapchatMarketingStream):
    """Docs: https://marketingapi.snapchat.com/docs/#get-all-creatives"""

    parent = Adaccounts


class Media(IncrementalSnapchatMarketingStream):
    """Docs: https://marketingapi.snapchat.com/docs/#get-all-media"""

    parent = Adaccounts

    @property
    def response_item_name(self):
        return self.response_root_name


class Campaigns(IncrementalSnapchatMarketingStream):
    """Docs: https://marketingapi.snapchat.com/docs/#get-all-campaigns"""

    parent = Adaccounts


class Ads(IncrementalSnapchatMarketingStream):
    """Docs: https://marketingapi.snapchat.com/docs/#get-all-ads-under-an-ad-account"""

    parent = Adaccounts


class Adsquads(IncrementalSnapchatMarketingStream):
    """Docs: https://marketingapi.snapchat.com/docs/#get-all-ad-squads-under-an-ad-account"""

    parent = Adaccounts


class Segments(IncrementalSnapchatMarketingStream):
    """Docs: https://marketingapi.snapchat.com/docs/#get-all-audience-segments"""

    parent = Adaccounts


class Stats(SnapchatMarketingStream, ABC):
    """Stats streams for LIFETIME granularity.
    Docs:  https://marketingapi.snapchat.com/docs/#measurement
    """
    primary_key = ['id', 'granularity']
    schema_name = 'basic_stats'
    parent_name: str = ''  # name of parent class
    response_root_name = 'lifetime_stats'
    granularity = GranularityType.LIFETIME

    @property
    @abstractmethod
    def parent(self) -> SnapchatMarketingStream:
        """Stream Class to extract entity ids from"""

    def stream_slices(self, **kwargs) -> Iterable[Optional[Mapping[str, Any]]]:
        """Each stream slice represents each entity id from parent stream"""

        parent_stream = self.parent(
            authenticator=self.authenticator,
            start_date=self.start_date,
            end_date=self.end_date
        )
        self.parent_name = parent_stream.name
        stream_slices = get_parent_ids(parent_stream)

        if not stream_slices:
            self.logger.error(f"No {'id'}s found. {self.name} cannot be extracted without {'id'}.")

        self.logger.info(f"Stats: stream_slices:{stream_slices}")

        return stream_slices

    def path(self, stream_slice: Mapping[str, Any] = None, **kwargs) -> str:
        return f"{self.parent_name}/{stream_slice['id']}/stats"

    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:

        params = super().request_params(stream_state=stream_state, stream_slice=stream_slice, next_page_token=next_page_token)
        params['granularity'] = self.granularity.value
        if self.metrics:
            params['fields'] = ','.join(self.metrics)

        return params

    def parse_response(self,
        response: requests.Response,
        *,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None
    ) -> Iterable[Mapping]:
        """Customized by adding stream state setting"""

        for record in super().parse_response(response=response, stream_state=stream_state, stream_slice=stream_slice, next_page_token=next_page_token):
            # move all 'stats' metrics to root level
            record.update(record.pop('stats', {}))

            yield record

    def get_json_schema(self) -> Mapping[str, Any]:
        """All stats streams have same schema"""
        return ResourceSchemaLoader(package_name_from_class(self.__class__)).get_schema(self.schema_name)


class StatsIncremental(Stats, IncrementalMixin):
    """Incremental Stats class for Daily and Hourly streams which requires start/end date param"""

    primary_key = ['id', 'granularity', 'start_time']
    cursor_field: str = 'start_time'
    slice_period: int = 7  # days https://marketingapi.snapchat.com/docs/#metrics-and-supported-granularities
    number_of_parent_ids: int = 0
    number_of_last_records: int = 0
    response_root_name = 'timeseries_stats'
    response_subitem_name = 'timeseries'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._state = {}

    def date_slices(self, stream_state=None) -> Iterable[Optional[Mapping[str, Any]]]:
        date_slices = []
        # use start_date from state or from config
        start_date_str = stream_state.get(self.cursor_field) if stream_state else self.start_date
        slice_start_date = pendulum.parse(start_date_str)
        end_date = pendulum.parse(self.end_date)
        while slice_start_date < end_date:
            slice_end_date_next = slice_start_date + pendulum.duration(days=self.slice_period)
            slice_end_date = min(slice_end_date_next, end_date)
            date_slices.append({
                'start_time': slice_start_date.to_date_string(),
                'end_time': slice_end_date.to_date_string()
            })
            slice_start_date = slice_end_date

        self.logger.info(f"{self.name} date_slices: {date_slices}.")

        return date_slices

    def stream_slices(
        self, *, sync_mode: SyncMode, cursor_field: List[str] = None, stream_state: Mapping[str, Any] = None
    ) -> Iterable[Optional[Mapping[str, Any]]]:
        """Incremental stream slices is a combinations of date and parent id slices"""

        stream_slices = []

        parent_ids = super().stream_slices(sync_mode=SyncMode.full_refresh)

        # save a number of parent ids we need to get stats for
        self.number_of_parent_ids = len(parent_ids)

        # within each date period extract data for all parent_ids
        # it allows incremental sync implementation with slice period granularity
        for date_slice in self.date_slices(stream_state=stream_state):
            for parent_id in parent_ids:
                stream_slices.append({**date_slice, **parent_id})

        self.logger.info(f"{self.name} stream_slices:{stream_slices}")

        return stream_slices

    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        """start/end date param should be set for Daily and Hourly streams"""

        params = super().request_params(stream_state=stream_state, stream_slice=stream_slice, next_page_token=next_page_token)
        params['start_time'] = stream_slice['start_time']
        params['end_time'] = stream_slice['end_time']
        return params

    def update_state_after_last_record(self, record):
        """Update state if last record has been read"""
        record_end_date = record.get('end_time', '').split('T')[0]  # "2022-07-06T00:00:00.000-07:00" --> "2022-07-06"
        if record_end_date == self.end_date:
            self.number_of_last_records += 1
            # Update state if 'last' records for all dependant entities have been read
            if self.number_of_parent_ids == self.number_of_last_records:
                self.state = record_end_date

    @property
    def state(self):
        return self._state

    @state.setter
    def state(self, value):
        self._state[self.cursor_field] = value

    def parse_response(self,
        response: requests.Response,
        *,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None
    ) -> Iterable[Mapping]:
        """Customized by adding stream state setting"""

        # Update state for each date slice (start_date), it ensures that previous date slices have been read
        # and can be skipped in next incremental sync
        self.state = stream_slice[self.cursor_field]

        for record in super().parse_response(response=response, stream_state=stream_state, stream_slice=stream_slice, next_page_token=next_page_token):

            record_identifiers = {
                'id': record['id'],
                'type': record['type'],
                'granularity': record['granularity'],
            }

            # Hourly/Daily stats contains nested structure (under self.response_subitem_name) with actual stats items
            for stat_item in record.get(self.response_subitem_name, []):
                # add common record identifiers
                stat_item.update(record_identifiers)
                # move all 'stats' metrics to root level
                stat_item.update(stat_item.pop('stats', {}))

                # Update state for last record in the stream
                self.update_state_after_last_record(stat_item)

                yield stat_item


class Granularity:
    granularity: GranularityType
    metrics = METRICS + METRICS_NOT_HOURLY


class Lifetime(Granularity):
    """Example of raw response:
    {
      "request_status": "SUCCESS",
      "request_id": "d0cb395f-c39d-480d-b62c-24878c7d0b76",
      "lifetime_stats": [{                                              <-- response_root_name
          "sub_request_status": "SUCCESS",
          "lifetime_stat": {                                            <-- response_item_name
            "id": "96e5549f-4065-490e-93dd-ffb9f7973b77",
            "type": "AD",
            "granularity": "LIFETIME",
            "stats": {
              "impressions": 0,
              "swipes": 0,
              "quartile_1": 0,
              "quartile_2": 0,
            },
            "start_time": "2016-09-26T00:00:00.000-07:00",
            "end_time": "2022-07-01T07:00:00.000-07:00",
            "finalized_data_end_time": "2022-07-01T07:00:00.000-07:00",
            "conversion_data_processed_end_time": "2022-07-01T00:00:00.000Z"
          }
        }
      ]
    }
    """
    granularity = GranularityType.LIFETIME


class Hourly(Granularity):
    """Example of raw response:
    {
      "request_status": "SUCCESS",
      "request_id": "c106d757-7c07-4415-b62f-78108d9dc668",
      "timeseries_stats": [{                                        <-- response_root_name
          "sub_request_status": "SUCCESS",
              "timeseries_stat": {                                  <-- response_item_name
            "id": "417d0269-80fb-496a-b5f3-ec0bac665144",
            "type": "AD",
            "granularity": "HOUR",
            "start_time": "2022-06-24T17:00:00.000-07:00",
            "end_time": "2022-06-28T17:00:00.000-07:00",
            "finalized_data_end_time": "2022-06-30T11:00:00.000-07:00",
            "conversion_data_processed_end_time": "2022-06-30T00:00:00.000Z",
            "timeseries": [{                                        <-- response_subitem_name
                "start_time": "2022-06-24T17:00:00.000-07:00",
                "end_time": "2022-06-24T18:00:00.000-07:00",
                "stats": {
                  "impressions": 0,
                  "swipes": 0,
                  "quartile_1": 0,
                  "quartile_2": 0,
                  "quartile_3": 0,
                  "view_completion": 0,
                }
              }, {
                "start_time": "2022-06-24T18:00:00.000-07:00",
                "end_time": "2022-06-24T19:00:00.000-07:00",
                "stats": {
                  "impressions": 0,
                  "swipes": 0,
                }
              }
            ]
          }
        }
      ]
    }
    """
    granularity = GranularityType.HOUR
    metrics = METRICS
    slice_period = 7  # days https://marketingapi.snapchat.com/docs/#metrics-and-supported-granularities


class Daily(Granularity):
    """Example of raw response:
    {
      "request_status": "SUCCESS",
      "request_id": "f2cba857-e246-43bf-b644-1a0a540e1f92",
      "timeseries_stats": [{                                        <-- response_root_name
          "sub_request_status": "SUCCESS",
          "timeseries_stat": {                                      <-- response_item_name
            "id": "417d0269-80fb-496a-b5f3-ec0bac665144",
            "type": "AD",
            "granularity": "DAY",
            "start_time": "2022-06-25T00:00:00.000-07:00",
            "end_time": "2022-06-29T00:00:00.000-07:00",
            "finalized_data_end_time": "2022-06-30T00:00:00.000-07:00",
            "conversion_data_processed_end_time": "2022-06-30T00:00:00.000Z",
            "timeseries": [{                                        <-- response_subitem_name
                "start_time": "2022-06-25T00:00:00.000-07:00",
                "end_time": "2022-06-26T00:00:00.000-07:00",
                "stats": {
                  "impressions": 0,
                  "swipes": 0,
                  "quartile_1": 0,
                  "quartile_2": 0,
                  "quartile_3": 0,
                }
              }, {
                "start_time": "2022-06-26T00:00:00.000-07:00",
                "end_time": "2022-06-27T00:00:00.000-07:00",
                "stats": {
                  "impressions": 0,
                  "swipes": 0,
                  "quartile_1": 0,
                  "quartile_2": 0,
                  "quartile_3": 0,
                },
              },

            ]
          }
        }
      ]
    }

    """
    granularity = GranularityType.DAY
    slice_period = 31  # days https://marketingapi.snapchat.com/docs/#metrics-and-supported-granularities


class AdaccountsStatsHourly(Hourly, StatsIncremental):
    """Adaccounts stats with Hourly granularity: https://marketingapi.snapchat.com/docs/#get-ad-account-stats"""
    parent = Adaccounts
    metrics = ['spend']


class AdaccountsStatsDaily(Daily, StatsIncremental):
    """Adaccounts stats with Daily granularity: https://marketingapi.snapchat.com/docs/#get-ad-account-stats"""
    parent = Adaccounts
    metrics = ['spend']


class AdaccountsStatsLifetime(Lifetime, Stats):
    """Adaccounts stats with Lifetime granularity: https://marketingapi.snapchat.com/docs/#get-ad-account-stats"""
    parent = Adaccounts
    metrics = ['spend']


class AdsStatsHourly(Hourly, StatsIncremental):
    """Ads stats with Hourly granularity: https://marketingapi.snapchat.com/docs/#get-ad-stats"""
    parent = Ads


class AdsStatsDaily(Daily, StatsIncremental):
    """Ads stats with Daily granularity: https://marketingapi.snapchat.com/docs/#get-ad-stats"""
    parent = Ads


class AdsStatsLifetime(Lifetime, Stats):
    """Ads stats with Lifetime granularity: https://marketingapi.snapchat.com/docs/#get-ad-stats"""
    parent = Ads


class AdsquadsStatsHourly(Hourly, StatsIncremental):
    """Adsquads stats with Hourly granularity: https://marketingapi.snapchat.com/docs/#get-ad-squad-stats"""
    parent = Adsquads


class AdsquadsStatsDaily(Daily, StatsIncremental):
    """Adsquads stats with Daily granularity: https://marketingapi.snapchat.com/docs/#get-ad-squad-stats"""
    parent = Adsquads


class AdsquadsStatsLifetime(Lifetime, Stats):
    """Adsquads stats with Lifetime granularity: https://marketingapi.snapchat.com/docs/#get-ad-squad-stats"""
    parent = Adsquads


class CampaignsStatsHourly(Hourly, StatsIncremental):
    """Campaigns stats with Hourly granularity: https://marketingapi.snapchat.com/docs/#get-campaign-stats"""
    parent = Campaigns


class CampaignsStatsDaily(Daily, StatsIncremental):
    """Campaigns stats with Daily granularity: https://marketingapi.snapchat.com/docs/#get-campaign-stats"""
    parent = Campaigns


class CampaignsStatsLifetime(Lifetime, Stats):
    """Campaigns stats with Lifetime granularity: https://marketingapi.snapchat.com/docs/#get-campaign-stats"""
    parent = Campaigns


class SnapchatAdsOauth2Authenticator(Oauth2Authenticator):
    """Request example for API token extraction:
    curl -X POST https://accounts.snapchat.com/login/oauth2/access_token \
      -d "refresh_token={refresh_token}" \
      -d "client_id={client_id}" \
      -d "client_secret={client_secret}"  \
      -d "grant_type=refresh_token"  \
    """

    def __init__(self, config):
        super().__init__(
            token_refresh_endpoint="https://accounts.snapchat.com/login/oauth2/access_token",
            client_id=config["client_id"],
            client_secret=config["client_secret"],
            refresh_token=config["refresh_token"],
        )

    def refresh_access_token(self) -> Tuple[str, int]:
        response_json = None
        try:
            response = requests.request(method="POST", url=self.token_refresh_endpoint, data=self.get_refresh_request_body())
            response_json = response.json()
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            if response_json and "error" in response_json:
                raise Exception(
                    "Error refreshing access token. Error: {}; Error details: {}; Exception: {}".format(
                        response_json["error"], response_json["error_description"], e
                    )
                ) from e
            raise Exception(f"Error refreshing access token: {e}") from e
        else:
            return response_json["access_token"], response_json["expires_in"]


# Source
class SourceSnapchatMarketing(AbstractSource):
    """Source Snapchat Marketing helps to retrieve the different Ad data from Snapchat business account"""

    def check_connection(self, logger, config) -> Tuple[bool, any]:

        try:
            auth = SnapchatAdsOauth2Authenticator(config)
            token = auth.get_access_token()
            url = f"{SnapchatMarketingStream.url_base}me"

            session = requests.get(url, headers={"Authorization": "Bearer {}".format(token)})
            session.raise_for_status()
            return True, None

        except requests.exceptions.RequestException as e:
            return False, e

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:

        # https://marketingapi.snapchat.com/docs/#core-metrics
        # IMPORTANT: Metrics are finalized 48 hours after the end of the day in the Ad Account’s timezone.
        DELAYED_DAYS = 2

        # Start/End dates should better be in YYYY-MM-DD format:
        # 1. minutes are not supported
        # 2. when timezone is not specified, default account's timezone will be used automatically
        default_end_date = pendulum.now().subtract(days=DELAYED_DAYS).to_date_string()
        kwargs = {
            "authenticator": SnapchatAdsOauth2Authenticator(config),
            "start_date": config["start_date"],
            "end_date": config.get("end_date", default_end_date)
        }

        return [
            # Base streams:
            Adaccounts(**kwargs),
            Ads(**kwargs),
            Adsquads(**kwargs),
            Campaigns(**kwargs),
            Creatives(**kwargs),
            Media(**kwargs),
            Organizations(**kwargs),
            Segments(**kwargs),
            # Stats streams:
            AdaccountsStatsHourly(**kwargs),
            AdaccountsStatsDaily(**kwargs),
            AdaccountsStatsLifetime(**kwargs),
            AdsStatsHourly(**kwargs),
            AdsStatsDaily(**kwargs),
            AdsStatsLifetime(**kwargs),
            AdsquadsStatsHourly(**kwargs),
            AdsquadsStatsDaily(**kwargs),
            AdsquadsStatsLifetime(**kwargs),
            CampaignsStatsHourly(**kwargs),
            CampaignsStatsDaily(**kwargs),
            CampaignsStatsLifetime(**kwargs),
        ]
