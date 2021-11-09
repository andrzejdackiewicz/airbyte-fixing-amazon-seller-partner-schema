#
# Copyright (c) 2021 Airbyte, Inc., all rights reserved.
#


from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Iterable, Mapping, MutableMapping, Optional
from urllib.parse import parse_qs, urlparse

import pendulum as pendulum
import requests
from airbyte_cdk.sources.streams.http import HttpStream


class ZendeskTalkStream(HttpStream, ABC):
    """Base class for streams"""

    primary_key = "id"

    def __init__(self, subdomain: str, **kwargs):
        """ Constructor, accepts subdomain to calculate correct url"""
        super().__init__(**kwargs)
        self._subdomain = subdomain

    @property
    @abstractmethod
    def data_field(self) -> str:
        """ Specifies root object name in a stream response"""

    @property
    def url_base(self) -> str:
        """ API base url based on configured subdomain"""
        return f"https://{self._subdomain}.zendesk.com/api/v2/channels/voice"

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        """
        This method should return a Mapping (e.g: dict) containing whatever information required to make paginated requests. This dict is passed
        to most other methods in this class to help you form headers, request bodies, query params, etc..

        :param response: the most recent response from the API
        :return If there is another page in the result, a mapping (e.g: dict) containing information needed to query the next page in the response.
                If there are no more pages in the result, return None.
        """
        response_json = response.json()
        next_page_url = response_json.get("next_page")
        if not next_page_url:
            return None

        if not response_json.get("count") or response_json.get("count") > 1:
            next_url = urlparse(next_page_url)
            next_params = parse_qs(next_url.query)
            return next_params

        return None

    def request_params(
        self,
        stream_state: Mapping[str, Any],
        stream_slice: Optional[Mapping[str, Any]] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> MutableMapping[str, Any]:
        """Usually contains common params e.g. pagination size etc."""
        return dict(next_page_token or {})

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        """ Simply parse json and iterates over root object"""
        response_json = response.json()
        if self.data_field:
            response_json = response_json[self.data_field]
        yield from response_json


class ZendeskTalkIncrementalStream(ZendeskTalkStream, ABC):
    """Stream that supports state and incremental read"""

    # required to support old format as well (only read, but save as new)
    legacy_cursor_field = "timestamp"

    def __init__(self, start_date: datetime, **kwargs):
        super().__init__(**kwargs)
        self._start_date = pendulum.instance(start_date)

    def get_updated_state(self, current_stream_state: MutableMapping[str, Any], latest_record: Mapping[str, Any]) -> Mapping[str, Any]:
        """
        Override to determine the latest state after reading the latest record. This typically compared the cursor_field from the latest record and
        the current state and picks the 'most' recent cursor. This is how a stream's state is determined. Required for incremental.
        """
        latest_state = current_stream_state.get(self.cursor_field, current_stream_state.get(self.legacy_cursor_field))
        new_cursor_value = max(latest_record[self.cursor_field], latest_state or latest_record[self.cursor_field])
        return {self.cursor_field: new_cursor_value}

    def request_params(self, stream_state=None, **kwargs):
        """ Add incremental parameters"""
        params = super().request_params(stream_state=stream_state, **kwargs)

        stream_state = stream_state or {}
        state_str = stream_state.get(self.cursor_field, stream_state.get(self.legacy_cursor_field))
        state = pendulum.parse(state_str) if state_str else self._start_date
        params["start_time"] = int(max(state, self._start_date).timestamp())

        return params


class PhoneNumbers(ZendeskTalkStream):
    """Phone Numbers
    Docs: https://developer.zendesk.com/api-reference/voice/talk-api/phone_numbers/#list-phone-numbers
    """

    path = "/phone_numbers"
    data_field = "phone_numbers"


class Addresses(ZendeskTalkStream):
    """Addresses
    Docs: https://developer.zendesk.com/api-reference/voice/talk-api/addresses/#list-addresses
    """

    path = "/addresses"
    data_field = "addresses"


class GreetingCategories(ZendeskTalkStream):
    """Greeting Categories
    Docs: https://developer.zendesk.com/rest_api/docs/voice-api/greetings#list-greeting-categories
    """

    path = "/greeting_categories"
    data_field = "greeting_categories"


class Greetings(ZendeskTalkStream):
    """Greetings
    Docs: https://developer.zendesk.com/rest_api/docs/voice-api/greetings#list-greetings
    """

    path = "/greetings"
    data_field = "greetings"


class IVRs(ZendeskTalkStream):
    """IVRs
    Docs: https://developer.zendesk.com/rest_api/docs/voice-api/ivrs#list-ivrs
    """

    path = "/ivr.json"
    data_field = "ivrs"
    use_cache = True


class IVRMenus(IVRs):
    """IVR Menus
    Docs: https://developer.zendesk.com/rest_api/docs/voice-api/ivrs#list-ivrs
    """

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        """ Simply parse json and iterates over root object"""
        ivrs = super().parse_response(response=response, **kwargs)
        for ivr in ivrs:
            for menu in ivr["menus"]:
                yield {"ivr_id": ivr["id"], **menu}


class IVRRoutes(IVRs):
    """IVR Routes
    Docs: https://developer.zendesk.com/rest_api/docs/voice-api/ivr_routes#list-ivr-routes
    """

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        """ Simply parse json and iterates over root object"""
        ivrs = super().parse_response(response=response, **kwargs)
        for ivr in ivrs:
            for menu in ivr["menus"]:
                for route in ivr["menus"]:
                    yield {"ivr_id": ivr["id"], "ivr_menu_id": menu["id"], **route}


class AccountOverview(ZendeskTalkStream):
    """Account Overview
    Docs: https://developer.zendesk.com/rest_api/docs/voice-api/stats#show-account-overview
    """

    path = "/stats/account_overview"
    data_field = "account_overview"


class AgentsActivity(ZendeskTalkStream):
    """Agents Activity
    Docs: https://developer.zendesk.com/rest_api/docs/voice-api/stats#list-agents-activity
    """

    path = "/stats/agents_activity"
    data_field = "agents_activity"


class AgentsOverview(ZendeskTalkStream):
    """Agents Overview
    Docs: https://developer.zendesk.com/rest_api/docs/voice-api/stats#show-agents-overview
    """

    path = "/stats/agents_overview"
    data_field = "agents_overview"


class CurrentQueueActivity(ZendeskTalkStream):
    """Current Queue Activity
    Docs: https://developer.zendesk.com/rest_api/docs/voice-api/stats#show-current-queue-activity
    """

    path = "/stats/current_queue_activity"
    data_field = "current_queue_activity"


class Calls(ZendeskTalkIncrementalStream):
    """Calls
    Docs: https://developer.zendesk.com/rest_api/docs/voice-api/incremental_exports#incremental-calls-export
    """

    path = "/stats/incremental/calls"
    data_field = "calls"
    cursor_field = "updated_at"


class CallLegs(ZendeskTalkIncrementalStream):
    """Call Legs
    Docs: https://developer.zendesk.com/rest_api/docs/voice-api/incremental_exports#incremental-call-legs-export
    """

    path = "/stats/incremental/legs"
    data_field = "legs"
    cursor_field = "updated_at"
