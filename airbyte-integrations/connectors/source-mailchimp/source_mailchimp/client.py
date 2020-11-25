"""
MIT License

Copyright (c) 2020 Airbyte

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"""

import json
import pkgutil
from dateutil import parser

from datetime import datetime

from typing import Dict, Generator, Any, DefaultDict
from airbyte_protocol import AirbyteStream, AirbyteMessage, AirbyteStateMessage, AirbyteRecordMessage, Type
from mailchimp3 import MailChimp
from mailchimp3.mailchimpclient import MailChimpError

from .models import HealthCheckError


class Client:
    API_MAILCHIMP_URL = "https://api.mailchimp.com/schema/3.0/Definitions/{}/Response.json"
    PAGINATION = 100
    _CAMPAIGNS = "Campaigns"
    _LISTS = "Lists"
    _ENTITIES = [_CAMPAIGNS, _LISTS]

    def __init__(self, username: str, apikey: str):
        self._client = MailChimp(mc_api=apikey, mc_user=username)

        """ TODO:
        Authorized Apps
        Automations
        Campaign Folders
        Chimp Chatter Activity
        Connected Sites
        Conversations
        E-Commerce Stores
        Facebook Ads
        Files
        Landing Pages
        Ping
        Reports
        """

    def health_check(self):
        try:
            self._client.ping.get()
            return True, None
        except MailChimpError as err:
            return False, HealthCheckError.parse_obj(err.args[0])

    def get_streams(self):
        streams = []
        for entity in self._ENTITIES:
            raw_schema = json.loads(pkgutil.get_data(self.__class__.__module__.split(".")[0], f"schemas/{entity}.json"))
            streams.append(AirbyteStream.parse_obj(raw_schema))
        return streams

    def lists(self, state: DefaultDict[str, any] = None) -> Generator[AirbyteMessage, None, None]:
        cursor_field = 'date_created'
        if state and self._LISTS in state and state[self._LISTS][cursor_field]:
            date_created = state[self._LISTS][cursor_field]
        else:
            date_created = None

        default_params = {'since_date_created': date_created, 'sort_field': cursor_field, 'sort_dir': 'ASC'}
        limit = self._client.lists.all(dict(default_params, count=1))["total_items"]
        offset = 0
        max_date_created = date_created
        while offset < limit:
            lists_response = self._client.lists.all(dict(default_params, count=self.PAGINATION, offset=offset))["lists"]
            for mc_list in lists_response:
                list_created_at = parser.isoparse(mc_list[cursor_field])
                max_date_created = max(max_date_created, list_created_at) if max_date_created else list_created_at
                yield self._record(stream=self._LISTS, data=mc_list)

            if max_date_created:
                state[self._LISTS][cursor_field] = max_date_created
                yield self._state(state)
            offset += self.PAGINATION

    def campaigns(self, state: DefaultDict[str, any]) -> Generator[AirbyteMessage, None, None]:
        cursor_field = 'create_time'
        if state and self._CAMPAIGNS in state and state[self._CAMPAIGNS][cursor_field]:
            create_time = state[self._CAMPAIGNS][cursor_field]
        else:
            create_time = None

        default_params = {'since_create_time': create_time, 'sort_field': cursor_field, 'sort_dir': 'ASC'}
        limit = self._client.campaigns.all(dict(default_params, count=1))["total_items"]
        offset = 0
        max_create_time = create_time
        while offset < limit:
            campaigns_response = self._client.campaigns.all(dict(default_params, count=self.PAGINATION, offset=offset))
            for campaign in campaigns_response["campaigns"]:
                campaign_created_at = parser.isoparse(campaign[cursor_field])
                max_create_time = max(max_create_time, campaign_created_at) if max_create_time else campaign_created_at
                yield self._record(stream=self._CAMPAIGNS, data=campaign)

            if max_create_time:
                state[self._CAMPAIGNS][cursor_field] = max_create_time
                yield self._state(state)

            offset += self.PAGINATION

    @staticmethod
    def _record(stream: str, data: Dict[str, any]) -> AirbyteMessage:
        now = int(datetime.now().timestamp()) * 1000
        return AirbyteMessage(type=Type.RECORD, record=AirbyteRecordMessage(stream=stream, data=data, emitted_at=now))

    @staticmethod
    def _state(data: Dict[str, any]):
        return AirbyteMessage(type=Type.STATE, state=AirbyteStateMessage(data=data))
