#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#


from abc import ABC
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple
import re
import requests
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.sources.streams.http.auth import TokenAuthenticator
from airbyte_cdk.models import SyncMode

"""
TODO: Most comments in this class are instructive and should be deleted after the source is implemented.

This file provides a stubbed example of how to use the Airbyte CDK to develop both a source connector which supports full refresh or and an
incremental syncs from an HTTP API.

The various TODOs are both implementation hints and steps - fulfilling all the TODOs should be sufficient to implement one basic and one incremental
stream from a source. This pattern is the same one used by Airbyte internally to implement connectors.

The approach here is not authoritative, and devs are free to use their own judgement.

There are additional required TODOs in the files within the integration_tests folder and the spec.yaml file.
"""


# Basic full refresh stream
class ZendeskSellStream(HttpStream, ABC):
    """
    TODO remove this comment

    This class represents a stream output by the connector.
    This is an abstract base class meant to contain all the common functionality at the API level e.g: the API base URL, pagination strategy,
    parsing responses etc..

    Each stream should extend this class (or another abstract subclass of it) to specify behavior unique to that stream.

    Typically for REST APIs each stream corresponds to a resource in the API. For example if the API
    contains the endpoints
        - GET v1/customers
        - GET v1/employees

    then you should have three classes:
    `class ZendeskSellStream(HttpStream, ABC)` which is the current class
    `class Customers(ZendeskSellStream)` contains behavior to pull data for customers using v1/customers
    `class Employees(ZendeskSellStream)` contains behavior to pull data for employees using v1/employees

    If some streams implement incremental sync, it is typical to create another class
    `class IncrementalZendeskSellStream((ZendeskSellStream), ABC)` then have concrete stream implementations extend it. An example
    is provided below.

    See the reference docs for the full list of configurable options.
    """

    url_base = "https://api.getbase.com/v2/"
    primary_key = None

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        """
        TODO: Override this method to define a pagination strategy. If you will not be using pagination, no action is required - just return None.

        This method should return a Mapping (e.g: dict) containing whatever information required to make paginated requests. This dict is passed
        to most other methods in this class to help you form headers, request bodies, query params, etc..

        For example, if the API accepts a 'page' parameter to determine which page of the result to return, and a response from the API contains a
        'page' number, then this method should probably return a dict {'page': response.json()['page'] + 1} to increment the page count by 1.
        The request_params method should then read the input next_page_token and set the 'page' param to next_page_token['page'].

        :param response: the most recent response from the API
        :return If there is another page in the result, a mapping (e.g: dict) containing information needed to query the next page in the response.
                If there are no more pages in the result, return None.
        """
        regex_page='[=?/]page[_=/-]?(\d{1,3})'
        meta_links = response.json()['meta']['links']
        if 'next_page' in meta_links.keys() :
            {'page' : re.findall(regex_page, meta_links['next_page'])[0]}
        else:
            return None

    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        """
        TODO: Override this method to define any query parameters to be set. Remove this method if you don't need to define request params.
        Usually contains common params e.g. pagination size etc.
        """
        return {'page': next_page_token, 'per_page' : 100}

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        """
        TODO: Override this method to define how a response is parsed.
        :return an iterable containing each record in the response
        """
        items = response.json()['items']
        return [item['data'] for item in items]


class Pipelines(ZendeskSellStream):
    """
    Docs: https://developer.zendesk.com/api-reference/sales-crm/resources/pipelines/
    """
    primary_key = "id"

    def path(self, **kwargs) -> str:
        return "pipelines"

class Stages(ZendeskSellStream):
    """
    Docs: https://developer.zendesk.com/api-reference/sales-crm/resources/stages/
    """
    primary_key = "id"

    def path(self, **kwargs) -> str:
        return "stages"


# # Basic incremental stream
# class IncrementalZendeskSellStream(ZendeskSellStream, ABC):
#     """
#     TODO fill in details of this class to implement functionality related to incremental syncs for your connector.
#          if you do not need to implement incremental sync for any streams, remove this class.
#     """

#     # TODO: Fill in to checkpoint stream reads after N records. This prevents re-reading of data if the stream fails for any reason.
#     state_checkpoint_interval = 100

#     @property
#     def cursor_field(self) -> str:
#         """
#         TODO
#         Override to return the cursor field used by this stream e.g: an API entity might always use created_at as the cursor field. This is
#         usually id or date based. This field's presence tells the framework this in an incremental stream. Required for incremental.

#         :return str: The name of the cursor field.
#         """
#         return "updated_at"

#     def get_updated_state(self, current_stream_state: MutableMapping[str, Any], latest_record: Mapping[str, Any]) -> Mapping[str, Any]:
#         """
#         Override to determine the latest state after reading the latest record. This typically compared the cursor_field from the latest record and
#         the current state and picks the 'most' recent cursor. This is how a stream's state is determined. Required for incremental.
#         """
#         return {}


class Contacts(ZendeskSellStream):
    """
    Docs: https://developer.zendesk.com/api-reference/sales-crm/resources/contacts/
    """
    primary_key = "id"

    def path(self, **kwargs) -> str:
        return "contacts"

class Deals(ZendeskSellStream):
    """
    Docs: https://developer.zendesk.com/api-reference/sales-crm/resources/deals/
    """
    primary_key = "id"

    def path(self, **kwargs) -> str:
        return "deals"

class Leads(ZendeskSellStream):
    """
    Docs: https://developer.zendesk.com/api-reference/sales-crm/resources/leads/
    """
    primary_key = "id"

    def path(self, **kwargs) -> str:
        return "leads"


# Source
class SourceZendeskSell(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, any]:
        """
        TODO: Implement a connection check to validate that the user-provided config can be used to connect to the underlying API

        See https://github.com/airbytehq/airbyte/blob/master/airbyte-integrations/connectors/source-stripe/source_stripe/source.py#L232
        for an example.

        :param config:  the user-input config object conforming to the connector's spec.yaml
        :param logger:  logger object
        :return Tuple[bool, any]: (True, None) if the input config can be used to connect to the API successfully, (False, error) otherwise.
        """
        try:
            authenticator = TokenAuthenticator(token = config["api_token"])
            stream = Contacts(authenticator=authenticator)
            records = stream.read_records(sync_mode=SyncMode.full_refresh)
            next(records)
            return True, None
        except Exception as e:
            return False, e


    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        """
        TODO: Replace the streams below with your own streams.

        :param config: A Mapping of the user input configuration as defined in the connector spec.
        """
        # TODO remove the authenticator if not required.
        auth = TokenAuthenticator(token=config["api_token"])  # Oauth2Authenticator is also available if you need oauth support
        return [Contacts(authenticator=auth), Deals(authenticator=auth), Leads(authenticator=auth), Pipelines(authenticator=auth), Stages(authenticator=auth)]
