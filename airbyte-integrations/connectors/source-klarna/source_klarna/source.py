#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#


from abc import ABC
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple
from urllib.parse import urlparse, parse_qs

import requests
from airbyte_cdk.models import SyncMode
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.sources.streams.http.requests_native_auth import BasicHttpAuthenticator

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
class KlarnaStream(HttpStream, ABC):
    def __init__(self, region: str, playground: bool, authenticator: BasicHttpAuthenticator, **kwargs):
        self.region = region
        self.playground = playground
        self.kwargs = kwargs
        super().__init__(authenticator=authenticator)

    page_size = 500

    @property
    def url_base(self) -> str:
        playground_path = 'playground.' if self.playground else ''
        if self.region == 'eu':
            endpoint = f"https://api.{playground_path}klarna.com/"
        else:
            endpoint = f"https://api-{self.region}.{playground_path}klarna.com/"
        return endpoint

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        response_json = response.json()
        if 'pagination' in response_json:
            if 'next' in response_json['pagination']:
                parsed_url = urlparse(response_json['pagination']['next'])
                query_params = parse_qs(parsed_url.query)
                # noinspection PyTypeChecker
                return query_params
        else:
            return None

    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        if next_page_token:
            return dict(next_page_token)
        else:
            return {"offset": 0, "size": self.page_size}


class Payouts(KlarnaStream):
    """
    Payouts read from Klarna Settlements API https://developers.klarna.com/api/?json#settlements-api
    """
    primary_key = "payout_date"  # TODO verify

    def path(
            self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None,
            next_page_token: Mapping[str, Any] = None
    ) -> str:
        return "/settlements/v1/payouts"

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        """
        :return an iterable containing each record in the response
        """
        payouts = response.json().get("payouts", [])
        yield from payouts


class Transactions(KlarnaStream):
    """
    Transactions read from Klarna Settlements API https://developers.klarna.com/api/?json#settlements-api
    """
    primary_key = "capture_id" # TODO verify

    def path(
            self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None,
            next_page_token: Mapping[str, Any] = None
    ) -> str:
        return "/settlements/v1/transactions"

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        """
        :return an iterable containing each record in the response
        """
        transactions = response.json().get("transactions", [])
        yield from transactions


# Source
class SourceKlarna(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, any]:
        """
        See https://github.com/airbytehq/airbyte/blob/master/airbyte-integrations/connectors/source-stripe/source_stripe/source.py#L232
        for an example.

        :param config:  the user-input config object conforming to the connector's spec.yaml
        :param logger:  logger object
        :return Tuple[bool, any]: (True, None) if the input config can be used to connect to the API successfully, (False, error) otherwise.
        """
        try:
            auth = BasicHttpAuthenticator(username=config['username'], password=config['password'])
            conn_test_stream = Transactions(authenticator=auth, **config)
            conn_test_stream.page_size = 1
            conn_test_stream.next_page_token = lambda x: None
            records = conn_test_stream.read_records(sync_mode=SyncMode.full_refresh)
            # Try to read one value from records iterator
            next(records, None)
            return True, None
        except Exception as e:
            print(e)
            return False, repr(e)

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        """
        :param config: A Mapping of the user input configuration as defined in the connector spec.
        """
        auth = BasicHttpAuthenticator(username=config['username'], password=config['password'])
        return [Payouts(authenticator=auth, **config), Transactions(authenticator=auth, **config)]
