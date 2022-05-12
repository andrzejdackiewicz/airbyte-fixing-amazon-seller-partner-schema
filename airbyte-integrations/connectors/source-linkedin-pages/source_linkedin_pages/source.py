#
# Copyright (c) 2021 Airbyte, Inc., all rights reserved.
#


from abc import ABC, abstractproperty
from typing import Any, Dict, Iterable, List, Mapping, MutableMapping, Optional, Tuple
from urllib.parse import urlencode

import requests
from airbyte_cdk import AirbyteLogger
from airbyte_cdk.models import SyncMode
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.sources.streams.http.auth import Oauth2Authenticator, TokenAuthenticator
# from .utils import get_parent_stream_values, transform_data


class LinkedinPagesStream(HttpStream):
    url_base = "https://api.linkedin.com/v2/"
    primary_key = None

    # Set this as a noop.
    primary_key = None

    def __init__(self, config: Dict):
        super().__init__(authenticator=config.get("authenticator"))
        self.config = config

    @property
    def org(self):
        """Property to return the list of the user Account Ids from input"""
        return ",".join(map(str, self.config.get("org_ids")))
        

    def next_page_token(self) -> Optional[Mapping[str, Any]]:
        # The API does not offer pagination, so we return None to indicate there are no more pages in the response
        return None

    def path(self, **kwargs) -> str:
        """Returns the API endpoint path for stream, from `endpoint` class attribute."""
        return self.endpoint

    def parse_response(
        self,
        response: requests.Response,
        stream_state: Mapping[str, Any] = None,
        stream_slice: Mapping[str, Any] = None
    ) -> Iterable[Mapping]:
        return [response.json()]


class OrganizationLookup(LinkedinPagesStream):

        endpoint = "organizations/35571209"

  # def request_params(self, stream_state: Mapping[str, Any], **kwargs) -> MutableMapping[str, Any]:
        """
        Override request_params() to have the ability to accept the specific account_ids from user's configuration.
        If we have list of account_ids, we need to make sure that the request_params are encoded correctly,
        We will get HTTP Error 500, if we use standard requests.urlencode methods to parse parameters,
        so the urlencode(..., safe=":(),") is used instead, to keep the values as they are.
        """
        # params = super().request_params(stream_state=stream_state, **kwargs)
        # if self.org:
        #     params[self.org]
        # return params


class SourceLinkedinPages(AbstractSource):
    """
    Abstract Source inheritance, provides:
    - implementation for `check` connector's connectivity
    - implementation to call each stream with it's input parameters.
    """

    @classmethod
    def get_authenticator(cls, config: Mapping[str, Any]) -> TokenAuthenticator:
        """
        Validate input parameters and generate a necessary Authentication object
        This connectors support 2 auth methods:
        1) direct access token with TTL = 2 months
        2) refresh token (TTL = 1 year) which can be converted to access tokens
           Every new refresh revokes all previous access tokens q
        """
        auth_method = config.get("credentials", {}).get("auth_method")
        if not auth_method or auth_method == "access_token":
            # support of backward compatibility with old exists configs
            access_token = config["credentials"]["access_token"] if auth_method else config["access_token"]
            return TokenAuthenticator(token=access_token)
        elif auth_method == "oAuth2.0":
            return Oauth2Authenticator(
                token_refresh_endpoint="https://www.linkedin.com/oauth/v2/accessToken",
                client_id=config["credentials"]["client_id"],
                client_secret=config["credentials"]["client_secret"],
                refresh_token=config["credentials"]["refresh_token"],
            )
        raise Exception("incorrect input parameters")

    def check_connection(self, logger: AirbyteLogger, config: Mapping[str, Any]) -> Tuple[bool, any]:
        """
        Testing connection availability for the connector.
        :: for this check method the Customer must have the "r_liteprofile" scope enabled.
        :: more info: https://docs.microsoft.com/linkedin/consumer/integrations/self-serve/sign-in-with-linkedin
        """

        config["authenticator"] = self.get_authenticator(config)
        stream = OrganizationLookup(config)
        # need to load the first item only
        stream.records_limit = 1
        try:
            next(stream.read_records(sync_mode=SyncMode.full_refresh), None)
            return True, None
        except Exception as e:
            return False, e


    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        """
        Mapping a input config of the user input configuration as defined in the connector spec.
        Passing config to the streams.
        """
        config["authenticator"] = self.get_authenticator(config)
        return [
            OrganizationLookup(config)
        ]