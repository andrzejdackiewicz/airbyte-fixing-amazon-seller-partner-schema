#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#

from dataclasses import InitVar, dataclass, field
from typing import Any, List, Mapping, Optional, Union

import pendulum
from airbyte_cdk.sources.declarative.auth.declarative_authenticator import DeclarativeAuthenticator
from airbyte_cdk.sources.declarative.interpolation.interpolated_mapping import InterpolatedMapping
from airbyte_cdk.sources.declarative.interpolation.interpolated_string import InterpolatedString
from airbyte_cdk.sources.streams.http.requests_native_auth.abstract_oauth import AbstractOauth2Authenticator
from dataclasses_jsonschema import JsonSchemaMixin


@dataclass
class DeclarativeOauth2Authenticator(AbstractOauth2Authenticator, DeclarativeAuthenticator, JsonSchemaMixin):
    """
    Generates OAuth2.0 access tokens from an OAuth2.0 refresh token and client credentials based on
    a declarative connector configuration file. Credentials can be defined explicitly or via interpolation
    at runtime. The generated access token is attached to each request via the Authorization header.

    Attributes:
        token_refresh_endpoint (Union[InterpolatedString, str]): The endpoint to refresh the access token
        client_id (Union[InterpolatedString, str]): The client id
        client_secret (Union[InterpolatedString, str]): Client secret
        refresh_token (Union[InterpolatedString, str]): The token used to refresh the access token
        access_token_name (Union[InterpolatedString, str]): THe field to extract access token from in the response
        expires_in_name (Union[InterpolatedString, str]): The field to extract expires_in from in the response
        config (Mapping[str, Any]): The user-provided configuration as specified by the source's spec
        scopes (Optional[List[str]]): The scopes to request
        token_expiry_date (Optional[Union[InterpolatedString, str]]): The access token expiration date
        token_expiry_date_format str: format of the datetime; provide it if expires_in is returned in datetime instead of seconds
        refresh_request_body (Optional[Mapping[str, Any]]): The request body to send in the refresh request
        grant_type: The grant_type to request for access_token
    """

    token_refresh_endpoint: Union[InterpolatedString, str]
    client_id: Union[InterpolatedString, str]
    client_secret: Union[InterpolatedString, str]
    refresh_token: Union[InterpolatedString, str]
    config: Mapping[str, Any]
    parameters: InitVar[Mapping[str, Any]]
    scopes: Optional[List[str]] = None
    token_expiry_date: Optional[Union[InterpolatedString, str]] = None
    _token_expiry_date: pendulum.DateTime = field(init=False, repr=False, default=None)
    token_expiry_date_format: str = None
    access_token_name: Union[InterpolatedString, str] = "access_token"
    expires_in_name: Union[InterpolatedString, str] = "expires_in"
    refresh_request_body: Optional[Mapping[str, Any]] = None
    grant_type: Union[InterpolatedString, str] = "refresh_token"

    def __post_init__(self, parameters: Mapping[str, Any]):
        self.token_refresh_endpoint = InterpolatedString.create(self.token_refresh_endpoint, parameters=parameters)
        self.client_id = InterpolatedString.create(self.client_id, parameters=parameters)
        self.client_secret = InterpolatedString.create(self.client_secret, parameters=parameters)
        self.refresh_token = InterpolatedString.create(self.refresh_token, parameters=parameters)
        self.access_token_name = InterpolatedString.create(self.access_token_name, parameters=parameters)
        self.expires_in_name = InterpolatedString.create(self.expires_in_name, parameters=parameters)
        self.grant_type = InterpolatedString.create(self.grant_type, parameters=parameters)
        self._refresh_request_body = InterpolatedMapping(self.refresh_request_body or {}, parameters=parameters)
        self._token_expiry_date = (
            pendulum.parse(InterpolatedString.create(self.token_expiry_date, parameters=parameters).eval(self.config))
            if self.token_expiry_date
            else pendulum.now().subtract(days=1)
        )
        self._access_token = None

    def get_token_refresh_endpoint(self) -> str:
        return self.token_refresh_endpoint.eval(self.config)

    def get_client_id(self) -> str:
        return self.client_id.eval(self.config)

    def get_client_secret(self) -> str:
        return self.client_secret.eval(self.config)

    def get_refresh_token(self) -> str:
        return self.refresh_token.eval(self.config)

    def get_scopes(self) -> [str]:
        return self.scopes

    def get_access_token_name(self) -> InterpolatedString:
        return self.access_token_name.eval(self.config)

    def get_expires_in_name(self) -> InterpolatedString:
        return self.expires_in_name.eval(self.config)

    def get_grant_type(self) -> InterpolatedString:
        return self.grant_type.eval(self.config)

    def get_refresh_request_body(self) -> Mapping[str, Any]:
        return self._refresh_request_body.eval(self.config)

    def get_token_expiry_date(self) -> pendulum.DateTime:
        return self._token_expiry_date

    def set_token_expiry_date(self, initial_time: pendulum.DateTime, value: Union[str, int]):
        if self.token_expiry_date_format:
            self._token_expiry_date = pendulum.from_format(value, self.token_expiry_date_format)
        else:
            self._token_expiry_date = initial_time.add(seconds=value)

    @property
    def access_token(self) -> str:
        return self._access_token

    @access_token.setter
    def access_token(self, value: str):
        self._access_token = value
