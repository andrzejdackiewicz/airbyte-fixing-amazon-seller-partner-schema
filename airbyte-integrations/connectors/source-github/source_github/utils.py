#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import time
from dataclasses import dataclass
from itertools import cycle
from typing import Any, List, Mapping

import pendulum
import requests
from airbyte_cdk.models import SyncMode
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.sources.streams.http.requests_native_auth import TokenAuthenticator
from airbyte_cdk.sources.streams.http.requests_native_auth.abstract_token import AbstractHeaderAuthenticator
from airbyte_cdk.utils import AirbyteTracedException
from airbyte_protocol.models import FailureType


def getter(D: dict, key_or_keys, strict=True):
    if not isinstance(key_or_keys, list):
        key_or_keys = [key_or_keys]
    for k in key_or_keys:
        if strict:
            D = D[k]
        else:
            D = D.get(k, {})
    return D


def read_full_refresh(stream_instance: Stream):
    slices = stream_instance.stream_slices(sync_mode=SyncMode.full_refresh)
    for _slice in slices:
        records = stream_instance.read_records(stream_slice=_slice, sync_mode=SyncMode.full_refresh)
        for record in records:
            yield record


@dataclass
class Token:
    count_rest: int
    count_graphql: int
    update_at: pendulum.DateTime
    reset_at_rest: pendulum.DateTime
    reset_at_graphql: pendulum.DateTime


class MultipleTokenAuthenticatorWithRateLimiter(AbstractHeaderAuthenticator):
    """
    Each token in the cycle is checked against the rate limiter.
    If a token exceeds the capacity limit, the system switches to another token.
    If all tokens are exhausted, the system will enter a sleep state until
    the first token becomes available again.
    """

    DURATION = pendulum.duration(seconds=3600)  # Duration at which the current rate limit window resets

    def __init__(self, tokens: List[str], auth_method: str = "Bearer", auth_header: str = "Authorization"):
        self._auth_method = auth_method
        self._auth_header = auth_header
        now = pendulum.now()
        self._tokens = {
            t: Token(count_rest=5000, count_graphql=5000, update_at=now, reset_at_rest=now, reset_at_graphql=now) for t in tokens
        }
        self.check_all_tokens()
        self._tokens_iter = cycle(self._tokens)
        self._active_token = next(self._tokens_iter)

    @property
    def auth_header(self) -> str:
        return self._auth_header

    def get_auth_header(self) -> Mapping[str, Any]:
        """The header to set on outgoing HTTP requests"""
        if self.auth_header:
            return {self.auth_header: self.token}
        return {}

    def __call__(self, request):
        """Attach the HTTP headers required to authenticate on the HTTP request"""
        current_token = self._tokens[self.current_active_token]
        while True:
            if "graphql" in request.path_url:
                if self.process_token(current_token, "count_graphql", "reset_at_graphql"):
                    break
            else:
                if self.process_token(current_token, "count_rest", "reset_at_rest"):
                    break

        request.headers.update(self.get_auth_header())

        return request

    @property
    def current_active_token(self) -> str:
        return self._active_token

    def update_token(self) -> None:
        self._active_token = next(self._tokens_iter)

    @property
    def token(self) -> str:

        token = self.current_active_token
        return f"{self._auth_method} {token}"

    def _check_token_limits(self, token: str):
        """check that token is not limited"""
        headers = {"Accept": "application/vnd.github+json", "X-GitHub-Api-Version": "2022-11-28"}
        rate_limit_info = (
            requests.get(
                "https://api.github.com/rate_limit", headers=headers, auth=TokenAuthenticator(token, auth_method=self._auth_method)
            )
            .json()
            .get("resources")
        )
        token_info = self._tokens[token]
        token_info.update_at = pendulum.now()
        remaining_info_core = rate_limit_info.get("core")
        token_info.count_rest, token_info.reset_at_rest = remaining_info_core.get("remaining"), pendulum.from_timestamp(
            remaining_info_core.get("reset")
        )

        remaining_info_graphql = rate_limit_info.get("graphql")
        token_info.count_graphql, token_info.reset_at_graphql = remaining_info_graphql.get("remaining"), pendulum.from_timestamp(
            remaining_info_graphql.get("reset")
        )

    def check_all_tokens(self):
        for token in self._tokens:
            self._check_token_limits(token)

    def process_token(self, current_token, count_attr, reset_attr):
        if getattr(current_token, count_attr) > 0:
            setattr(current_token, count_attr, getattr(current_token, count_attr) - 1)
            return True
        elif all(getattr(x, count_attr) == 0 for x in self._tokens.values()):
            min_time_to_wait = min((getattr(x, reset_attr) - pendulum.now()).in_seconds() for x in self._tokens.values())
            if min_time_to_wait < HttpStream.max_time:
                time.sleep(min_time_to_wait)
                self.check_all_tokens()
            else:
                raise AirbyteTracedException(
                    failure_type=FailureType.config_error, message=f"Rate limits for all tokens ({count_attr}) were reached"
                )
        else:
            self.update_token()
        return False
