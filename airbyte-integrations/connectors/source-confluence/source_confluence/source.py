#
# Copyright (c) 2021 Airbyte, Inc., all rights reserved.
#


from abc import ABC
from base64 import b64encode
from typing import Any, Dict, Iterable, List, Mapping, MutableMapping, Optional, Tuple

import requests
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.sources.streams.http.auth import TokenAuthenticator


# Basic full refresh stream
class ConfluenceStream(HttpStream, ABC):
    url_base = "https://{}/wiki/rest/api/"
    primary_key = "id"
    limit = 50
    start = 0
    expand = []

    def __init__(self, config: Dict):
        super().__init__(authenticator=config["authenticator"])
        self.config = config
        self.url_base = self.url_base.format(config["domain_name"])

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        json_response = response.json()
        links = json_response.get("_links")
        next_link = links.get("next")
        if next_link:
            self.start += self.limit
            return {"start": self.start}

    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        params = {"limit": self.limit, "expand": ",".join(self.expand)}
        if next_page_token:
            params.update({"start": next_page_token["start"]})
        return params

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        json_response = response.json()
        records = json_response.get("results", [])
        yield from records

    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        return self.api_name


class BaseContentStream(ConfluenceStream, ABC):
    api_name = "content"
    expand = [
        "history",
        "history.lastUpdated",
        "history.previousVersion",
        "history.contributors",
        "restrictions.read.restrictions.user",
        "version",
        "descendants.comment",
    ]
    limit = 25
    content_type = None

    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        params = super().request_params(stream_state, stream_slice=stream_slice, next_page_token=next_page_token)
        params.update({"type": self.content_type})
        return params


class Pages(BaseContentStream):
    content_type = "page"


class BlogPosts(BaseContentStream):
    content_type = "blogpost"


class Space(ConfluenceStream):
    api_name = "space"
    expand = ["permissions", "icon", "description.plain", "description.view"]


class Group(ConfluenceStream):
    api_name = "group"


class Theme(ConfluenceStream):
    api_name = "settings/theme"


class Audit(ConfluenceStream):
    api_name = "audit"
    limit = 1000


# Source
class HttpBasicAuthenticator(TokenAuthenticator):
    def __init__(self, email: str, token: str, auth_method: str = "Basic", **kwargs):
        auth_string = f"{email}:{token}".encode("utf8")
        b64_encoded = b64encode(auth_string).decode("utf8")
        super().__init__(token=b64_encoded, auth_method=auth_method, **kwargs)


class SourceConfluence(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, any]:
        auth = HttpBasicAuthenticator(config["email"], config["api_token"], auth_method="Basic").get_auth_header()
        url = f"https://{config['domain_name']}/wiki/rest/api/space"
        try:
            response = requests.get(url, headers=auth)
            response.raise_for_status()
            return True, None
        except requests.exceptions.RequestException as e:
            return False, e

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        auth = HttpBasicAuthenticator(config["email"], config["api_token"], auth_method="Basic")
        config["authenticator"] = auth
        return [Pages(config), BlogPosts(config), Space(config), Group(config), Theme(config), Audit(config)]
