# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
from __future__ import annotations

import json
import os
from copy import copy
from dataclasses import dataclass
from pathlib import Path

import requests

from airbyte_lib import exceptions as exc
from airbyte_lib.version import get_version


__cache: dict[str, ConnectorMetadata] | None = None


REGISTRY_ENV_VAR = "AIRBYTE_LOCAL_REGISTRY"
REGISTRY_URL = "https://connectors.airbyte.com/files/registries/v0/oss_registry.json"


@dataclass
class ConnectorMetadata:
    name: str
    latest_available_version: str


def _get_registry_url() -> str:
    if REGISTRY_ENV_VAR in os.environ:
        return str(os.environ.get(REGISTRY_ENV_VAR))

    return REGISTRY_URL


def _get_registry_cache(*, force_refresh: bool = False) -> dict[str, ConnectorMetadata]:
    """Return the registry cache."""
    global __cache
    if __cache and not force_refresh:
        return __cache

    registry_url = _get_registry_url()
    if registry_url.startswith("http"):
        response = requests.get(
            registry_url, headers={"User-Agent": f"airbyte-lib-{get_version()}"}
        )
        response.raise_for_status()
        data = response.json()
    else:
        # Assume local file
        with Path(registry_url).open() as f:
            data = json.load(f)

    new_cache: dict[str, ConnectorMetadata] = {}

    for connector in data["sources"]:
        name = connector["dockerRepository"].replace("airbyte/", "")
        new_cache[name] = ConnectorMetadata(name, connector["dockerImageTag"])

    if len(new_cache) == 0:
        raise exc.AirbyteLibInternalError(
            message="Connector registry is empty.",
            context={
                "registry_url": _get_registry_url(),
            },
        )

    __cache = new_cache
    return __cache


def get_connector_metadata(name: str) -> ConnectorMetadata:
    """Check the cache for the connector.

    If the cache is empty, populate by calling update_cache.
    """
    cache = copy(_get_registry_cache())
    if not cache:
        raise exc.AirbyteLibInternalError(
            message="Connector registry could not be loaded.",
            context={
                "registry_url": _get_registry_url(),
            },
        )
    if name not in cache:
        raise exc.AirbyteConnectorNotRegisteredError(
            connector_name=name,
            context={
                "registry_url": _get_registry_url(),
                "available_connectors": sorted(cache.keys()),
            },
        )
    return cache[name]
