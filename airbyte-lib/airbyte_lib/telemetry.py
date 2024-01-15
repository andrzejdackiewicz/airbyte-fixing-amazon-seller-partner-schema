# Copyright (c) 2023 Airbyte, Inc., all rights reserved.

import datetime
import os
from dataclasses import asdict, dataclass
from enum import Enum
from typing import Any, Optional

import requests

from airbyte_lib.version import get_version


# TODO: Use production tracking key
TRACKING_KEY = "jxT1qP9WEKwR3vtKMwP9qKhfQEGFtIM1" or str(os.environ.get("AIRBYTE_TRACKING_KEY"))


class SourceType(str, Enum):
    VENV = "venv"
    LOCAL_INSTALL = "local_install"


class CacheType(str, Enum):
    DUCKDB = "duckdb"
    POSTGRES = "postgres"
    SNOWFLAKE = "snowflake"
    STREAMING = "streaming"


class SyncState(str, Enum):
    STARTED = "started"
    FAILED = "failed"
    SUCCEEDED = "succeeded"


@dataclass
class SourceTelemetryInfo:
    name: str
    type: SourceType
    version: Optional[str]


def send_telemetry(
    source: SourceTelemetryInfo,
    cache_type: CacheType,
    state: SyncState,
    number_of_records: Optional[int] = None,
):
    # If DO_NOT_TRACK is set, we don't send any telemetry
    if os.environ.get("DO_NOT_TRACK"):
        return

    current_time = datetime.datetime.utcnow().isoformat()
    payload: dict[str, Any] = {
        "anonymousId": "airbyte-lib-user",
        "event": "sync",
        "properties": {
            "version": get_version(),
            "source": asdict(source),
            "state": state,
            "cache_type": cache_type,
            # explicitly set to 0.0.0.0 to avoid leaking IP addresses
            "ip": "0.0.0.0",
        },
        "timestamp": current_time,
    }
    if number_of_records is not None:
        payload["properties"]["number_of_records"] = number_of_records

    # Do not handle the response, we don't want to block the execution
    _ = requests.post("https://api.segment.io/v1/track", auth=(TRACKING_KEY, ""), json=payload)
