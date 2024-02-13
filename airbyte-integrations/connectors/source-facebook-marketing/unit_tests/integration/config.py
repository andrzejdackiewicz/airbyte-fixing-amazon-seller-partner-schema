#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#


from __future__ import annotations

from typing import Any, List, MutableMapping

ACCESS_TOKEN = "test_access_token"
ACCOUNT_ID = "111111111111111"
CLIENT_ID = "test_client_id"
CLIENT_SECRET = "test_client_secret"
END_DATE = "2023-01-01T23:59:59Z"
START_DATE = "2023-01-01T00:00:00Z"


class ConfigBuilder:
    def __init__(self) -> None:
        self._config: MutableMapping[str, Any] = {
            "account_ids": [ACCOUNT_ID],
            "access_token": ACCESS_TOKEN,
            "start_date": START_DATE,
            "end_date": END_DATE,
            "include_deleted": True,
            "fetch_thumbnail_images": True,
            "custom_insights": [],
            "page_size": 100,
            "insights_lookback_window": 28,
            "insights_job_timeout": 60,
            "action_breakdowns_allow_empty": True,
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
        }

    def with_account_ids(self, account_ids: List[str]) -> ConfigBuilder:
        self._config["account_ids"] = account_ids
        return self

    def with_start_date(self, start_date: str) -> ConfigBuilder:
        self._config["start_date"] = start_date
        return self

    def with_end_date(self, end_date: str) -> ConfigBuilder:
        self._config["end_date"] = end_date
        return self

    def build(self) -> MutableMapping[str, Any]:
        return self._config
