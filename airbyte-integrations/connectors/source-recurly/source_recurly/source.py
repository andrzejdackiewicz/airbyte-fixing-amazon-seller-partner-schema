#
# Copyright (c) 2021 Airbyte, Inc., all rights reserved.
#

from typing import Any, List, Mapping, Optional, Tuple

from airbyte_cdk.logger import AirbyteLogger
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from recurly import ApiError, Client

from .streams import AccountCouponRedemptions, Accounts, Coupons, ExportDates, Invoices, MeasuredUnits, Plans, Subscriptions, Transactions


class SourceRecurly(AbstractSource):
    """
    Recurly API Reference: https://developers.recurly.com/api/v2021-02-25/
    """

    def __init__(self):
        super(SourceRecurly, self).__init__()

        self.__client = None

    def check_connection(self, logger: AirbyteLogger, config: Mapping[str, Any]) -> Tuple[bool, Optional[Any]]:
        try:
            # Checking the API key by trying a test API call to get the first account
            self._client(config["api_key"]).list_accounts().first()
            return True, None
        except ApiError as err:
            return False, err.args[0]

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        client = self._client(api_key=config["api_key"])

        args = {"client": client, "begin_time": config.get("start_date")}

        return [
            Accounts(**args),
            Coupons(**args),
            AccountCouponRedemptions(**args),
            Invoices(**args),
            MeasuredUnits(**args),
            Plans(**args),
            Subscriptions(**args),
            Transactions(**args),
            ExportDates(**args),
        ]

    def _client(self, api_key: str) -> Client:
        if not self.__client:
            self.__client = Client(api_key=api_key)

        return self.__client
