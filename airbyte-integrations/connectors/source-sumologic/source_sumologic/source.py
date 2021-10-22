#
# Copyright (c) 2021 Airbyte, Inc., all rights reserved.
#


import datetime
from abc import ABC
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple

from airbyte_cdk.models import SyncMode
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream

from .client import Client


# Basic full refresh stream
class SumologicStream(Stream, ABC):

    primary_key = "_messageid"

    def __init__(self, client: Client, config: Mapping[str, Any]) -> None:
        super().__init__()
        self.client = client
        self.config = config


# Basic incremental stream
class IncrementalSumologicStream(SumologicStream, ABC):

    # Not confident with the order returned by sumologic, so don't check in until all data are read
    state_checkpoint_interval = None

    @property
    def cursor_field(self) -> str:
        return "_receipttime" if self.config.get("by_receipt_time", False) else "_messagetime"

    def get_updated_state(self, current_stream_state: MutableMapping[str, Any], latest_record: Mapping[str, Any]) -> Mapping[str, Any]:
        return {self.cursor_field: max(int(latest_record.get(self.cursor_field, 0)), int(current_stream_state.get(self.cursor_field, 0)))}


class Messages(IncrementalSumologicStream):
    def read_records(
        self,
        sync_mode: SyncMode,
        cursor_field: Optional[List[str]] = None,
        stream_slice: Optional[Mapping[str, Any]] = None,
        stream_state: Optional[Mapping[str, Any]] = None,
    ) -> Iterable[Mapping[str, Any]]:
        stream_state = stream_state or {}
        cursor_epoch = stream_state.get(self.cursor_field)
        from_time: Optional[str] = None
        if cursor_epoch:
            from_time = datetime.datetime.utcfromtimestamp(int(cursor_epoch) / 1000.0).replace(microsecond=0).isoformat()
        else:
            from_time = self.config.get("from_time")

        records = self.client.search(
            query=self.config["query"],
            from_time=from_time,
            to_time=self.config.get("to_time", datetime.datetime.utcnow().replace(microsecond=0).isoformat()),
            limit=self.config.get("limit", 10000),
            offset=self.config.get("offset", 0),
            by_receipt_time=self.config.get("by_receipt_time", False),
        )
        return records


# Source
class SourceSumologic(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, Any]:
        """Connection check

        See https://github.com/airbytehq/airbyte/blob/master/airbyte-integrations/connectors/source-stripe/source_stripe/source.py#L232
        for an example.

        :param config:  the user-input config object conforming to the connector's spec.json
        :param logger:  logger object
        :return Tuple[bool, Any]: (True, None) if the input config can be used to connect to the API successfully, (False, error) otherwise.
        """

        try:
            client = Client(config["access_id"], config["access_key"])
            client.check()
            return True, None
        except Exception as e:
            return False, e

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        """
        :param config: A Mapping of the user input configuration as defined in the connector spec.
        """
        client = Client(config["access_id"], config["access_key"])
        return [Messages(client, config)]
