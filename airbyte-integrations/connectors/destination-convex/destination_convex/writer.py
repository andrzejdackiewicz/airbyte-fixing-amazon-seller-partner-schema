#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#

from collections.abc import Mapping
import time
from typing import List

from destination_convex.client import ConvexClient


class ConvexWriter:
    """
    Buffers messages before sending them to Convex.
    """

    write_buffer = []
    flush_interval = 1000

    def __init__(self, client: ConvexClient):
        self.client = client

    def delete_stream_entries(self, stream_names: List[str]):
        """Deletes all the records belonging to the input stream"""
        if len(stream_names) > 0:
            self.client.delete(stream_names)

    def add_indexes(self, indexes: Mapping[str, List[List[str]]]):
        self.client.add_indexes(indexes)
        self.__poll_for_indexes(indexes)

    def __poll_for_indexes(self, indexes: Mapping[str, List[List[str]]]):
        """Polls until the indexes specified are ready"""
        tables = list(indexes.keys())
        while True:
            resp = self.client.indexes_ready(tables)
            if resp.json()["indexesReady"]:
                break
            else:
                time.sleep(1)
        return

    def queue_write_operation(self, message: Mapping):
        """Adds messages to the write queue and flushes if the buffer is full"""
        self.write_buffer.append(message)
        if len(self.write_buffer) == self.flush_interval:
            self.flush()

    def flush(self):
        """Writes to Convex"""

        self.client.batch_write(self.write_buffer)
        self.write_buffer.clear()
