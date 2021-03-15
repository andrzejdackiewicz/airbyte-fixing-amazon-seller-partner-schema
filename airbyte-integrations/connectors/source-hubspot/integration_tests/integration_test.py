"""
MIT License

Copyright (c) 2020 Airbyte

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"""

import json
from pathlib import Path
from typing import List, Tuple, Any, MutableMapping, Mapping, Iterable

from airbyte_protocol import ConfiguredAirbyteCatalog, Type, SyncMode
from base_python import AirbyteLogger
from source_hubspot.source import SourceHubspot
import pytest


HERE = Path(__file__).parent.absolute()


@pytest.fixture(scope="session")
def config() -> Mapping[str, Any]:
    config_filename = HERE.parent / "secrets" / "config.json"

    if not config_filename.exists():
        raise RuntimeError(f"Please provide config in {config_filename}")

    with open(str(config_filename)) as json_file:
        return json.load(json_file)


@pytest.fixture
def configured_catalog() -> ConfiguredAirbyteCatalog:
    catalog_filename = HERE.parent / "sample_files" / "configured_catalog.json"
    if not catalog_filename.exists():
        raise RuntimeError(f"Please provide configured catalog in {catalog_filename}")

    return ConfiguredAirbyteCatalog.parse_file(catalog_filename)


@pytest.fixture
def configured_catalog_with_incremental(configured_catalog) -> ConfiguredAirbyteCatalog:
    streams = []
    for stream in configured_catalog.streams:
        if SyncMode.incremental in stream.stream.supported_sync_modes:
            stream.sync_mode = SyncMode.incremental
            streams.append(stream)

    configured_catalog.streams = streams
    return configured_catalog


def read_stream(source: SourceHubspot, config: Mapping, catalog: ConfiguredAirbyteCatalog, state: MutableMapping = None) -> Tuple[List, List]:
    records = []
    states = []
    for message in source.read(AirbyteLogger(), config, catalog, state):
        if message.type == Type.RECORD:
            records.append(message.record)
        elif message.type == Type.STATE:
            states.append(message.state)
            print(message.state.data)

    return records, states


def records_older(records: Iterable, than: int) -> Iterable:
    for record in records:
        if record.data["created"] < than:
            yield record


class TestIncrementalSync:
    def test_sync_with_latest_state(self, config, configured_catalog_with_incremental):
        """Sync first time, save the state and sync second time with saved state from previous sync"""
        records1, states1 = read_stream(SourceHubspot(), config, configured_catalog_with_incremental)

        assert states1, "should have at least one state emitted"
        assert records1, "should have at lest few records emitted"

        records2, states2 = read_stream(SourceHubspot(), config, configured_catalog_with_incremental, states1[-1].data)

        assert states1 == states2
        assert list(records_older(records1, than=records2[0].data["created"])), "should have older records from the first read"
        assert not list(records_older(records2, than=records2[0].data["created"])), "should not have older records from the second read"
