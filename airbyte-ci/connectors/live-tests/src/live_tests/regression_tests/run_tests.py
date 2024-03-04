# Copyright (c) 2023 Airbyte, Inc., all rights reserved.

import asyncio
import logging
import sys
from enum import Enum
from typing import Dict, List, Optional

import dagger
from airbyte_protocol.models import ConfiguredAirbyteCatalog

from live_tests.commons.connector_runner import ConnectorRunner
from live_tests.commons.models import SecretDict, ConnectorUnderTest, Command
from live_tests.backends import BaseBackend, FileBackend
from live_tests.regression_tests.comparators import DiffComparator
from live_tests.commons.utils import get_connector_under_test

logging.basicConfig(level=logging.INFO)

logger = logging.getLogger(__name__)



async def run_tests(
    connector_name: str,
    control_image_name: str,
    target_image_name: str,
    output_directory: str,
    commands: List[Command],
    config: Optional[SecretDict],
    catalog: Optional[ConfiguredAirbyteCatalog],
    state: Optional[Dict],
):
    async with (dagger.Connection(config=dagger.Config(log_output=sys.stderr)) as client):
        control_connector = await get_connector_under_test(client, connector_name, control_image_name)
        target_connector = await get_connector_under_test(client, connector_name, target_image_name)
        await _run_tests(
            control_connector, target_connector, output_directory, commands, config, catalog, state
        )
        await DiffComparator(client, output_directory).compare(control_connector, target_connector)


async def _run_tests(
    control_connector: ConnectorUnderTest,
    target_connector: ConnectorUnderTest,
    output_directory: str,
    commands: List[Command],
    config: Optional[SecretDict],
    catalog: Optional[ConfiguredAirbyteCatalog],
    state: Optional[Dict],
):
    # TODO: maybe use proxy to cache the response from the first round and use the cache for the second round
    #   (this may only make sense for syncs with an input state)
    tasks = []
    for command in commands:
        tasks.extend([
            _dispatch(
                connector.container,
                FileBackend(f"{output_directory}/{connector.version}/{command}"),
                f"{output_directory}/{connector.version}",
                Command(command),
                config,
                catalog,
                state,
            ) for connector in [control_connector, target_connector]
        ])
    await asyncio.gather(*tasks)


async def _dispatch(
    container: dagger.Container,
    backend: BaseBackend,
    output_directory: str,
    command: Command,
    config: Optional[SecretDict],
    catalog: Optional[ConfiguredAirbyteCatalog],
    state: Optional[Dict],
):
    runner = ConnectorRunner(container, backend, f"{output_directory}/{command}")

    if command == Command.CHECK:
        await runner.call_check(config)

    elif command == Command.DISCOVER:
        await runner.call_discover(config)

    elif command == Command.READ:
        await runner.call_read(config, catalog)

    elif command == Command.READ_WITH_STATE:
        await runner.call_read_with_state(config, catalog, state)

    elif command == Command.SPEC:
        await runner.call_spec()

    else:
        raise NotImplementedError(f"{command} is not recognized. Must be one of {', '.join(c.value for c in Command)}")
