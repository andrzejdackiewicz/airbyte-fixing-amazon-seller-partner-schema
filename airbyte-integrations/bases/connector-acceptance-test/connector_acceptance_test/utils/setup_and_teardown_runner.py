#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import json
import os
from pathlib import Path
from typing import List

import dagger
from connector_acceptance_test.utils import SecretDict

IN_CONTAINER_CONFIG_PATH = Path("/tmp/config.json")
IN_CONTAINER_OUTPUT_PATH = Path("/tmp/output.txt")


async def _build_container(dagger_client: dagger.Client, dockerfile_path: Path) -> dagger.Container:
    workspace = (
        dagger_client.container()
        .with_mounted_directory("/tmp/setup_teardown_context", dagger_client.host().directory(os.path.dirname(dockerfile_path)))
        .directory("/tmp/setup_teardown_context")
    )
    return await dagger_client.container().build(context=workspace, dockerfile="Dockerfile.cat_setup_teardown")


async def _build_setup_container(dagger_client: dagger.Client, connector_path: Path, dockerfile_path: Path) -> dagger.Container:
    container = await _build_container(dagger_client, dockerfile_path)
    return container.with_mounted_directory("/connector", dagger_client.host().directory(str(connector_path)))


async def _run_with_config(container: dagger.Container, command: List[str], config: SecretDict) -> dagger.Container:
    container = container.with_new_file(str(IN_CONTAINER_CONFIG_PATH), contents=json.dumps(dict(config)))
    return await _run(container, command)


async def _run(container: dagger.Container, command: List[str]) -> dagger.Container:
    return await container.with_exec(command, skip_entrypoint=True)


async def do_setup(
    dagger_client: dagger.Client, connector_path: Path, dockerfile_path: Path, command: List[str], connector_config: SecretDict
):
    return await _run_with_config(await _build_setup_container(dagger_client, connector_path, dockerfile_path), command, connector_config)


async def do_teardown(container: dagger.Container, command: List[str]):
    return await _run(container, command)
