#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


import argparse
import sys
from typing import Any, List, Mapping, Tuple

from airbyte_cdk.connector import BaseConnector
from airbyte_cdk.entrypoint import AirbyteEntrypoint, launch
from airbyte_cdk.sources.declarative.manifest_declarative_source import ManifestDeclarativeSource
from connector_builder.connector_builder_handler import resolve_manifest


def create_source(config: Mapping[str, Any]) -> ManifestDeclarativeSource:
    manifest = config.get("__injected_declarative_manifest")
    return ManifestDeclarativeSource(manifest)


def get_config_from_args(args: List[str]) -> Mapping[str, Any]:
    command, config_filepath = preparse(args)
    if command == "spec":
        raise ValueError("spec command is not supported for injected declarative manifest")

    config = BaseConnector.read_config(config_filepath)

    if "__injected_declarative_manifest" not in config:
        raise ValueError(
            f"Invalid config: `__injected_declarative_manifest` should be provided at the root of the config but config only has keys {list(config.keys())}"
        )

    return config


def preparse(args: List[str]) -> Tuple[str, str]:
    parser = argparse.ArgumentParser()
    parser.add_argument("command", type=str, help="Airbyte Protocol command")
    parser.add_argument("--config", type=str, required=True, help="path to the json configuration file")
    parsed, _ = parser.parse_known_args(args)
    return parsed.command, parsed.config


def handle_connector_builder_request(source: ManifestDeclarativeSource, config: Mapping[str, Any]):
    command = config.get("__command")
    if command == "resolve_manifest":
        return resolve_manifest(source)
    elif command == "read":
        raise NotImplementedError
    raise ValueError(f"Unrecognized command {command}.")


def handle_connector_request(source: ManifestDeclarativeSource, args: List[str]):
    # Verify that the correct args are present for the production codepaths.
    AirbyteEntrypoint.parse_args(args)
    launch(source, sys.argv[1:])


def handle_request(args: List[str]):
    config = get_config_from_args(args)
    source = create_source(config)
    if "__command" in config:
        print(handle_connector_builder_request(source, config))
    else:
        handle_connector_request(source, args)


if __name__ == "__main__":
    handle_request(sys.argv[1:])
