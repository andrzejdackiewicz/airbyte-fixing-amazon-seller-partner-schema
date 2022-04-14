#
# Copyright (c) 2021 Airbyte, Inc., all rights reserved.
#


import json
import os
from datetime import datetime
from typing import Dict, Generator

from airbyte_cdk.logger import AirbyteLogger
from airbyte_cdk.models import (
    AirbyteCatalog,
    AirbyteConnectionStatus,
    AirbyteMessage,
    AirbyteRecordMessage,
    AirbyteStateMessage,
    AirbyteStream,
    ConfiguredAirbyteCatalog,
    Status,
    SyncMode,
    Type,
)
from airbyte_cdk.sources import Source
from faker import Faker


class SourceFaker(Source):
    def check(self, logger: AirbyteLogger, config: Dict[str, any]) -> AirbyteConnectionStatus:
        """
        Tests if the input configuration can be used to successfully connect to the integration
            e.g: if a provided Stripe API token can be used to connect to the Stripe API.

        :param logger: Logging object to display debug/info/error to the logs
            (logs will not be accessible via airbyte UI if they are not passed to this logger)
        :param config: Json object containing the configuration of this source, content of this json is as specified in
        the properties of the spec.json file

        :return: AirbyteConnectionStatus indicating a Success or Failure
        """

        # As this is an in-memory source, it always succeeds
        return AirbyteConnectionStatus(status=Status.SUCCEEDED)

    def discover(self, logger: AirbyteLogger, config: Dict[str, any]) -> AirbyteCatalog:
        """
        Returns an AirbyteCatalog representing the available streams and fields in this integration.
        For example, given valid credentials to a Postgres database,
        returns an Airbyte catalog where each postgres table is a stream, and each table column is a field.

        :param logger: Logging object to display debug/info/error to the logs
            (logs will not be accessible via airbyte UI if they are not passed to this logger)
        :param config: Json object containing the configuration of this source, content of this json is as specified in
        the properties of the spec.json file

        :return: AirbyteCatalog is an object describing a list of all available streams in this source.
            A stream is an AirbyteStream object that includes:
            - its stream name (or table name in the case of Postgres)
            - json_schema providing the specifications of expected schema for this stream (a list of columns described
            by their names and types)
        """
        streams = []

        # Fake Users
        dirname = os.path.dirname(os.path.realpath(__file__))
        spec_path = os.path.join(dirname, "catalog.json")
        catalog = read_json(spec_path)
        streams.append(AirbyteStream(name="Users", json_schema=catalog, supported_sync_modes=["full_refresh", "incremental"]))
        return AirbyteCatalog(streams=streams)

    def read(
        self, logger: AirbyteLogger, config: Dict[str, any], catalog: ConfiguredAirbyteCatalog, state: Dict[str, any]
    ) -> Generator[AirbyteMessage, None, None]:
        """
        Returns a generator of the AirbyteMessages generated by reading the source with the given configuration,
        catalog, and state.

        :param logger: Logging object to display debug/info/error to the logs
            (logs will not be accessible via airbyte UI if they are not passed to this logger)
        :param config: Json object containing the configuration of this source, content of this json is as specified in
            the properties of the spec.json file
        :param catalog: The input catalog is a ConfiguredAirbyteCatalog which is almost the same as AirbyteCatalog
            returned by discover(), but
        in addition, it's been configured in the UI! For each particular stream and field, there may have been provided
        with extra modifications such as: filtering streams and/or columns out, renaming some entities, etc
        :param state: When a Airbyte reads data from a source, it might need to keep a checkpoint cursor to resume
            replication in the future from that saved checkpoint.
            This is the object that is provided with state from previous runs and avoid replicating the entire set of
            data everytime.

        :return: A generator that produces a stream of AirbyteRecordMessage contained in AirbyteMessage object.
        """

        count: int = config["count"] if "count" in config else 0
        seed: int = config["seed"] if "seed" in config else state["seed"] if "seed" in state else None
        Faker.seed(seed)
        fake = Faker()

        for stream in catalog.streams:
            if stream.stream.name == "Users":
                cursor = get_stream_cursor(state, stream.stream.name)
                total_records = cursor

                for i in range(cursor, count):
                    yield AirbyteMessage(
                        type=Type.RECORD,
                        record=AirbyteRecordMessage(
                            stream=stream.stream.name, data=emit_user(fake, i), emitted_at=int(datetime.now().timestamp()) * 1000
                        ),
                    )
                    total_records = total_records + 1

                yield emit_state(stream.stream.name, total_records, seed)
            else:
                raise ValueError(stream.stream.name)


def get_stream_cursor(state: Dict[str, any], stream: str) -> int:
    cursor = (state[stream]["cursor"] or 0) if stream in state else 0
    return cursor


def emit_state(stream: str, value: int, seed: int):
    message = AirbyteMessage(type=Type.STATE, state=AirbyteStateMessage(data={stream: {"cursor": value, "seed": seed}}))
    return message


def emit_user(fake: Faker, idx: int):
    profile = fake.profile()
    time_a = fake.date_time()
    time_b = fake.date_time()
    metadata = {
        "id": idx,
        "created_at": time_a if time_a <= time_b else time_b,
        "updated_at": time_a if time_a > time_b else time_b,
    }
    profile.update(metadata)
    return profile


def read_json(filepath):
    with open(filepath, "r") as f:
        return json.loads(f.read())
