#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import concurrent.futures
import json
from datetime import datetime
from functools import partial
from typing import Dict, Generator

from apify_client import ApifyClient

from airbyte_cdk.logger import AirbyteLogger
from airbyte_cdk.models import (
    AirbyteCatalog,
    AirbyteConnectionStatus,
    AirbyteMessage,
    AirbyteRecordMessage,
    AirbyteStream,
    ConfiguredAirbyteCatalog,
    Status,
    Type,
)
from airbyte_cdk.models.airbyte_protocol import SyncMode
from airbyte_cdk.sources import Source

DATASET_ITEMS_STREAM_NAME = "DatasetItems"

# Batch size for downloading dataset items from Apify dataset
BATCH_SIZE = 50000


class SourceApifyDataset(Source):
    def _apify_get_dataset_items(self, dataset_client, clean, offset):
        """Wrapper around Apify dataset client that returns a single page with dataset items.
        This function needs to be defined explicitly so it can be called in parallel in the main read function.

        :param dataset_client: Apify dataset client
        :param clean: whether to fetch only clean items (clean are non-empty ones excluding hidden columns)
        :param offset: page offset

        :return: dictionary where .items field contains the fetched dataset items
        """
        return dataset_client.list_items(offset=offset, limit=BATCH_SIZE, clean=clean)

    def check(self, logger: AirbyteLogger, config: json) -> AirbyteConnectionStatus:
        """Tests if the input configuration can be used to successfully connect to the Apify integration.
        This is tested by trying to access the Apify user object with the provided userId and Apify token.

        :param logger: Logging object to display debug/info/error to the logs
            (logs will not be accessible via airbyte UI if they are not passed to this logger)
        :param config: Json object containing the configuration of this source, content of this json is as specified in
        the properties of the spec.json file

        :return: AirbyteConnectionStatus indicating a Success or Failure
        """
        try:
            dataset_id = config["datasetId"]
            dataset = ApifyClient().dataset(dataset_id).get()
            if dataset is None:
                msg = f"Dataset {dataset_id} does not exist"
                raise ValueError(msg)
        except Exception as e:
            return AirbyteConnectionStatus(status=Status.FAILED, message=f"An exception occurred: {e!s}")
        else:
            return AirbyteConnectionStatus(status=Status.SUCCEEDED)

    def discover(self, logger: AirbyteLogger, config: json) -> AirbyteCatalog:
        """Returns an AirbyteCatalog representing the available streams and fields in this integration.
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
        stream_name = DATASET_ITEMS_STREAM_NAME
        json_schema = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": {
                "data": {
                    "type": "object",
                    "additionalProperties": True,
                },
            },
        }

        return AirbyteCatalog(
            streams=[AirbyteStream(name=stream_name, supported_sync_modes=[SyncMode.full_refresh], json_schema=json_schema)],
        )

    def read(
        self, logger: AirbyteLogger, config: json, catalog: ConfiguredAirbyteCatalog, state: Dict[str, any],
    ) -> Generator[AirbyteMessage, None, None]:
        """Returns a generator of the AirbyteMessages generated by reading the source with the given configuration,
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
        logger.info("Reading data from Apify dataset")

        dataset_id = config["datasetId"]
        clean = config.get("clean", False)

        client = ApifyClient()
        dataset_client = client.dataset(dataset_id)

        # Get total number of items in dataset. This will be used in pagination
        dataset = dataset_client.get()
        num_items = dataset["itemCount"]

        with concurrent.futures.ThreadPoolExecutor() as executor:
            for result in executor.map(partial(self._apify_get_dataset_items, dataset_client, clean), range(0, num_items, BATCH_SIZE)):
                for data in result.items:
                    yield AirbyteMessage(
                        type=Type.RECORD,
                        record=AirbyteRecordMessage(
                            stream=DATASET_ITEMS_STREAM_NAME, data={"data": data}, emitted_at=int(datetime.now().timestamp()) * 1000,
                        ),
                    )
