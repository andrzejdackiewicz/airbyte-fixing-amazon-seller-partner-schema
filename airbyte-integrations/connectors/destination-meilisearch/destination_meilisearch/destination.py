#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#


from typing import Any, Iterable, Mapping

from airbyte_cdk import AirbyteLogger
from airbyte_cdk.destinations import Destination
from airbyte_cdk.models import AirbyteConnectionStatus, AirbyteMessage, ConfiguredAirbyteCatalog, Status, DestinationSyncMode, Type
from meilisearch import Client


def get_client(config: Mapping[str, Any]) -> Client:
    host = config.get("host")
    api_key = config.get("api_key")
    return Client(host, api_key)

class DestinationMeilisearch(Destination):
    def write(
        self, config: Mapping[str, Any], configured_catalog: ConfiguredAirbyteCatalog, input_messages: Iterable[AirbyteMessage]
    ) -> Iterable[AirbyteMessage]:

        client = get_client(config=config)

        for configured_stream in configured_catalog.streams:
            if configured_stream.destination_sync_mode == DestinationSyncMode.overwrite:
                client.delete_index(configured_stream.stream.name)
            client.create_index(configured_stream.stream.name, {'primaryKey': '_ab_pk'})

        for message in input_messages:
            if message.type == Type.STATE:
                yield message
            elif message.type == Type.RECORD:
                record = message.record
                client.index(record.stream).add_documents([record.data])
            else:
                continue

    def check(self, logger: AirbyteLogger, config: Mapping[str, Any]) -> AirbyteConnectionStatus:
        try:
            # Verify write access by attempting to create an index, add a document and then delete the index
            client = get_client(config=config)
            client.create_index('_airbyte', {'primaryKey': 'id'})
            client.index('_airbyte').add_documents([{
                'id': 287947,
                'title': 'Shazam',
                'overview': 'A boy is given the ability to become an adult superhero in times of need with a single magic word.'
            }])
            client.index('_airbyte').search('Shazam')
            client.delete_index('_airbyte')
            return AirbyteConnectionStatus(status=Status.SUCCEEDED)
        except Exception as e:
            logger.error(f"Check connection failed. Error: {e}")
            return AirbyteConnectionStatus(status=Status.FAILED, message=f"An exception occurred: {repr(e)}")
