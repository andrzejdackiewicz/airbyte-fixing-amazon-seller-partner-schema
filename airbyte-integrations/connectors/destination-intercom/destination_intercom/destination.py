#
# Copyright (c) 2021 Airbyte, Inc., all rights reserved.
#


from typing import Mapping, Any, Iterable

from airbyte_cdk import AirbyteLogger
from airbyte_cdk.destinations import Destination
from airbyte_cdk.models import AirbyteConnectionStatus, ConfiguredAirbyteCatalog, AirbyteMessage, Status, Type

import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

from destination_intercom.writers import create_writer


class DestinationIntercom(Destination):
    def write(
            self,
            config: Mapping[str, Any],
            configured_catalog: ConfiguredAirbyteCatalog,
            input_messages: Iterable[AirbyteMessage]
    ) -> Iterable[AirbyteMessage]:

        """
        TODO
        Reads the input stream of messages, config, and catalog to write data to the destination.

        This method returns an iterable (typically a generator of AirbyteMessages via yield) containing state messages received
        in the input message stream. Outputting a state message means that every AirbyteRecordMessage which came before it has been
        successfully persisted to the destination. This is used to ensure fault tolerance in the case that a sync fails before fully completing,
        then the source is given the last state message output from this method as the starting point of the next sync.

        :param config: dict of JSON configuration matching the configuration declared in spec.json
        :param configured_catalog: The Configured Catalog describing the schema of the data being received and how it should be persisted in the
                                    destination
        :param input_messages: The stream of input messages received from the source
        :return: Iterable of AirbyteStateMessages wrapped in AirbyteMessage structs
        """
        writer = create_writer(**config)

        # for configured_stream in configured_catalog.streams:
        #     if configured_stream.destination_sync_mode == DestinationSyncMode.overwrite:
        #         writer.delete_stream_entries(configured_stream.stream.name)

        for message in input_messages:
            if message.type == Type.STATE:
                # Emitting a state message indicates that all records which came before it have been written to the destination. So we flush
                # the queue to ensure writes happen, then output the state message to indicate it's safe to checkpoint state
                writer.write_queue()
                yield message
            elif message.type == Type.RECORD:
                record = message.record
                writer.queue_record(record.stream, record.data)
            else:
                # ignore other message types for now
                continue

        # Make sure to flush any records still in the queue
        writer.write_queue()

    def check(self, logger: AirbyteLogger, config: Mapping[str, Any]) -> AirbyteConnectionStatus:
        """
        Tests if the input configuration can be used to successfully connect to the destination with the needed permissions
            e.g: if a provided API token or password can be used to connect and write to the destination.

        :param logger: Logging object to display debug/info/error to the logs
            (logs will not be accessible via airbyte UI if they are not passed to this logger)
        :param config: Json object containing the configuration of this destination, content of this json is as specified in
        the properties of the spec.json file

        :return: AirbyteConnectionStatus indicating a Success or Failure
        """
        try:
            # check Intercom API access by trying to list all contacts
            writer = create_writer(**config)
            writer.client._request("get", endpoint="companies")
        except requests.exceptions.HTTPError as err:
            return AirbyteConnectionStatus(status=Status.FAILED, message=f"Intercom error - {err}")
        except Exception as e:
            return AirbyteConnectionStatus(status=Status.FAILED, message=f"An exception occurred: {repr(e)}")
        else:
            return AirbyteConnectionStatus(status=Status.SUCCEEDED)
