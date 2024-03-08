# Copyright (c) 2024 Airbyte, Inc., all rights reserved.

from airbyte_cdk.sources.connector_state_manager import HashableStreamDescriptor
from airbyte_protocol.models import AirbyteMessage


def get_stream_descriptor(message: AirbyteMessage) -> HashableStreamDescriptor:
    if message.record:
        return HashableStreamDescriptor(name=message.record.stream, namespace=message.record.namespace)
    elif message.state:
        return HashableStreamDescriptor(
            name=message.state.stream.stream_descriptor.name, namespace=message.state.stream.stream_descriptor.namespace
        )
    else:
        raise ValueError("Message format does not contain a stream descriptor.")
