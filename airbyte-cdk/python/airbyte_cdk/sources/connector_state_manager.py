#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#

import copy
from typing import Any, List, Mapping, MutableMapping, Optional, Tuple, Union

from airbyte_cdk.models import AirbyteMessage, AirbyteStateBlob, AirbyteStateMessage, AirbyteStateType, AirbyteStreamState, StreamDescriptor
from airbyte_cdk.models import Type as MessageType
from airbyte_cdk.sources.streams import Stream
from pydantic import Extra


class HashableStreamDescriptor(StreamDescriptor):
    """
    Helper class that overrides the existing StreamDescriptor class that is auto generated from the Airbyte Protocol and
    freezes its fields so that it be used as a hash key. This is only marked public because we use it outside for unit tests.
    """

    class Config:
        extra = Extra.allow
        frozen = True


class ConnectorStateManager:
    """
    ConnectorStateManager consolidates the various forms of a stream's incoming state message (STREAM / GLOBAL / LEGACY) under a common
    interface. It also provides methods to extract and update state
    """

    def __init__(self, stream_instance_map: Mapping[str, Stream], state: Union[List[AirbyteStateMessage], MutableMapping[str, Any]] = None):
        shared_state, per_stream_states = self._extract_from_state_message(state, stream_instance_map)

        # We explicitly throw an error if we receive a GLOBAL state message that contains a shared_state because API sources are
        # designed to checkpoint state independently of one another. API sources should never be emitting a state message where
        # shared_state is populated. Rather than define how to handle shared_state without a clear use case, we're opting to throw an
        # error instead and if/when we find one, we will then implement processing of the shared_state value.
        if shared_state:
            raise ValueError(
                "Received a GLOBAL AirbyteStateMessages that contain a shared_state. But this library only ever generated per-STREAM "
                "STATE messages was not generated this connector. This must be an orchestrator or platform error. GLOBAL state messages "
                "with shared_state will not be processed correctly. "
            )
        self.per_stream_states = per_stream_states

    def get_stream_state(self, stream_name: str, namespace: Optional[str]) -> Mapping[str, Any]:
        """
        Retrieves the state of a given stream based on its descriptor (name + namespace) including any global shared state.
        Stream state takes precedence over shared state when there are conflicts
        :param stream_name: Name of the stream being fetched
        :param namespace: Namespace of the stream being fetched
        :return: The combined shared state and per-stream state of a stream
        """
        per_stream_state = self.per_stream_states.get(HashableStreamDescriptor(name=stream_name, namespace=namespace))
        if per_stream_state:
            return per_stream_state.dict()
        return {}

    def get_legacy_state(self) -> Mapping[str, Any]:
        """
        Using the current per-stream state, creates a mapping of all the stream states for the connector being synced
        :return: A deep copy of the mapping of stream name to stream state value
        """
        return {descriptor.name: per_stream.dict() if per_stream else {} for descriptor, per_stream in self.per_stream_states.items()}

    def update_state_for_stream(self, stream_name: str, namespace: Optional[str], value: Mapping[str, Any]):
        """
        Overwrites the state blob of a specific stream based on the provided stream name and optional namespace
        :param stream_name: The name of the stream whose state is being updated
        :param namespace: The namespace of the stream if it exists
        :param value: A stream state mapping that is being updated for a stream
        :return:
        """
        stream_descriptor = HashableStreamDescriptor(name=stream_name, namespace=namespace)
        self.per_stream_states[stream_descriptor] = AirbyteStateBlob.parse_obj(value)

    def create_state_message(self, stream_name: str, namespace: str) -> AirbyteMessage:
        """
        Generates an AirbyteMessage using the current per-stream state of a specified stream
        :param stream_name: The name of the stream for the message that is being created
        :param namespace: The namespace of the stream for the message that is being created
        :return: The Airbyte state message to be emitted by the connector during a sync
        """
        state_message = AirbyteMessage(
            type=MessageType.STATE, state=AirbyteStateMessage(type=AirbyteStateType.STREAM, data=dict(self.get_legacy_state()))
        )
        stream_descriptor = HashableStreamDescriptor(name=stream_name, namespace=namespace)
        stream_state = self.per_stream_states.get(stream_descriptor, {})
        state_message.state.stream = AirbyteStreamState(
            stream_descriptor=StreamDescriptor(name=stream_descriptor.name, namespace=stream_descriptor.namespace),
            stream_state=stream_state,
        )
        return state_message

    @classmethod
    def _extract_from_state_message(
        cls, state: Union[List[AirbyteStateMessage], MutableMapping[str, Any]], stream_instance_map: Mapping[str, Stream]
    ) -> Tuple[Optional[AirbyteStateBlob], MutableMapping[HashableStreamDescriptor, Optional[AirbyteStateBlob]]]:
        """
        Takes an incoming list of state messages or the legacy state format and extracts state attributes according to type
        which can then be assigned to the new state manager being instantiated
        :param state: The incoming state input
        :return: A tuple of shared state and per stream state assembled from the incoming state list
        """
        if state is None:
            return None, {}

        is_legacy = isinstance(state, dict)
        is_migrated_legacy = cls._is_migrated_legacy_state(state)
        is_global = cls._is_global_state(state)
        is_per_stream = isinstance(state, List)

        # Incoming pure legacy object format
        if is_legacy:
            streams = cls._create_descriptor_to_stream_state_mapping(state, stream_instance_map)
            return None, streams

        # When processing incoming state in source.read_state(), legacy state gets deserialized into List[AirbyteStateMessage]
        # which can be translated into independent per-stream state values
        if is_migrated_legacy:
            streams = cls._create_descriptor_to_stream_state_mapping(state[0].data, stream_instance_map)
            return None, streams

        if is_global:
            global_state = state[0].global_
            shared_state = copy.deepcopy(global_state.shared_state, {})
            streams = {
                HashableStreamDescriptor(
                    name=per_stream_state.stream_descriptor.name, namespace=per_stream_state.stream_descriptor.namespace
                ): per_stream_state.stream_state
                for per_stream_state in global_state.stream_states
            }
            return shared_state, streams

        if is_per_stream:
            streams = {
                HashableStreamDescriptor(
                    name=per_stream_state.stream.stream_descriptor.name, namespace=per_stream_state.stream.stream_descriptor.namespace
                ): per_stream_state.stream.stream_state
                for per_stream_state in state
                if per_stream_state.type == AirbyteStateType.STREAM and hasattr(per_stream_state, "stream")
            }
            return None, streams
        else:
            raise ValueError("Input state should come in the form of list of Airbyte state messages or a mapping of states")

    @staticmethod
    def _create_descriptor_to_stream_state_mapping(
        state: MutableMapping[str, Any], stream_to_instance_map: Mapping[str, Stream]
    ) -> MutableMapping[HashableStreamDescriptor, Optional[AirbyteStateBlob]]:
        """
        Takes incoming state received in the legacy format and transforms it into a mapping of StreamDescriptor to AirbyteStreamState
        :param state: A mapping object representing the complete state of all streams in the legacy format
        :param stream_to_instance_map: A mapping of stream name to stream instance used to retrieve a stream's namespace
        :return: The mapping of all of a sync's streams to the corresponding stream state
        """
        streams = {}
        for stream_name, state_value in state.items():
            namespace = stream_to_instance_map[stream_name].namespace if stream_name in stream_to_instance_map else None
            stream_descriptor = HashableStreamDescriptor(name=stream_name, namespace=namespace)
            streams[stream_descriptor] = AirbyteStateBlob.parse_obj(state_value or {})
        return streams

    @staticmethod
    def _is_migrated_legacy_state(state: Union[List[AirbyteStateMessage], MutableMapping[str, Any]]) -> bool:
        return (
            isinstance(state, List)
            and len(state) == 1
            and isinstance(state[0], AirbyteStateMessage)
            and state[0].type == AirbyteStateType.LEGACY
        )

    @staticmethod
    def _is_global_state(state: Union[List[AirbyteStateMessage], MutableMapping[str, Any]]) -> bool:
        return (
            isinstance(state, List)
            and len(state) == 1
            and isinstance(state[0], AirbyteStateMessage)
            and state[0].type == AirbyteStateType.GLOBAL
        )
