# Copyright (c) 2024 Airbyte, Inc., all rights reserved.

from dataclasses import InitVar, dataclass
from typing import Any, Iterable, Mapping, Optional, Callable

from airbyte_protocol.models import FailureType

from airbyte_cdk import AirbyteTracedException
from airbyte_cdk.sources.declarative.async_job.job_orchestrator import AsyncJobOrchestrator
from airbyte_cdk.sources.declarative.retrievers import Retriever
from airbyte_cdk.sources.declarative.stream_slicers import StreamSlicer
from airbyte_cdk.sources.declarative.types import StreamSlice, StreamState, Record
from airbyte_cdk.sources.streams.core import StreamData


class AsyncRetriever(Retriever):

    def __init__(self, stream_slicer: StreamSlicer, job_orchestrator_factory: Callable[[Iterable[StreamSlice]], AsyncJobOrchestrator]) -> None:
        self._stream_slicer = stream_slicer
        self._job_orchestrator_factory = job_orchestrator_factory
        self.__job_orchestrator: Optional[AsyncJobOrchestrator] = None

    @property
    def state(self) -> StreamState:
        """
        As a first iteration for sendgrid, there is no state to be managed
        """
        return {}

    @state.setter
    def state(self, value: StreamState) -> None:
        """
        As a first iteration for sendgrid, there is no state to be managed
        """
        pass

    @property
    def _job_orchestrator(self) -> AsyncJobOrchestrator:
        if self.__job_orchestrator:
            return self.__job_orchestrator

        raise AirbyteTracedException(
            message="Invalid state within AsyncJobRetriever. Please contact Airbyte Support",
            internal_message="AsyncPartitionRepository is expected to be accessed only after `stream_slices`",
            failure_type=FailureType.system_error,
        )

    def stream_slices(self) -> Iterable[Optional[StreamSlice]]:
        self.__job_orchestrator = self._job_orchestrator_factory(self._stream_slicer.stream_slices())

        for completed_partition in self._job_orchestrator.create_and_get_completed_partitions():
            yield StreamSlice(
                partition={"partition": completed_partition},
                cursor_slice={}
            )

    def read_records(
        self, 
        records_schema: Mapping[str, Any], 
        stream_slice: Optional[StreamSlice] = None,
    ) -> Iterable[StreamData]:
        if not isinstance(stream_slice, StreamSlice) or "partition" not in stream_slice.partition:
            raise AirbyteTracedException(
                message="Invalid arguments to AsyncJobRetriever.read_records: stream_slice is no optional. Please contact Airbyte Support",
                failure_type=FailureType.system_error,
            )
        for record in self._job_orchestrator.fetch_records(stream_slice["partition"]):
            yield Record(record, stream_slice)
