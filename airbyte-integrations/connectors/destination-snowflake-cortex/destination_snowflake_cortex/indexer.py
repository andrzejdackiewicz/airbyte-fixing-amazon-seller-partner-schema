#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#

import uuid
from typing import Any, Iterable, Optional

import dpath.util
from airbyte._processors.sql.snowflakecortex import SnowflakeCortexSqlProcessor, SnowflakeSqlProcessor
from airbyte.caches import SnowflakeCache
from airbyte.strategies import WriteStrategy
from airbyte_cdk.destinations.vector_db_based.document_processor import METADATA_RECORD_ID_FIELD, METADATA_STREAM_FIELD
from airbyte_cdk.destinations.vector_db_based.indexer import Indexer
from airbyte_cdk.models import (
    AirbyteMessage,
    AirbyteStateMessage,
    AirbyteStateType,
    AirbyteStreamState,
    ConfiguredAirbyteCatalog,
    DestinationSyncMode,
    StreamDescriptor,
    Type,
)
from destination_snowflake_cortex.config import SnowflakeCortexIndexingModel

# extra columns to be added to the Airbyte message
DOCUMENT_ID_COLUMN = "document_id"
CHUNK_ID_COLUMN = "chunk_id"
METADATA_COLUMN = "metadata"
PAGE_CONTENT_COLUMN = "page_content"
EMBEDDING_COLUMN = "embedding"


class SnowflakeCortexIndexer(Indexer):
    config: SnowflakeCortexIndexingModel

    def __init__(self, config: SnowflakeCortexIndexingModel, embedding_dimensions: int, configured_catalog: ConfiguredAirbyteCatalog):
        super().__init__(config)
        self.embedding_dimensions = embedding_dimensions
        self.catalog = configured_catalog
        self._init_db_connection(config)

    def _init_db_connection(self, config: SnowflakeCortexIndexingModel):
        account = config.account
        username = config.username
        password = config.password
        database = config.database
        warehouse = config.warehouse
        role = config.role
        self.cache = SnowflakeCache(
            account=account, username=username, password=password, database=database, warehouse=warehouse, role=role
        )
        self.defaut_processor = SnowflakeSqlProcessor(cache=self.cache)

    def _get_airbyte_messsages_from_chunks(
        self,
        document_chunks,
    ) -> Iterable[AirbyteMessage]:
        """Creates Airbyte messages from chunk records."""
        airbyte_messages = []
        for i in range(len(document_chunks)):
            chunk = document_chunks[i]
            message = AirbyteMessage(type=Type.RECORD, record=chunk.record)
            new_data = {}
            new_data[DOCUMENT_ID_COLUMN] = self._create_document_id(message)
            new_data[CHUNK_ID_COLUMN] = str(uuid.uuid4().int)
            new_data[METADATA_COLUMN] = chunk.metadata
            new_data[PAGE_CONTENT_COLUMN] = chunk.page_content
            new_data[EMBEDDING_COLUMN] = chunk.embedding
            message.record.data = new_data
            airbyte_messages.append(message)
        return airbyte_messages

    def _get_updated_catalog(self) -> ConfiguredAirbyteCatalog:
        """Adds following columns to catalog
        document_id (primary key) -> unique per record/document
        chunk_id -> unique per chunk
        page_content -> text content of the page
        metadata -> metadata of the record
        embedding -> embedding of the page content
        """
        updated_catalog = self.catalog
        # update each stream in the catalog
        for stream in updated_catalog.streams:
            stream.stream.json_schema["properties"][DOCUMENT_ID_COLUMN] = {"type": "string"}
            stream.stream.json_schema["properties"][CHUNK_ID_COLUMN] = {"type": "string"}
            stream.stream.json_schema["properties"][PAGE_CONTENT_COLUMN] = {"type": "string"}
            stream.stream.json_schema["properties"][METADATA_COLUMN] = {"type": "object"}
            stream.stream.json_schema["properties"][EMBEDDING_COLUMN] = {"type": "vector_array"}
            # set primary key, okay to override if already set
            stream.primary_key = [[DOCUMENT_ID_COLUMN]]
            # TODO: do we want to set chunk_id as a constraint in the catalog
        return updated_catalog

    def _get_primary_keys(self, stream_name: str) -> Optional[str]:
        for stream in self.catalog.streams:
            if stream.stream.name == stream_name:
                return stream.primary_key
        return None

    def _get_record_primary_key(self, record: AirbyteMessage) -> Optional[str]:
        """Create primary key for the record by appending the primary keys."""
        stream_name = record.record.stream
        primary_keys = self._get_primary_keys(stream_name)

        if not primary_keys:
            return None

        primary_key = []
        for key in primary_keys:
            try:
                primary_key.append(str(dpath.util.get(record.record.data, key)))
            except KeyError:
                primary_key.append("__not_found__")
        # return a stringified version of all primary keys
        stringified_primary_key = "_".join(primary_key)
        return stringified_primary_key

    def _create_document_id(self, record: AirbyteMessage) -> str:
        """Create document id based on the primary key values. Returns a random uuid if no primary key is found"""
        stream_name = record.record.stream
        primary_key = self._get_record_primary_key(record)
        if primary_key is not None:
            return f"Stream_{stream_name}_Key_{primary_key}"
        return str(uuid.uuid4().int)

    def _create_state_message(self, stream, namespace, data: dict[str, Any]) -> AirbyteMessage:
        """Create a state message for the stream"""
        stream = AirbyteStreamState(stream_descriptor=StreamDescriptor(name=stream, namespace=namespace))
        return AirbyteMessage(
            type=Type.STATE,
            state=AirbyteStateMessage(type=AirbyteStateType.STREAM, stream=stream, data=data),
        )

    def get_write_strategy(self, stream_name: str) -> WriteStrategy:
        for stream in self.catalog.streams:
            if stream.stream.name == stream_name:
                if stream.destination_sync_mode == DestinationSyncMode.overwrite:
                    return WriteStrategy.REPLACE
                if stream.destination_sync_mode == DestinationSyncMode.append:
                    return WriteStrategy.APPEND
                if stream.destination_sync_mode == DestinationSyncMode.append_dedup:
                    return WriteStrategy.MERGE
        return WriteStrategy.AUTO

    def index(self, document_chunks, namespace, stream):
        # get list of airbyte messages from the document chunks
        airbyte_messages = self._get_airbyte_messsages_from_chunks(document_chunks)
        # assuming it's per stream, let's add a stream specific state message to the list
        # todo: remove state messages and see if things still work
        airbyte_messages.append(self._create_state_message(stream, namespace, {}))

        # update catalog to match all columns in the airbyte messages
        if airbyte_messages is not None and len(airbyte_messages) > 0:
            updated_catalog = self._get_updated_catalog()
            cortex_processor = SnowflakeCortexSqlProcessor(
                cache=self.cache,
                catalog=updated_catalog,
                vector_length=self.embedding_dimensions,
                source_name="vector_db_based",
                stream_names=[stream],
            )
            cortex_processor.process_airbyte_messages(airbyte_messages, self.get_write_strategy(stream))

    def delete(self, delete_ids, namespace, stream):
        # TODO: Confirm PyAirbyte SQL processor will handle deletes when needed.
        pass

    def check(self) -> Optional[str]:
        self.defaut_processor._get_tables_list()
        # TODO: check to see if vector type is available in snowflake instance ?
