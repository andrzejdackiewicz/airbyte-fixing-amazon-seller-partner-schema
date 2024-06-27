#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


import logging
from typing import Any, Iterable, Mapping

from airbyte_cdk.destinations import Destination
from airbyte_cdk.destinations.vector_db_based.document_processor import DocumentProcessor
from airbyte_cdk.destinations.vector_db_based.embedder import Embedder, create_from_config
from airbyte_cdk.destinations.vector_db_based.indexer import Indexer
from airbyte_cdk.destinations.vector_db_based.writer import Writer
from airbyte_cdk.models import AirbyteConnectionStatus, AirbyteMessage, ConfiguredAirbyteCatalog, ConnectorSpecification, Status
from airbyte_cdk.models.airbyte_protocol import DestinationSyncMode
from destination_weaviate.config import ConfigModel
from destination_weaviate.indexer import WeaviateIndexer
from destination_weaviate.no_embedder import NoEmbedder


class DestinationWeaviate(Destination):
    indexer: Indexer
    embedder: Embedder

    def _init_indexer(self, config: ConfigModel):
        self.embedder = (
            create_from_config(config.embedding, config.processing)
            if config.embedding.mode != "no_embedding"
            else NoEmbedder(config.embedding)
        )
        self.indexer = WeaviateIndexer(config.indexing)

    def write(
        self, config: Mapping[str, Any], configured_catalog: ConfiguredAirbyteCatalog, input_messages: Iterable[AirbyteMessage]
    ) -> Iterable[AirbyteMessage]:
        config_model = ConfigModel.parse_obj(config)
        self._init_indexer(config_model)
        writer = Writer(
            config_model.processing,
            self.indexer,
            self.embedder,
            batch_size=config_model.indexing.batch_size,
            omit_raw_text=config_model.omit_raw_text,
        )
        yield from writer.write(configured_catalog, input_messages)

    def check(self, logger: logging.Logger, config: Mapping[str, Any]) -> AirbyteConnectionStatus:
        parsed_config = ConfigModel.parse_obj(config)
        self._init_indexer(parsed_config)
        checks = [self.embedder.check(), self.indexer.check(), DocumentProcessor.check_config(parsed_config.processing)]
        errors = [error for error in checks if error is not None]
        if len(errors) > 0:
            return AirbyteConnectionStatus(status=Status.FAILED, message="\n".join(errors))
        else:
            return AirbyteConnectionStatus(status=Status.SUCCEEDED)

    def spec(self, *args: Any, **kwargs: Any) -> ConnectorSpecification:
        return ConnectorSpecification(
            documentationUrl="https://docs.airbyte.com/integrations/destinations/weaviate",
            supportsIncremental=True,
            supported_destination_sync_modes=[DestinationSyncMode.overwrite, DestinationSyncMode.append, DestinationSyncMode.append_dedup],
            connectionSpecification=ConfigModel.schema(),  # type: ignore[attr-defined]
        )
