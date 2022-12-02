#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#

import logging
from typing import Any, Dict, Iterable, List

from airbyte_cdk.models import AirbyteLogMessage, AirbyteMessage, Level
from airbyte_cdk.models import ConfiguredAirbyteCatalog
from airbyte_cdk.models import Type as MessageType
from airbyte_cdk.sources.declarative.declarative_stream import DeclarativeStream
from airbyte_cdk.sources.declarative.yaml_declarative_source import \
    ManifestDeclarativeSource
from airbyte_cdk.sources.streams.http import HttpStream


class LowCodeSourceAdapter:
    logger = logging.getLogger("airbyte.connector-builder")

    def __init__(self, manifest: Dict[str, Any]):
        # Request and response messages are only emitted for a sources that have debug turned on
        self._source = ManifestDeclarativeSource(manifest, debug=True)

    def get_http_streams(self, config: Dict[str, Any]) -> List[HttpStream]:
        http_streams = []
        for stream in self._source.streams(config=config):
            if isinstance(stream, DeclarativeStream):
                if isinstance(stream.retriever, HttpStream):
                    http_streams.append(stream.retriever)
                else:
                    raise TypeError(
                        f"A declarative stream should only have a retriever of type HttpStream, but received: {stream.retriever.__class__}")
            else:
                raise TypeError(
                    f"A declarative source should only contain streams of type DeclarativeStream, but received: {stream.__class__}")
        return http_streams

    def read_stream(self, stream: str, config: Dict[str, Any]) -> Iterable[AirbyteMessage]:
        configured_catalog = ConfiguredAirbyteCatalog.parse_obj(
            {
                "streams": [
                    {
                        "stream": {
                            "name": stream,
                            "json_schema": {},
                            "supported_sync_modes": ["full_refresh", "incremental"],
                        },
                        "sync_mode": "full_refresh",
                        "destination_sync_mode": "overwrite",
                    }
                ]
            }
        )
        generator = self._source.read(logger=self._source.logger, config=config, catalog=configured_catalog)
        while True:
            try:
                message = next(generator)
                yield message
            except StopIteration:
                return
            except Exception as e:
                yield AirbyteMessage(type=MessageType.LOG, log=AirbyteLogMessage(level=Level.INFO, message=str(e)))
                return
