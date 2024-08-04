#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

from typing import Any, List, Mapping

from airbyte_cdk.sources.declarative.yaml_declarative_source import YamlDeclarativeSource
from airbyte_cdk.sources.streams.core import Stream

"""
This file provides the necessary constructs to interpret a provided declarative YAML configuration file into
source connector.

WARNING: Do not modify this file.
"""


# Declarative Source
class SourceWeatherstack(YamlDeclarativeSource):
    def __init__(self):
        super().__init__(**{"path_to_yaml": "manifest.yaml"})

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        streams = super().streams(config)
        if not config.get("is_paid_account"):
            return [stream for stream in streams if stream.name not in ["location_lookup", "historical"]]
        return streams