#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#
from __future__ import (  # Used to evaluate type hints at runtime, a NameError: name 'ConfigObserver' is not defined is thrown otherwise
    annotations,
)

import time
from typing import Any, MutableMapping

from airbyte_cdk.models import AirbyteControlConnectorConfigMessage, AirbyteControlMessage, AirbyteMessage, OrchestratorType, Type


class ObservedDict(dict):
    def __init__(self, non_observed_mapping: MutableMapping, observer: ConfigObserver, update_on_unchanged_value=True) -> None:
        self.observer = observer
        self.update_on_unchanged_value = update_on_unchanged_value
        for item, value in self.items():
            if isinstance(value, MutableMapping):
                self[item] = ObservedDict(value, observer)
        super().__init__(non_observed_mapping)

    def __setitem__(self, item: Any, value: Any):
        """Override dict__setitem__ by:
        1. Observing the new value if it is a dict
        2. Call observer update if the new value is different from the previous one
        """
        previous_value = self.get(item)
        value = ObservedDict(value, self.observer) if isinstance(value, MutableMapping) else value
        super(ObservedDict, self).__setitem__(item, value)
        if self.update_on_unchanged_value or value != previous_value:
            self.observer.update()


class ConfigObserver:
    """This class is made to track mutations on ObservedDict config.
    When update is called the observed configuration is saved on disk a CONNECTOR_CONFIG control message is emitted on stdout.
    """

    def set_config(self, config: ObservedDict) -> None:
        self.config = config

    def update(self) -> None:
        self._emit_airbyte_control_message()

    def _emit_airbyte_control_message(self) -> None:
        control_message = AirbyteControlMessage(
            type=OrchestratorType.CONNECTOR_CONFIG,
            emitted_at=time.time() * 1000,
            connectorConfig=AirbyteControlConnectorConfigMessage(config=self.config),
        )
        airbyte_message = AirbyteMessage(type=Type.CONTROL, control=control_message)
        print(airbyte_message.json(exclude_unset=True))
