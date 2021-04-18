"""
MIT License

Copyright (c) 2020 Airbyte

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"""

import json
import logging
from pathlib import Path
from typing import Iterable, Mapping, Optional, List

import docker
from airbyte_protocol import AirbyteMessage, ConfiguredAirbyteCatalog


class ConnectorRunner:
    def __init__(self, image_name: str, volume: Path):
        self._client = docker.from_env()
        self._image = self._client.images.pull(image_name)
        self._runs = 0
        self._volume_base = volume

    def _prepare_volumes(self, config: Optional[Mapping], state: Optional[Mapping], catalog: Optional[ConfiguredAirbyteCatalog]):
        input_path = self._volume_base / f"run_{self._runs}" / "input"
        output_path = self._volume_base / f"run_{self._runs}" / "output"
        input_path.mkdir(parents=True)
        output_path.mkdir(parents=True)

        if config:
            with open(str(input_path / "tap_config.json"), "w") as outfile:
                json.dump(dict(config), outfile)

        if state:
            with open(str(input_path / "state.json"), "w") as outfile:
                json.dump(dict(state), outfile)

        if catalog:
            with open(str(input_path / "catalog.json"), "w") as outfile:
                outfile.write(catalog.json())

        volumes = {
            str(input_path): {
                "bind": "/data",
                # "mode": "ro",
            },
            str(output_path): {
                "bind": "/local",
                "mode": "rw",
            },
        }
        return volumes

    def call_spec(self, **kwargs) -> List[AirbyteMessage]:
        cmd = "spec"
        output = list(self.run(cmd=cmd, **kwargs))
        return output

    def call_check(self, config, **kwargs) -> List[AirbyteMessage]:
        cmd = "check --config tap_config.json"
        output = list(self.run(cmd=cmd, config=config, **kwargs))
        return output

    def call_discover(self, config, **kwargs) -> List[AirbyteMessage]:
        cmd = "discover --config tap_config.json"
        output = list(self.run(cmd=cmd, config=config, **kwargs))
        return output

    def call_read(self, config, catalog, **kwargs) -> List[AirbyteMessage]:
        cmd = "read --config tap_config.json --catalog catalog.json"
        output = list(self.run(cmd=cmd, config=config, catalog=catalog, **kwargs))
        return output

    def call_read_with_state(self, config, catalog, state, **kwargs) -> List[AirbyteMessage]:
        cmd = "read --config tap_config.json --catalog catalog.json --state state.json"
        output = list(self.run(cmd=cmd, config=config, catalog=catalog, state=state, **kwargs))
        return output

    def run(self, cmd, config=None, state=None, catalog=None, **kwargs) -> Iterable[AirbyteMessage]:
        self._runs += 1
        volumes = self._prepare_volumes(config, state, catalog)
        logs = self._client.containers.run(
            image=self._image, command=cmd, working_dir="/data", volumes=volumes, network="host", stdout=True, stderr=True, **kwargs
        )
        logging.info("Running docker, folders: %s", volumes)
        for line in logs.decode("utf-8").splitlines():
            logging.info(AirbyteMessage.parse_raw(line).type)
            yield AirbyteMessage.parse_raw(line)
