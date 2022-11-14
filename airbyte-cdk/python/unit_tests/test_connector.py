#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#


import json
import logging
import os
import sys
import tempfile
import time
from pathlib import Path
from typing import Any, Mapping

import pytest
import yaml
from airbyte_cdk import AirbyteSpec, Connector
from airbyte_cdk.connector import MutableConfig
from airbyte_cdk.models import AirbyteConnectionStatus

logger = logging.getLogger("airbyte")

MODULE = sys.modules[__name__]
MODULE_PATH = os.path.abspath(MODULE.__file__)
SPEC_ROOT = os.path.dirname(MODULE_PATH)


class TestAirbyteSpec:
    VALID_SPEC = {
        "documentationUrl": "https://google.com",
        "connectionSpecification": {
            "type": "object",
            "required": ["api_token"],
            "additionalProperties": False,
            "properties": {"api_token": {"type": "string"}},
        },
    }

    def test_from_file(self):
        expected = self.VALID_SPEC
        with tempfile.NamedTemporaryFile("w") as f:
            f.write(json.dumps(self.VALID_SPEC))
            f.flush()
            actual = AirbyteSpec.from_file(f.name)
            assert expected == json.loads(actual.spec_string)

    def test_from_file_nonexistent(self):
        with pytest.raises(OSError):
            AirbyteSpec.from_file("/tmp/i do not exist")


class MockConnector(Connector):
    def check(self, logger: logging.Logger, config: Mapping[str, Any]) -> AirbyteConnectionStatus:
        pass


@pytest.fixture()
def mock_config():
    return {"bogus": "file"}


@pytest.fixture
def nonempty_file(mock_config):
    with tempfile.NamedTemporaryFile("w") as file:
        file.write(json.dumps(mock_config))
        file.flush()
        yield file


@pytest.fixture
def integration():
    return MockConnector()


def test_read_config(nonempty_file, integration: Connector, mock_config):
    actual = integration.read_config(nonempty_file.name)
    assert mock_config == actual


def test_write_config(integration, mock_config):
    config_path = Path(tempfile.gettempdir()) / "config.json"
    integration.write_config(mock_config, str(config_path))
    with open(config_path, "r") as actual:
        assert mock_config == json.loads(actual.read())


class TestConnectorSpec:
    CONNECTION_SPECIFICATION = {
        "type": "object",
        "required": ["api_token"],
        "additionalProperties": False,
        "properties": {"api_token": {"type": "string"}},
    }

    @pytest.fixture
    def use_json_spec(self):
        spec = {
            "documentationUrl": "https://airbyte.com/#json",
            "connectionSpecification": self.CONNECTION_SPECIFICATION,
        }

        json_path = os.path.join(SPEC_ROOT, "spec.json")
        with open(json_path, "w") as f:
            f.write(json.dumps(spec))
        yield
        os.remove(json_path)

    @pytest.fixture
    def use_yaml_spec(self):
        spec = {"documentationUrl": "https://airbyte.com/#yaml", "connectionSpecification": self.CONNECTION_SPECIFICATION}

        yaml_path = os.path.join(SPEC_ROOT, "spec.yaml")
        with open(yaml_path, "w") as f:
            f.write(yaml.dump(spec))
        yield
        os.remove(yaml_path)

    def test_spec_from_json_file(self, integration, use_json_spec):
        connector_spec = integration.spec(logger)
        assert connector_spec.documentationUrl == "https://airbyte.com/#json"
        assert connector_spec.connectionSpecification == self.CONNECTION_SPECIFICATION

    def test_spec_from_yaml_file(self, integration, use_yaml_spec):
        connector_spec = integration.spec(logger)
        assert connector_spec.documentationUrl == "https://airbyte.com/#yaml"
        assert connector_spec.connectionSpecification == self.CONNECTION_SPECIFICATION

    def test_multiple_spec_files_raises_exception(self, integration, use_yaml_spec, use_json_spec):
        with pytest.raises(RuntimeError, match="spec.yaml or spec.json"):
            integration.spec(logger)

    def test_no_spec_file_raises_exception(self, integration):
        with pytest.raises(FileNotFoundError, match="Unable to find spec."):
            integration.spec(logger)


class TestMutableConfig:
    def test_emit_message_when_updated_with_new_value(self, capsys):
        config = MutableConfig({"key_a": "value_a", "key_b": "value_b"})
        # Check nothing is printed on init
        captured = capsys.readouterr()
        assert not captured.out
        before_time = time.time() * 1000
        config["key_a"] = "new_value_a"
        after_time = time.time() * 1000
        captured = capsys.readouterr()
        raw_control_message = json.loads(captured.out)
        assert raw_control_message["type"] == "CONNECTOR_CONFIG"
        assert raw_control_message["connectorConfig"] == {"config": {"key_a": "new_value_a", "key_b": "value_b"}}
        assert before_time < raw_control_message["emitted_at"] < after_time

    def test_emit_message_when_updated_with_new_nested_value(self, capsys):
        config = MutableConfig({"key_a": {"key_a_a": "value_a_a"}, "key_b": "value_b"})
        # Check nothing is printed on
        captured = capsys.readouterr()
        assert not captured.out
        before_time = time.time() * 1000
        config["key_a"]["key_a_a"] = "new_value_a_a"
        after_time = time.time() * 1000
        captured = capsys.readouterr()
        raw_control_message = json.loads(captured.out)
        assert raw_control_message["type"] == "CONNECTOR_CONFIG"
        assert raw_control_message["connectorConfig"] == {"config": {"key_a": {"key_a_a": "new_value_a_a"}, "key_b": "value_b"}}
        assert before_time < raw_control_message["emitted_at"] < after_time

    def test_not_emit_message_when_updated_with_same_value(self, capsys):
        config = MutableConfig({"key_a": "value_a", "key_b": "value_b"})
        config["key_a"] = "value_a"
        captured = capsys.readouterr()
        assert not captured.out
