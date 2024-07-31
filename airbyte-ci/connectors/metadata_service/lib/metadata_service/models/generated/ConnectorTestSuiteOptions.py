# generated by datamodel-codegen:
#   filename:  ConnectorTestSuiteOptions.yaml

from __future__ import annotations

from typing import List, Optional

from pydantic import BaseModel, Extra, Field
from typing_extensions import Literal


class SecretStore(BaseModel):
    class Config:
        extra = Extra.forbid

    alias: Optional[str] = Field(None, description="The alias of the secret store which can map to its actual secret address")
    type: Optional[Literal["GSM"]] = Field(None, description="The type of the secret store")


class TestConnections(BaseModel):
    class Config:
        extra = Extra.forbid

    name: str = Field(..., description="The connection name")
    id: str = Field(..., description="The connection ID")


class Secret(BaseModel):
    class Config:
        extra = Extra.forbid

    name: str = Field(..., description="The secret name in the secret store")
    fileName: Optional[str] = Field(None, description="The name of the file to which the secret value would be persisted")
    secretStore: SecretStore


class ConnectorTestSuiteOptions(BaseModel):
    class Config:
        extra = Extra.forbid

    suite: Literal["unitTests", "integrationTests", "acceptanceTests", "liveTests"] = Field(
        ..., description="Name of the configured test suite"
    )
    testSecrets: Optional[List[Secret]] = Field(None, description="List of secrets required to run the test suite")
    testConnections: Optional[List[TestConnections]] = Field(
        None, description="List of sandbox cloud connections that tests can be run against"
    )
