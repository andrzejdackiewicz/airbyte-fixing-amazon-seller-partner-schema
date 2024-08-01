# generated by datamodel-codegen:
#   filename:  TestConnections.yaml

from __future__ import annotations

from pydantic import BaseModel, Extra, Field


class TestConnections(BaseModel):
    class Config:
        extra = Extra.forbid

    name: str = Field(..., description="The connection name")
    id: str = Field(..., description="The connection ID")
