#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

# generated by datamodel-codegen:
#   filename:  ConnectorReleases.yaml

from __future__ import annotations

from datetime import date
from typing import Dict, Optional

from pydantic import AnyUrl, BaseModel, Extra, Field, constr


class VersionBreakingChange(BaseModel):
    class Config:
        extra = Extra.forbid

    upgradeDeadline: date = Field(
        ...,
        description="The deadline by which to upgrade before the breaking change takes effect.",
    )
    message: str = Field(..., description="Descriptive message detailing the breaking change.")
    migrationDocumentationUrl: Optional[AnyUrl] = Field(
        None,
        description="URL to documentation on how to migrate to the current version. Defaults to ${documentationUrl}-migrations#${version}",
    )


class ConnectorBreakingChanges(BaseModel):
    class Config:
        extra = Extra.forbid

    __root__: Dict[constr(regex=r"^\d+\.\d+\.\d+$"), VersionBreakingChange] = Field(
        ...,
        description="Each entry denotes a breaking change in a specific version of a connector that requires user action to upgrade.",
    )


class ConnectorReleases(BaseModel):
    class Config:
        extra = Extra.forbid

    breakingChanges: ConnectorBreakingChanges
    migrationDocumentationUrl: Optional[AnyUrl] = Field(
        None,
        description="URL to documentation on how to migrate from the previous version to the current version. Defaults to ${documentationUrl}-migrations",
    )
