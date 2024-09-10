# generated by datamodel-codegen:
#   filename:  ConnectorReleases.yaml

from __future__ import annotations

from datetime import date
from typing import Any, Dict, List, Optional

from pydantic import AnyUrl, BaseModel, Extra, Field, conint, constr


class RolloutConfiguration(BaseModel):
    class Config:
        extra = Extra.forbid

    initialPercentage: Optional[conint(ge=0, le=100)] = Field(
        0, description="The percentage of users that should receive the new version initially."
    )
    maxPercentage: Optional[conint(ge=0, le=100)] = Field(
        50, description="The percentage of users who should receive the release candidate during the test phase before full rollout."
    )
    advanceDelayMinutes: Optional[conint(ge=10)] = Field(
        10, description="The number of minutes to wait before advancing the rollout percentage."
    )


class StreamBreakingChangeScope(BaseModel):
    class Config:
        extra = Extra.forbid

    scopeType: Any = Field("stream", const=True)
    impactedScopes: List[str] = Field(..., description="List of streams that are impacted by the breaking change.", min_items=1)


class BreakingChangeScope(BaseModel):
    __root__: StreamBreakingChangeScope = Field(..., description="A scope that can be used to limit the impact of a breaking change.")


class VersionBreakingChange(BaseModel):
    class Config:
        extra = Extra.forbid

    upgradeDeadline: date = Field(..., description="The deadline by which to upgrade before the breaking change takes effect.")
    message: str = Field(..., description="Descriptive message detailing the breaking change.")
    migrationDocumentationUrl: Optional[AnyUrl] = Field(
        None,
        description="URL to documentation on how to migrate to the current version. Defaults to ${documentationUrl}-migrations#${version}",
    )
    scopedImpact: Optional[List[BreakingChangeScope]] = Field(
        None,
        description="List of scopes that are impacted by the breaking change. If not specified, the breaking change cannot be scoped to reduce impact via the supported scope types.",
        min_items=1,
    )


class ConnectorBreakingChanges(BaseModel):
    class Config:
        extra = Extra.forbid

    __root__: Dict[constr(regex=r"^\d+\.\d+\.\d+$"), VersionBreakingChange] = Field(
        ...,
        description="Each entry denotes a breaking change in a specific version of a connector that requires user action to upgrade.",
        title="ConnectorBreakingChanges",
    )


class ConnectorReleases(BaseModel):
    class Config:
        extra = Extra.forbid

    isReleaseCandidate: Optional[bool] = Field(False, description="Whether the release is eligible to be a release candidate.")
    rolloutConfiguration: Optional[RolloutConfiguration] = None
    breakingChanges: ConnectorBreakingChanges
    migrationDocumentationUrl: Optional[AnyUrl] = Field(
        None,
        description="URL to documentation on how to migrate from the previous version to the current version. Defaults to ${documentationUrl}-migrations",
    )
