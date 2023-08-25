#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

# generated by datamodel-codegen:

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from pydantic import AnyUrl, BaseModel, Extra, Field, constr

if TYPE_CHECKING:
    from datetime import date
    from uuid import UUID

    from typing_extensions import Literal


class ReleaseStage(BaseModel):
    __root__: Literal["alpha", "beta", "generally_available", "custom"] = Field(
        ...,
        description="enum that describes a connector's release stage",
        title="ReleaseStage",
    )


class SupportLevel(BaseModel):
    __root__: Literal["community", "certified"] = Field(
        ...,
        description="enum that describes a connector's release stage",
        title="SupportLevel",
    )


class ResourceRequirements(BaseModel):
    class Config:
        extra = Extra.forbid

    cpu_request: str | None = None
    cpu_limit: str | None = None
    memory_request: str | None = None
    memory_limit: str | None = None


class JobType(BaseModel):
    __root__: Literal[
        "get_spec",
        "check_connection",
        "discover_schema",
        "sync",
        "reset_connection",
        "connection_updater",
        "replicate",
    ] = Field(
        ...,
        description="enum that describes the different types of jobs that the platform runs.",
        title="JobType",
    )


class AllowedHosts(BaseModel):
    class Config:
        extra = Extra.allow

    hosts: list[str] | None = Field(
        None,
        description="An array of hosts that this connector can connect to.  AllowedHosts not being present for the source or destination means that access to all hosts is allowed.  An empty list here means that no network access is granted.",
    )


class SuggestedStreams(BaseModel):
    class Config:
        extra = Extra.allow

    streams: list[str] | None = Field(
        None,
        description="An array of streams that this connector suggests the average user will want.  SuggestedStreams not being present for the source means that all streams are suggested.  An empty list here means that no streams are suggested.",
    )


class VersionBreakingChange(BaseModel):
    class Config:
        extra = Extra.forbid

    upgradeDeadline: date = Field(
        ...,
        description="The deadline by which to upgrade before the breaking change takes effect.",
    )
    message: str = Field(
        ..., description="Descriptive message detailing the breaking change.",
    )
    migrationDocumentationUrl: AnyUrl | None = Field(
        None,
        description="URL to documentation on how to migrate to the current version. Defaults to ${documentationUrl}-migrations#${version}",
    )


class AirbyteInternal(BaseModel):
    class Config:
        extra = Extra.allow

    sl: Literal[100, 200, 300] | None = None
    ql: Literal[100, 200, 300, 400, 500, 600] | None = None


class JobTypeResourceLimit(BaseModel):
    class Config:
        extra = Extra.forbid

    jobType: JobType
    resourceRequirements: ResourceRequirements


class ConnectorBreakingChanges(BaseModel):
    class Config:
        extra = Extra.forbid

    __root__: dict[constr(regex=r"^\d+\.\d+\.\d+$"), VersionBreakingChange] = Field(
        ...,
        description="Each entry denotes a breaking change in a specific version of a connector that requires user action to upgrade.",
    )


class ActorDefinitionResourceRequirements(BaseModel):
    class Config:
        extra = Extra.forbid

    default: ResourceRequirements | None = Field(
        None,
        description="if set, these are the requirements that should be set for ALL jobs run for this actor definition.",
    )
    jobSpecific: list[JobTypeResourceLimit] | None = None


class ConnectorReleases(BaseModel):
    class Config:
        extra = Extra.forbid

    breakingChanges: ConnectorBreakingChanges
    migrationDocumentationUrl: AnyUrl | None = Field(
        None,
        description="URL to documentation on how to migrate from the previous version to the current version. Defaults to ${documentationUrl}-migrations",
    )


class ConnectorRegistrySourceDefinition(BaseModel):
    class Config:
        extra = Extra.allow

    sourceDefinitionId: UUID
    name: str
    dockerRepository: str
    dockerImageTag: str
    documentationUrl: str
    icon: str | None = None
    iconUrl: str | None = None
    sourceType: Literal["api", "file", "database", "custom"] | None = None
    spec: dict[str, Any]
    tombstone: bool | None = Field(
        False,
        description="if false, the configuration is active. if true, then this configuration is permanently off.",
    )
    public: bool | None = Field(
        False,
        description="true if this connector definition is available to all workspaces",
    )
    custom: bool | None = Field(
        False, description="whether this is a custom connector definition",
    )
    releaseStage: ReleaseStage | None = None
    supportLevel: SupportLevel | None = None
    releaseDate: date | None = Field(
        None,
        description="The date when this connector was first released, in yyyy-mm-dd format.",
    )
    resourceRequirements: ActorDefinitionResourceRequirements | None = None
    protocolVersion: str | None = Field(
        None, description="the Airbyte Protocol version supported by the connector",
    )
    allowedHosts: AllowedHosts | None = None
    suggestedStreams: SuggestedStreams | None = None
    maxSecondsBetweenMessages: int | None = Field(
        None,
        description="Number of seconds allowed between 2 airbyte protocol messages. The source will timeout if this delay is reach",
    )
    releases: ConnectorReleases | None = None
    ab_internal: AirbyteInternal | None = None
