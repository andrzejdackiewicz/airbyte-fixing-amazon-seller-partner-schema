# generated by datamodel-codegen:
#   filename:  ConnectorRegistryDestinationDefinition.yaml

from __future__ import annotations

from datetime import date
from typing import Any, Dict, List, Optional
from uuid import UUID

from pydantic import AnyUrl, BaseModel, Extra, Field, constr
from typing_extensions import Literal


class ReleaseStage(BaseModel):
    __root__: Literal["alpha", "beta", "generally_available", "custom"] = Field(
        ..., description="enum that describes a connector's release stage", title="ReleaseStage"
    )


class SupportLevel(BaseModel):
    __root__: Literal["community", "certified", "archived"] = Field(
        ..., description="enum that describes a connector's release stage", title="SupportLevel"
    )


class ResourceRequirements(BaseModel):
    class Config:
        extra = Extra.forbid

    cpu_request: Optional[str] = None
    cpu_limit: Optional[str] = None
    memory_request: Optional[str] = None
    memory_limit: Optional[str] = None


class JobType(BaseModel):
    __root__: Literal["get_spec", "check_connection", "discover_schema", "sync", "reset_connection", "connection_updater", "replicate"] = (
        Field(..., description="enum that describes the different types of jobs that the platform runs.", title="JobType")
    )


class NormalizationDestinationDefinitionConfig(BaseModel):
    class Config:
        extra = Extra.allow

    normalizationRepository: str = Field(
        ...,
        description="a field indicating the name of the repository to be used for normalization. If the value of the flag is NULL - normalization is not used.",
    )
    normalizationTag: str = Field(..., description="a field indicating the tag of the docker repository to be used for normalization.")
    normalizationIntegrationType: str = Field(
        ..., description="a field indicating the type of integration dialect to use for normalization."
    )


class AllowedHosts(BaseModel):
    class Config:
        extra = Extra.allow

    hosts: Optional[List[str]] = Field(
        None,
        description="An array of hosts that this connector can connect to.  AllowedHosts not being present for the source or destination means that access to all hosts is allowed.  An empty list here means that no network access is granted.",
    )


class StreamBreakingChangeScope(BaseModel):
    class Config:
        extra = Extra.forbid

    scopeType: Any = Field("stream", const=True)
    impactedScopes: List[str] = Field(..., description="List of streams that are impacted by the breaking change.", min_items=1)


class AirbyteInternal(BaseModel):
    class Config:
        extra = Extra.allow

    sl: Optional[Literal[100, 200, 300]] = None
    ql: Optional[Literal[100, 200, 300, 400, 500, 600]] = None


class JobTypeResourceLimit(BaseModel):
    class Config:
        extra = Extra.forbid

    jobType: JobType
    resourceRequirements: ResourceRequirements


class BreakingChangeScope(BaseModel):
    __root__: StreamBreakingChangeScope = Field(..., description="A scope that can be used to limit the impact of a breaking change.")


class ActorDefinitionResourceRequirements(BaseModel):
    class Config:
        extra = Extra.forbid

    default: Optional[ResourceRequirements] = Field(
        None, description="if set, these are the requirements that should be set for ALL jobs run for this actor definition."
    )
    jobSpecific: Optional[List[JobTypeResourceLimit]] = None


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
        ..., description="Each entry denotes a breaking change in a specific version of a connector that requires user action to upgrade."
    )


class ConnectorReleases(BaseModel):
    class Config:
        extra = Extra.forbid

    breakingChanges: ConnectorBreakingChanges
    migrationDocumentationUrl: Optional[AnyUrl] = Field(
        None,
        description="URL to documentation on how to migrate from the previous version to the current version. Defaults to ${documentationUrl}-migrations",
    )


class ConnectorRegistryDestinationDefinition(BaseModel):
    class Config:
        extra = Extra.allow

    destinationDefinitionId: UUID
    name: str
    dockerRepository: str
    dockerImageTag: str
    documentationUrl: str
    icon: Optional[str] = None
    iconUrl: Optional[str] = None
    spec: Dict[str, Any]
    tombstone: Optional[bool] = Field(
        False, description="if false, the configuration is active. if true, then this configuration is permanently off."
    )
    public: Optional[bool] = Field(False, description="true if this connector definition is available to all workspaces")
    custom: Optional[bool] = Field(False, description="whether this is a custom connector definition")
    releaseStage: Optional[ReleaseStage] = None
    supportLevel: Optional[SupportLevel] = None
    releaseDate: Optional[date] = Field(None, description="The date when this connector was first released, in yyyy-mm-dd format.")
    tags: Optional[List[str]] = Field(
        None, description="An array of tags that describe the connector. E.g: language:python, keyword:rds, etc."
    )
    resourceRequirements: Optional[ActorDefinitionResourceRequirements] = None
    protocolVersion: Optional[str] = Field(None, description="the Airbyte Protocol version supported by the connector")
    normalizationConfig: Optional[NormalizationDestinationDefinitionConfig] = None
    supportsDbt: Optional[bool] = Field(
        None,
        description="an optional flag indicating whether DBT is used in the normalization. If the flag value is NULL - DBT is not used.",
    )
    allowedHosts: Optional[AllowedHosts] = None
    releases: Optional[ConnectorReleases] = None
    ab_internal: Optional[AirbyteInternal] = None
