#
# Copyright (c) 2021 Airbyte, Inc., all rights reserved.
#

# generated by datamodel-codegen:
#   filename:  airbyte_protocol.yaml

from __future__ import annotations

from enum import Enum
from typing import Any, Dict, List, Optional, Union

from pydantic import AnyUrl, BaseModel, Extra, Field


class Type(Enum):
    RECORD = "RECORD"
    STATE = "STATE"
    LOG = "LOG"
    SPEC = "SPEC"
    CONNECTION_STATUS = "CONNECTION_STATUS"
    CATALOG = "CATALOG"


class AirbyteRecordMessage(BaseModel):
    class Config:
        extra = Extra.allow

    stream: str = Field(..., description="the name of this record's stream")
    data: Dict[str, Any] = Field(..., description="the record data")
    emitted_at: int = Field(
        ...,
        description="when the data was emitted from the source. epoch in millisecond.",
    )
    namespace: Optional[str] = Field(None, description="the namespace of this record's stream")


class AirbyteStateMessage(BaseModel):
    class Config:
        extra = Extra.allow

    data: Dict[str, Any] = Field(..., description="the state data")


class Level(Enum):
    FATAL = "FATAL"
    CRITICAL = "CRITICAL"
    ERROR = "ERROR"
    WARN = "WARN"
    WARNING = "WARNING"
    INFO = "INFO"
    DEBUG = "DEBUG"
    TRACE = "TRACE"


class AirbyteLogMessage(BaseModel):
    class Config:
        extra = Extra.allow

    level: Level = Field(..., description="the type of logging")
    message: str = Field(..., description="the log message")


class Status(Enum):
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"


class AirbyteConnectionStatus(BaseModel):
    class Config:
        extra = Extra.allow

    status: Status
    message: Optional[str] = None


class SyncMode(Enum):
    full_refresh = "full_refresh"
    incremental = "incremental"


class DestinationSyncMode(Enum):
    append = "append"
    overwrite = "overwrite"
    append_dedup = "append_dedup"


class OAuth2Specification(BaseModel):
    class Config:
        extra = Extra.allow

    rootObject: Optional[List[Union[str, int]]] = Field(
        None,
        description="A list of strings representing a pointer to the root object which contains any oauth parameters in the ConnectorSpecification.\nExamples:\nif oauth parameters were contained inside the top level, rootObject=[] If they were nested inside another object {'credentials': {'app_id' etc...}, rootObject=['credentials'] If they were inside a oneOf {'switch': {oneOf: [{client_id...}, {non_oauth_param]}},  rootObject=['switch', 0] ",
    )
    oauthFlowInitParameters: Optional[List[List[str]]] = Field(
        None,
        description="Pointers to the fields in the rootObject needed to obtain the initial refresh/access tokens for the OAuth flow. Each inner array represents the path in the rootObject of the referenced field. For example. Assume the rootObject contains params 'app_secret', 'app_id' which are needed to get the initial refresh token. If they are not nested in the rootObject, then the array would look like this [['app_secret'], ['app_id']] If they are nested inside an object called 'auth_params' then this array would be [['auth_params', 'app_secret'], ['auth_params', 'app_id']]",
    )
    oauthFlowOutputParameters: Optional[List[List[str]]] = Field(
        None,
        description="Pointers to the fields in the rootObject which can be populated from successfully completing the oauth flow using the init parameters. This is typically a refresh/access token. Each inner array represents the path in the rootObject of the referenced field.",
    )


class AuthType(Enum):
    oauth2_0 = "oauth2.0"


class AuthSpecification(BaseModel):
    auth_type: Optional[AuthType] = None
    oauth2Specification: Optional[OAuth2Specification] = Field(
        None,
        description="If the connector supports OAuth, this field should be non-null.",
    )


class ConnectorSpecification(BaseModel):
    class Config:
        extra = Extra.allow

    documentationUrl: Optional[AnyUrl] = None
    changelogUrl: Optional[AnyUrl] = None
    connectionSpecification: Dict[str, Any] = Field(
        ...,
        description="ConnectorDefinition specific blob. Must be a valid JSON string.",
    )
    supportsIncremental: Optional[bool] = Field(None, description="If the connector supports incremental mode or not.")
    supportsNormalization: Optional[bool] = Field(False, description="If the connector supports normalization or not.")
    supportsDBT: Optional[bool] = Field(False, description="If the connector supports DBT or not.")
    supported_destination_sync_modes: Optional[List[DestinationSyncMode]] = Field(
        None, description="List of destination sync modes supported by the connector"
    )
    authSpecification: Optional[AuthSpecification] = None


class AirbyteStream(BaseModel):
    class Config:
        extra = Extra.allow

    name: str = Field(..., description="Stream's name.")
    json_schema: Dict[str, Any] = Field(..., description="Stream schema using Json Schema specs.")
    supported_sync_modes: Optional[List[SyncMode]] = None
    source_defined_cursor: Optional[bool] = Field(
        None,
        description="If the source defines the cursor field, then any other cursor field inputs will be ignored. If it does not, either the user_provided one is used, or the default one is used as a backup.",
    )
    default_cursor_field: Optional[List[str]] = Field(
        None,
        description="Path to the field that will be used to determine if a record is new or modified since the last sync. If not provided by the source, the end user will have to specify the comparable themselves.",
    )
    source_defined_primary_key: Optional[List[List[str]]] = Field(
        None,
        description="If the source defines the primary key, paths to the fields that will be used as a primary key. If not provided by the source, the end user will have to specify the primary key themselves.",
    )
    namespace: Optional[str] = Field(
        None,
        description="Optional Source-defined namespace. Currently only used by JDBC destinations to determine what schema to write to. Airbyte streams from the same sources should have the same namespace.",
    )


class ConfiguredAirbyteStream(BaseModel):
    class Config:
        extra = Extra.allow

    stream: AirbyteStream
    sync_mode: SyncMode
    cursor_field: Optional[List[str]] = Field(
        None,
        description="Path to the field that will be used to determine if a record is new or modified since the last sync. This field is REQUIRED if `sync_mode` is `incremental`. Otherwise it is ignored.",
    )
    destination_sync_mode: DestinationSyncMode
    primary_key: Optional[List[List[str]]] = Field(
        None,
        description="Paths to the fields that will be used as primary key. This field is REQUIRED if `destination_sync_mode` is `*_dedup`. Otherwise it is ignored.",
    )


class AirbyteCatalog(BaseModel):
    class Config:
        extra = Extra.allow

    streams: List[AirbyteStream]


class ConfiguredAirbyteCatalog(BaseModel):
    class Config:
        extra = Extra.allow

    streams: List[ConfiguredAirbyteStream]


class AirbyteMessage(BaseModel):
    class Config:
        extra = Extra.allow

    type: Type = Field(..., description="Message type")
    log: Optional[AirbyteLogMessage] = Field(
        None,
        description="log message: any kind of logging you want the platform to know about.",
    )
    spec: Optional[ConnectorSpecification] = None
    connectionStatus: Optional[AirbyteConnectionStatus] = None
    catalog: Optional[AirbyteCatalog] = Field(
        None,
        description="log message: any kind of logging you want the platform to know about.",
    )
    record: Optional[AirbyteRecordMessage] = Field(None, description="record message: the record")
    state: Optional[AirbyteStateMessage] = Field(
        None,
        description="schema message: the state. Must be the last message produced. The platform uses this information",
    )


class AirbyteProtocol(BaseModel):
    airbyte_message: Optional[AirbyteMessage] = None
    configured_airbyte_catalog: Optional[ConfiguredAirbyteCatalog] = None
