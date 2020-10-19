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

from __future__ import annotations

from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import AnyUrl, BaseModel, Field


class Type(Enum):
    RECORD = 'RECORD'
    STATE = 'STATE'
    LOG = 'LOG'
    SPEC = 'SPEC'
    CONNECTION_STATUS = 'CONNECTION_STATUS'
    CATALOG = 'CATALOG'


class AirbyteType(Enum):
    boolean = 'boolean'
    number = 'number'
    text = 'text'
    object = 'object'


class AirbyteRecordMessage(BaseModel):
    stream: str = Field(..., description='the name of the stream for this record')
    data: Dict[str, Any] = Field(..., description='the record data')
    emitted_at: int = Field(
        ...,
        description='when the data was emitted from the source. epoch in millisecond.',
    )


class AirbyteStateMessage(BaseModel):
    data: Dict[str, Any] = Field(..., description='the state data')


class Level(Enum):
    FATAL = 'FATAL'
    ERROR = 'ERROR'
    WARN = 'WARN'
    INFO = 'INFO'
    DEBUG = 'DEBUG'
    TRACE = 'TRACE'


class AirbyteLogMessage(BaseModel):
    level: Level = Field(..., description='the type of logging')
    message: str = Field(..., description='the log message')


class Status(Enum):
    SUCCEEDED = 'SUCCEEDED'
    FAILED = 'FAILED'


class AirbyteConnectionStatus(BaseModel):
    status: Status
    message: Optional[str] = None


class AirbyteStream(BaseModel):
    name: str = Field(..., description="Stream's name.")
    json_schema: Optional[Dict[str, Any]] = Field(
        None, description='Stream schema using Json Schema specs.'
    )


class ConnectorSpecification(BaseModel):
    documentationUrl: Optional[AnyUrl] = None
    changelogUrl: Optional[AnyUrl] = None
    connectionSpecification: Dict[str, Any] = Field(
        ...,
        description='ConnectorDefinition specific blob. Must be a valid JSON string.',
    )


class AirbyteCatalog(BaseModel):
    streams: List[AirbyteStream]


class AirbyteMessage(BaseModel):
    type: Type = Field(..., description='Message type')
    log: Optional[AirbyteLogMessage] = Field(
        None,
        description='log message: any kind of logging you want the platform to know about.',
    )
    spec: Optional[ConnectorSpecification] = None
    connectionStatus: Optional[AirbyteConnectionStatus] = None
    catalog: Optional[AirbyteCatalog] = Field(
        None,
        description='log message: any kind of logging you want the platform to know about.',
    )
    record: Optional[AirbyteRecordMessage] = Field(
        None, description='record message: the record'
    )
    state: Optional[AirbyteStateMessage] = Field(
        None,
        description='schema message: the state. Must be the last message produced. The platform uses this information',
    )
