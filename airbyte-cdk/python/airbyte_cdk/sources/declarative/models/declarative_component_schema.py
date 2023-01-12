#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#

# generated by datamodel-codegen:
#   filename:  declarative_component_schema.yaml

from __future__ import annotations

from enum import Enum
from typing import Any, Dict, List, Optional, Union

from pydantic import BaseModel, Extra, Field
from typing_extensions import Literal


class AddedFieldDefinition(BaseModel):
    type: Literal["AddedFieldDefinition"]
    path: List[str]
    value: str
    options: Optional[Dict[str, Any]] = Field(None, alias="$options")


class AddFields(BaseModel):
    type: Literal["AddFields"]
    fields: List[AddedFieldDefinition]
    options: Optional[Dict[str, Any]] = Field(None, alias="$options")


class ApiKeyAuthenticator(BaseModel):
    type: Literal["ApiKeyAuthenticator"]
    api_token: str
    header: Optional[str] = None
    options: Optional[Dict[str, Any]] = Field(None, alias="$options")


class BasicHttpAuthenticator(BaseModel):
    type: Literal["BasicHttpAuthenticator"]
    username: str = Field(
        ...,
        description="The username that will be combined with the password, base64 encoded and used to make requests",
    )
    password: Optional[str] = Field(
        "",
        description="The password that will be combined with the username, base64 encoded and used to make requests",
    )
    options: Optional[Dict[str, Any]] = Field(None, alias="$options")


class BearerAuthenticator(BaseModel):
    type: Literal["BearerAuthenticator"]
    api_token: str
    options: Optional[Dict[str, Any]] = Field(None, alias="$options")


class CheckStream(BaseModel):
    type: Literal["CheckStream"]
    stream_names: List[str]


class ConstantBackoffStrategy(BaseModel):
    type: Literal["ConstantBackoffStrategy"]
    backoff_time_in_seconds: Union[float, str]
    options: Optional[Dict[str, Any]] = Field(None, alias="$options")


class CustomAuthenticator(BaseModel):
    class Config:
        extra = Extra.allow

    type: Literal["CustomAuthenticator"]
    class_name: str
    options: Optional[Dict[str, Any]] = Field(None, alias="$options")


class CustomBackoffStrategy(BaseModel):
    class Config:
        extra = Extra.allow

    type: Literal["CustomBackoffStrategy"]
    class_name: str
    options: Optional[Dict[str, Any]] = Field(None, alias="$options")


class CustomErrorHandler(BaseModel):
    class Config:
        extra = Extra.allow

    type: Literal["CustomErrorHandler"]
    class_name: str
    options: Optional[Dict[str, Any]] = Field(None, alias="$options")


class CustomPaginationStrategy(BaseModel):
    class Config:
        extra = Extra.allow

    type: Literal["CustomPaginationStrategy"]
    class_name: str
    options: Optional[Dict[str, Any]] = Field(None, alias="$options")


class CustomRecordExtractor(BaseModel):
    class Config:
        extra = Extra.allow

    type: Literal["CustomRecordExtractor"]
    class_name: str
    options: Optional[Dict[str, Any]] = Field(None, alias="$options")


class CustomRequester(BaseModel):
    class Config:
        extra = Extra.allow

    type: Literal["CustomRequester"]
    class_name: str
    options: Optional[Dict[str, Any]] = Field(None, alias="$options")


class CustomRequestOptionsProvider(BaseModel):
    class Config:
        extra = Extra.allow

    type: Literal["CustomRequestOptionsProvider"]
    class_name: str
    options: Optional[Dict[str, Any]] = Field(None, alias="$options")


class CustomRetriever(BaseModel):
    class Config:
        extra = Extra.allow

    type: Literal["CustomRetriever"]
    class_name: str
    options: Optional[Dict[str, Any]] = Field(None, alias="$options")


class CustomStreamSlicer(BaseModel):
    class Config:
        extra = Extra.allow

    type: Literal["CustomStreamSlicer"]
    class_name: str
    options: Optional[Dict[str, Any]] = Field(None, alias="$options")


class CustomTransformation(BaseModel):
    class Config:
        extra = Extra.allow

    type: Literal["CustomTransformation"]
    class_name: str
    options: Optional[Dict[str, Any]] = Field(None, alias="$options")


class OAuthAuthenticator(BaseModel):
    type: Literal["OAuthAuthenticator"]
    client_id: str
    client_secret: str
    refresh_token: str
    token_refresh_endpoint: str
    access_token_name: Optional[str] = "access_token"
    expires_in_name: Optional[str] = "expires_in"
    grant_type: Optional[str] = "refresh_token"
    refresh_request_body: Optional[Dict[str, Any]] = None
    scopes: Optional[List[str]] = None
    token_expiry_date: Optional[str] = None
    token_expiry_date_format: Optional[str] = Field(
        None,
        description="The format of the datetime; provide it if expires_in is returned in datetime instead of seconds",
    )
    options: Optional[Dict[str, Any]] = Field(None, alias="$options")


class ExponentialBackoffStrategy(BaseModel):
    type: Literal["ExponentialBackoffStrategy"]
    factor: Optional[Union[float, str]] = 5
    options: Optional[Dict[str, Any]] = Field(None, alias="$options")


class HttpMethodEnum(Enum):
    GET = "GET"
    POST = "POST"


class Action(Enum):
    SUCCESS = "SUCCESS"
    FAIL = "FAIL"
    RETRY = "RETRY"
    IGNORE = "IGNORE"


class HttpResponseFilter(BaseModel):
    type: Literal["HttpResponseFilter"]
    action: Action
    error_message: Optional[str] = None
    error_message_contains: Optional[str] = None
    http_codes: Optional[List[int]] = None
    predicate: Optional[str] = None
    options: Optional[Dict[str, Any]] = Field(None, alias="$options")


class InlineSchemaLoader(BaseModel):
    type: Literal["InlineSchemaLoader"]
    schema_: Optional[Dict[str, Any]] = Field(None, alias="schema")


class InterpolatedRequestOptionsProvider(BaseModel):
    type: Literal["InterpolatedRequestOptionsProvider"]
    request_body_data: Optional[Union[str, Dict[str, str]]] = None
    request_body_json: Optional[Union[str, Dict[str, str]]] = None
    request_headers: Optional[Union[str, Dict[str, str]]] = None
    request_parameters: Optional[Union[str, Dict[str, str]]] = None
    options: Optional[Dict[str, Any]] = Field(None, alias="$options")


class Type(Enum):
    JsonFileSchemaLoader = "JsonFileSchemaLoader"
    JsonSchema = "JsonSchema"


class JsonFileSchemaLoader(BaseModel):
    type: Type
    file_path: Optional[str] = None
    options: Optional[Dict[str, Any]] = Field(None, alias="$options")


class JsonDecoder(BaseModel):
    type: Literal["JsonDecoder"]


class MinMaxDatetime(BaseModel):
    type: Literal["MinMaxDatetime"]
    datetime: str
    datetime_format: Optional[str] = ""
    max_datetime: Optional[str] = None
    min_datetime: Optional[str] = None
    options: Optional[Dict[str, Any]] = Field(None, alias="$options")


class NoAuth(BaseModel):
    type: Literal["NoAuth"]
    options: Optional[Dict[str, Any]] = Field(None, alias="$options")


class NoPagination(BaseModel):
    type: Literal["NoPagination"]


class OffsetIncrement(BaseModel):
    type: Literal["OffsetIncrement"]
    page_size: Union[int, str] = Field(..., description="The number of records to request")
    options: Optional[Dict[str, Any]] = Field(None, alias="$options")


class PageIncrement(BaseModel):
    type: Literal["PageIncrement"]
    page_size: int = Field(..., description="The number of records to request")
    start_from_page: Optional[int] = 0
    options: Optional[Dict[str, Any]] = Field(None, alias="$options")


class PrimaryKey(BaseModel):
    __root__: Union[str, List[str], List[List[str]]] = Field(..., description="The stream field to be used to distinguish unique rows")


class RecordFilter(BaseModel):
    type: Literal["RecordFilter"]
    condition: Optional[str] = Field(
        "",
        description="The predicate to filter a record. Records will be removed if evaluated to False",
    )
    options: Optional[Dict[str, Any]] = Field(None, alias="$options")


class RemoveFields(BaseModel):
    type: Literal["RemoveFields"]
    field_pointers: List[List[str]]


class InjectInto(Enum):
    request_parameter = "request_parameter"
    header = "header"
    path = "path"
    body_data = "body_data"
    body_json = "body_json"


class RequestOption(BaseModel):
    type: Literal["RequestOption"]
    inject_into: InjectInto
    field_name: Optional[str] = None


class Schemas(BaseModel):
    pass

    class Config:
        extra = Extra.allow


class SessionTokenAuthenticator(BaseModel):
    type: Literal["SessionTokenAuthenticator"]
    api_url: str
    header: str
    login_url: str
    session_token: str
    session_token_response_key: str
    username: str
    validate_session_url: str
    password: Optional[str] = ""
    options: Optional[Dict[str, Any]] = Field(None, alias="$options")


class SingleSlice(BaseModel):
    type: Literal["SingleSlice"]
    options: Optional[Dict[str, Any]] = Field(None, alias="$options")


class Spec(BaseModel):
    type: Literal["Spec"]
    connection_specification: Dict[str, Any]
    documentation_url: Optional[str] = None


class WaitTimeFromHeader(BaseModel):
    type: Literal["WaitTimeFromHeader"]
    header: str
    regex: Optional[str] = None
    options: Optional[Dict[str, Any]] = Field(None, alias="$options")


class WaitUntilTimeFromHeader(BaseModel):
    type: Literal["WaitUntilTimeFromHeader"]
    header: str
    min_wait: Optional[Union[float, str]] = None
    regex: Optional[str] = None
    options: Optional[Dict[str, Any]] = Field(None, alias="$options")


class CursorPagination(BaseModel):
    type: Literal["CursorPagination"]
    cursor_value: str
    page_size: Optional[int] = None
    stop_condition: Optional[str] = None
    decoder: Optional[JsonDecoder] = None
    options: Optional[Dict[str, Any]] = Field(None, alias="$options")


class DatetimeStreamSlicer(BaseModel):
    type: Literal["DatetimeStreamSlicer"]
    cursor_field: str
    datetime_format: str
    cursor_granularity: str
    end_datetime: Union[str, MinMaxDatetime]
    start_datetime: Union[str, MinMaxDatetime]
    step: str
    end_time_option: Optional[RequestOption] = None
    lookback_window: Optional[str] = None
    start_time_option: Optional[RequestOption] = None
    stream_state_field_end: Optional[str] = None
    stream_state_field_start: Optional[str] = None
    options: Optional[Dict[str, Any]] = Field(None, alias="$options")


class DefaultErrorHandler(BaseModel):
    type: Literal["DefaultErrorHandler"]
    backoff_strategies: Optional[
        List[
            Union[
                ConstantBackoffStrategy,
                CustomBackoffStrategy,
                ExponentialBackoffStrategy,
                WaitTimeFromHeader,
                WaitUntilTimeFromHeader,
            ]
        ]
    ] = None
    max_retries: Optional[int] = 5
    response_filters: Optional[List[HttpResponseFilter]] = None
    options: Optional[Dict[str, Any]] = Field(None, alias="$options")


class DefaultPaginator(BaseModel):
    type: Literal["DefaultPaginator"]
    pagination_strategy: Union[CursorPagination, CustomPaginationStrategy, OffsetIncrement, PageIncrement]
    url_base: str
    decoder: Optional[JsonDecoder] = None
    page_size_option: Optional[RequestOption] = None
    page_token_option: Optional[RequestOption] = None
    options: Optional[Dict[str, Any]] = Field(None, alias="$options")


class DpathExtractor(BaseModel):
    type: Literal["DpathExtractor"]
    field_pointer: List[str]
    decoder: Optional[JsonDecoder] = None
    options: Optional[Dict[str, Any]] = Field(None, alias="$options")


class ListStreamSlicer(BaseModel):
    type: Literal["ListStreamSlicer"]
    cursor_field: str
    slice_values: Union[str, List[str]]
    request_option: Optional[RequestOption] = None
    options: Optional[Dict[str, Any]] = Field(None, alias="$options")


class RecordSelector(BaseModel):
    type: Literal["RecordSelector"]
    extractor: Union[CustomRecordExtractor, DpathExtractor]
    record_filter: Optional[RecordFilter] = None
    options: Optional[Dict[str, Any]] = Field(None, alias="$options")


class CompositeErrorHandler(BaseModel):
    type: Literal["CompositeErrorHandler"]
    error_handlers: List[Union[CompositeErrorHandler, DefaultErrorHandler]]
    options: Optional[Dict[str, Any]] = Field(None, alias="$options")


class HttpRequester(BaseModel):
    type: Literal["HttpRequester"]
    name: str
    path: str
    url_base: str
    authenticator: Optional[
        Union[
            ApiKeyAuthenticator,
            BasicHttpAuthenticator,
            BearerAuthenticator,
            CustomAuthenticator,
            OAuthAuthenticator,
            NoAuth,
            SessionTokenAuthenticator,
        ]
    ] = None
    error_handler: Optional[Union[DefaultErrorHandler, CustomErrorHandler, CompositeErrorHandler]] = None
    http_method: Optional[Union[str, HttpMethodEnum]] = "GET"
    request_options_provider: Optional[Union[CustomRequestOptionsProvider, InterpolatedRequestOptionsProvider]] = None
    options: Optional[Dict[str, Any]] = Field(None, alias="$options")


class DeclarativeSource(BaseModel):
    class Config:
        extra = Extra.forbid

    type: Literal["DeclarativeSource"]
    check: CheckStream
    streams: List[DeclarativeStream]
    version: str
    schemas: Optional[Schemas] = None
    definitions: Optional[Dict[str, Any]] = None
    spec: Optional[Spec] = None


class CartesianProductStreamSlicer(BaseModel):
    type: Literal["CartesianProductStreamSlicer"]
    stream_slicers: List[
        Union[
            CustomStreamSlicer,
            DatetimeStreamSlicer,
            ListStreamSlicer,
            SingleSlice,
            SubstreamSlicer,
        ]
    ]
    options: Optional[Dict[str, Any]] = Field(None, alias="$options")


class DeclarativeStream(BaseModel):
    class Config:
        extra = Extra.allow

    type: Literal["DeclarativeStream"]
    retriever: Union[CustomRetriever, SimpleRetriever]
    checkpoint_interval: Optional[int] = None
    name: Optional[str] = ""
    primary_key: Optional[Union[str, List[str], List[List[str]]]] = ""
    schema_loader: Optional[Union[InlineSchemaLoader, JsonFileSchemaLoader]] = None
    stream_cursor_field: Optional[Union[str, List[str]]] = None
    transformations: Optional[List[Union[AddFields, CustomTransformation, RemoveFields]]] = None
    options: Optional[Dict[str, Any]] = Field(None, alias="$options")


class ParentStreamConfig(BaseModel):
    type: Literal["ParentStreamConfig"]
    parent_key: str
    stream: DeclarativeStream
    stream_slice_field: str
    request_option: Optional[RequestOption] = None
    options: Optional[Dict[str, Any]] = Field(None, alias="$options")


class SimpleRetriever(BaseModel):
    type: Literal["SimpleRetriever"]
    record_selector: RecordSelector
    requester: Union[CustomRequester, HttpRequester]
    name: Optional[str] = ""
    paginator: Optional[Union[DefaultPaginator, NoPagination]] = None
    primary_key: Optional[PrimaryKey] = None
    stream_slicer: Optional[
        Union[
            CartesianProductStreamSlicer,
            CustomStreamSlicer,
            DatetimeStreamSlicer,
            ListStreamSlicer,
            SingleSlice,
            SubstreamSlicer,
        ]
    ] = None
    options: Optional[Dict[str, Any]] = Field(None, alias="$options")


class SubstreamSlicer(BaseModel):
    type: Literal["SubstreamSlicer"]
    parent_stream_configs: List[ParentStreamConfig]
    options: Optional[Dict[str, Any]] = Field(None, alias="$options")


CompositeErrorHandler.update_forward_refs()
DeclarativeSource.update_forward_refs()
CartesianProductStreamSlicer.update_forward_refs()
DeclarativeStream.update_forward_refs()
SimpleRetriever.update_forward_refs()
