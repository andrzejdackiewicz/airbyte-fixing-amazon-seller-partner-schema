# coding: utf-8

from __future__ import annotations
from datetime import date, datetime  # noqa: F401

import re  # noqa: F401
from typing import Any, Dict, List, Optional  # noqa: F401

from pydantic import AnyUrl, BaseModel, EmailStr, Field, validator  # noqa: F401
from connector_builder.generated.models.any_of_cursor_pagination_strategy_offset_increment_page_increment import AnyOfCursorPaginationStrategyOffsetIncrementPageIncrement
from connector_builder.generated.models.any_of_interpolated_stringstring import AnyOfInterpolatedStringstring
from connector_builder.generated.models.default_paginator_all_of import DefaultPaginatorAllOf
from connector_builder.generated.models.json_decoder import JsonDecoder
from connector_builder.generated.models.paginator import Paginator
from connector_builder.generated.models.request_option import RequestOption


class DefaultPaginator(BaseModel):
    """NOTE: This class is auto generated by OpenAPI Generator (https://openapi-generator.tech).

    Do not edit the class manually.

    DefaultPaginator - a model defined in OpenAPI

        pagination_strategy: The pagination_strategy of this DefaultPaginator.
        config: The config of this DefaultPaginator.
        url_base: The url_base of this DefaultPaginator.
        decoder: The decoder of this DefaultPaginator [Optional].
        token: The token of this DefaultPaginator [Optional].
        page_size_option: The page_size_option of this DefaultPaginator [Optional].
        page_token_option: The page_token_option of this DefaultPaginator [Optional].
    """

    pagination_strategy: AnyOfCursorPaginationStrategyOffsetIncrementPageIncrement = Field(alias="pagination_strategy")
    config: Dict[str, Any] = Field(alias="config")
    url_base: AnyOfInterpolatedStringstring = Field(alias="url_base")
    decoder: Optional[JsonDecoder] = Field(alias="decoder", default=None)
    token: Optional[object] = Field(alias="_token", default=None)
    page_size_option: Optional[RequestOption] = Field(alias="page_size_option", default=None)
    page_token_option: Optional[RequestOption] = Field(alias="page_token_option", default=None)

DefaultPaginator.update_forward_refs()