# coding: utf-8

from __future__ import annotations
from datetime import date, datetime  # noqa: F401

import re  # noqa: F401
from typing import Any, Dict, List, Optional  # noqa: F401

from pydantic import AnyUrl, BaseModel, EmailStr, validator  # noqa: F401
from connector_builder.generated.models.any_of_cartesian_product_stream_slicer_datetime_stream_slicer_list_stream_slicer_single_slice_substream_slicer import AnyOfCartesianProductStreamSlicerDatetimeStreamSlicerListStreamSlicerSingleSliceSubstreamSlicer
from connector_builder.generated.models.any_of_default_paginator_no_pagination import AnyOfDefaultPaginatorNoPagination
from connector_builder.generated.models.any_of_interpolated_stringstring import AnyOfInterpolatedStringstring
from connector_builder.generated.models.any_ofarrayarraystring import AnyOfarrayarraystring
from connector_builder.generated.models.http_requester import HttpRequester
from connector_builder.generated.models.record_selector import RecordSelector


class SimpleRetrieverAllOf(BaseModel):
    """NOTE: This class is auto generated by OpenAPI Generator (https://openapi-generator.tech).

    Do not edit the class manually.

    SimpleRetrieverAllOf - a model defined in OpenAPI

        requester: The requester of this SimpleRetrieverAllOf.
        record_selector: The record_selector of this SimpleRetrieverAllOf.
        config: The config of this SimpleRetrieverAllOf.
        name: The name of this SimpleRetrieverAllOf [Optional].
        name: The name of this SimpleRetrieverAllOf [Optional].
        primary_key: The primary_key of this SimpleRetrieverAllOf [Optional].
        primary_key: The primary_key of this SimpleRetrieverAllOf [Optional].
        paginator: The paginator of this SimpleRetrieverAllOf [Optional].
        stream_slicer: The stream_slicer of this SimpleRetrieverAllOf [Optional].
    """

    requester: HttpRequester
    record_selector: RecordSelector
    config: Dict[str, Any]
    name: Optional[str] = None
    name: Optional[AnyOfInterpolatedStringstring] = None
    primary_key: Optional[AnyOfarrayarraystring] = None
    primary_key: Optional[str] = None
    paginator: Optional[AnyOfDefaultPaginatorNoPagination] = None
    stream_slicer: Optional[AnyOfCartesianProductStreamSlicerDatetimeStreamSlicerListStreamSlicerSingleSliceSubstreamSlicer] = None

SimpleRetrieverAllOf.update_forward_refs()
