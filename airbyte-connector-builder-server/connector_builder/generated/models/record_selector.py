# coding: utf-8

from __future__ import annotations
from datetime import date, datetime  # noqa: F401

import re  # noqa: F401
from typing import Any, Dict, List, Optional  # noqa: F401

from pydantic import AnyUrl, BaseModel, EmailStr, Field, validator  # noqa: F401
from connector_builder.generated.models.dpath_extractor import DpathExtractor
from connector_builder.generated.models.record_filter import RecordFilter
from connector_builder.generated.models.record_selector_all_of import RecordSelectorAllOf


class RecordSelector(BaseModel):
    """NOTE: This class is auto generated by OpenAPI Generator (https://openapi-generator.tech).

    Do not edit the class manually.

    RecordSelector - a model defined in OpenAPI

        extractor: The extractor of this RecordSelector.
        record_filter: The record_filter of this RecordSelector [Optional].
    """

    extractor: DpathExtractor = Field(alias="extractor")
    record_filter: Optional[RecordFilter] = Field(alias="record_filter", default=None)

RecordSelector.update_forward_refs()