# coding: utf-8

from __future__ import annotations
from datetime import date, datetime  # noqa: F401

import re  # noqa: F401
from typing import Any, Dict, List, Optional  # noqa: F401

from pydantic import AnyUrl, BaseModel, EmailStr, Field, validator  # noqa: F401


class OffsetIncrementAllOf(BaseModel):
    """NOTE: This class is auto generated by OpenAPI Generator (https://openapi-generator.tech).

    Do not edit the class manually.

    OffsetIncrementAllOf - a model defined in OpenAPI

        page_size: The page_size of this OffsetIncrementAllOf.
    """

    page_size: int = Field(alias="page_size")

OffsetIncrementAllOf.update_forward_refs()