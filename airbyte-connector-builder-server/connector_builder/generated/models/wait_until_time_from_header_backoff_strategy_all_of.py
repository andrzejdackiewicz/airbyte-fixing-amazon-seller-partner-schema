# coding: utf-8

from __future__ import annotations
from datetime import date, datetime  # noqa: F401

import re  # noqa: F401
from typing import Any, Dict, List, Optional  # noqa: F401

from pydantic import AnyUrl, BaseModel, EmailStr, validator  # noqa: F401
from connector_builder.generated.models.any_of_interpolated_stringnumberstring import AnyOfInterpolatedStringnumberstring
from connector_builder.generated.models.any_of_interpolated_stringstring import AnyOfInterpolatedStringstring


class WaitUntilTimeFromHeaderBackoffStrategyAllOf(BaseModel):
    """NOTE: This class is auto generated by OpenAPI Generator (https://openapi-generator.tech).

    Do not edit the class manually.

    WaitUntilTimeFromHeaderBackoffStrategyAllOf - a model defined in OpenAPI

        header: The header of this WaitUntilTimeFromHeaderBackoffStrategyAllOf.
        config: The config of this WaitUntilTimeFromHeaderBackoffStrategyAllOf.
        min_wait: The min_wait of this WaitUntilTimeFromHeaderBackoffStrategyAllOf [Optional].
        regex: The regex of this WaitUntilTimeFromHeaderBackoffStrategyAllOf [Optional].
    """

    header: AnyOfInterpolatedStringstring
    config: Dict[str, Any]
    min_wait: Optional[AnyOfInterpolatedStringnumberstring] = None
    regex: Optional[AnyOfInterpolatedStringstring] = None

WaitUntilTimeFromHeaderBackoffStrategyAllOf.update_forward_refs()
