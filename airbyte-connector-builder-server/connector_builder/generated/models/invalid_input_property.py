# coding: utf-8

from __future__ import annotations

import re  # noqa: F401
from datetime import date, datetime  # noqa: F401
from typing import Any, Dict, List, Optional  # noqa: F401

from pydantic import AnyUrl, BaseModel, EmailStr, validator  # noqa: F401


class InvalidInputProperty(BaseModel):
    """NOTE: This class is auto generated by OpenAPI Generator (https://openapi-generator.tech).

    Do not edit the class manually.

    InvalidInputProperty - a model defined in OpenAPI

        property_path: The property_path of this InvalidInputProperty.
        invalid_value: The invalid_value of this InvalidInputProperty [Optional].
        message: The message of this InvalidInputProperty [Optional].
    """

    property_path: str
    invalid_value: Optional[str] = None
    message: Optional[str] = None


InvalidInputProperty.update_forward_refs()
