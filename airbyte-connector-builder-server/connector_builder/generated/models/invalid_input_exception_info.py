# coding: utf-8

from __future__ import annotations

import re  # noqa: F401
from datetime import date, datetime  # noqa: F401
from typing import Any, Dict, List, Optional  # noqa: F401

from pydantic import AnyUrl, BaseModel, EmailStr, validator  # noqa: F401

from connector_builder.generated.models.invalid_input_property import (
    InvalidInputProperty,
)


class InvalidInputExceptionInfo(BaseModel):
    """NOTE: This class is auto generated by OpenAPI Generator (https://openapi-generator.tech).

    Do not edit the class manually.

    InvalidInputExceptionInfo - a model defined in OpenAPI

        message: The message of this InvalidInputExceptionInfo.
        exception_class_name: The exception_class_name of this InvalidInputExceptionInfo [Optional].
        exception_stack: The exception_stack of this InvalidInputExceptionInfo [Optional].
        validation_errors: The validation_errors of this InvalidInputExceptionInfo.
    """

    message: str
    exception_class_name: Optional[str] = None
    exception_stack: Optional[List[str]] = None
    validation_errors: List[InvalidInputProperty]


InvalidInputExceptionInfo.update_forward_refs()
