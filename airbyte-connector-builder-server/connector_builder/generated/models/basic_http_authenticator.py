# coding: utf-8

from __future__ import annotations
from datetime import date, datetime  # noqa: F401

import re  # noqa: F401
from typing import Any, Dict, List, Optional  # noqa: F401

from pydantic import AnyUrl, BaseModel, EmailStr, Field, validator  # noqa: F401
from connector_builder.generated.models.any_of_interpolated_stringstring import AnyOfInterpolatedStringstring
from connector_builder.generated.models.basic_http_authenticator_all_of import BasicHttpAuthenticatorAllOf


class BasicHttpAuthenticator(BaseModel):
    """NOTE: This class is auto generated by OpenAPI Generator (https://openapi-generator.tech).

    Do not edit the class manually.

    BasicHttpAuthenticator - a model defined in OpenAPI

        username: The username of this BasicHttpAuthenticator.
        config: The config of this BasicHttpAuthenticator.
        password: The password of this BasicHttpAuthenticator [Optional].
    """

    username: AnyOfInterpolatedStringstring = Field(alias="username")
    config: Dict[str, Any] = Field(alias="config")
    password: Optional[AnyOfInterpolatedStringstring] = Field(alias="password", default=None)

BasicHttpAuthenticator.update_forward_refs()