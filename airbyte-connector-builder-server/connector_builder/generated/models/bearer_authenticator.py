# coding: utf-8

from __future__ import annotations
from datetime import date, datetime  # noqa: F401

import re  # noqa: F401
from typing import Any, Dict, List, Optional  # noqa: F401

from pydantic import AnyUrl, BaseModel, EmailStr, Field, validator  # noqa: F401
from connector_builder.generated.models.any_of_interpolated_stringstring import AnyOfInterpolatedStringstring
from connector_builder.generated.models.bearer_authenticator_all_of import BearerAuthenticatorAllOf


class BearerAuthenticator(BaseModel):
    """NOTE: This class is auto generated by OpenAPI Generator (https://openapi-generator.tech).

    Do not edit the class manually.

    BearerAuthenticator - a model defined in OpenAPI

        api_token: The api_token of this BearerAuthenticator.
        config: The config of this BearerAuthenticator.
    """

    api_token: AnyOfInterpolatedStringstring = Field(alias="api_token")
    config: Dict[str, Any] = Field(alias="config")

BearerAuthenticator.update_forward_refs()