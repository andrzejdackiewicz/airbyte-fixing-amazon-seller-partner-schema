# coding: utf-8

from __future__ import annotations
from datetime import date, datetime  # noqa: F401

import re  # noqa: F401
from typing import Any, Dict, List, Optional  # noqa: F401

from pydantic import AnyUrl, BaseModel, EmailStr, validator  # noqa: F401
from connector_builder.generated.models.any_ofmapstring import AnyOfmapstring


class InterpolatedRequestOptionsProviderAllOf(BaseModel):
    """NOTE: This class is auto generated by OpenAPI Generator (https://openapi-generator.tech).

    Do not edit the class manually.

    InterpolatedRequestOptionsProviderAllOf - a model defined in OpenAPI

        config: The config of this InterpolatedRequestOptionsProviderAllOf [Optional].
        request_parameters: The request_parameters of this InterpolatedRequestOptionsProviderAllOf [Optional].
        request_headers: The request_headers of this InterpolatedRequestOptionsProviderAllOf [Optional].
        request_body_data: The request_body_data of this InterpolatedRequestOptionsProviderAllOf [Optional].
        request_body_json: The request_body_json of this InterpolatedRequestOptionsProviderAllOf [Optional].
    """

    config: Optional[Dict[str, Any]] = None
    request_parameters: Optional[AnyOfmapstring] = None
    request_headers: Optional[AnyOfmapstring] = None
    request_body_data: Optional[AnyOfmapstring] = None
    request_body_json: Optional[AnyOfmapstring] = None

InterpolatedRequestOptionsProviderAllOf.update_forward_refs()
