# coding: utf-8

from __future__ import annotations
from datetime import date, datetime  # noqa: F401

import re  # noqa: F401
from typing import Any, Dict, List, Optional  # noqa: F401

from pydantic import AnyUrl, BaseModel, EmailStr, validator  # noqa: F401


class ResolveManifest(BaseModel):
    """NOTE: This class is auto generated by OpenAPI Generator (https://openapi-generator.tech).

    Do not edit the class manually.

    ResolveManifest - a model defined in OpenAPI

        manifest: The manifest of this ResolveManifest.
    """

    manifest: Dict[str, Any]

ResolveManifest.update_forward_refs()
