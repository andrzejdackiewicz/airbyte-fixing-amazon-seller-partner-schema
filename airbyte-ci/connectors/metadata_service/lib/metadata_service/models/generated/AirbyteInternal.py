#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

# generated by datamodel-codegen:
#   filename:  AirbyteInternal.yaml

from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, Extra
from typing_extensions import Literal


class AirbyteInternal(BaseModel):
    class Config:
        extra = Extra.allow

    sl: Optional[Literal[100, 200, 300]] = None
    ql: Optional[Literal[100, 200, 300, 400, 500, 600]] = None
