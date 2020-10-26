# generated by datamodel-codegen:
#   filename:  spreadsheet_values.yaml

from __future__ import annotations

from typing import List, Optional

from pydantic import BaseModel, Extra


class Values(BaseModel):
    __root__: List[str]


class ValueRange(BaseModel):
    class Config:
        extra = Extra.allow

    values: Optional[List[Values]] = None


class SpreadsheetValues(BaseModel):
    class Config:
        extra = Extra.allow

    spreadsheetId: str
    valueRanges: List[ValueRange]
