# generated by datamodel-codegen:
#   filename:  SuggestedStreams.yaml

from __future__ import annotations

from typing import List, Optional

from pydantic import BaseModel, Extra, Field


class SuggestedStreams(BaseModel):
    class Config:
        extra = Extra.allow

    streams: Optional[List[str]] = Field(
        None,
        description='An array of streams that this connector suggests the average user will want.  SuggestedStreams not being present for the source means that all streams are suggested.  An empty list here means that no streams are suggested.',
    )
