# generated by datamodel-codegen:
#   filename:  JobType.yaml

from __future__ import annotations

from pydantic import BaseModel, Field
from typing_extensions import Literal


class JobType(BaseModel):
    __root__: Literal["get_spec", "check_connection", "discover_schema", "sync", "reset_connection", "connection_updater", "replicate"] = (
        Field(..., description="enum that describes the different types of jobs that the platform runs.", title="JobType")
    )
