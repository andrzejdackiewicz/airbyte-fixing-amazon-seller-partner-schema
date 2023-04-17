# generated by datamodel-codegen:
#   filename:  ActorDefinitionResourceRequirements.yaml

from __future__ import annotations

from typing import List, Optional

from pydantic import BaseModel, Extra, Field
from typing_extensions import Literal


class ResourceRequirements(BaseModel):
    class Config:
        extra = Extra.forbid

    cpu_request: Optional[str] = None
    cpu_limit: Optional[str] = None
    memory_request: Optional[str] = None
    memory_limit: Optional[str] = None


class JobType(BaseModel):
    __root__: Literal[
        'get_spec',
        'check_connection',
        'discover_schema',
        'sync',
        'reset_connection',
        'connection_updater',
        'replicate',
    ] = Field(
        ...,
        description='enum that describes the different types of jobs that the platform runs.',
        title='JobType',
    )


class JobTypeResourceLimit(BaseModel):
    class Config:
        extra = Extra.forbid

    jobType: JobType
    resourceRequirements: ResourceRequirements


class ActorDefinitionResourceRequirements(BaseModel):
    class Config:
        extra = Extra.forbid

    default: Optional[ResourceRequirements] = Field(
        None,
        description='if set, these are the requirements that should be set for ALL jobs run for this actor definition.',
    )
    jobSpecific: Optional[List[JobTypeResourceLimit]] = None
