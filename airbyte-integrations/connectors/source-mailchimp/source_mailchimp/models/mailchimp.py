#
# Copyright (c) 2020 Airbyte, Inc., all rights reserved.
#


from pydantic import BaseModel, Extra, Field


class HealthCheckError(BaseModel):
    type: str
    title: str
    status: int
    detail: str
    instance: str
