#
# Copyright (c) 2021 Airbyte, Inc., all rights reserved.
#


from pydantic import BaseModel


class HealthCheckError(BaseModel):
    type: str
    title: str
    status: int
    detail: str
    instance: str
