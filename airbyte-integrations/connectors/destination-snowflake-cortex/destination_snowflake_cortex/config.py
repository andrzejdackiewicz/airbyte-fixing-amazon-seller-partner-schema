#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#


from airbyte_cdk.destinations.vector_db_based.config import VectorDBConfigModel
from pydantic import BaseModel, Field


# to-do - https://github.com/airbytehq/airbyte/issues/38007 - add Snowflake supported models to config
class SnowflakeCortexIndexingModel(BaseModel):
    account: str = Field(
        ...,
        title="Account",
        airbyte_secret=True,
        description="Enter the account name you want to use to access the database.",
        examples=["xxx.us-east-2.aws"],
    )
    username: str = Field(
        ...,
        title="Username",
        airbyte_secret=True,
        description="Enter the name of the user you want to use to access the database",
        examples=["AIRBYTE_USER"],
    )
    password: str = Field(
        ..., title="Password", airbyte_secret=True, description="Enter the password associated with the user you entered above"
    )
    database: str = Field(
        ...,
        title="Database",
        airbyte_secret=True,
        description="Enter the name of the database that you want to sync data into",
        examples=["AIRBYTE_DATABASE"],
    )
    warehouse: str = Field(
        ...,
        title="Warehouse",
        airbyte_secret=True,
        description="Enter the name of the warehouse that you want to sync data into",
        examples=["AIRBYTE_WAREHOUSE"],
    )
    role: str = Field(
        ...,
        title="Role",
        airbyte_secret=True,
        description="Enter the name of the role that you want to sync data into",
        examples=["AIRBYTE_ROLE", "ACCOUNTADMIN"],
    )

    class Config:
        title = "Indexing"
        schema_extra = {
            "description": "Snowflake can be used to store vector data and retrieve embeddings.",
            "group": "indexing",
        }


class ConfigModel(VectorDBConfigModel):
    indexing: SnowflakeCortexIndexingModel
