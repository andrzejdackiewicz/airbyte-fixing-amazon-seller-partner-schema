#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

from typing import Literal, Optional, Union

import dpath.util
from airbyte_cdk.destinations.vector_db_based.config import (
    CohereEmbeddingConfigModel,
    FakeEmbeddingConfigModel,
    FromFieldEmbeddingConfigModel,
    OpenAIEmbeddingConfigModel,
    ProcessingConfigModel,
)
from airbyte_cdk.utils.spec_schema_transformations import resolve_refs
from pydantic import BaseModel, Field


class UsernamePasswordAuth(BaseModel):
    mode: Literal["username_password"] = Field("username_password", const=True)
    username: str = Field(..., title="Username", description="Username for the Milvus instance", order=1)
    password: str = Field(..., title="Password", description="Password for the Milvus instance", airbyte_secret=True, order=2)

    class Config:
        title = "Username/Password"
        schema_extra = {"description": "Authenticate using username and password (suitable for self-managed Weaviate clusters)"}


class NoAuth(BaseModel):
    mode: Literal["no_auth"] = Field("no_auth", const=True)

    class Config:
        title = "No auth"
        schema_extra = {
            "description": "Do not authenticate (suitable for locally running test clusters, do not use for clusters with public IP addresses)"
        }


class TokenAuth(BaseModel):
    mode: Literal["token"] = Field("token", const=True)
    token: str = Field(..., title="API Token", description="API Token for the Weaviate instance", airbyte_secret=True)

    class Config:
        title = "API Token"
        schema_extra = {"description": "Authenticate using an API token (suitable for Weaviate Cloud)"}


class WeaviateIndexingConfigModel(BaseModel):
    host: str = Field(
        ...,
        title="Public Endpoint",
        order=1,
        description="The public endpoint of the Milvus instance. ",
        examples=["https://my-cluster.weaviate.network", "http://host.docker.internal:8080"],
    )
    class_name: str = Field(..., title="Class name", description="The class to load data into", order=3)
    auth: Union[TokenAuth, UsernamePasswordAuth, NoAuth] = Field(
        ..., title="Authentication", description="Authentication method", discriminator="mode", type="object", order=2
    )
    batch_size: int = Field(title="Batch Size", description="The number of records to send to Weaviate in each batch", default=128)
    text_field: str = Field(title="Text Field", description="The field in the object that contains the embedded text", default="text")

    class Config:
        title = "Indexing"
        schema_extra = {
            "group": "indexing",
            "description": "Indexing configuration",
        }


class NoEmbeddingConfigModel(BaseModel):
    mode: Literal["no_embedding"] = Field("no_embedding", const=True)

    class Config:
        title = "Weaviate vectorizer"
        schema_extra = {
            "description": "Do not calculate embeddings (suitable for classes with configured vectorizers to calculate embeddings within Weaviate)"
        }


class ConfigModel(BaseModel):
    processing: ProcessingConfigModel
    embedding: Union[
        OpenAIEmbeddingConfigModel,
        CohereEmbeddingConfigModel,
        FromFieldEmbeddingConfigModel,
        NoEmbeddingConfigModel,
        FakeEmbeddingConfigModel,
    ] = Field(..., title="Embedding", description="Embedding configuration", discriminator="mode", group="embedding", type="object")
    indexing: WeaviateIndexingConfigModel

    class Config:
        title = "Milvus Destination Config"
        schema_extra = {
            "groups": [
                {"id": "processing", "title": "Processing"},
                {"id": "embedding", "title": "Embedding"},
                {"id": "indexing", "title": "Indexing"},
            ]
        }

    @staticmethod
    def remove_discriminator(schema: dict) -> None:
        """pydantic adds "discriminator" to the schema for oneOfs, which is not treated right by the platform as we inline all references"""
        dpath.util.delete(schema, "properties/*/discriminator")
        dpath.util.delete(schema, "properties/**/discriminator")

    @classmethod
    def schema(cls):
        """we're overriding the schema classmethod to enable some post-processing"""
        schema = super().schema()
        schema = resolve_refs(schema)
        cls.remove_discriminator(schema)
        return schema
