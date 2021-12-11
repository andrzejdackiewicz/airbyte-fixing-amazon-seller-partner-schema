#
# Copyright (c) 2021 Airbyte, Inc., all rights reserved.
#

from typing import Any, Dict

from pydantic import BaseModel

from airbyte_cdk.sources.utils.schema_helpers import expand_refs, rename_key


class BaseConfig(BaseModel):
    """Base class for connector spec, adds the following behaviour:

    - resolve $ref and replace it with definition
    - replace all occurrences of anyOf with oneOf
    - drop description
    """

    @classmethod
    def schema(cls, **kwargs) -> Dict[str, Any]:
        """We're overriding the schema classmethod to enable some post-processing"""
        schema = super().schema(**kwargs)
        rename_key(schema, old_key="anyOf", new_key="oneOf")  # UI supports only oneOf
        expand_refs(schema)
        schema.pop("description", None)  # description added from the docstring
        return schema
