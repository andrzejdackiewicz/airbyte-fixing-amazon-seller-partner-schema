#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

from collections import defaultdict
from typing import Any, Dict, List, Mapping, Optional

from airbyte_cdk.models import AirbyteRecordMessage
from genson import SchemaBuilder, SchemaNode
from genson.schema.strategies.object import Object
from genson.schema.strategies.scalar import Number


class NoRequiredObj(Object):
    """
    This class has Object behaviour, but it does not generate "required[]" fields
    every time it parses object. So we dont add unnecessary extra field.
    """

    def to_schema(self) -> Mapping[str, Any]:
        schema: Dict[str, Any] = super(NoRequiredObj, self).to_schema()
        schema.pop("required", None)
        return schema


class IntegerToNumber(Number):
    """
    This class has the regular Number behaviour, but it will never emit an integer type.
    """

    def __init__(self, node_class: SchemaNode):
        super().__init__(node_class)
        self._type = "number"


class NoRequiredSchemaBuilder(SchemaBuilder):
    EXTRA_STRATEGIES = (NoRequiredObj, IntegerToNumber)


# This type is inferred from the genson lib, but there is no alias provided for it - creating it here for type safety
InferredSchema = Dict[str, Any]


class SchemaValidationException(Exception):
    @classmethod
    def merge_exceptions(cls, exceptions: List["SchemaValidationException"]) -> "SchemaValidationException":
        # We assume the schema is the same for all SchemaValidationException
        return SchemaValidationException(exceptions[0].schema, [x for exception in exceptions for x in exception.validation_errors])

    def __init__(self, schema: InferredSchema, validation_errors: List[Exception]):
        self._schema = schema
        self._validation_errors = validation_errors

    @property
    def schema(self) -> InferredSchema:
        return self._schema

    @property
    def validation_errors(self) -> List[str]:
        return list(map(lambda error: str(error), self._validation_errors))


class SchemaInferrer:
    """
    This class is used to infer a JSON schema which fits all the records passed into it
    throughout its lifecycle via the accumulate method.

    Instances of this class are stateful, meaning they build their inferred schemas
    from every record passed into the accumulate method.

    """

    stream_to_builder: Dict[str, SchemaBuilder]

    def __init__(self, pk: List[List[str]] = None, cursor_field: List[List[str]] = None) -> None:
        self.stream_to_builder = defaultdict(NoRequiredSchemaBuilder)
        self._pk = pk
        self._cursor_field = cursor_field

    def accumulate(self, record: AirbyteRecordMessage) -> None:
        """Uses the input record to add to the inferred schemas maintained by this object"""
        self.stream_to_builder[record.stream].add_object(record.data)

    def _clean(self, node: InferredSchema) -> InferredSchema:
        """
        Recursively cleans up a produced schema:
        - remove anyOf if one of them is just a null value
        - remove properties of type "null"
        """
        if isinstance(node, dict):
            if "anyOf" in node:
                if len(node["anyOf"]) == 2 and {"type": "null"} in node["anyOf"]:
                    real_type = node["anyOf"][1] if node["anyOf"][0]["type"] == "null" else node["anyOf"][0]
                    node.update(real_type)
                    node["type"] = ["null", node["type"]]
                    node.pop("anyOf")
            if "properties" in node and isinstance(node["properties"], dict):
                for key, value in list(node["properties"].items()):
                    if isinstance(value, dict) and value.get("type", None) == "null":
                        node["properties"].pop(key)
                    else:
                        self._clean(value)
            if "items" in node:
                self._clean(node["items"])

            # this check needs to follow the "anyOf" cleaning as it might populate `type`
            if isinstance(node["type"], list):
                if "null" not in node["type"]:
                    node["type"] = ["null"] + node["type"]
            else:
                node["type"] = ["null", node["type"]]
        return node

    def _add_required_properties(self, node: InferredSchema) -> InferredSchema:
        # Removing nullable for the root as when we call `_clean`, we make everything nullable
        node["type"] = "object"

        exceptions = []
        for field in [x for x in [self._pk, self._cursor_field] if x]:
            try:
                self._add_fields_as_required(node, field)
            except SchemaValidationException as exception:
                exceptions.append(exception)

        if exceptions:
            raise SchemaValidationException.merge_exceptions(exceptions)

        return node

    def _add_fields_as_required(self, node: InferredSchema, composite_keys: List[List[str]]) -> None:
        errors: List[Exception] = []

        for path in composite_keys:
            try:
                self._add_field_as_required(node, path)
            except ValueError as exception:
                errors.append(exception)

        if errors:
            raise SchemaValidationException(node, errors)

    def _remove_null_from_type(self, node: InferredSchema) -> None:
        if isinstance(node["type"], list):
            if "null" in node["type"]:
                node["type"].remove("null")
            if len(node["type"]) == 1:
                node["type"] = node["type"][0]

    def _add_field_as_required(self, node: InferredSchema, path: List[str], traveled_path: List[str] = None) -> None:
        if self._is_leaf(path):
            self._remove_null_from_type(node)
            return

        if not traveled_path:
            traveled_path = []

        if "properties" not in node:
            # This validation is only relevant when `traveled_path` is empty oskdfoskfo
            raise ValueError(
                f"Path {traveled_path} does not refer to an object but is `{node}` and hence {path} can't be marked as required."
            )

        next_node = path[0]
        if next_node not in node["properties"]:
            raise ValueError(f"Path {traveled_path} does not have field `{next_node}` in the schema and hence can't be marked as required.")

        if "type" not in node:
            # We do not expect this case to happen but we added a specific error message just in case
            raise ValueError(
                f"Unknown schema error: {traveled_path} is expected to have a type but did not. Schema inferrence is probably broken"
            )

        if node["type"] not in ["object", ["null", "object"]]:
            raise ValueError(f"Path {traveled_path} is expected to be an object but was of type `{node['properties'][next_node]['type']}`")

        if "required" not in node or not node["required"]:
            node["required"] = [next_node]
        elif next_node not in node["required"]:
            node["required"].append(next_node)

        traveled_path.append(next_node)
        self._add_field_as_required(node["properties"][next_node], path[1:], traveled_path)

    def _is_leaf(self, path: List[str]) -> bool:
        return len(path) == 0

    def get_stream_schema(self, stream_name: str) -> Optional[InferredSchema]:
        """
        Returns the inferred JSON schema for the specified stream. Might be `None` if there were no records for the given stream name.
        """
        return (
            self._add_required_properties(self._clean(self.stream_to_builder[stream_name].to_schema()))
            if stream_name in self.stream_to_builder
            else None
        )
