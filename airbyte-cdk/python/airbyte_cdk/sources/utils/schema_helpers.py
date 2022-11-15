#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#


import importlib
import json
import os
import pkgutil
from typing import Any, ClassVar, Dict, List, Mapping, MutableMapping, Optional, Tuple, Union

import jsonref
from airbyte_cdk.models import ConnectorSpecification, FailureType
from airbyte_cdk.utils.traced_exception import AirbyteTracedException
from jsonschema import RefResolver, validate
from jsonschema.exceptions import ValidationError
from pydantic import BaseModel, Field


class JsonFileLoader:
    """
    Custom json file loader to resolve references to resources located in "shared" directory.
    We need this for compatability with existing schemas cause all of them have references
    pointing to shared_schema.json file instead of shared/shared_schema.json
    """

    def __init__(self, uri_base: str, shared: str):
        self.shared = shared
        self.uri_base = uri_base

    def __call__(self, uri: str) -> Dict[str, Any]:
        uri = uri.replace(self.uri_base, f"{self.uri_base}/{self.shared}/")
        return json.load(open(uri))


def resolve_ref_links(obj: Any) -> Union[Dict[str, Any], List[Any]]:
    """
    Scan resolved schema and convert jsonref.JsonRef object to JSON serializable dict.

    :param obj - jsonschema object with ref field resolved.
    :return JSON serializable object with references without external dependencies.
    """
    if isinstance(obj, jsonref.JsonRef):
        obj = resolve_ref_links(obj.__subject__)
        # Omit existing definitions for external resource since
        # we dont need it anymore.
        obj.pop("definitions", None)
        return obj
    elif isinstance(obj, dict):
        return {k: resolve_ref_links(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [resolve_ref_links(item) for item in obj]
    else:
        return obj


def _expand_refs(schema: Any, ref_resolver: Optional[RefResolver] = None) -> None:
    """Internal function to iterate over schema and replace all occurrences of $ref with their definitions. Recursive.

    :param schema: schema that will be patched
    :param ref_resolver: resolver to get definition from $ref, if None pass it will be instantiated
    """
    ref_resolver = ref_resolver or RefResolver.from_schema(schema)

    if isinstance(schema, MutableMapping):
        if "$ref" in schema:
            ref_url = schema.pop("$ref")
            _, definition = ref_resolver.resolve(ref_url)
            _expand_refs(definition, ref_resolver=ref_resolver)  # expand refs in definitions as well
            schema.update(definition)
        else:
            for key, value in schema.items():
                _expand_refs(value, ref_resolver=ref_resolver)
    elif isinstance(schema, List):
        for value in schema:
            _expand_refs(value, ref_resolver=ref_resolver)


def expand_refs(schema: Any) -> None:
    """Iterate over schema and replace all occurrences of $ref with their definitions.

    :param schema: schema that will be patched
    """
    _expand_refs(schema)
    schema.pop("definitions", None)  # remove definitions created by $ref


def rename_key(schema: Any, old_key: str, new_key: str) -> None:
    """Iterate over nested dictionary and replace one key with another. Used to replace anyOf with oneOf. Recursive."

    :param schema: schema that will be patched
    :param old_key: name of the key to replace
    :param new_key: new name of the key
    """
    if not isinstance(schema, MutableMapping):
        return

    for key, value in schema.items():
        rename_key(value, old_key, new_key)
        if old_key in schema:
            schema[new_key] = schema.pop(old_key)


class ResourceSchemaLoader:
    """JSONSchema loader from package resources"""

    def __init__(self, package_name: str):
        self.package_name = package_name

    def get_schema(self, name: str) -> dict:
        """
        This method retrieves a JSON schema from the schemas/ folder.


        The expected file structure is to have all top-level schemas (corresponding to streams) in the "schemas/" folder, with any shared $refs
        living inside the "schemas/shared/" folder. For example:

        schemas/shared/<shared_definition>.json
        schemas/<name>.json # contains a $ref to shared_definition
        schemas/<name2>.json # contains a $ref to shared_definition
        """

        schema_filename = f"schemas/{name}.json"
        raw_file = pkgutil.get_data(self.package_name, schema_filename)
        if not raw_file:
            raise IOError(f"Cannot find file {schema_filename}")
        try:
            raw_schema = json.loads(raw_file)
        except ValueError as err:
            raise RuntimeError(f"Invalid JSON file format for file {schema_filename}") from err

        return self.__resolve_schema_references(raw_schema)

    def __resolve_schema_references(self, raw_schema: dict) -> dict:
        """
        Resolve links to external references and move it to local "definitions" map.

        :param raw_schema jsonschema to lookup for external links.
        :return JSON serializable object with references without external dependencies.
        """

        package = importlib.import_module(self.package_name)
        base = os.path.dirname(package.__file__) + "/"
        resolved = jsonref.JsonRef.replace_refs(raw_schema, loader=JsonFileLoader(base, "schemas/shared"), base_uri=base)
        resolved = resolve_ref_links(resolved)
        return resolved


def check_config_against_spec_or_exit(config: Mapping[str, Any], spec: ConnectorSpecification):
    """
    Check config object against spec. In case of spec is invalid, throws
    an exception with validation error description.

    :param config - config loaded from file specified over command line
    :param spec - spec object generated by connector
    """
    spec_schema = spec.connectionSpecification
    try:
        validate(instance=config, schema=spec_schema)
    except ValidationError as validation_error:
        raise AirbyteTracedException(
            message="Config validation error: " + validation_error.message,
            internal_message=validation_error.message,
            failure_type=FailureType.config_error,
        ) from None  # required to prevent logging config secrets from the ValidationError's stacktrace


class InternalConfig(BaseModel):
    KEYWORDS: ClassVar[set] = {"_limit", "_page_size"}
    limit: int = Field(None, alias="_limit")
    page_size: int = Field(None, alias="_page_size")

    def dict(self, *args, **kwargs):
        kwargs["by_alias"] = True
        kwargs["exclude_unset"] = True
        return super().dict(*args, **kwargs)


def split_config(config: MutableMapping[str, Any]) -> Tuple[MutableMapping, InternalConfig]:
    """
    Remove internal config keywords from config and build an InternalConfig from it.

    :param
     config - MutableMapping object that has been loaded from config file.

    :return tuple of user defined config MutableMapping with filtered out internal
    parameters and SAT internal config object.
    """
    internal_config = {}
    for key in list(config.keys()):
        if key in InternalConfig.KEYWORDS:
            internal_config[key] = config.pop(key)
    return config, InternalConfig.parse_obj(internal_config)
