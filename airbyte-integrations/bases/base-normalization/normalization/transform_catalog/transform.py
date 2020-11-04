"""
MIT License

Copyright (c) 2020 Airbyte

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"""

import argparse
import json
import os
from typing import Tuple, Union, Optional


class TransformCatalog:
    config: dict = {}

    def run(self, args):
        self.parse(args)
        catalog = self.read_json_catalog()
        result = generate_dbt_model(catalog=catalog, json_col="json_blob", from_table="`airbytesandbox.data.one_recipe_json`")
        self.output_sql_models(result)

    def parse(self, args):
        parser = argparse.ArgumentParser(add_help=False)
        parser.add_argument("--catalog", type=str, required=True, help="path to Catalog (JSON Schema) file")
        parser.add_argument("--out", type=str, required=True, help="path to output generated DBT Models to")
        parsed_args = parser.parse_args(args)
        self.config = {
            "catalog": parsed_args.catalog,
            "output_path": parsed_args.out,
        }

    def read_json_catalog(self) -> dict:
        input_path = self.config["catalog"]
        with open(input_path, "r") as file:
            contents = file.read()
        return json.loads(contents)

    def output_sql_models(self, result: dict):
        output = self.config["output_path"]
        if result:
            if not os.path.exists(output):
                os.makedirs(output)
            for file, sql in result.items():
                print(f"Generating {file.lower()}.sql in {output}")
                with open(os.path.join(output, f"{file}.sql").lower(), "w") as f:
                    f.write(sql)


def is_string(property_type) -> bool:
    return property_type == "string" or "string" in property_type


def is_integer(property_type) -> bool:
    return property_type == "integer" or "integer" in property_type


def is_boolean(property_type) -> bool:
    return property_type == "boolean" or "boolean" in property_type


def is_array(property_type) -> bool:
    return property_type == "array" or "array" in property_type


def is_object(property_type) -> bool:
    return property_type == "object" or "object" in property_type


def find_combining_schema(properties: dict):
    return set(properties).intersection({"anyOf", "oneOf", "allOf"})


def json_extract_base_property(path: str, json_col: str, name: str, definition: dict) -> Optional[str]:
    current = ".".join([path, name])
    if "type" not in definition:
        return None
    elif is_string(definition["type"]):
        return f"cast(json_extract_scalar({json_col}, '{current}') as string) as {name}"
    elif is_integer(definition["type"]):
        return f"cast(json_extract_scalar({json_col}, '{current}') as int64) as {name}"
    elif is_boolean(definition["type"]):
        return f"cast(json_extract_scalar({json_col}, '{current}') as boolean) as {name}"
    else:
        return None


def json_extract_nested_property(path: str, json_col: str, name: str, definition: dict) -> Union[Tuple[None, None], Tuple[str, str]]:
    current = ".".join([path, name])
    if definition is None or "type" not in definition:
        return None, None
    elif is_array(definition["type"]):
        return f"json_extract_array({json_col}, '{current}') as {name}", f"cross join unnest({name}) as {name}"
    elif is_object(definition["type"]):
        return f"json_extract({json_col}, '{current}') as {name}", ""
    else:
        return None, None


def select_table(table: str, columns="*"):
    return f"\nselect {columns} from {table}"


def extract_node_properties(path: str, json_col: str, properties: dict) -> dict:
    result = {}
    if properties:
        for field in properties.keys():
            sql_field = json_extract_base_property(path=path, json_col=json_col, name=field, definition=properties[field])
            if sql_field:
                result[field] = sql_field
    return result


def find_properties_object(path: str, field: str, properties) -> dict:
    if isinstance(properties, str) or isinstance(properties, int):
        return {}
    else:
        if "items" in properties:
            return find_properties_object(path, field, properties["items"])
        elif "properties" in properties:
            # we found a properties object
            return {field: properties["properties"]}
        elif "type" in properties and json_extract_base_property(path=path, json_col="", name="", definition=properties):
            # we found a basic type
            return {field: None}
        elif isinstance(properties, dict):
            for key in properties.keys():
                if not json_extract_base_property(path, "", key, properties[key]):
                    child = find_properties_object(path, key, properties[key])
                    if child:
                        return child
        elif isinstance(properties, list):
            for item in properties:
                child = find_properties_object(path=path, field=field, properties=item)
                if child:
                    return child
    return {}


def extract_nested_properties(path: str, field: str, properties: dict) -> dict:
    result = {}
    if properties:
        for key in properties.keys():
            combining = find_combining_schema(properties[key])
            if combining:
                # skip combining schemas
                for combo in combining:
                    found = find_properties_object(path=f"{path}.{field}.{key}", field=key, properties=properties[key][combo])
                    result.update(found)
            elif "type" not in properties[key]:
                pass
            elif is_array(properties[key]["type"]):
                combining = find_combining_schema(properties[key]["items"])
                if combining:
                    # skip combining schemas
                    for combo in combining:
                        found = find_properties_object(path=f"{path}.{key}", field=key, properties=properties[key]["items"][combo])
                        result.update(found)
                else:
                    found = find_properties_object(path=f"{path}.{key}", field=key, properties=properties[key]["items"])
                    result.update(found)
            elif is_object(properties[key]["type"]):
                found = find_properties_object(path=f"{path}.{key}", field=key, properties=properties[key])
                result.update(found)
    return result


def process_node(path: str, json_col: str, name: str, properties: dict, from_table: str = "", previous="with ", inject_cols="") -> dict:
    result = {}
    if previous == "with ":
        prefix = previous
    else:
        prefix = previous + ","
    node_properties = extract_node_properties(path=path, json_col=json_col, properties=properties)
    node_columns = ",\n    ".join([sql for sql in node_properties.values()])
    # FIXME: use DBT macros to be cross_db compatible instead
    hash_node_columns = (
        "coalesce(cast("
        + ' as string), ""),\n      coalesce(cast('.join([column for column in node_properties.keys()])
        + ' as string), "")'
    )
    node_sql = f"""{prefix}
{name}_node as (
  select     
    {inject_cols}
    {node_columns}
  from {from_table}
),
{name}_with_id as (
  select
    *,
    to_hex(md5(concat(
      {hash_node_columns}
    ))) as _{name}_hashid
  from {name}_node
)"""
    # SQL Query for current node's basic properties
    result[name] = node_sql + select_table(f"{name}_with_id")

    children_columns = extract_nested_properties(path=path, field=name, properties=properties)
    if children_columns:
        for col in children_columns.keys():
            child_col, join_child_table = json_extract_nested_property(path=path, json_col=json_col, name=col, definition=properties[col])
            child_sql = f"""{prefix}
{name}_node as (
  select     
    {child_col},
    {node_columns}
  from {from_table}
),
{name}_with_id as (
  select
    to_hex(md5(concat(
      {hash_node_columns}
    ))) as _{name}_hashid,
    {col}
  from {name}_node
  {join_child_table}
)"""
            if children_columns[col]:
                children = process_node(
                    path="$",
                    json_col=col,
                    name=f"{name}_{col}",
                    properties=children_columns[col],
                    from_table=f"{name}_with_id",
                    previous=child_sql,
                    inject_cols=f"_{name}_hashid as _{name}_foreign_hashid,",
                )
                result.update(children)
            else:
                # SQL Query for current node's basic properties
                result[f"{name}_{col}"] = child_sql + select_table(
                    f"{name}_with_id",
                    columns=f"""
  _{name}_hashid as _{name}_foreign_hashid,
  {col}
""",
                )
    return result


def generate_dbt_model(catalog: dict, json_col: str, from_table: str) -> dict:
    result = {}
    for obj in catalog["streams"]:
        name = obj["name"]
        properties = {}
        if "json_schema" in obj:
            properties = obj["json_schema"]["properties"]
        elif "schema" in obj:
            properties = obj["schema"]["properties"]
        result.update(process_node(path="$", json_col=json_col, name=name, properties=properties, from_table=from_table))
    return result


def main(args=None):
    TransformCatalog().run(args)
