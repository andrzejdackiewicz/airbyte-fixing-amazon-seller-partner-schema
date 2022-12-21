package io.airbyte.commons.protocol.migrations.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Utility class for recursively modifying JsonSchemas. Useful for up/downgrading
 * AirbyteCatalog objects.
 *
 * See {@link io.airbyte.commons.protocol.migrations.v1.AirbyteMessageMigrationV1}
 * for example usage.
 */
public class SchemaMigrations {

  /**
   * Generic utility method that recurses through all type declarations in the schema. For each type
   * declaration that are accepted by matcher, mutate them using transformer. For all other type
   * declarations, recurse into their subschemas (if any).
   * <p>
   * Note that this modifies the schema in-place. Callers who need a copy of the old schema should
   * save schema.deepCopy() before calling this method.
   *
   * @param schema The JsonSchema node to walk down
   * @param matcher A function which returns true on any schema node that needs to be transformed
   * @param transformer A function which mutates a schema node
   */
  public static void mutateSchemas(Function<JsonNode, Boolean> matcher, Consumer<JsonNode> transformer, JsonNode schema) {
    if (schema.isBoolean()) {
      // We never want to modify a schema of `true` or `false` (e.g. additionalProperties: true)
      // so just return immediately
      return;
    }
    if (matcher.apply(schema)) {
      // Base case: If this schema should be mutated, then we need to mutate it
      transformer.accept(schema);
    } else {
      // Otherwise, we need to find all the subschemas and mutate them.
      // technically, it might be more correct to do something like:
      // if schema["type"] == "array": find subschemas for items, additionalItems, contains
      // else if schema["type"] == "object": find subschemas for properties, patternProperties,
      // additionalProperties
      // else if oneof, allof, etc
      // but that sounds really verbose for no real benefit
      List<JsonNode> subschemas = new ArrayList<>();

      // array schemas
      findSubschemas(subschemas, schema, "items");
      findSubschemas(subschemas, schema, "additionalItems");
      findSubschemas(subschemas, schema, "contains");

      // object schemas
      if (schema.hasNonNull("properties")) {
        ObjectNode propertiesNode = (ObjectNode) schema.get("properties");
        Iterator<Entry<String, JsonNode>> propertiesIterator = propertiesNode.fields();
        while (propertiesIterator.hasNext()) {
          Entry<String, JsonNode> property = propertiesIterator.next();
          subschemas.add(property.getValue());
        }
      }
      if (schema.hasNonNull("patternProperties")) {
        ObjectNode propertiesNode = (ObjectNode) schema.get("patternProperties");
        Iterator<Entry<String, JsonNode>> propertiesIterator = propertiesNode.fields();
        while (propertiesIterator.hasNext()) {
          Entry<String, JsonNode> property = propertiesIterator.next();
          subschemas.add(property.getValue());
        }
      }
      findSubschemas(subschemas, schema, "additionalProperties");

      // combining restrictions - destinations have limited support for these, but we should handle the
      // schemas correctly anyway
      findSubschemas(subschemas, schema, "allOf");
      findSubschemas(subschemas, schema, "oneOf");
      findSubschemas(subschemas, schema, "anyOf");
      findSubschemas(subschemas, schema, "not");

      // recurse into each subschema
      for (JsonNode subschema : subschemas) {
        mutateSchemas(matcher, transformer, subschema);
      }
    }
  }

  /**
   * If schema contains key, then grab the subschema(s) at schema[key] and add them to the subschemas
   * list.
   * <p>
   * For example:
   * <ul>
   *   <li>
   *     schema = {"items": [{"type": "string}]}
   *     <p>
   *     key = "items"
   *     <p>
   *     -> add {"type": "string"} to subschemas
   *   </li>
   *   <li>
   *     schema = {"items": {"type": "string"}}
   *     <p>
   *     key = "items"
   *     <p>
   *     -> add {"type": "string"} to subschemas
   *   </li>
   *   <li>
   *     schema = {"additionalProperties": true}
   *     <p>
   *     key = "additionalProperties"
   *     <p>
   *     -> add nothing to subschemas
   *     <p>
   *     (technically `true` is a valid JsonSchema, but we don't want to modify it)
   *   </li>
   * </ul>
   */
  public static void findSubschemas(List<JsonNode> subschemas, JsonNode schema, String key) {
    if (schema.hasNonNull(key)) {
      JsonNode subschemaNode = schema.get(key);
      if (subschemaNode.isArray()) {
        for (JsonNode subschema : subschemaNode) {
          subschemas.add(subschema);
        }
      } else if (subschemaNode.isObject()) {
        subschemas.add(subschemaNode);
      }
    }
  }
}
