/*
 * Copyright (c) 2022 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.protocol.migrations.v1;

import static io.airbyte.protocol.models.JsonSchemaReferenceTypes.REF_KEY;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.protocol.migrations.AirbyteMessageMigration;
import io.airbyte.commons.protocol.migrations.util.RecordMigrations;
import io.airbyte.commons.protocol.migrations.util.RecordMigrations.MigratedNode;
import io.airbyte.commons.version.AirbyteProtocolVersion;
import io.airbyte.commons.version.Version;
import io.airbyte.protocol.models.ConfiguredAirbyteCatalog;
import io.airbyte.protocol.models.v0.AirbyteMessage;
import java.util.Optional;
import io.airbyte.protocol.models.AirbyteStream;
import io.airbyte.protocol.models.ConfiguredAirbyteStream;
import io.airbyte.protocol.models.JsonSchemaReferenceTypes;
import io.airbyte.protocol.models.AirbyteMessage;
import io.airbyte.protocol.models.AirbyteMessage.Type;
import io.airbyte.protocol.models.AirbyteStream;
import io.airbyte.validation.json.JsonSchemaValidator;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;

public class AirbyteMessageMigrationV1 implements AirbyteMessageMigration<io.airbyte.protocol.models.v0.AirbyteMessage, AirbyteMessage> {

  private final ConfiguredAirbyteCatalog catalog;
  private final JsonSchemaValidator validator;

  public AirbyteMessageMigrationV1(ConfiguredAirbyteCatalog catalog, JsonSchemaValidator validator) {
    this.catalog = catalog;
    this.validator = validator;
  }

  @Override
<<<<<<< HEAD
  public AirbyteMessage downgrade(final io.airbyte.protocol.models.AirbyteMessage message,
                                  final Optional<ConfiguredAirbyteCatalog> configuredAirbyteCatalog) {
    return Jsons.object(Jsons.jsonNode(message), AirbyteMessage.class);
=======
  public io.airbyte.protocol.models.v0.AirbyteMessage downgrade(AirbyteMessage oldMessage) {
    io.airbyte.protocol.models.v0.AirbyteMessage newMessage = Jsons.object(
        Jsons.jsonNode(oldMessage),
        io.airbyte.protocol.models.v0.AirbyteMessage.class);
    if (oldMessage.getType() == Type.CATALOG) {
      for (io.airbyte.protocol.models.v0.AirbyteStream stream : newMessage.getCatalog().getStreams()) {
        JsonNode schema = stream.getJsonSchema();
        SchemaMigrationV1.downgradeSchema(schema);
      }
    } else if (oldMessage.getType() == Type.RECORD) {
      io.airbyte.protocol.models.v0.AirbyteRecordMessage record = newMessage.getRecord();
      Optional<ConfiguredAirbyteStream> maybeStream = catalog.getStreams().stream()
          .filter(stream -> Objects.equals(stream.getStream().getName(), record.getStream())
              && Objects.equals(stream.getStream().getNamespace(), record.getNamespace()))
          .findFirst();
      // If this record doesn't belong to any configured stream, then there's no point downgrading it
      // So only do the downgrade if we can find its stream
      if (maybeStream.isPresent()) {
        JsonNode schema = maybeStream.get().getStream().getJsonSchema();
        JsonNode oldData = record.getData();
        MigratedNode downgradedNode = downgradeNode(oldData, schema);
        record.setData(downgradedNode.node());
      }
    }
    return newMessage;
>>>>>>> 18c3e46222 (Data types update: Implement protocol message downgrade path (#19909))
  }

  @Override
  public AirbyteMessage upgrade(io.airbyte.protocol.models.v0.AirbyteMessage oldMessage) {
    // We're not introducing any changes to the structure of the record/catalog
    // so just clone a new message object, which we can edit in-place
    AirbyteMessage newMessage = Jsons.object(
        Jsons.jsonNode(oldMessage),
        AirbyteMessage.class);
    if (oldMessage.getType() == io.airbyte.protocol.models.v0.AirbyteMessage.Type.CATALOG) {
      for (AirbyteStream stream : newMessage.getCatalog().getStreams()) {
        JsonNode schema = stream.getJsonSchema();
        SchemaMigrationV1.upgradeSchema(schema);
      }
    } else if (oldMessage.getType() == io.airbyte.protocol.models.v0.AirbyteMessage.Type.RECORD) {
      JsonNode oldData = newMessage.getRecord().getData();
      JsonNode newData = upgradeRecord(oldData);
      newMessage.getRecord().setData(newData);
    }
    return newMessage;
  }

  /**
   * Returns a copy of oldData, with numeric values converted to strings. String and boolean values
   * are returned as-is for convenience, i.e. this is not a true deep copy.
   */
  private static JsonNode upgradeRecord(JsonNode oldData) {
    if (oldData.isNumber()) {
      // Base case: convert numbers to strings
      return Jsons.convertValue(oldData.asText(), TextNode.class);
    } else if (oldData.isObject()) {
      // Recurse into each field of the object
      ObjectNode newData = (ObjectNode) Jsons.emptyObject();

      Iterator<Entry<String, JsonNode>> fieldsIterator = oldData.fields();
      while (fieldsIterator.hasNext()) {
        Entry<String, JsonNode> next = fieldsIterator.next();
        String key = next.getKey();
        JsonNode value = next.getValue();

        JsonNode newValue = upgradeRecord(value);
        newData.set(key, newValue);
      }

      return newData;
    } else if (oldData.isArray()) {
      // Recurse into each element of the array
      ArrayNode newData = Jsons.arrayNode();
      for (JsonNode element : oldData) {
        newData.add(upgradeRecord(element));
      }
      return newData;
    } else {
      // Base case: this is a string or boolean, so we don't need to modify it
      return oldData;
    }
  }

  /**
   * We need the schema to recognize which fields are integers, since it would be wrong to just assume
   * any numerical string should be parsed out.
   *
   * Works on a best-effort basis. If the schema doesn't match the data, we'll do our best to
   * downgrade anything that we can definitively say is a number. Should _not_ throw an exception if
   * bad things happen (e.g. we try to parse a non-numerical string as a number).
   */
  private MigratedNode downgradeNode(JsonNode data, JsonNode schema) {
    return RecordMigrations.mutateDataNode(
        validator,
        s -> {
          if (s.hasNonNull(REF_KEY)) {
            String type = s.get(REF_KEY).asText();
            return JsonSchemaReferenceTypes.INTEGER_REFERENCE.equals(type)
                || JsonSchemaReferenceTypes.NUMBER_REFERENCE.equals(type);
          } else {
            return false;
          }
        },
        (s, d) -> {
          if (d.asText().matches("-?\\d+(\\.\\d+)?")) {
            // If this string is a numeric literal, convert it to a numeric node.
            return new MigratedNode(Jsons.deserialize(d.asText()), true);
          } else {
            // Otherwise, just leave the node unchanged.
            return new MigratedNode(d, false);
          }
        },
        data, schema
    );
  }


  @Override
  public Version getPreviousVersion() {
    return AirbyteProtocolVersion.V0;
  }

  @Override
  public Version getCurrentVersion() {
    return AirbyteProtocolVersion.V1;
  }

}
