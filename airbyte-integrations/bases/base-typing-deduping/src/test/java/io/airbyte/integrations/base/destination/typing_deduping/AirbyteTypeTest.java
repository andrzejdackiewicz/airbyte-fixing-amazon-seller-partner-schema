/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.base.destination.typing_deduping;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.airbyte.commons.json.Jsons;
import io.airbyte.integrations.base.destination.typing_deduping.AirbyteType.AirbyteProtocolType;
import io.airbyte.integrations.base.destination.typing_deduping.AirbyteType.Array;
import io.airbyte.integrations.base.destination.typing_deduping.AirbyteType.OneOf;
import io.airbyte.integrations.base.destination.typing_deduping.AirbyteType.Struct;
import io.airbyte.integrations.base.destination.typing_deduping.AirbyteType.UnsupportedOneOf;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import org.junit.jupiter.api.Test;

public class AirbyteTypeTest {
  /*
   *
   * map of JsonNode schema to expected AirbyteType run fromJsonSchema and assert equivalence
   *
   * more edge cases
   *
   *
   * map of AirbyteType to ParsedType run toDialectType and assert equivalence
   *
   */

  @Test
  public void testStruct() {
    final String structSchema = """
                                {
                                  "type": "object",
                                  "properties": {
                                    "key1": {
                                      "type": "boolean"
                                    },
                                    "key2": {
                                      "type": "integer"
                                    },
                                    "key3": {
                                      "type": "number",
                                      "airbyte_type": "integer"
                                    },
                                    "key4": {
                                      "type": "number"
                                    },
                                    "key5": {
                                      "type": "string",
                                      "format": "date"
                                    },
                                    "key6": {
                                      "type": "string",
                                      "format": "time",
                                      "airbyte_type": "timestamp_without_timezone"
                                    },
                                    "key7": {
                                      "type": "string",
                                      "format": "time",
                                      "airbyte_type": "timestamp_with_timezone"
                                    },
                                    "key8": {
                                      "type": "string",
                                      "format": "date-time",
                                      "airbyte_type": "timestamp_without_timezone"
                                    },
                                    "key9": {
                                      "type": "string",
                                      "format": ["date-time", "foo"],
                                      "airbyte_type": "timestamp_with_timezone"
                                    },
                                    "key10": {
                                      "type": "string",
                                      "format": "date-time"
                                    },
                                    "key11": {
                                      "type": "string"
                                    }
                                  }
                                }
                                """;

    final LinkedHashMap<String, AirbyteType> propertiesMap = new LinkedHashMap<>();
    propertiesMap.put("key1", AirbyteProtocolType.BOOLEAN);
    propertiesMap.put("key2", AirbyteProtocolType.INTEGER);
    propertiesMap.put("key3", AirbyteProtocolType.INTEGER);
    propertiesMap.put("key4", AirbyteProtocolType.NUMBER);
    propertiesMap.put("key5", AirbyteProtocolType.DATE);
    propertiesMap.put("key6", AirbyteProtocolType.TIME_WITHOUT_TIMEZONE);
    propertiesMap.put("key7", AirbyteProtocolType.TIME_WITH_TIMEZONE);
    propertiesMap.put("key8", AirbyteProtocolType.TIMESTAMP_WITHOUT_TIMEZONE);
    propertiesMap.put("key9", AirbyteProtocolType.TIMESTAMP_WITH_TIMEZONE);
    propertiesMap.put("key10", AirbyteProtocolType.TIMESTAMP_WITH_TIMEZONE);
    propertiesMap.put("key11", AirbyteProtocolType.STRING);

    final AirbyteType struct = new Struct(propertiesMap);
    assertEquals(struct, AirbyteType.fromJsonSchema(Jsons.deserialize(structSchema)));
  }

  @Test
  public void testArray() {
    final String arraySchema = """
                               {
                                 "type": "array",
                                 "items": {
                                   "type": "string",
                                   "format": "date-time",
                                   "airbyte_type": "timestamp_with_timezone"
                                 }
                               }
                               """;

    final AirbyteType array = new Array(AirbyteProtocolType.TIMESTAMP_WITH_TIMEZONE);
    assertEquals(array, AirbyteType.fromJsonSchema(Jsons.deserialize(arraySchema)));
  }

  @Test
  public void testUnsupportedOneOf() {
    final String unsupportedOneOfSchema = """
                                          {
                                            "oneOf": ["number", "string"]
                                          }
                                          """;

    final List<AirbyteType> options = new ArrayList<>();
    options.add(AirbyteProtocolType.NUMBER);
    options.add(AirbyteProtocolType.STRING);

    final UnsupportedOneOf unsupportedOneOf = new UnsupportedOneOf(options);
    assertEquals(unsupportedOneOf, AirbyteType.fromJsonSchema(Jsons.deserialize(unsupportedOneOfSchema)));
  }

  @Test
  public void testOneOf() {

    final String oneOfSchema = """
                               {
                                 "type": ["string", "number"]
                               }
                               """;

    final List<AirbyteType> options = new ArrayList<>();
    options.add(AirbyteProtocolType.STRING);
    options.add(AirbyteProtocolType.NUMBER);

    final OneOf oneOf = new OneOf(options);
    assertEquals(oneOf, AirbyteType.fromJsonSchema(Jsons.deserialize(oneOfSchema)));
  }

  @Test
  public void testEmpty() {
    final String emptySchema = "{}";
    assertEquals(AirbyteProtocolType.UNKNOWN, AirbyteType.fromJsonSchema(Jsons.deserialize(emptySchema)));
  }

  @Test
  public void testInvalidTextualType() {
    final String invalidTypeSchema = """
                                     {
                                       "type": "foo"
                                     }
                                     """;
    assertEquals(AirbyteProtocolType.UNKNOWN, AirbyteType.fromJsonSchema(Jsons.deserialize(invalidTypeSchema)));
  }

  @Test
  public void testInvalidBooleanType() {
    final String invalidTypeSchema = """
                                     {
                                       "type": true
                                     }
                                     """;
    assertEquals(AirbyteProtocolType.UNKNOWN, AirbyteType.fromJsonSchema(Jsons.deserialize(invalidTypeSchema)));
  }

}
