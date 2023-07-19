/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.base.destination.typing_deduping;

import static io.airbyte.integrations.base.destination.typing_deduping.AirbyteType.AirbyteProtocolType.*;
import static io.airbyte.integrations.base.destination.typing_deduping.AirbyteType.fromJsonSchema;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableList;
import io.airbyte.commons.json.Jsons;
import io.airbyte.integrations.base.destination.typing_deduping.AirbyteType.Array;
import io.airbyte.integrations.base.destination.typing_deduping.AirbyteType.OneOf;
import io.airbyte.integrations.base.destination.typing_deduping.AirbyteType.Struct;
import io.airbyte.integrations.base.destination.typing_deduping.AirbyteType.UnsupportedOneOf;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import org.junit.jupiter.api.Test;

public class AirbyteTypeTest {

  @Test
  public void testStruct() {
    final List<String> structSchema = new ArrayList<>();
    structSchema.add("""
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
                           "airbyte_type": "time_without_timezone"
                         },
                         "key7": {
                           "type": "string",
                           "format": "time",
                           "airbyte_type": "time_with_timezone"
                         },
                         "key8": {
                           "type": "string",
                           "format": "date-time",
                           "airbyte_type": "timestamp_without_timezone"
                         },
                         "key9": {
                           "type": "string",
                           "format": "date-time",
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
                     """);
    structSchema.add("""
                     {
                       "type": ["object"],
                       "properties": {
                         "key1": {
                           "type": ["boolean"]
                         },
                         "key2": {
                           "type": ["integer"]
                         },
                         "key3": {
                           "type": ["number"],
                           "airbyte_type": "integer"
                         },
                         "key4": {
                           "type": ["number"]
                         },
                         "key5": {
                           "type": ["string"],
                           "format": "date"
                         },
                         "key6": {
                           "type": ["string"],
                           "format": "time",
                           "airbyte_type": "time_without_timezone"
                         },
                         "key7": {
                           "type": ["string"],
                           "format": "time",
                           "airbyte_type": "time_with_timezone"
                         },
                         "key8": {
                           "type": ["string"],
                           "format": "date-time",
                           "airbyte_type": "timestamp_without_timezone"
                         },
                         "key9": {
                           "type": ["string"],
                           "format": "date-time",
                           "airbyte_type": "timestamp_with_timezone"
                         },
                         "key10": {
                           "type": ["string"],
                           "format": "date-time"
                         },
                         "key11": {
                           "type": ["string"]
                         }
                       }
                     }
                     """);
    structSchema.add("""
                     {
                       "type": ["null", "object"],
                       "properties": {
                         "key1": {
                           "type": ["null", "boolean"]
                         },
                         "key2": {
                           "type": ["null", "integer"]
                         },
                         "key3": {
                           "type": ["null", "number"],
                           "airbyte_type": "integer"
                         },
                         "key4": {
                           "type": ["null", "number"]
                         },
                         "key5": {
                           "type": ["null", "string"],
                           "format": "date"
                         },
                         "key6": {
                           "type": ["null", "string"],
                           "format": "time",
                           "airbyte_type": "time_without_timezone"
                         },
                         "key7": {
                           "type": ["null", "string"],
                           "format": "time",
                           "airbyte_type": "time_with_timezone"
                         },
                         "key8": {
                           "type": ["null", "string"],
                           "format": "date-time",
                           "airbyte_type": "timestamp_without_timezone"
                         },
                         "key9": {
                           "type": ["null", "string"],
                           "format": "date-time",
                           "airbyte_type": "timestamp_with_timezone"
                         },
                         "key10": {
                           "type": ["null", "string"],
                           "format": "date-time"
                         },
                         "key11": {
                           "type": ["null", "string"]
                         }
                       }
                     }
                     """);

    final LinkedHashMap<String, AirbyteType> propertiesMap = new LinkedHashMap<>();
    propertiesMap.put("key1", BOOLEAN);
    propertiesMap.put("key2", INTEGER);
    propertiesMap.put("key3", INTEGER);
    propertiesMap.put("key4", NUMBER);
    propertiesMap.put("key5", DATE);
    propertiesMap.put("key6", TIME_WITHOUT_TIMEZONE);
    propertiesMap.put("key7", TIME_WITH_TIMEZONE);
    propertiesMap.put("key8", TIMESTAMP_WITHOUT_TIMEZONE);
    propertiesMap.put("key9", TIMESTAMP_WITH_TIMEZONE);
    propertiesMap.put("key10", TIMESTAMP_WITH_TIMEZONE);
    propertiesMap.put("key11", STRING);

    final AirbyteType struct = new Struct(propertiesMap);
    for (final String schema : structSchema) {
      assertEquals(struct, fromJsonSchema(Jsons.deserialize(schema)));
    }
  }

  @Test
  public void testEmptyStruct() {
    final List<String> structSchema = new ArrayList<>();
    structSchema.add("""
                     {
                       "type": "object"
                     }
                     """);
    structSchema.add("""
                     {
                       "type": ["object"]
                     }
                     """);
    structSchema.add("""
                     {
                       "type": ["null", "object"]
                     }
                     """);

    final AirbyteType struct = new Struct(new LinkedHashMap<>());
    for (final String schema : structSchema) {
      assertEquals(struct, fromJsonSchema(Jsons.deserialize(schema)));
    }
  }

  @Test
  public void testImplicitStruct() {
    final String structSchema = """
                                {
                                  "properties": {
                                    "key1": {
                                      "type": "boolean"
                                    }
                                  }
                                }
                                """;

    final LinkedHashMap<String, AirbyteType> propertiesMap = new LinkedHashMap<>();
    propertiesMap.put("key1", BOOLEAN);

    final AirbyteType struct = new Struct(propertiesMap);
    assertEquals(struct, fromJsonSchema(Jsons.deserialize(structSchema)));
  }

  @Test
  public void testArray() {
    final List<String> arraySchema = new ArrayList<>();
    arraySchema.add("""
                    {
                      "type": "array",
                      "items": {
                        "type": "string",
                        "format": "date-time",
                        "airbyte_type": "timestamp_with_timezone"
                      }
                    }
                    """);
    arraySchema.add("""
                    {
                      "type": ["array"],
                      "items": {
                        "type": ["string"],
                        "format": "date-time",
                        "airbyte_type": "timestamp_with_timezone"
                      }
                    }
                    """);
    arraySchema.add("""
                    {
                      "type": ["null", "array"],
                      "items": {
                        "type": ["null", "string"],
                        "format": "date-time",
                        "airbyte_type": "timestamp_with_timezone"
                      }
                    }
                    """);

    final AirbyteType array = new Array(TIMESTAMP_WITH_TIMEZONE);
    for (final String schema : arraySchema) {
      assertEquals(array, fromJsonSchema(Jsons.deserialize(schema)));
    }
  }

  @Test
  public void testEmptyArray() {
    final List<String> arraySchema = new ArrayList<>();
    arraySchema.add("""
                    {
                      "type": "array"
                    }
                    """);
    arraySchema.add("""
                    {
                      "type": ["array"]
                    }
                    """);

    arraySchema.add("""
                    {
                      "type": ["null", "array"]
                    }
                    """);

    final AirbyteType array = new Array(UNKNOWN);
    for (final String schema : arraySchema) {
      assertEquals(array, fromJsonSchema(Jsons.deserialize(schema)));
    }
  }

  @Test
  public void testUnsupportedOneOf() {
    final String unsupportedOneOfSchema = """
                                          {
                                            "oneOf": ["number", "string"]
                                          }
                                          """;

    final List<AirbyteType> options = new ArrayList<>();
    options.add(NUMBER);
    options.add(STRING);

    final UnsupportedOneOf unsupportedOneOf = new UnsupportedOneOf(options);
    assertEquals(unsupportedOneOf, fromJsonSchema(Jsons.deserialize(unsupportedOneOfSchema)));
  }

  @Test
  public void testOneOf() {

    final String oneOfSchema = """
                               {
                                 "type": ["string", "number"]
                               }
                               """;

    final List<AirbyteType> options = new ArrayList<>();
    options.add(STRING);
    options.add(NUMBER);

    final OneOf oneOf = new OneOf(options);
    assertEquals(oneOf, fromJsonSchema(Jsons.deserialize(oneOfSchema)));
  }

  @Test
  public void testOneOfComplex() {
    final JsonNode schema = Jsons.deserialize("""
                                        {
                                          "type": ["string", "object", "array", "null", "string", "object", "array", "null"],
                                          "properties": {
                                            "foo": {"type": "string"}
                                          },
                                          "items": {"type": "string"}
                                        }
                                        """);

    final AirbyteType parsed = fromJsonSchema(schema);

    final AirbyteType expected = new OneOf(List.of(
        STRING,
        new Struct(new LinkedHashMap<>() {

          {
            put("foo", STRING);
          }

        }),
        new Array(STRING)));
    assertEquals(expected, parsed);
  }

  @Test
  public void testOneOfUnderspecifiedNonPrimitives() {
    final JsonNode schema = Jsons.deserialize("""
                                        {
                                          "type": ["string", "object", "array", "null", "string", "object", "array", "null"]
                                        }
                                        """);

    final AirbyteType parsed = fromJsonSchema(schema);

    final AirbyteType expected = new OneOf(List.of(
        STRING,
        new Struct(new LinkedHashMap<>()),
        new Array(UNKNOWN)));
    assertEquals(expected, parsed);
  }

  @Test
  public void testInvalidTextualType() {
    final String invalidTypeSchema = """
                                     {
                                       "type": "foo"
                                     }
                                     """;
    assertEquals(UNKNOWN, fromJsonSchema(Jsons.deserialize(invalidTypeSchema)));
  }

  @Test
  public void testInvalidBooleanType() {
    final String invalidTypeSchema = """
                                     {
                                       "type": true
                                     }
                                     """;
    assertEquals(UNKNOWN, fromJsonSchema(Jsons.deserialize(invalidTypeSchema)));
  }

  @Test
  public void testInvalid() {
    final List<String> invalidSchema = new ArrayList<>();
    invalidSchema.add("");
    invalidSchema.add("null");
    invalidSchema.add("true");
    invalidSchema.add("false");
    invalidSchema.add("1");
    invalidSchema.add("\"\"");
    invalidSchema.add("[]");
    invalidSchema.add("{}");

    for (final String schema : invalidSchema) {
      assertEquals(UNKNOWN, fromJsonSchema(Jsons.deserialize(schema)));
    }
  }

  @Test
  public void testChooseOneOf() {
    // test ordering

    OneOf o = new OneOf(ImmutableList.of(STRING, DATE));
    assertEquals(DATE, AirbyteTypeUtils.chooseOneOfType(o));

    final Array a = new Array(TIME_WITH_TIMEZONE);
    o = new OneOf(ImmutableList.of(TIMESTAMP_WITH_TIMEZONE, a));
    assertEquals(a, AirbyteTypeUtils.chooseOneOfType(o));

    final LinkedHashMap<String, AirbyteType> properties = new LinkedHashMap<>();
    properties.put("key1", UNKNOWN);
    properties.put("key2", TIME_WITHOUT_TIMEZONE);
    final Struct s = new Struct(properties);
    o = new OneOf(ImmutableList.of(TIMESTAMP_WITHOUT_TIMEZONE, s));
    assertEquals(s, AirbyteTypeUtils.chooseOneOfType(o));

    // test exclusion

    o = new OneOf(ImmutableList.of(BOOLEAN, INTEGER));
    assertEquals(INTEGER, AirbyteTypeUtils.chooseOneOfType(o));

    o = new OneOf(ImmutableList.of(INTEGER, NUMBER, DATE));
    assertEquals(NUMBER, AirbyteTypeUtils.chooseOneOfType(o));

    o = new OneOf(ImmutableList.of(BOOLEAN, NUMBER, STRING));
    assertEquals(STRING, AirbyteTypeUtils.chooseOneOfType(o));
  }

  @Test
  public void testAsColumns() {
    final OneOf o = new OneOf(List.of(
        STRING,
        new Struct(new LinkedHashMap<>() {

          {
            put("foo", STRING);
          }

        }),
        new Array(STRING),
        // This is bad behavior, but it matches current behavior so we'll test it.
        // Ideally, we would recognize that the sub-oneOfs are also objects.
        new OneOf(List.of(new Struct(new LinkedHashMap<>()))),
        new UnsupportedOneOf(List.of(new Struct(new LinkedHashMap<>())))));

    final LinkedHashMap<String, AirbyteType> columns = o.asColumns();

    assertEquals(
        new LinkedHashMap<>() {

          {
            put("foo", STRING);
          }

        },
        columns);
  }

  @Test
  public void testAsColumnsMultipleObjects() {
    final OneOf o = new OneOf(List.of(
        new Struct(new LinkedHashMap<>()),
        new Struct(new LinkedHashMap<>())));

    // This prooobably should throw an exception, but for the sake of smooth rollout it just logs a
    // warning for now.
    assertEquals(new LinkedHashMap<>(), o.asColumns());
  }

  @Test
  public void testAsColumnsNoObjects() {
    final OneOf o = new OneOf(List.of(
        STRING,
        new Array(STRING),
        new UnsupportedOneOf(new ArrayList<>()),
        // Similar to testAsColumns(), this is bad behavior.
        new OneOf(List.of(new Struct(new LinkedHashMap<>()))),
        new UnsupportedOneOf(List.of(new Struct(new LinkedHashMap<>())))));

    // This prooobably should throw an exception, but for the sake of smooth rollout it just logs a
    // warning for now.
    assertEquals(new LinkedHashMap<>(), o.asColumns());
  }

}
