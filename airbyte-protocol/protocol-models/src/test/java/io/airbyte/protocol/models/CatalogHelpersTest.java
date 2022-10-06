/*
 * Copyright (c) 2022 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.protocol.models;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.Sets;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.resources.MoreResources;
import io.airbyte.protocol.models.transform_models.FieldTransform;
import io.airbyte.protocol.models.transform_models.StreamTransform;
import io.airbyte.protocol.models.transform_models.StreamTransformType;
import io.airbyte.protocol.models.transform_models.UpdateFieldSchemaTransform;
import io.airbyte.protocol.models.transform_models.UpdateStreamTransform;
import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;
import lombok.val;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.Condition;
import org.elasticsearch.common.collect.Map;
import org.junit.jupiter.api.Test;

@SuppressWarnings("PMD.JUnitTestsShouldIncludeAssert")
class CatalogHelpersTest {

  // handy for debugging test only.
  private static final Comparator<StreamTransform> STREAM_TRANSFORM_COMPARATOR =
      Comparator.comparing(StreamTransform::getTransformType);
  private static final String CAD = "CAD";
  private static final String ITEMS = "items";
  private static final String SOME_ARRAY = "someArray";
  private static final String PROPERTIES = "properties";
  private static final String USERS = "users";
  private static final String COMPANIES_VALID = "companies_schema.json";
  private static final String COMPANIES_INVALID = "companies_schema_invalid.json";

  @Test
  void testFieldToJsonSchema() {
    final String expected = """
                                {
                                  "type": "object",
                                  "properties": {
                                    "name": {
                                      "type": "string"
                                    },
                                    "test_object": {
                                      "type": "object",
                                      "properties": {
                                        "thirdLevelObject": {
                                          "type": "object",
                                          "properties": {
                                            "data": {
                                              "type": "string"
                                            },
                                            "intData": {
                                              "type": "number"
                                            }
                                          }
                                        },
                                        "name": {
                                          "type": "string"
                                        }
                                      }
                                    }
                                  }
                                }
                            """;
    final JsonNode actual = CatalogHelpers.fieldsToJsonSchema(Field.of("name", JsonSchemaType.STRING),
        Field.of("test_object", JsonSchemaType.OBJECT, List.of(
            Field.of("name", JsonSchemaType.STRING),
            Field.of("thirdLevelObject", JsonSchemaType.OBJECT, List.of(
                Field.of("data", JsonSchemaType.STRING),
                Field.of("intData", JsonSchemaType.NUMBER))))));

    assertEquals(Jsons.deserialize(expected), actual);
  }

  @Test
  void testGetTopLevelFieldNames() {
    final String json = "{ \"type\": \"object\", \"properties\": { \"name\": { \"type\": \"string\" } } } ";
    final Set<String> actualFieldNames =
        CatalogHelpers.getTopLevelFieldNames(new ConfiguredAirbyteStream().withStream(new AirbyteStream().withJsonSchema(Jsons.deserialize(json))));

    assertEquals(Sets.newHashSet("name"), actualFieldNames);
  }

  @Test
  void testGetFieldNames() throws IOException {
    final JsonNode node = Jsons.deserialize(MoreResources.readResource("valid_schema.json"));
    final Set<String> actualFieldNames = CatalogHelpers.getAllFieldNames(node);
    final List<String> expectedFieldNames =
        List.of(CAD, "DKK", "HKD", "HUF", "ISK", "PHP", "date", "nestedkey", "somekey", "something", "something2", "文", SOME_ARRAY, ITEMS,
            "oldName");

    // sort so that the diff is easier to read.
    assertEquals(expectedFieldNames.stream().sorted().toList(), actualFieldNames.stream().sorted().toList());
  }

  @Test
  void testGetCatalogDiff() throws IOException {
    final JsonNode schema1 = Jsons.deserialize(MoreResources.readResource("valid_schema.json"));
    final JsonNode schema2 = Jsons.deserialize(MoreResources.readResource("valid_schema2.json"));
    final AirbyteCatalog catalog1 = new AirbyteCatalog().withStreams(List.of(
        new AirbyteStream().withName(USERS).withJsonSchema(schema1),
        new AirbyteStream().withName("accounts").withJsonSchema(Jsons.emptyObject())));
    final AirbyteCatalog catalog2 = new AirbyteCatalog().withStreams(List.of(
        new AirbyteStream().withName(USERS).withJsonSchema(schema2),
        new AirbyteStream().withName("sales").withJsonSchema(Jsons.emptyObject())));

    final Set<StreamTransform> actualDiff = CatalogHelpers.getCatalogDiff(catalog1, catalog2);
    final List<StreamTransform> expectedDiff = Stream.of(
        StreamTransform.createAddStreamTransform(new StreamDescriptor().withName("sales")),
        StreamTransform.createRemoveStreamTransform(new StreamDescriptor().withName("accounts")),
        StreamTransform.createUpdateStreamTransform(new StreamDescriptor().withName(USERS), new UpdateStreamTransform(Set.of(
            FieldTransform.createAddFieldTransform(List.of("COD"), schema2.get(PROPERTIES).get("COD")),
            FieldTransform.createRemoveFieldTransform(List.of("something2"), schema1.get(PROPERTIES).get("something2")),
            FieldTransform.createRemoveFieldTransform(List.of("HKD"), schema1.get(PROPERTIES).get("HKD")),
            FieldTransform.createUpdateFieldTransform(List.of(CAD), new UpdateFieldSchemaTransform(
                schema1.get(PROPERTIES).get(CAD),
                schema2.get(PROPERTIES).get(CAD))),
            FieldTransform.createUpdateFieldTransform(List.of(SOME_ARRAY), new UpdateFieldSchemaTransform(
                schema1.get(PROPERTIES).get(SOME_ARRAY),
                schema2.get(PROPERTIES).get(SOME_ARRAY))),
            FieldTransform.createUpdateFieldTransform(List.of(SOME_ARRAY, ITEMS), new UpdateFieldSchemaTransform(
                schema1.get(PROPERTIES).get(SOME_ARRAY).get(ITEMS),
                schema2.get(PROPERTIES).get(SOME_ARRAY).get(ITEMS))),
            FieldTransform.createRemoveFieldTransform(List.of(SOME_ARRAY, ITEMS, "oldName"),
                schema1.get(PROPERTIES).get(SOME_ARRAY).get(ITEMS).get(PROPERTIES).get("oldName")),
            FieldTransform.createAddFieldTransform(List.of(SOME_ARRAY, ITEMS, "newName"),
                schema2.get(PROPERTIES).get(SOME_ARRAY).get(ITEMS).get(PROPERTIES).get("newName"))))))
        .sorted(STREAM_TRANSFORM_COMPARATOR)
        .toList();

    Assertions.assertThat(actualDiff).containsAll(expectedDiff);
  }

  @Test
  void testExtractIncrementalStreamDescriptors() {
    final ConfiguredAirbyteCatalog configuredCatalog = new ConfiguredAirbyteCatalog()
        .withStreams(List.of(
            new ConfiguredAirbyteStream()
                .withSyncMode(SyncMode.INCREMENTAL)
                .withStream(
                    new AirbyteStream()
                        .withName("one")),
            new ConfiguredAirbyteStream()
                .withSyncMode(SyncMode.FULL_REFRESH)
                .withStream(
                    new AirbyteStream()
                        .withName("one"))));

    final List<StreamDescriptor> streamDescriptors = CatalogHelpers.extractIncrementalStreamDescriptors(configuredCatalog);

    assertEquals(1, streamDescriptors.size());
    assertEquals("one", streamDescriptors.get(0).getName());
  }

  @Test
  void testGetFullyQualifiedFieldNamesWithTypes() throws IOException {
    CatalogHelpers.getFullyQualifiedFieldNamesWithTypes(
        Jsons.deserialize(MoreResources.readResource(COMPANIES_VALID))).stream().collect(
            () -> new HashMap<>(),
            CatalogHelpers::collectInHashMap,
            CatalogHelpers::combineAccumulator);
  }

  @Test
  void testGetFullyQualifiedFieldNamesWithTypesOnInvalidSchema() throws IOException {
    val resultField = CatalogHelpers.getFullyQualifiedFieldNamesWithTypes(
        Jsons.deserialize(MoreResources.readResource(COMPANIES_INVALID))).stream().collect(
            () -> new HashMap<>(),
            CatalogHelpers::collectInHashMap,
            CatalogHelpers::combineAccumulator);

    Assertions.assertThat(resultField)
        .contains(
            Map.entry(
                List.of("tags", "tags", "items"),
                CatalogHelpers.DUPLICATED_SCHEMA));
  }

  @Test
  void testGetCatalogDiffWithInvalidSchema() throws IOException {
    final JsonNode schema1 = Jsons.deserialize(MoreResources.readResource(COMPANIES_INVALID));
    final JsonNode schema2 = Jsons.deserialize(MoreResources.readResource(COMPANIES_VALID));
    final AirbyteCatalog catalog1 = new AirbyteCatalog().withStreams(List.of(
        new AirbyteStream().withName(USERS).withJsonSchema(schema1)));
    final AirbyteCatalog catalog2 = new AirbyteCatalog().withStreams(List.of(
        new AirbyteStream().withName(USERS).withJsonSchema(schema2)));

    final Set<StreamTransform> actualDiff = CatalogHelpers.getCatalogDiff(catalog1, catalog2);

    Assertions.assertThat(actualDiff).hasSize(1);
    Assertions.assertThat(actualDiff).first()
        .has(new Condition<StreamTransform>(streamTransform -> streamTransform.getTransformType() == StreamTransformType.UPDATE_STREAM,
            "Check update"));
  }

  @Test
  void testGetCatalogDiffWithBothInvalidSchema() throws IOException {
    final JsonNode schema1 = Jsons.deserialize(MoreResources.readResource(COMPANIES_INVALID));
    final JsonNode schema2 = Jsons.deserialize(MoreResources.readResource(COMPANIES_INVALID));
    final AirbyteCatalog catalog1 = new AirbyteCatalog().withStreams(List.of(
        new AirbyteStream().withName(USERS).withJsonSchema(schema1)));
    final AirbyteCatalog catalog2 = new AirbyteCatalog().withStreams(List.of(
        new AirbyteStream().withName(USERS).withJsonSchema(schema2)));

    final Set<StreamTransform> actualDiff = CatalogHelpers.getCatalogDiff(catalog1, catalog2);

    Assertions.assertThat(actualDiff).hasSize(0);
  }

}
