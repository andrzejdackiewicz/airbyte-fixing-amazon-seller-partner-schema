/*
 * MIT License
 *
 * Copyright (c) 2020 Airbyte
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package io.airbyte.migrate.migrations;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.google.common.collect.ImmutableMap;
import io.airbyte.commons.functional.ListConsumer;
import io.airbyte.commons.resources.MoreResources;
import io.airbyte.commons.yaml.Yamls;
import io.airbyte.migrate.MigrationTestUtils;
import io.airbyte.migrate.MigrationUtils;
import io.airbyte.migrate.Migrations;
import io.airbyte.migrate.ResourceId;
import io.airbyte.migrate.ResourceType;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.junit.jupiter.api.Test;

public class MigrateV0_28_0Test {

  private static final String INPUT_CONFIG_PATH = "migrations/migrationV0_28_0/input_config";
  private static final String OUTPUT_CONFIG_PATH = "migrations/migrationV0_28_0/output_config";

  private static final ResourceId CONNECTION_RESOURCE_ID = ResourceId.fromConstantCase(ResourceType.CONFIG, "STANDARD_SYNC");
  private static final ResourceId SOURCE_RESOURCE_ID = ResourceId
      .fromConstantCase(ResourceType.CONFIG, "SOURCE_CONNECTION");
  private static final ResourceId OPERATION_RESOURCE_ID = ResourceId
      .fromConstantCase(ResourceType.CONFIG, "STANDARD_SYNC_OPERATION");

  private Stream<JsonNode> getResourceStream(String resourcePath) throws IOException {
    final ArrayNode nodeArray = (ArrayNode) Yamls
        .deserialize(MoreResources.readResource(resourcePath));
    return StreamSupport
        .stream(Spliterators.spliteratorUnknownSize(nodeArray.iterator(), 0), false);
  }

  @Test
  void testMigration() throws IOException {
    final MigrationV0_28_0 migration = (MigrationV0_28_0) Migrations.MIGRATIONS
        .stream()
        .filter(m -> m instanceof MigrationV0_28_0)
        .findAny()
        .orElse(null);
    assertNotNull(migration);

    final Map<ResourceId, Stream<JsonNode>> inputConfigs = ImmutableMap.of(
        CONNECTION_RESOURCE_ID,
        getResourceStream(INPUT_CONFIG_PATH + "/STANDARD_SYNC.yaml"),
        SOURCE_RESOURCE_ID,
        getResourceStream(INPUT_CONFIG_PATH + "/SOURCE_CONNECTION.yaml"),
        OPERATION_RESOURCE_ID,
        getResourceStream(INPUT_CONFIG_PATH + "/STANDARD_SYNC_OPERATION.yaml"));

    final Map<ResourceId, ListConsumer<JsonNode>> outputConsumer = MigrationTestUtils
        .createOutputConsumer(migration.getOutputSchema().keySet());

    migration.migrate(inputConfigs, MigrationUtils.mapRecordConsumerToConsumer(outputConsumer));

    final Map<ResourceId, List<JsonNode>> expectedOutputOverrides = ImmutableMap.of(
        CONNECTION_RESOURCE_ID,
        getResourceStream(INPUT_CONFIG_PATH + "/STANDARD_SYNC.yaml")
            .collect(Collectors.toList()),
        SOURCE_RESOURCE_ID,
        getResourceStream(INPUT_CONFIG_PATH + "/SOURCE_CONNECTION.yaml")
            .collect(Collectors.toList()),
        OPERATION_RESOURCE_ID,
        getResourceStream(OUTPUT_CONFIG_PATH + "/STANDARD_SYNC_OPERATION.yaml")
            .collect(Collectors.toList()));

    final Map<ResourceId, List<JsonNode>> expectedOutput = MigrationTestUtils
        .createExpectedOutput(migration.getOutputSchema().keySet(), expectedOutputOverrides);

    final Map<ResourceId, List<JsonNode>> outputAsList = MigrationTestUtils
        .collectConsumersToList(outputConsumer);

    assertEquals(expectedOutput, outputAsList);
  }

}
