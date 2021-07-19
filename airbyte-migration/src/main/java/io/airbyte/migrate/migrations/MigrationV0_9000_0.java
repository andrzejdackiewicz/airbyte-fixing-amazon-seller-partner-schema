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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.annotations.VisibleForTesting;
import io.airbyte.migrate.Migration;
import io.airbyte.migrate.MigrationUtils;
import io.airbyte.migrate.ResourceId;
import io.airbyte.migrate.ResourceType;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.Stream;

public class MigrationV0_9000_0 extends BaseMigration implements Migration {

  private static final UUID DEFAULT_WORKSPACE_ID = UUID.fromString("5ae6b09b-fdec-41af-aaf7-7d94cfc33ef6");

  protected static final ResourceId STANDARD_WORKSPACE_RESOURCE_ID = ResourceId
      .fromConstantCase(ResourceType.CONFIG, "STANDARD_WORKSPACE");
  protected static final ResourceId STANDARD_SYNC_OPERATION_RESOURCE_ID = ResourceId
      .fromConstantCase(ResourceType.CONFIG, "STANDARD_SYNC_OPERATION");

  private static final String MIGRATION_VERSION = "0.9000.0-alpha";
  @VisibleForTesting
  protected final Migration previousMigration;

  public MigrationV0_9000_0(Migration previousMigration) {
    super(previousMigration);
    this.previousMigration = previousMigration;
  }

  @Override
  public String getVersion() {
    return MIGRATION_VERSION;
  }

  private static final Path RESOURCE_PATH = Path.of("migrations/migrationV0_9000_0/");

  @Override
  public Map<ResourceId, JsonNode> getOutputSchema() {
    final Map<ResourceId, JsonNode> outputSchema = new HashMap<>(previousMigration.getOutputSchema());
    outputSchema.put(
        STANDARD_SYNC_OPERATION_RESOURCE_ID,
        MigrationUtils.getSchemaFromResourcePath(RESOURCE_PATH, STANDARD_SYNC_OPERATION_RESOURCE_ID));
    return outputSchema;
  }

  @Override
  public void migrate(Map<ResourceId, Stream<JsonNode>> inputData,
                      Map<ResourceId, Consumer<JsonNode>> outputData) {
    for (final Map.Entry<ResourceId, Stream<JsonNode>> entry : inputData.entrySet()) {
      final Consumer<JsonNode> recordConsumer = outputData.get(entry.getKey());

      entry.getValue().forEach(r -> {
        // as of this moment it is safe to assume that every instance of airbyte has a workspace with the
        // default uuid.
        if (entry.getKey().equals(STANDARD_SYNC_OPERATION_RESOURCE_ID)) {
          ((ObjectNode) r).put("workspaceId", DEFAULT_WORKSPACE_ID.toString());
        }

        recordConsumer.accept(r);
      });
    }
  }

}
