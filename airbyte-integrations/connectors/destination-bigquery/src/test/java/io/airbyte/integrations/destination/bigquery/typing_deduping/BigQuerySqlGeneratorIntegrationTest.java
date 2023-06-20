/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.bigquery.typing_deduping;

import static com.google.cloud.bigquery.LegacySQLTypeName.legacySQLTypeName;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.DatasetInfo;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Field.Mode;
import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableResult;
import io.airbyte.commons.json.Jsons;
import io.airbyte.integrations.destination.bigquery.BigQueryDestination;
import io.airbyte.integrations.destination.bigquery.BigQueryUtils;
import io.airbyte.integrations.destination.bigquery.typing_deduping.AirbyteType.AirbyteProtocolType;
import io.airbyte.integrations.destination.bigquery.typing_deduping.AirbyteType.Struct;
import io.airbyte.integrations.destination.bigquery.typing_deduping.CatalogParser.ParsedType;
import io.airbyte.integrations.destination.bigquery.typing_deduping.CatalogParser.StreamConfig;
import io.airbyte.integrations.destination.bigquery.typing_deduping.SqlGenerator.ColumnId;
import io.airbyte.integrations.destination.bigquery.typing_deduping.SqlGenerator.StreamId;
import io.airbyte.protocol.models.v0.DestinationSyncMode;
import io.airbyte.protocol.models.v0.SyncMode;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.commons.text.StringSubstitutor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO this proooobably belongs in test-integration? make sure to update the build.gradle
// concurrent runner stuff if you do this
@Execution(ExecutionMode.CONCURRENT)
public class BigQuerySqlGeneratorIntegrationTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(BigQuerySqlGeneratorIntegrationTest.class);
  private static final BigQuerySqlGenerator GENERATOR = new BigQuerySqlGenerator();
  public static final ColumnId CURSOR = GENERATOR.buildColumnId("updated_at");
  public static final List<ColumnId> PRIMARY_KEY = List.of(GENERATOR.buildColumnId("id"));
  public static final String QUOTE = "`";
  private static final LinkedHashMap<ColumnId, ParsedType<StandardSQLTypeName>> COLUMNS;
  private static final LinkedHashMap<ColumnId, ParsedType<StandardSQLTypeName>> CDC_COLUMNS;

  private static BigQuery bq;

  private String testDataset;
  private StreamId streamId;

  static {
    COLUMNS = new LinkedHashMap<>();
    COLUMNS.put(GENERATOR.buildColumnId("id"), new ParsedType<>(StandardSQLTypeName.INT64, AirbyteProtocolType.INTEGER));
    COLUMNS.put(GENERATOR.buildColumnId("updated_at"), new ParsedType<>(StandardSQLTypeName.TIMESTAMP, AirbyteProtocolType.TIMESTAMP_WITH_TIMEZONE));
    COLUMNS.put(GENERATOR.buildColumnId("name"), new ParsedType<>(StandardSQLTypeName.STRING, AirbyteProtocolType.STRING));

    LinkedHashMap<String, AirbyteType> addressProperties = new LinkedHashMap<>();
    addressProperties.put("city", AirbyteProtocolType.STRING);
    addressProperties.put("state", AirbyteProtocolType.STRING);
    COLUMNS.put(GENERATOR.buildColumnId("address"), new ParsedType<>(StandardSQLTypeName.JSON, new Struct(addressProperties)));

    COLUMNS.put(GENERATOR.buildColumnId("age"), new ParsedType<>(StandardSQLTypeName.INT64, AirbyteProtocolType.INTEGER));

    CDC_COLUMNS = new LinkedHashMap<>();
    CDC_COLUMNS.put(GENERATOR.buildColumnId("id"), new ParsedType<>(StandardSQLTypeName.INT64, AirbyteProtocolType.INTEGER));
    CDC_COLUMNS.put(GENERATOR.buildColumnId("_ab_cdc_lsn"), new ParsedType<>(StandardSQLTypeName.INT64, AirbyteProtocolType.INTEGER));
    CDC_COLUMNS.put(GENERATOR.buildColumnId("_ab_cdc_deleted_at"),
        new ParsedType<>(StandardSQLTypeName.TIMESTAMP, AirbyteProtocolType.TIMESTAMP_WITH_TIMEZONE));
    CDC_COLUMNS.put(GENERATOR.buildColumnId("name"), new ParsedType<>(StandardSQLTypeName.STRING, AirbyteProtocolType.STRING));
    // This is a bit unrealistic - DB sources don't actually declare explicit properties in their JSONB
    // columns, and JSONB isn't necessarily a Struct anyway.
    CDC_COLUMNS.put(GENERATOR.buildColumnId("address"), new ParsedType<>(StandardSQLTypeName.JSON, new Struct(addressProperties)));
    CDC_COLUMNS.put(GENERATOR.buildColumnId("age"), new ParsedType<>(StandardSQLTypeName.INT64, AirbyteProtocolType.INTEGER));
  }

  @BeforeAll
  public static void setup() throws Exception {
    String rawConfig = Files.readString(Path.of("secrets/credentials-gcs-staging.json"));
    JsonNode config = Jsons.deserialize(rawConfig);

    final BigQueryOptions.Builder bigQueryBuilder = BigQueryOptions.newBuilder();
    final GoogleCredentials credentials = BigQueryDestination.getServiceAccountCredentials(config);
    bq = bigQueryBuilder
        .setProjectId(config.get("project_id").asText())
        .setCredentials(credentials)
        .setHeaderProvider(BigQueryUtils.getHeaderProvider())
        .build()
        .getService();
  }

  @BeforeEach
  public void setupDataset() {
    testDataset = "bq_sql_generator_test_" + UUID.randomUUID().toString().replace("-", "_");
    // This is not a typical stream ID would look like, but we're just using this to isolate our tests
    // to a specific dataset.
    // In practice, the final table would be testDataset.users, and the raw table would be
    // airbyte.testDataset_users.
    streamId = new StreamId(testDataset, "users_final", testDataset, "users_raw", testDataset, "users_final");
    LOGGER.info("Running in dataset {}", testDataset);

    bq.create(DatasetInfo.newBuilder(testDataset)
        // This unfortunately doesn't delete the actual dataset after 3 days, but at least we can clear out
        // the tables if the AfterEach is skipped.
        .setDefaultTableLifetime(Duration.ofDays(3).toMillis())
        .build());
  }

  @AfterEach
  public void teardownDataset() {
    bq.delete(testDataset, BigQuery.DatasetDeleteOption.deleteContents());
  }

  @Test
  public void testCreateTableIncremental() throws InterruptedException {
    final String sql = GENERATOR.createTable(incrementalDedupStreamConfig(), "");
    logAndExecute(sql);

    final Table table = bq.getTable(testDataset, "users_final");
    // The table should exist
    assertNotNull(table);
    final Schema schema = table.getDefinition().getSchema();
    // And we should know exactly what columns it contains
    assertEquals(
        // Would be nice to assert directly against StandardSQLTypeName, but bigquery returns schemas of
        // LegacySQLTypeName. So we have to translate.
        Schema.of(
            Field.newBuilder("_airbyte_raw_id", legacySQLTypeName(StandardSQLTypeName.STRING)).setMode(Mode.REQUIRED).build(),
            Field.newBuilder("_airbyte_extracted_at", legacySQLTypeName(StandardSQLTypeName.TIMESTAMP)).setMode(Mode.REQUIRED).build(),
            Field.newBuilder("_airbyte_meta", legacySQLTypeName(StandardSQLTypeName.JSON)).setMode(Mode.REQUIRED).build(),
            Field.of("id", legacySQLTypeName(StandardSQLTypeName.INT64)),
            Field.of("updated_at", legacySQLTypeName(StandardSQLTypeName.TIMESTAMP)),
            Field.of("name", legacySQLTypeName(StandardSQLTypeName.STRING)),
            Field.of("address", legacySQLTypeName(StandardSQLTypeName.JSON)),
            Field.of("age", legacySQLTypeName(StandardSQLTypeName.INT64))),
        schema);
    // TODO this should assert partitioning/clustering configs
  }

  @Test
  public void testVerifyPrimaryKeysIncremental() throws InterruptedException {
    createRawTable();
    bq.query(QueryJobConfiguration.newBuilder(
        new StringSubstitutor(Map.of(
            "dataset", testDataset)).replace("""
                                             INSERT INTO ${dataset}.users_raw (`_airbyte_data`, `_airbyte_raw_id`, `_airbyte_extracted_at`) VALUES
                                               (JSON'{}', '10d6e27d-ae7a-41b5-baf8-c4c277ef9c11', '2023-01-01T00:00:00Z'),
                                               (JSON'{"id": 1}', '5ce60e70-98aa-4fe3-8159-67207352c4f0', '2023-01-01T00:00:00Z');
                                             """))
        .build());

    // This variable is declared outside of the transaction, so we need to do it manually here
    final String sql = "DECLARE missing_pk_count INT64;" + GENERATOR.validatePrimaryKeys(streamId, List.of(new ColumnId("id", "id", "id")), COLUMNS);
    final BigQueryException e = assertThrows(
        BigQueryException.class,
        () -> logAndExecute(sql));

    assertTrue(e.getError().getMessage().startsWith("Raw table has 1 rows missing a primary key at"),
        "Message was actually: " + e.getError().getMessage());
  }

  @Test
  public void testInsertNewRecordsIncremental() throws InterruptedException {
    createRawTable();
    createFinalTable();
    bq.query(QueryJobConfiguration.newBuilder(
        new StringSubstitutor(Map.of(
            "dataset", testDataset)).replace(
                """
                INSERT INTO ${dataset}.users_raw (`_airbyte_data`, `_airbyte_raw_id`, `_airbyte_extracted_at`) VALUES
                  (JSON'{"id": 1, "name": "Alice", "address": {"city": "San Francisco", "state": "CA"}, "updated_at": "2023-01-01T01:00:00Z"}', '972fa08a-aa06-4b91-a6af-a371aee4cb1c', '2023-01-01T00:00:00Z'),
                  (JSON'{"id": 1, "name": "Alice", "address": {"city": "San Diego", "state": "CA"}, "updated_at": "2023-01-01T02:00:00Z"}', '233ad43d-de50-4a47-bbe6-7a417ce60d9d', '2023-01-01T00:00:00Z'),
                  (JSON'{"id": 2, "name": "Bob", "age": "oops", "updated_at": "2023-01-01T03:00:00Z"}', 'd4aeb036-2d95-4880-acd2-dc69b42b03c6', '2023-01-01T00:00:00Z');
                """))
        .build());

    final String sql = GENERATOR.insertNewRecords(streamId, "", COLUMNS);
    logAndExecute(sql);

    final TableResult result = bq.query(QueryJobConfiguration.newBuilder("SELECT * FROM " + streamId.finalTableId(QUOTE)).build());

    assertQueryResult(
        List.of(
            Map.of(
                "id", Optional.of(1L),
                "name", Optional.of("Alice"),
                "address", Optional.of(Jsons.deserialize("""
                    {"city": "San Francisco", "state": "CA"}
                    """)),
                "age", Optional.empty(),
                "updated_at", Optional.of(Instant.parse("2023-01-01T01:00:00Z")),
                "_airbyte_extracted_at", Optional.of(Instant.parse("2023-01-01T00:00:00Z")),
                "_airbyte_meta", Optional.of(Jsons.deserialize("""
                    {"errors":[]}
                    """))
            ),
            Map.of(
                "id", Optional.of(1L),
                "name", Optional.of("Alice"),
                "address", Optional.of(Jsons.deserialize("""
                    {"city": "San Diego", "state": "CA"}
                    """)),
                "age", Optional.empty(),
                "updated_at", Optional.of(Instant.parse("2023-01-01T02:00:00Z")),
                "_airbyte_extracted_at", Optional.of(Instant.parse("2023-01-01T00:00:00Z")),
                "_airbyte_meta", Optional.of(Jsons.deserialize("""
                    {"errors":[]}"""))
            ),
            Map.of(
                "id", Optional.of(2L),
                "name", Optional.of("Bob"),
                "address", Optional.empty(),
                "updated_at", Optional.of(Instant.parse("2023-01-01T03:00:00Z")),
                "_airbyte_extracted_at", Optional.of(Instant.parse("2023-01-01T00:00:00Z")),
                "_airbyte_meta", Optional.of(Jsons.deserialize("""
                    {"errors":["Problem with `age`"]}
                    """))
            )
        ),
        result
    );
  }

  @Test
  public void testDedupFinalTable() throws InterruptedException {
    createRawTable();
    createFinalTable();
    bq.query(QueryJobConfiguration.newBuilder(
        new StringSubstitutor(Map.of(
            "dataset", testDataset)).replace(
                """
                INSERT INTO ${dataset}.users_raw (`_airbyte_data`, `_airbyte_raw_id`, `_airbyte_extracted_at`) VALUES
                  (JSON'{"id": 1, "name": "Alice", "address": {"city": "San Francisco", "state": "CA"}, "age": 42, "updated_at": "2023-01-01T01:00:00Z"}', 'd7b81af0-01da-4846-a650-cc398986bc99', '2023-01-01T00:00:00Z'),
                  (JSON'{"id": 1, "name": "Alice", "address": {"city": "San Diego", "state": "CA"}, "age": 84, "updated_at": "2023-01-01T02:00:00Z"}', '80c99b54-54b4-43bd-b51b-1f67dafa2c52', '2023-01-01T00:00:00Z'),
                  (JSON'{"id": 2, "name": "Bob", "age": "oops", "updated_at": "2023-01-01T03:00:00Z"}', 'ad690bfb-c2c2-4172-bd73-a16c86ccbb67', '2023-01-01T00:00:00Z');

                INSERT INTO ${dataset}.users_final (_airbyte_raw_id, _airbyte_extracted_at, _airbyte_meta, id, updated_at, name, address, age) values
                  ('d7b81af0-01da-4846-a650-cc398986bc99', '2023-01-01T00:00:00Z', JSON'{"errors":[]}', 1, '2023-01-01T01:00:00Z', 'Alice', JSON'{"city": "San Francisco", "state": "CA"}', 42),
                  ('80c99b54-54b4-43bd-b51b-1f67dafa2c52', '2023-01-01T00:00:00Z', JSON'{"errors":[]}', 1, '2023-01-01T02:00:00Z', 'Alice', JSON'{"city": "San Diego", "state": "CA"}', 84),
                  ('ad690bfb-c2c2-4172-bd73-a16c86ccbb67', '2023-01-01T00:00:00Z', JSON'{"errors": ["blah blah age"]}', 2, '2023-01-01T03:00:00Z', 'Bob', NULL, NULL);
                """))
        .build());

    final String sql = GENERATOR.dedupFinalTable(streamId, "", PRIMARY_KEY, CURSOR, COLUMNS);
    logAndExecute(sql);

    // TODO more stringent asserts
    final long finalRows = bq.query(QueryJobConfiguration.newBuilder("SELECT * FROM " + streamId.finalTableId(QUOTE)).build()).getTotalRows();
    assertEquals(2, finalRows);
  }

  @Test
  public void testDedupRawTable() throws InterruptedException {
    createRawTable();
    createFinalTable();
    bq.query(QueryJobConfiguration.newBuilder(
        new StringSubstitutor(Map.of(
            "dataset", testDataset)).replace(
                """
                INSERT INTO ${dataset}.users_raw (`_airbyte_data`, `_airbyte_raw_id`, `_airbyte_extracted_at`) VALUES
                  (JSON'{"id": 1, "name": "Alice", "address": {"city": "San Francisco", "state": "CA"}, "age": 42, "updated_at": "2023-01-01T01:00:00Z"}', 'd7b81af0-01da-4846-a650-cc398986bc99', '2023-01-01T00:00:00Z'),
                  (JSON'{"id": 1, "name": "Alice", "address": {"city": "San Diego", "state": "CA"}, "age": 84, "updated_at": "2023-01-01T02:00:00Z"}', '80c99b54-54b4-43bd-b51b-1f67dafa2c52', '2023-01-01T00:00:00Z'),
                  (JSON'{"id": 2, "name": "Bob", "age": "oops", "updated_at": "2023-01-01T03:00:00Z"}', 'ad690bfb-c2c2-4172-bd73-a16c86ccbb67', '2023-01-01T00:00:00Z');

                INSERT INTO ${dataset}.users_final (_airbyte_raw_id, _airbyte_extracted_at, _airbyte_meta, id, updated_at, name, address, age) values
                  ('80c99b54-54b4-43bd-b51b-1f67dafa2c52', '2023-01-01T00:00:00Z', JSON'{"errors":[]}', 1, '2023-01-01T02:00:00Z', 'Alice', JSON'{"city": "San Diego", "state": "CA"}', 84),
                  ('ad690bfb-c2c2-4172-bd73-a16c86ccbb67', '2023-01-01T00:00:00Z', JSON'{"errors": ["blah blah age"]}', 2, '2023-01-01T03:00:00Z', 'Bob', NULL, NULL);
                """))
        .build());

    final String sql = GENERATOR.dedupRawTable(streamId, "", CDC_COLUMNS);
    logAndExecute(sql);

    // TODO more stringent asserts
    final long rawRows = bq.query(QueryJobConfiguration.newBuilder("SELECT * FROM " + streamId.rawTableId(QUOTE)).build()).getTotalRows();
    assertEquals(2, rawRows);
  }

  @Test
  public void testCommitRawTable() throws InterruptedException {
    createRawTable();
    bq.query(QueryJobConfiguration.newBuilder(
        new StringSubstitutor(Map.of(
            "dataset", testDataset)).replace(
                """
                INSERT INTO ${dataset}.users_raw (`_airbyte_data`, `_airbyte_raw_id`, `_airbyte_extracted_at`) VALUES
                  (JSON'{"id": 1, "name": "Alice", "address": {"city": "San Diego", "state": "CA"}, "age": 84, "updated_at": "2023-01-01T02:00:00Z"}', '80c99b54-54b4-43bd-b51b-1f67dafa2c52', '2023-01-01T00:00:00Z'),
                  (JSON'{"id": 2, "name": "Bob", "age": "oops", "updated_at": "2023-01-01T03:00:00Z"}', 'ad690bfb-c2c2-4172-bd73-a16c86ccbb67', '2023-01-01T00:00:00Z');
                """))
        .build());

    final String sql = GENERATOR.commitRawTable(streamId);
    logAndExecute(sql);

    // TODO more stringent asserts
    final long rawUntypedRows = bq.query(QueryJobConfiguration.newBuilder(
        "SELECT * FROM " + streamId.rawTableId(QUOTE) + " WHERE _airbyte_loaded_at IS NULL").build()).getTotalRows();
    assertEquals(0, rawUntypedRows);
  }

  // TODO some of these test cases don't actually need a suffix. Figure out which ones make sense and
  // which ones don't.
  @Test
  public void testFullUpdateIncrementalDedup() throws InterruptedException {
    createRawTable();
    createFinalTable("_foo");
    bq.query(QueryJobConfiguration.newBuilder(
        new StringSubstitutor(Map.of(
            "dataset", testDataset)).replace(
                """
                INSERT INTO ${dataset}.users_raw (`_airbyte_data`, `_airbyte_raw_id`, `_airbyte_extracted_at`) VALUES
                  (JSON'{"id": 1, "name": "Alice", "address": {"city": "San Francisco", "state": "CA"}, "age": 42, "updated_at": "2023-01-01T01:00:00Z"}', 'd7b81af0-01da-4846-a650-cc398986bc99', '2023-01-01T00:00:00Z'),
                  (JSON'{"id": 1, "name": "Alice", "address": {"city": "San Diego", "state": "CA"}, "age": 84, "updated_at": "2023-01-01T02:00:00Z"}', '80c99b54-54b4-43bd-b51b-1f67dafa2c52', '2023-01-01T00:00:00Z'),
                  (JSON'{"id": 2, "name": "Bob", "age": "oops", "updated_at": "2023-01-01T03:00:00Z"}', 'ad690bfb-c2c2-4172-bd73-a16c86ccbb67', '2023-01-01T00:00:00Z');
                """))
        .build());

    final String sql = GENERATOR.updateTable("_foo", incrementalDedupStreamConfig());
    logAndExecute(sql);

    // TODO
    final long finalRows = bq.query(QueryJobConfiguration.newBuilder("SELECT * FROM " + streamId.finalTableId("_foo", QUOTE)).build()).getTotalRows();
    assertEquals(2, finalRows);
    final long rawRows = bq.query(QueryJobConfiguration.newBuilder("SELECT * FROM " + streamId.rawTableId(QUOTE)).build()).getTotalRows();
    assertEquals(2, rawRows);
    final long rawUntypedRows = bq.query(QueryJobConfiguration.newBuilder(
        "SELECT * FROM " + streamId.rawTableId(QUOTE) + " WHERE _airbyte_loaded_at IS NULL").build()).getTotalRows();
    assertEquals(0, rawUntypedRows);
  }

  @Test
  public void testFullUpdateIncrementalAppend() throws InterruptedException {
    createRawTable();
    createFinalTable("_foo");
    bq.query(QueryJobConfiguration.newBuilder(
        new StringSubstitutor(Map.of(
            "dataset", testDataset)).replace(
                """
                INSERT INTO ${dataset}.users_raw (`_airbyte_data`, `_airbyte_raw_id`, `_airbyte_extracted_at`) VALUES
                  (JSON'{"id": 1, "name": "Alice", "address": {"city": "San Francisco", "state": "CA"}, "age": 42, "updated_at": "2023-01-01T01:00:00Z"}', 'd7b81af0-01da-4846-a650-cc398986bc99', '2023-01-01T00:00:00Z'),
                  (JSON'{"id": 1, "name": "Alice", "address": {"city": "San Diego", "state": "CA"}, "age": 84, "updated_at": "2023-01-01T02:00:00Z"}', '80c99b54-54b4-43bd-b51b-1f67dafa2c52', '2023-01-01T00:00:00Z'),
                  (JSON'{"id": 2, "name": "Bob", "age": "oops", "updated_at": "2023-01-01T03:00:00Z"}', 'ad690bfb-c2c2-4172-bd73-a16c86ccbb67', '2023-01-01T00:00:00Z');
                """))
        .build());

    final String sql = GENERATOR.updateTable("_foo", incrementalAppendStreamConfig());
    logAndExecute(sql);

    // TODO
    final long finalRows = bq.query(QueryJobConfiguration.newBuilder("SELECT * FROM " + streamId.finalTableId("_foo", QUOTE)).build()).getTotalRows();
    assertEquals(3, finalRows);
    final long rawRows = bq.query(QueryJobConfiguration.newBuilder("SELECT * FROM " + streamId.rawTableId(QUOTE)).build()).getTotalRows();
    assertEquals(3, rawRows);
    final long rawUntypedRows = bq.query(QueryJobConfiguration.newBuilder(
        "SELECT * FROM " + streamId.rawTableId(QUOTE) + " WHERE _airbyte_loaded_at IS NULL").build()).getTotalRows();
    assertEquals(0, rawUntypedRows);
  }

  // This is also effectively the full refresh overwrite test case.
  // In the overwrite case, we rely on the destination connector to tell us to write to a final table
  // with a _tmp suffix, and then call overwriteFinalTable at the end of the sync.
  @Test
  public void testFullUpdateFullRefreshAppend() throws InterruptedException {
    createRawTable();
    createFinalTable("_foo");
    bq.query(QueryJobConfiguration.newBuilder(
        new StringSubstitutor(Map.of(
            "dataset", testDataset)).replace(
                """
                INSERT INTO ${dataset}.users_raw (`_airbyte_data`, `_airbyte_raw_id`, `_airbyte_extracted_at`) VALUES
                  (JSON'{"id": 1, "name": "Alice", "address": {"city": "San Francisco", "state": "CA"}, "age": 42, "updated_at": "2023-01-01T01:00:00Z"}', 'd7b81af0-01da-4846-a650-cc398986bc99', '2023-01-01T00:00:00Z'),
                  (JSON'{"id": 1, "name": "Alice", "address": {"city": "San Diego", "state": "CA"}, "age": 84, "updated_at": "2023-01-01T02:00:00Z"}', '80c99b54-54b4-43bd-b51b-1f67dafa2c52', '2023-01-01T00:00:00Z'),
                  (JSON'{"id": 2, "name": "Bob", "age": "oops", "updated_at": "2023-01-01T03:00:00Z"}', 'ad690bfb-c2c2-4172-bd73-a16c86ccbb67', '2023-01-01T00:00:00Z');

                INSERT INTO ${dataset}.users_final_foo (_airbyte_raw_id, _airbyte_extracted_at, _airbyte_meta, id, updated_at, name, address, age) values
                  ('64f4390f-3da1-4b65-b64a-a6c67497f18d', '2022-12-31T00:00:00Z', JSON'{"errors": []}', 1, '2022-12-31T00:00:00Z', 'Alice', NULL, NULL);
                """))
        .build());

    final String sql = GENERATOR.updateTable("_foo", fullRefreshAppendStreamConfig());
    logAndExecute(sql);

    // TODO
    final long finalRows = bq.query(QueryJobConfiguration.newBuilder("SELECT * FROM " + streamId.finalTableId("_foo", QUOTE)).build()).getTotalRows();
    assertEquals(4, finalRows);
    final long rawRows = bq.query(QueryJobConfiguration.newBuilder("SELECT * FROM " + streamId.rawTableId(QUOTE)).build()).getTotalRows();
    assertEquals(3, rawRows);
    final long rawUntypedRows = bq.query(QueryJobConfiguration.newBuilder(
        "SELECT * FROM " + streamId.rawTableId(QUOTE) + " WHERE _airbyte_loaded_at IS NULL").build()).getTotalRows();
    assertEquals(0, rawUntypedRows);
  }

  @Test
  public void testRenameFinalTable() throws InterruptedException {
    createFinalTable("_tmp");

    final String sql = GENERATOR.overwriteFinalTable("_tmp", fullRefreshOverwriteStreamConfig()).get();
    logAndExecute(sql);

    final Table table = bq.getTable(testDataset, "users_final");
    // TODO this should assert table schema + partitioning/clustering configs
    assertNotNull(table);
  }

  @Test
  public void testCdcUpdate() throws InterruptedException {
    createRawTable();
    createFinalTableCdc();
    bq.query(QueryJobConfiguration.newBuilder(
        new StringSubstitutor(Map.of(
            "dataset", testDataset)).replace(
                """
                -- records from a previous sync
                INSERT INTO ${dataset}.users_raw (`_airbyte_data`, `_airbyte_raw_id`, `_airbyte_extracted_at`, `_airbyte_loaded_at`) VALUES
                  (JSON'{"id": 1, "_ab_cdc_lsn": 10000, "name": "spooky ghost"}', '64f4390f-3da1-4b65-b64a-a6c67497f18d', '2022-12-31T00:00:00Z', '2022-12-31T00:00:01Z'),
                  (JSON'{"id": 0, "_ab_cdc_lsn": 9999, "name": "zombie", "_ab_cdc_deleted_at": "2022-12-31T00:O0:00Z"}', generate_uuid(), '2022-12-31T00:00:00Z', '2022-12-31T00:00:01Z');
                INSERT INTO ${dataset}.users_final (_airbyte_raw_id, _airbyte_extracted_at, _airbyte_meta, id, _ab_cdc_lsn, name, address, age) values
                  ('64f4390f-3da1-4b65-b64a-a6c67497f18d', '2022-12-31T00:00:00Z', JSON'{}', 1, 1000, 'spooky ghost', NULL, NULL);

                -- new records from the current sync
                INSERT INTO ${dataset}.users_raw (`_airbyte_data`, `_airbyte_raw_id`, `_airbyte_extracted_at`) VALUES
                  (JSON'{"id": 2, "_ab_cdc_lsn": 10001, "name": "alice"}', generate_uuid(), '2023-01-01T00:00:00Z'),
                  (JSON'{"id": 2, "_ab_cdc_lsn": 10002, "name": "alice2"}', generate_uuid(), '2023-01-01T00:00:00Z'),
                  (JSON'{"id": 3, "_ab_cdc_lsn": 10003, "name": "bob"}', generate_uuid(), '2023-01-01T00:00:00Z'),
                  (JSON'{"id": 1, "_ab_cdc_lsn": 10004, "_ab_cdc_deleted_at": "2022-12-31T23:59:59Z"}', generate_uuid(), '2023-01-01T00:00:00Z'),
                  (JSON'{"id": 0, "_ab_cdc_lsn": 10005, "name": "zombie_returned"}', generate_uuid(), '2023-01-01T00:00:00Z');
                """))
        .build());

    final String sql = GENERATOR.updateTable("", cdcStreamConfig());
    logAndExecute(sql);

    // TODO
    final long finalRows = bq.query(QueryJobConfiguration.newBuilder("SELECT * FROM " + streamId.finalTableId("", QUOTE)).build()).getTotalRows();
    assertEquals(3, finalRows);
    final long rawRows = bq.query(QueryJobConfiguration.newBuilder("SELECT * FROM " + streamId.rawTableId(QUOTE)).build()).getTotalRows();
    /*
     * Explanation: id=0 has two raw records (the old deletion record + zombie_returned) id=1 has one
     * raw record (the new deletion record; the old raw record was deleted) id=2 has one raw record (the
     * newer alice2 record) id=3 has one raw record
     */
    assertEquals(5, rawRows);
    final long rawUntypedRows = bq.query(QueryJobConfiguration.newBuilder(
        "SELECT * FROM " + streamId.rawTableId(QUOTE) + " WHERE _airbyte_loaded_at IS NULL").build()).getTotalRows();
    assertEquals(0, rawUntypedRows);
  }

  /**
   * source operations:
   * <ol>
   * <li>insert id=1 (lsn 10000)</li>
   * <li>delete id=1 (lsn 10001)</li>
   * </ol>
   * <p>
   * But the destination writes lsn 10001 before 10000. We should still end up with no records in the
   * final table.
   * <p>
   * All records have the same emitted_at timestamp. This means that we live or die purely based on
   * our ability to use _ab_cdc_lsn.
   */
  @Test
  public void testCdcOrdering_updateAfterDelete() throws InterruptedException {
    createRawTable();
    createFinalTableCdc();
    bq.query(QueryJobConfiguration.newBuilder(
        new StringSubstitutor(Map.of(
            "dataset", testDataset)).replace(
                """
                -- Write raw deletion record from the first batch, which resulted in an empty final table.
                -- Note the non-null loaded_at - this is to simulate that we previously ran T+D on this record.
                INSERT INTO ${dataset}.users_raw (`_airbyte_data`, `_airbyte_raw_id`, `_airbyte_extracted_at`, `_airbyte_loaded_at`) VALUES
                  (JSON'{"id": 1, "_ab_cdc_lsn": 10001, "_ab_cdc_deleted_at": "2023-01-01T00:01:00Z"}', generate_uuid(), '2023-01-01T00:00:00Z', '2023-01-01T00:00:01Z');

                -- insert raw record from the second record batch - this is an outdated record that should be ignored.
                INSERT INTO ${dataset}.users_raw (`_airbyte_data`, `_airbyte_raw_id`, `_airbyte_extracted_at`) VALUES
                  (JSON'{"id": 1, "_ab_cdc_lsn": 10000, "name": "alice"}', generate_uuid(), '2023-01-01T00:00:00Z');
                """))
        .build());

    final String sql = GENERATOR.updateTable("", cdcStreamConfig());
    logAndExecute(sql);

    // TODO better asserts
    final long finalRows = bq.query(QueryJobConfiguration.newBuilder("SELECT * FROM " + streamId.finalTableId("", QUOTE)).build()).getTotalRows();
    assertEquals(0, finalRows);
    final long rawRows = bq.query(QueryJobConfiguration.newBuilder("SELECT * FROM " + streamId.rawTableId(QUOTE)).build()).getTotalRows();
    assertEquals(1, rawRows);
    final long rawUntypedRows = bq.query(QueryJobConfiguration.newBuilder(
        "SELECT * FROM " + streamId.rawTableId(QUOTE) + " WHERE _airbyte_loaded_at IS NULL").build()).getTotalRows();
    assertEquals(0, rawUntypedRows);
  }

  /**
   * source operations:
   * <ol>
   * <li>arbitrary history...</li>
   * <li>delete id=1 (lsn 10001)</li>
   * <li>reinsert id=1 (lsn 10002)</li>
   * </ol>
   * <p>
   * But the destination receives LSNs 10002 before 10001. In this case, we should keep the reinserted
   * record in the final table.
   * <p>
   * All records have the same emitted_at timestamp. This means that we live or die purely based on
   * our ability to use _ab_cdc_lsn.
   */
  @Test
  public void testCdcOrdering_insertAfterDelete() throws InterruptedException {
    createRawTable();
    createFinalTableCdc();
    bq.query(QueryJobConfiguration.newBuilder(
        new StringSubstitutor(Map.of(
            "dataset", testDataset)).replace(
                """
                -- records from the first batch
                INSERT INTO ${dataset}.users_raw (`_airbyte_data`, `_airbyte_raw_id`, `_airbyte_extracted_at`, `_airbyte_loaded_at`) VALUES
                  (JSON'{"id": 1, "_ab_cdc_lsn": 10002, "name": "alice_reinsert"}', '64f4390f-3da1-4b65-b64a-a6c67497f18d', '2023-01-01T00:00:00Z', '2023-01-01T00:00:01Z');
                INSERT INTO ${dataset}.users_final (_airbyte_raw_id, _airbyte_extracted_at, _airbyte_meta, id, _ab_cdc_lsn, name) values
                  ('64f4390f-3da1-4b65-b64a-a6c67497f18d', '2023-01-01T00:00:00Z', JSON'{}', 1, 10002, 'alice_reinsert');

                -- second record batch
                INSERT INTO ${dataset}.users_raw (`_airbyte_data`, `_airbyte_raw_id`, `_airbyte_extracted_at`) VALUES
                  (JSON'{"id": 1, "_ab_cdc_lsn": 10001, "_ab_cdc_deleted_at": "2023-01-01T00:01:00Z"}', generate_uuid(), '2023-01-01T00:00:00Z');
                """))
        .build());
    // Run the second round of typing and deduping. This should do nothing to the final table, because
    // the delete is outdated.
    final String sql = GENERATOR.updateTable("", cdcStreamConfig());
    logAndExecute(sql);

    final long finalRows = bq.query(QueryJobConfiguration.newBuilder("SELECT * FROM " + streamId.finalTableId("", QUOTE)).build()).getTotalRows();
    assertEquals(1, finalRows);
    final long rawRows = bq.query(QueryJobConfiguration.newBuilder("SELECT * FROM " + streamId.rawTableId(QUOTE)).build()).getTotalRows();
    assertEquals(2, rawRows);
    final long rawUntypedRows = bq.query(QueryJobConfiguration.newBuilder(
        "SELECT * FROM " + streamId.rawTableId(QUOTE) + " WHERE _airbyte_loaded_at IS NULL").build()).getTotalRows();
    assertEquals(0, rawUntypedRows);
  }

  private StreamConfig<StandardSQLTypeName> incrementalDedupStreamConfig() {
    return new StreamConfig<>(
        streamId,
        SyncMode.INCREMENTAL,
        DestinationSyncMode.APPEND_DEDUP,
        PRIMARY_KEY,
        Optional.of(CURSOR),
        COLUMNS);
  }

  private StreamConfig<StandardSQLTypeName> cdcStreamConfig() {
    return new StreamConfig<>(
        streamId,
        SyncMode.INCREMENTAL,
        DestinationSyncMode.APPEND_DEDUP,
        PRIMARY_KEY,
        // Much like the rest of this class - this is purely for test purposes. Real CDC cursors may not be
        // exactly the same as this.
        Optional.of(GENERATOR.buildColumnId("_ab_cdc_lsn")),
        CDC_COLUMNS);
  }

  private StreamConfig<StandardSQLTypeName> incrementalAppendStreamConfig() {
    return new StreamConfig<>(
        streamId,
        SyncMode.INCREMENTAL,
        DestinationSyncMode.APPEND,
        null,
        Optional.of(CURSOR),
        COLUMNS);
  }

  private StreamConfig<StandardSQLTypeName> fullRefreshAppendStreamConfig() {
    return new StreamConfig<>(
        streamId,
        SyncMode.FULL_REFRESH,
        DestinationSyncMode.APPEND,
        null,
        Optional.empty(),
        COLUMNS);
  }

  private StreamConfig<StandardSQLTypeName> fullRefreshOverwriteStreamConfig() {
    return new StreamConfig<>(
        streamId,
        SyncMode.FULL_REFRESH,
        DestinationSyncMode.OVERWRITE,
        null,
        Optional.empty(),
        COLUMNS);
  }

  // These are known-good methods for doing stuff with bigquery.
  // Some of them are identical to what the sql generator does, and that's intentional.
  private void createRawTable() throws InterruptedException {
    bq.query(QueryJobConfiguration.newBuilder(
        new StringSubstitutor(Map.of(
            "dataset", testDataset)).replace("""
                                             CREATE TABLE ${dataset}.users_raw (
                                               _airbyte_raw_id STRING NOT NULL,
                                               _airbyte_data JSON NOT NULL,
                                               _airbyte_extracted_at TIMESTAMP NOT NULL,
                                               _airbyte_loaded_at TIMESTAMP
                                             ) PARTITION BY (
                                               DATE_TRUNC(_airbyte_extracted_at, DAY)
                                             ) CLUSTER BY _airbyte_loaded_at;
                                             """))
        .build());
  }

  private void createFinalTable() throws InterruptedException {
    createFinalTable("");
  }

  private void createFinalTable(String suffix) throws InterruptedException {
    bq.query(QueryJobConfiguration.newBuilder(
        new StringSubstitutor(Map.of(
            "dataset", testDataset,
            "suffix", suffix)).replace("""
                                       CREATE TABLE ${dataset}.users_final${suffix} (
                                         _airbyte_raw_id STRING NOT NULL,
                                         _airbyte_extracted_at TIMESTAMP NOT NULL,
                                         _airbyte_meta JSON NOT NULL,
                                         id INT64,
                                         updated_at TIMESTAMP,
                                         name STRING,
                                         address JSON,
                                         age INT64
                                       )
                                       PARTITION BY (DATE_TRUNC(_airbyte_extracted_at, DAY))
                                       CLUSTER BY id, _airbyte_extracted_at;
                                       """))
        .build());
  }

  private void createFinalTableCdc() throws InterruptedException {
    bq.query(QueryJobConfiguration.newBuilder(
        new StringSubstitutor(Map.of(
            "dataset", testDataset)).replace("""
                                             CREATE TABLE ${dataset}.users_final (
                                               _airbyte_raw_id STRING NOT NULL,
                                               _airbyte_extracted_at TIMESTAMP NOT NULL,
                                               _airbyte_meta JSON NOT NULL,
                                               id INT64,
                                               _ab_cdc_deleted_at TIMESTAMP,
                                               _ab_cdc_lsn INT64,
                                               name STRING,
                                               address JSON,
                                               age INT64
                                             )
                                             PARTITION BY (DATE_TRUNC(_airbyte_extracted_at, DAY))
                                             CLUSTER BY id, _airbyte_extracted_at;
                                             """))
        .build());
  }

  private static void logAndExecute(final String sql) throws InterruptedException {
    LOGGER.info("Executing sql: {}", sql);
    bq.query(QueryJobConfiguration.newBuilder(sql).build());
  }

  private Map<String, Object> toMap(Schema schema, FieldValueList row) {
    final Map<String, Object> map = new HashMap<>();
    for (int i = 0; i < schema.getFields().size(); i++) {
      final Field field = schema.getFields().get(i);
      final FieldValue value = row.get(i);
      Object typedValue;
      if (value.getValue() == null) {
        typedValue = null;
      } else {
        typedValue = switch (field.getType().getStandardType()) {
          case BOOL -> value.getBooleanValue();
          case INT64 -> value.getLongValue();
          case FLOAT64 -> value.getDoubleValue();
          case NUMERIC, BIGNUMERIC -> value.getNumericValue();
          case STRING -> value.getStringValue();
          case BYTES -> value.getBytesValue();
          case TIMESTAMP -> value.getTimestampInstant();
          // value.getTimestampInstant() fails to parse these types
          case DATE, DATETIME, TIME -> value.getStringValue();
          // bigquery returns JSON columns as string; manually parse it into a JsonNode
          case JSON -> Jsons.deserialize(value.getStringValue());

          // Default case for weird types (struct, array, geography, interval)
          default -> value.getStringValue();
        };
      }
      map.put(field.getName(), typedValue);
    }
    return map;
  }

  private void assertQueryResult(final List<Map<String, Optional<Object>>> expectedRows, final TableResult result) {
    List<Map<String, Object>> actualRows = result.streamAll().map(row -> toMap(result.getSchema(), row)).toList();
    LOGGER.info("Got rows: {}", actualRows);
    List<Map<String, Optional<Object>>> missingRows = new ArrayList<>();
    Set<Map<String, Object>> matchedRows = new HashSet<>();
    boolean foundMultiMatch = false;
    // For each expected row, iterate through all actual rows to find a match.
    for (Map<String, Optional<Object>> expectedRow : expectedRows) {
      final List<Map<String, Object>> matchingRows = actualRows.stream().filter(actualRow -> {
        // We only want to check the fields that are specified in the expected row.
        // E.g.we shouldn't assert against randomized UUIDs.
        for (Entry<String, Optional<Object>> expectedEntry : expectedRow.entrySet()) {
          // If the expected value is empty, we just check that the actual value is null.
          if (expectedEntry.getValue().isEmpty()) {
            if (actualRow.get(expectedEntry.getKey()) != null) {
              // It wasn't null, so this actualRow doesn't match the expected row
              return false;
            } else {
              // It _was_ null, so we can move on the next key.
              continue;
            }
          }
          // If the expected value is non-empty, we check that the actual value matches.
          if (!expectedEntry.getValue().get().equals(actualRow.get(expectedEntry.getKey()))) {
            return false;
          }
        }
        return true;
      }).toList();

      if (matchingRows.size() == 0) {
        missingRows.add(expectedRow);
      } else if (matchingRows.size() > 1) {
        foundMultiMatch = true;
      }
      matchedRows.addAll(matchingRows);
    }

    boolean success = true;
    String errorMessage = "";

    if (foundMultiMatch) {
      success = false;
      // TODO is this true? E.g. what if we try to write the same row twice (because of a retry)? Are we guaranteed to have some differentiator?
      errorMessage += "Some expected rows appeared multiple times in the actual table. This is probably a bug in the test itself. \n";
    }
    if (!missingRows.isEmpty()) {
      success = false;
      final String missingRowsRendered = missingRows.stream()
          .map(Object::toString)
          .collect(Collectors.joining("\n"));
      errorMessage += "There were %d rows missing from the actual table:\n%s\n".formatted(missingRows.size(), missingRowsRendered);
    }
    if (matchedRows.size() != actualRows.size()) {
      success = false;
      final String extraRowsRendered = actualRows.stream()
          .filter(row -> !matchedRows.contains(row))
          .map(Object::toString)
          .collect(Collectors.joining("\n"));
      errorMessage += "There were %d rows in the actual table, which were not expected:\n%s\n".formatted(
          actualRows.size() - matchedRows.size(),
          extraRowsRendered);
    }
    assertTrue(success, errorMessage);
  }

}
