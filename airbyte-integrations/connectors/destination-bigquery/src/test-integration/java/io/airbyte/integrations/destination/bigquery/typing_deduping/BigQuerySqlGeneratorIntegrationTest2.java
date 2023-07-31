package io.airbyte.integrations.destination.bigquery.typing_deduping;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.DatasetInfo;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableResult;
import io.airbyte.commons.json.Jsons;
import io.airbyte.integrations.base.destination.typing_deduping.BaseSqlGeneratorIntegrationTest;
import io.airbyte.integrations.base.destination.typing_deduping.StreamId;
import io.airbyte.integrations.destination.bigquery.BigQueryDestination;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.apache.commons.text.StringSubstitutor;

public class BigQuerySqlGeneratorIntegrationTest2 extends BaseSqlGeneratorIntegrationTest<TableDefinition> {

  private BigQuery bq;

  @Override
  protected JsonNode generateConfig() throws Exception {
    final String rawConfig = Files.readString(Path.of("secrets/credentials-gcs-staging.json"));
    final JsonNode config = Jsons.deserialize(rawConfig);
    bq = BigQueryDestination.getBigQuery(config);
    return config;
  }

  @Override
  protected BigQuerySqlGenerator getSqlGenerator() {
    return new BigQuerySqlGenerator("US");
  }

  @Override
  protected BigQueryDestinationHandler getDestinationHandler() {
    return new BigQueryDestinationHandler(bq, "US");
  }

  @Override
  protected void createNamespace(String namespace) {
    bq.create(DatasetInfo.newBuilder(namespace)
        // This unfortunately doesn't delete the actual dataset after 3 days, but at least we'll clear out old tables automatically
        .setDefaultTableLifetime(Duration.ofDays(3).toMillis())
        .build());
  }

  @Override
  protected void createRawTable(StreamId streamId) throws InterruptedException {
    bq.query(QueryJobConfiguration.newBuilder(
            new StringSubstitutor(Map.of(
                "raw_table_id", streamId.rawTableId(BigQuerySqlGenerator.QUOTE))).replace(
                """
                    CREATE TABLE ${raw_table_id} (
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

  @Override
  protected void createFinalTable(boolean includeCdcDeletedAt, StreamId streamId, String suffix) throws InterruptedException {
    String cdcDeletedAt = includeCdcDeletedAt ? "`_ab_cdc_deleted_at` TIMESTAMP," : "";
    bq.query(QueryJobConfiguration.newBuilder(
            new StringSubstitutor(Map.of(
                "final_table_id", streamId.finalTableId(suffix, BigQuerySqlGenerator.QUOTE)),
                "cdc_deleted_at", cdcDeletedAt).replace(
                """
                    CREATE TABLE ${final_table_id} (
                      _airbyte_raw_id STRING NOT NULL,
                      _airbyte_extracted_at TIMESTAMP NOT NULL,
                      _airbyte_meta JSON NOT NULL,
                      `id1` INT64,
                      `id2` INT64,
                      `updated_at` TIMESTAMP,
                      ${cdc_deleted_at}
                      `struct` JSON,
                      `array` JSON,
                      `string` STRING,
                      `number` NUMERIC,
                      `integer` INT64,
                      `boolean` BOOL,
                      `timestamp_with_timezone` TIMESTAMP,
                      `timestamp_without_timezone` DATETIME,
                      `time_with_timezone` STRING,
                      `time_without_timezone` TIME,
                      `date` DATE,
                      `unknown` JSON
                    )
                    PARTITION BY (DATE_TRUNC(_airbyte_extracted_at, DAY))
                    CLUSTER BY id, _airbyte_extracted_at;
                    """))
        .build());
  }

  @Override
  protected void insertFinalTableRecords(StreamId streamId, String suffix, List<JsonNode> records) {

  }

  @Override
  protected void insertRawTableRecords(StreamId streamId, List<JsonNode> records) throws InterruptedException {
    bq.query(QueryJobConfiguration.newBuilder(
            new StringSubstitutor(Map.of(
                "raw_table_id", streamId.rawTableId(BigQuerySqlGenerator.QUOTE),
                "records", records.stream().map(Jsons::serialize).reduce((a, b) -> a + "," + b).orElse(""))).replace(
                """
                    INSERT INTO ${raw_table_id} (_airbyte_raw_id, _airbyte_data, _airbyte_extracted_at, _airbyte_loaded_at)
                    VALUES ${records};
                    """))
        .build());
  }

  @Override
  protected List<JsonNode> dumpRawTableRecords(StreamId streamId) throws Exception {
    TableResult result = bq.query(QueryJobConfiguration.of("SELECT * FROM " + streamId.rawTableId(BigQuerySqlGenerator.QUOTE)));
    return BigQuerySqlGeneratorIntegrationTest.toJsonRecords(result);
  }

  @Override
  protected List<JsonNode> dumpFinalTableRecords(StreamId streamId, String suffix) throws Exception {
    TableResult result = bq.query(QueryJobConfiguration.of("SELECT * FROM " + streamId.finalTableId(BigQuerySqlGenerator.QUOTE, suffix)));
    return BigQuerySqlGeneratorIntegrationTest.toJsonRecords(result);
  }

  @Override
  protected void teardownNamespace(String namespace) {
    bq.delete(namespace, BigQuery.DatasetDeleteOption.deleteContents());
  }
}
