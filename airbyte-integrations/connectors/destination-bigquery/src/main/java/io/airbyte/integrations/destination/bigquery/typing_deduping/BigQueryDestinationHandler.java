/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.bigquery.typing_deduping;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.JobStatistics;
import com.google.cloud.bigquery.JobException;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import io.airbyte.integrations.base.destination.typing_deduping.StreamConfig;
import io.airbyte.integrations.base.destination.typing_deduping.StreamId;
import java.util.Optional;
import java.util.UUID;

import io.airbyte.integrations.base.destination.typing_deduping.TableNotMigratedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO this stuff almost definitely exists somewhere else in our codebase.
public class BigQueryDestinationHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(BigQueryDestinationHandler.class);

  private final BigQuery bq;

  public BigQueryDestinationHandler(final BigQuery bq) {
    this.bq = bq;
  }

  public Optional<TableDefinition> findExistingTable(StreamId id) {
    final Table table = bq.getTable(id.finalNamespace(), id.finalName());
    return Optional.ofNullable(table).map(Table::getDefinition);
  }

  public Table getFinalTable(StreamId id) {
    return bq.getTable(TableId.of(id.finalNamespace(), id.finalName()));
  }

  public void execute(final String sql) throws InterruptedException {
    if ("".equals(sql)) {
      return;
    }
    final UUID queryId = UUID.randomUUID();
    LOGGER.info("Executing sql {}: {}", queryId, sql);

    Job job = bq.create(JobInfo.of(QueryJobConfiguration.newBuilder(sql).build()));
    job = job.waitFor();

    JobStatistics.QueryStatistics statistics = job.getStatistics();
    LOGGER.info("Completed sql {} in {} ms; processed {} bytes; billed for {} bytes",
        queryId,
        statistics.getEndTime() - statistics.getStartTime(),
        statistics.getTotalBytesProcessed(),
        statistics.getTotalBytesBilled());
  }

  public void prepareFinalTable(final BigQuerySqlGenerator sqlGenerator, final StreamConfig stream, final TableDefinition existingTable) {
    try {
      if (!sqlGenerator.existingSchemaMatchesStreamConfig(stream, existingTable)) {
        attemptSoftReset(sqlGenerator, stream);
      } else {
        LOGGER.info("Existing Schema matches expected schema, no alterations needed");
      }
    } catch (TableNotMigratedException nm) {
      throw new RuntimeException("Cannot complete destinations v2 sync, final table not migrated: %s".formatted(stream.id().finalName()));
    }
  }

  public void attemptSoftReset(final BigQuerySqlGenerator sqlGenerator, final StreamConfig stream) {
    LOGGER.info("Attempting Soft Reset for Stream {}", stream.id().finalName());
    sqlGenerator.softReset(stream).forEach(sql -> {
      try {
        execute(sql);
      } catch (InterruptedException | JobException ex) {
        throw new RuntimeException(ex);
      }
    });
  }

}
