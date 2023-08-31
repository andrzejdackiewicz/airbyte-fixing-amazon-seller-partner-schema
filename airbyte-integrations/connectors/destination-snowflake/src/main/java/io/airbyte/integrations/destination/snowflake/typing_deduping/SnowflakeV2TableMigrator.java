package io.airbyte.integrations.destination.snowflake.typing_deduping;

import static io.airbyte.integrations.base.JavaBaseConstants.DEFAULT_AIRBYTE_INTERNAL_NAMESPACE;
import static io.airbyte.integrations.destination.snowflake.SnowflakeInternalStagingDestination.RAW_SCHEMA_OVERRIDE;

import io.airbyte.db.jdbc.JdbcDatabase;
import io.airbyte.integrations.base.TypingAndDedupingFlag;
import io.airbyte.integrations.base.destination.typing_deduping.ColumnId;
import io.airbyte.integrations.base.destination.typing_deduping.StreamConfig;
import io.airbyte.integrations.base.destination.typing_deduping.StreamId;
import io.airbyte.integrations.base.destination.typing_deduping.V2TableMigrator;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Optional;

public class SnowflakeV2TableMigrator implements V2TableMigrator<SnowflakeTableDefinition> {
  private final JdbcDatabase database;
  private final String rawNamespace;
  private final String databaseName;
  private final SnowflakeSqlGenerator generator;
  private final SnowflakeDestinationHandler handler;

  public SnowflakeV2TableMigrator(final JdbcDatabase database, final String databaseName, final SnowflakeSqlGenerator generator, final SnowflakeDestinationHandler handler) {
    this.database = database;
    this.databaseName = databaseName;
    this.generator = generator;
    this.handler = handler;
    this.rawNamespace = TypingAndDedupingFlag.getRawNamespaceOverride(RAW_SCHEMA_OVERRIDE).orElse(DEFAULT_AIRBYTE_INTERNAL_NAMESPACE);
  }

  @Override
  public void migrateIfNecessary(final StreamConfig streamConfig) throws Exception {
    final StreamId lowercasedStreamId = buildStreamId_lowercase(
        streamConfig.id().originalNamespace(),
        streamConfig.id().originalName(),
        rawNamespace);
    final Optional<SnowflakeTableDefinition> existingTableLowercase = findExistingTable_lowercase(lowercasedStreamId);

    if (existingTableLowercase.isPresent()) {
      handler.execute(generator.softReset(streamConfig));
    }
  }

  // These methods were copied from https://github.com/airbytehq/airbyte/blob/d5fdb1b982d464f54941bf9a830b9684fb47d249/airbyte-integrations/connectors/destination-snowflake/src/main/java/io/airbyte/integrations/destination/snowflake/typing_deduping/SnowflakeSqlGenerator.java
  // which is the highest version of destination-snowflake that still uses quoted+lowercased identifiers
  private static StreamId buildStreamId_lowercase(final String namespace, final String name, final String rawNamespaceOverride) {
    // No escaping needed, as far as I can tell. We quote all our identifier names.
    return new StreamId(
        escapeIdentifier_lowercase(namespace),
        escapeIdentifier_lowercase(name),
        escapeIdentifier_lowercase(rawNamespaceOverride),
        escapeIdentifier_lowercase(StreamId.concatenateRawTableName(namespace, name)),
        namespace,
        name);
  }

  private static String escapeIdentifier_lowercase(final String identifier) {
    // Note that we don't need to escape backslashes here!
    // The only special character in an identifier is the double-quote, which needs to be doubled.
    return identifier.replace("\"", "\"\"");
  }

  // And this was taken from https://github.com/airbytehq/airbyte/blob/master/airbyte-integrations/connectors/destination-snowflake/src/main/java/io/airbyte/integrations/destination/snowflake/typing_deduping/SnowflakeDestinationHandler.java
  public Optional<SnowflakeTableDefinition> findExistingTable_lowercase(final StreamId id) throws SQLException {
    // The obvious database.getMetaData().getColumns() solution doesn't work, because JDBC translates
    // VARIANT as VARCHAR
    final LinkedHashMap<String, String> columns = database.queryJsons(
            """
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_catalog = ?
              AND table_schema = ?
              AND table_name = ?
            ORDER BY ordinal_position;
            """,
            databaseName.toUpperCase(),
            id.finalNamespace(),
            id.finalName()).stream()
        .collect(LinkedHashMap::new,
            (map, row) -> map.put(row.get("COLUMN_NAME").asText(), row.get("DATA_TYPE").asText()),
            LinkedHashMap::putAll);
    // TODO query for indexes/partitioning/etc

    if (columns.isEmpty()) {
      return Optional.empty();
    } else {
      return Optional.of(new SnowflakeTableDefinition(columns));
    }
  }

}
