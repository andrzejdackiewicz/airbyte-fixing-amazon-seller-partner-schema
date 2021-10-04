/*
 * Copyright (c) 2021 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.source.jdbc;

import com.fasterxml.jackson.databind.JsonNode;
import io.airbyte.db.jdbc.PostgresJdbcStreamingQueryConfiguration;
import io.airbyte.integrations.base.IntegrationRunner;
import io.airbyte.integrations.base.Source;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JdbcSource extends AbstractJdbcSource implements Source {

  private static final Logger LOGGER = LoggerFactory.getLogger(JdbcSource.class);

  public JdbcSource() {
    super("org.postgresql.Driver", new PostgresJdbcStreamingQueryConfiguration());
  }

  // no-op for JdbcSource since the config it receives is designed to be use for JDBC.
  @Override
  public JsonNode toDatabaseConfig(JsonNode config) {
    return config;
  }

  @Override
  public Set<String> getExcludedInternalNameSpaces() {
    return Set.of("information_schema", "pg_catalog", "pg_internal", "catalog_history");
  }

  public static void main(String[] args) throws Exception {
    final Source source = new JdbcSource();
    LOGGER.info("starting source: {}", JdbcSource.class);
    new IntegrationRunner(source).run(args);
    LOGGER.info("completed source: {}", JdbcSource.class);
  }

}
