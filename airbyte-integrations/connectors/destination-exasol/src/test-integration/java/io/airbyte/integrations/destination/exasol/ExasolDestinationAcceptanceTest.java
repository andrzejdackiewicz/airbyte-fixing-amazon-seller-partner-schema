/*
 * Copyright (c) 2022 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.exasol;

import com.exasol.containers.ExasolContainer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableMap;
import io.airbyte.commons.json.Jsons;
import io.airbyte.db.Database;
import io.airbyte.db.factory.DSLContextFactory;
import io.airbyte.db.factory.DataSourceFactory;
import io.airbyte.db.factory.DatabaseDriver;
import io.airbyte.db.jdbc.DefaultJdbcDatabase;
import io.airbyte.db.jdbc.JdbcDatabase;
import io.airbyte.db.jdbc.JdbcUtils;
import io.airbyte.integrations.base.JavaBaseConstants;
import io.airbyte.integrations.destination.NamingConventionTransformer;
import io.airbyte.integrations.standardtest.destination.DestinationAcceptanceTest;
import org.jooq.DSLContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ExasolDestinationAcceptanceTest extends DestinationAcceptanceTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(ExasolDestinationAcceptanceTest.class);

  private static final ExasolContainer<? extends ExasolContainer<?>> EXASOL = new ExasolContainer<>()
          .withTemporaryCredentialsDirectory(Path.of("build/"))
          .withReuse(true);

  private final NamingConventionTransformer namingResolver = new ExasolSQLNameTransformer();
  private static JsonNode config;

  @BeforeAll
  static void startExasolContainer() {
    EXASOL.start();
    config = createExasolConfig(EXASOL);
  }

  private static JsonNode createExasolConfig(ExasolContainer<? extends ExasolContainer<?>> exasol) {
    return Jsons.jsonNode(ImmutableMap.builder()
            .put(JdbcUtils.HOST_KEY, exasol.getHost())
            .put(JdbcUtils.PORT_KEY, exasol.getFirstMappedDatabasePort())
            .put("certificateFingerprint", exasol.getTlsCertificateFingerprint().orElseThrow())
            .put(JdbcUtils.USERNAME_KEY, exasol.getUsername())
            .put(JdbcUtils.PASSWORD_KEY, exasol.getPassword())
            .put(JdbcUtils.SCHEMA_KEY, "TEST")
            .build());
  }

  @AfterAll
  static void stopExasolContainer() {
    EXASOL.stop();
  }

  @Override
  protected String getImageName() {
    return "airbyte/destination-exasol:dev";
  }

  @Override
  protected JsonNode getConfig() {
    return Jsons.clone(config);
  }

  @Override
  protected JsonNode getFailCheckConfig() {
    final JsonNode clone = Jsons.clone(getConfig());
    ((ObjectNode) clone).put(JdbcUtils.PASSWORD_KEY, "wrong password");
    return clone;
  }

  @Override
  protected List<JsonNode> retrieveRecords(TestDestinationEnv testEnv,
                                           String streamName,
                                           String namespace,
                                           JsonNode streamSchema) throws SQLException {
    return retrieveRecordsFromTable(namingResolver.getRawTableName(streamName), "\"_"+namespace+"_\"")
            .stream()
            .map(r -> r.get(JavaBaseConstants.COLUMN_NAME_DATA.toUpperCase()))
            .map(node -> Jsons.deserialize(node.asText()))
            .collect(Collectors.toList());
  }

  private List<JsonNode> retrieveRecordsFromTable(final String tableName, final String schemaName) throws SQLException {
    try (final DSLContext dslContext = getDSLContext(config)) {
      final List<org.jooq.Record> result = new Database(dslContext)
              .query(ctx -> new ArrayList<>(ctx.fetch(
                      String.format("SELECT * FROM %s.%s ORDER BY %s ASC", schemaName, tableName,
                              ExasolSqlOperations.COLUMN_NAME_EMITTED_AT))));
      return result
              .stream()
              .map(r -> r.formatJSON(JdbcUtils.getDefaultJSONFormat()))
              .map(Jsons::deserialize)
              .collect(Collectors.toList());
    }
  }

  private static DSLContext getDSLContext(final JsonNode config) {
    String jdbcUrl = String.format(DatabaseDriver.EXASOL.getUrlFormatString(), config.get(JdbcUtils.HOST_KEY).asText(), config.get(JdbcUtils.PORT_KEY).asInt());
    Map<String, String> jdbcConnectionProperties = Map.of("fingerprint", config.get("certificateFingerprint").asText());
    return DSLContextFactory.create(
            config.get(JdbcUtils.USERNAME_KEY).asText(),
            config.get(JdbcUtils.PASSWORD_KEY).asText(),
            DatabaseDriver.EXASOL.getDriverClassName(),
            jdbcUrl,
            null,
            jdbcConnectionProperties);
  }

  protected boolean implementsNamespaces() {
    return false;
  }

  @Override
  protected void setup(TestDestinationEnv testEnv) {
    // Nothing to do
  }

  @Override
  protected void tearDown(TestDestinationEnv testEnv) throws Exception {
    EXASOL.purgeDatabase();
  }
}
