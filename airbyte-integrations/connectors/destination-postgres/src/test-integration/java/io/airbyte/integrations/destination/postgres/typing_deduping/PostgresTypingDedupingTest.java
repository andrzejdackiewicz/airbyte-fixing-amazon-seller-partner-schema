/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.postgres.typing_deduping;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.airbyte.integrations.destination.postgres.PostgresDestination;
import io.airbyte.integrations.destination.postgres.PostgresTestDatabase;
import javax.sql.DataSource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

public class PostgresTypingDedupingTest extends AbstractPostgresTypingDedupingTest {

  protected static PostgresTestDatabase testContainer;

  private static final int DEFAULT_VARCHAR_LIMIT_IN_JDBC_GEN = 65535;

  private static final Random RANDOM = new Random();

  private String generateBigString() {
    // Generate exactly 2 chars over the limit
    final int length = DEFAULT_VARCHAR_LIMIT_IN_JDBC_GEN + 2;
    return RANDOM
        .ints('a', 'z' + 1)
        .limit(length)
        .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
        .toString();
  }

  @BeforeAll
  public static void setupPostgres() {
    testContainer = PostgresTestDatabase.in(PostgresTestDatabase.BaseImage.POSTGRES_13);
  }

  @AfterAll
  public static void teardownPostgres() {
    testContainer.close();
  }

  @Override
  protected ObjectNode getBaseConfig() {
    return (ObjectNode) testContainer.configBuilder()
        .with("schema", "public")
        .withDatabase()
        .withResolvedHostAndPort()
        .withCredentials()
        .withoutSsl()
        .build();
  }

  @Override
  protected DataSource getDataSource(final JsonNode config) {
    // Intentionally ignore the config and rebuild it.
    // The config param has the resolved (i.e. in-docker) host/port.
    // We need the unresolved host/port since the test wrapper code is running from the docker host
    // rather than in a container.
    return new PostgresDestination().getDataSource(testContainer.configBuilder()
        .with("schema", "public")
        .withDatabase()
        .withHostAndPort()
        .withCredentials()
        .withoutSsl()
        .build());
  }

  @Override
  protected String getImageName() {
    return "airbyte/destination-postgres:dev";
  }

}
