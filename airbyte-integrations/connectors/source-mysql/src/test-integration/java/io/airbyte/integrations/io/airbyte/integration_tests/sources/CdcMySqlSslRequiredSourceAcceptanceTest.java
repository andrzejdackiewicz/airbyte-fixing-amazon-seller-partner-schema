/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.io.airbyte.integration_tests.sources;

import static io.airbyte.integrations.io.airbyte.integration_tests.sources.utils.TestConstants.INITIAL_CDC_WAITING_SECONDS;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableMap;
import io.airbyte.commons.features.EnvVariableFeatureFlags;
import io.airbyte.commons.json.Jsons;
import io.airbyte.db.jdbc.JdbcUtils;
import io.airbyte.integrations.standardtest.source.TestDestinationEnv;
import io.airbyte.integrations.util.HostPortResolver;
import org.testcontainers.containers.MySQLContainer;

public class CdcMySqlSslRequiredSourceAcceptanceTest extends CdcMySqlSourceAcceptanceTest {

  @Override
  protected void setupEnvironment(final TestDestinationEnv environment) {
    container = new MySQLContainer<>("mysql:8.0");
    container.start();
    environmentVariables.set(EnvVariableFeatureFlags.USE_STREAM_CAPABLE_STATE, "true");

    final var sslMode = ImmutableMap.builder()
        .put(JdbcUtils.MODE_KEY, "required")
        .build();
    final JsonNode replicationMethod = Jsons.jsonNode(ImmutableMap.builder()
        .put("method", "CDC")
        .put("initial_waiting_seconds", INITIAL_CDC_WAITING_SECONDS)
        .build());

    config = Jsons.jsonNode(ImmutableMap.builder()
        .put(JdbcUtils.HOST_KEY, HostPortResolver.resolveHost(container))
        .put(JdbcUtils.PORT_KEY, HostPortResolver.resolvePort(container))
        .put(JdbcUtils.DATABASE_KEY, container.getDatabaseName())
        .put(JdbcUtils.USERNAME_KEY, container.getUsername())
        .put(JdbcUtils.PASSWORD_KEY, container.getPassword())
        .put(JdbcUtils.SSL_KEY, true)
        .put(JdbcUtils.SSL_MODE_KEY, sslMode)
        .put("replication_method", replicationMethod)
        .put("is_test", true)
        .build());

    revokeAllPermissions();
    grantCorrectPermissions();
    alterUserRequireSsl();
    createAndPopulateTables();
  }

  private void alterUserRequireSsl() {
    executeQuery("ALTER USER " + container.getUsername() + " REQUIRE SSL;");
  }

}
