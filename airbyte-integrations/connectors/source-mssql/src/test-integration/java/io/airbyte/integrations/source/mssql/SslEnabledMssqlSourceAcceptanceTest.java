/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.source.mssql;

import com.fasterxml.jackson.databind.JsonNode;
import io.airbyte.cdk.integrations.standardtest.source.TestDestinationEnv;
import io.airbyte.commons.features.FeatureFlags;
import io.airbyte.commons.features.FeatureFlagsWrapper;
import java.util.Map;

public class SslEnabledMssqlSourceAcceptanceTest extends MssqlSourceAcceptanceTest {

  @Override
  protected JsonNode getConfig() {
    return testdb.integrationTestConfigBuilder()
        .withSsl(Map.of("ssl_method", "encrypted_trust_server_certificate"))
        .build();
  }

  @Override
  protected FeatureFlags featureFlags() {
    return FeatureFlagsWrapper.overridingUseStreamCapableState(super.featureFlags(), true);
  }

  @Override
  protected void setupEnvironment(final TestDestinationEnv environment) {
    final var container = new MsSQLContainerFactory().shared("mcr.microsoft.com/mssql/server:2022-latest");
    testdb = new MsSQLTestDatabase(container);
    testdb = testdb
        .withConnectionProperty("encrypt", "true")
        .withConnectionProperty("trustServerCertificate", "true")
        .withConnectionProperty("databaseName", testdb.getDatabaseName())
        .initialized()
        .with("CREATE TABLE id_and_name(id INTEGER, name VARCHAR(200), born DATETIMEOFFSET(7));")
        .with("INSERT INTO id_and_name (id, name, born) VALUES " +
            "(1, 'picard', '2124-03-04T01:01:01Z'), " +
            "(2, 'crusher', '2124-03-04T01:01:01Z'), " +
            "(3, 'vash', '2124-03-04T01:01:01Z');");
  }

}
