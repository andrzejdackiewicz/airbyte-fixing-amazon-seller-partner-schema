package io.airbyte.integrations.io.airbyte.integration_tests.sources;

public class CDCPostgresSourceCaCertificateSslAcceptanceLegacyCtidTest extends CDCPostgresSourceCaCertificateSslAcceptanceTest{
  @Override
  protected String getServerImageName() {
    return "postgres:12-bullseye";
  }
}
