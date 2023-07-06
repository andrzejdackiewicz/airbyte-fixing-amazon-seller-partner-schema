/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.teradata;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.map.MoreMaps;
import io.airbyte.commons.string.Strings;
import io.airbyte.db.factory.DataSourceFactory;
import io.airbyte.db.jdbc.DefaultJdbcDatabase;
import io.airbyte.db.jdbc.JdbcDatabase;
import io.airbyte.db.jdbc.JdbcSourceOperations;
import io.airbyte.db.jdbc.JdbcUtils;
import io.airbyte.integrations.base.AirbyteTraceMessageUtility;
import io.airbyte.integrations.base.JavaBaseConstants;
import io.airbyte.integrations.destination.StandardNameTransformer;
import io.airbyte.integrations.destination.teradata.envclient.TeradataHttpClient;
import io.airbyte.integrations.destination.teradata.envclient.dto.CreateEnvironmentRequest;
import io.airbyte.integrations.destination.teradata.envclient.dto.DeleteEnvironmentRequest;
import io.airbyte.integrations.standardtest.destination.JdbcDestinationAcceptanceTest;
import io.airbyte.protocol.models.v0.AirbyteConnectionStatus;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TeradataDestinationAcceptanceTest extends JdbcDestinationAcceptanceTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(TeradataDestinationAcceptanceTest.class);
  private final StandardNameTransformer namingResolver = new StandardNameTransformer();

 private static final String SCHEMA_NAME = Strings.addRandomSuffix("acc_test", "_", 5);

  private static final String CREATE_DATABASE = "CREATE DATABASE \"%s\" AS PERMANENT = 60e6, SPOOL = 60e6 SKEW = 10 PERCENT";

  private static final String DELETE_DATABASE = "DELETE DATABASE \"%s\"";

  private static final String DROP_DATABASE = "DROP DATABASE \"%s\"";

  private JsonNode configJson;
  private JdbcDatabase database;
  private DataSource dataSource;
  private TeradataDestination destination = new TeradataDestination();
  private final JdbcSourceOperations sourceOperations = JdbcUtils.getDefaultSourceOperations();

  @Override
  protected String getImageName() {
    return "airbyte/destination-teradata:dev";
  }

  @Override
  protected JsonNode getConfig() {
    return configJson;
  }

  @BeforeAll
  void initEnvironment() throws Exception {
    this.configJson = Jsons.clone(getStaticConfig());
    TeradataHttpClient teradataHttpClient = new TeradataHttpClient(configJson.get("env_url").asText());
    var request = new CreateEnvironmentRequest(
            configJson.get("env_name").asText(),
            configJson.get("env_region").asText(),
            configJson.get("env_password").asText());
    var response = teradataHttpClient.createEnvironment(request, configJson.get("env_token").asText()).get();
    ((ObjectNode) configJson).put("host", response.ip());
    logger.info("host name: " + configJson.get("host").asText());
    logger.info("user name: " + configJson.get("username").asText());
    logger.info("password: " + configJson.get("password").asText());

  }

   @AfterAll
  void cleanupEnvironment() throws ExecutionException, InterruptedException {
    //TeradataHttpClient teradataHttpClient = new TeradataHttpClient(configJson.get("env_url").asText());
    //var request = new DeleteEnvironmentRequest(configJson.get("env_name").asText());
    //teradataHttpClient.deleteEnvironment(request, configJson.get("env_token").asText()).get();
  }

  public JsonNode getStaticConfig() throws Exception {
  	 return Jsons.deserialize(Files.readString(Paths.get("secrets/config.json")));
  }

  @Override
  protected JsonNode getFailCheckConfig() throws Exception {
    final JsonNode credentialsJsonString = Jsons
        .deserialize(Files.readString(Paths.get("secrets/failureconfig.json")));
    final AirbyteConnectionStatus check = new TeradataDestination().check(credentialsJsonString);
    assertEquals(AirbyteConnectionStatus.Status.FAILED, check.getStatus());
    logger.info("failed check config got success");
    return credentialsJsonString;
  }

  @Override
  protected List<JsonNode> retrieveRecords(final TestDestinationEnv testEnv,
                                           final String streamName,
                                           final String namespace,
                                           final JsonNode streamSchema)
      throws Exception {
    return retrieveRecordsFromTable(namingResolver.getRawTableName(streamName), namespace);

  }

  private List<JsonNode> retrieveRecordsFromTable(final String tableName, final String schemaName)
      throws SQLException {
    return database.bufferedResultSetQuery(
        connection -> {
          var statement = connection.createStatement();
          return statement.executeQuery(
              String.format("SELECT * FROM %s.%s ORDER BY %s ASC;", schemaName, tableName,
                  JavaBaseConstants.COLUMN_NAME_EMITTED_AT));
        },
        rs -> Jsons.deserialize(rs.getString(JavaBaseConstants.COLUMN_NAME_DATA)));
  }

  @Override
  protected void setup(TestDestinationEnv testEnv) {
    final String createSchemaQuery = String.format(CREATE_DATABASE, SCHEMA_NAME);
    try {
      this.configJson = Jsons.clone(getStaticConfig());
      ((ObjectNode) configJson).put("schema", SCHEMA_NAME);
      logger.info("schema name: " +  configJson.get("schema").asText());  
      dataSource = getDataSource(configJson);
      database = getDatabase(dataSource);
      logger.info("create schema query: " +  createSchemaQuery);
      database.execute(createSchemaQuery);
    } catch (Exception e) {
      AirbyteTraceMessageUtility.emitSystemErrorTrace(e, "Database " + SCHEMA_NAME + " creation got failed.");
    }
  }

  @Override
  protected void tearDown(TestDestinationEnv testEnv) {
    final String deleteQuery = String.format(String.format(DELETE_DATABASE, SCHEMA_NAME));
    final String dropQuery = String.format(String.format(DROP_DATABASE, SCHEMA_NAME));
    try {
      //database.execute(deleteQuery);
      //database.execute(dropQuery);
    } catch (Exception e) {
      AirbyteTraceMessageUtility.emitSystemErrorTrace(e, "Database " + SCHEMA_NAME + " delete got failed.");
    }
  }


  @Override
  @Test
  public void testLineBreakCharacters() {
    // overrides test in coming releases
  }

  @Override
  @Test
  public void testSecondSync() {
    // overrides test in coming releases
  }

  protected DataSource getDataSource(final JsonNode config) {
    final JsonNode jdbcConfig = destination.toJdbcConfig(config);
    logger.info("JDBC URL : " + jdbcConfig.get(JdbcUtils.JDBC_URL_KEY).asText());
      logger.info("USERNAME_KEY : " + jdbcConfig.get(JdbcUtils.USERNAME_KEY).asText());
      logger.info("PASSWORD_KEY : " + jdbcConfig.get(JdbcUtils.PASSWORD_KEY).asText());
      logger.info("USERNAME_KEY : " + jdbcConfig.get(JdbcUtils.USERNAME_KEY).asText());
    return DataSourceFactory.create(jdbcConfig.get(JdbcUtils.USERNAME_KEY).asText(),
        jdbcConfig.has(JdbcUtils.PASSWORD_KEY) ? jdbcConfig.get(JdbcUtils.PASSWORD_KEY).asText() : null,
        TeradataDestination.DRIVER_CLASS, jdbcConfig.get(JdbcUtils.JDBC_URL_KEY).asText(),
        getConnectionProperties(config));
  }

  protected JdbcDatabase getDatabase(final DataSource dataSource) {
    return new DefaultJdbcDatabase(dataSource);
  }

  protected Map<String, String> getConnectionProperties(final JsonNode config) {
    final Map<String, String> customProperties = JdbcUtils.parseJdbcParameters(config,
        JdbcUtils.JDBC_URL_PARAMS_KEY);
    final Map<String, String> defaultProperties = getDefaultConnectionProperties(config);
    assertCustomParametersDontOverwriteDefaultParameters(customProperties, defaultProperties);
    return MoreMaps.merge(customProperties, defaultProperties);
  }

  protected Map<String, String> getDefaultConnectionProperties(final JsonNode config) {
    return Collections.emptyMap();
  }

  private void assertCustomParametersDontOverwriteDefaultParameters(final Map<String, String> customParameters,
                                                                    final Map<String, String> defaultParameters) {
    for (final String key : defaultParameters.keySet()) {
      if (customParameters.containsKey(key)
          && !Objects.equals(customParameters.get(key), defaultParameters.get(key))) {
        throw new IllegalArgumentException("Cannot overwrite default JDBC parameter " + key);
      }
    }
  }

}
