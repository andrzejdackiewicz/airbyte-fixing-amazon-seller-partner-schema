/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.source.postgres;

import static io.airbyte.integrations.source.postgres.utils.PostgresUnitTestsUtil.createRecord;
import static io.airbyte.integrations.source.postgres.utils.PostgresUnitTestsUtil.extractSpecificFieldFromCombinedMessages;
import static io.airbyte.integrations.source.postgres.utils.PostgresUnitTestsUtil.extractStateMessage;
import static io.airbyte.integrations.source.postgres.utils.PostgresUnitTestsUtil.filterRecords;
import static io.airbyte.integrations.source.postgres.utils.PostgresUnitTestsUtil.map;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.airbyte.commons.features.EnvVariableFeatureFlags;
import io.airbyte.commons.io.IOs;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.resources.MoreResources;
import io.airbyte.commons.string.Strings;
import io.airbyte.commons.util.MoreIterators;
import io.airbyte.db.factory.DataSourceFactory;
import io.airbyte.db.jdbc.JdbcUtils;
import io.airbyte.db.jdbc.StreamingJdbcDatabase;
import io.airbyte.db.jdbc.streaming.AdaptiveStreamingQueryConfig;
import io.airbyte.integrations.source.jdbc.AbstractJdbcSource;
import io.airbyte.integrations.source.jdbc.test.JdbcSourceAcceptanceTest;
import io.airbyte.integrations.source.postgres.ctid.CtidFeatureFlags;
import io.airbyte.integrations.source.postgres.internal.models.InternalModels.StateType;
import io.airbyte.integrations.source.postgres.internal.models.StandardStatus;
import io.airbyte.integrations.source.relationaldb.models.DbStreamState;
import io.airbyte.protocol.models.Field;
import io.airbyte.protocol.models.JsonSchemaType;
import io.airbyte.protocol.models.v0.AirbyteCatalog;
import io.airbyte.protocol.models.v0.AirbyteConnectionStatus;
import io.airbyte.protocol.models.v0.AirbyteMessage;
import io.airbyte.protocol.models.v0.AirbyteMessage.Type;
import io.airbyte.protocol.models.v0.AirbyteRecordMessage;
import io.airbyte.protocol.models.v0.AirbyteStateMessage;
import io.airbyte.protocol.models.v0.AirbyteStateMessage.AirbyteStateType;
import io.airbyte.protocol.models.v0.AirbyteStream;
import io.airbyte.protocol.models.v0.AirbyteStreamState;
import io.airbyte.protocol.models.v0.CatalogHelpers;
import io.airbyte.protocol.models.v0.ConfiguredAirbyteCatalog;
import io.airbyte.protocol.models.v0.ConfiguredAirbyteStream;
import io.airbyte.protocol.models.v0.ConnectorSpecification;
import io.airbyte.protocol.models.v0.DestinationSyncMode;
import io.airbyte.protocol.models.v0.StreamDescriptor;
import io.airbyte.protocol.models.v0.SyncMode;
import io.airbyte.test.utils.PostgreSQLContainerHelper;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.MountableFile;
import uk.org.webcompere.systemstubs.environment.EnvironmentVariables;
import uk.org.webcompere.systemstubs.jupiter.SystemStub;
import uk.org.webcompere.systemstubs.jupiter.SystemStubsExtension;

@ExtendWith(SystemStubsExtension.class)
class PostgresJdbcSourceAcceptanceTest extends JdbcSourceAcceptanceTest {

  @SystemStub
  private EnvironmentVariables environmentVariables;

  private static final String DATABASE = "new_db";
  protected static final String USERNAME_WITHOUT_PERMISSION = "new_user";
  protected static final String PASSWORD_WITHOUT_PERMISSION = "new_password";
  private static PostgreSQLContainer<?> PSQL_DB;
  public static String COL_WAKEUP_AT = "wakeup_at";
  public static String COL_LAST_VISITED_AT = "last_visited_at";
  public static String COL_LAST_COMMENT_AT = "last_comment_at";

  @BeforeAll
  static void init() {
    PSQL_DB = new PostgreSQLContainer<>("postgres:13-alpine");
    PSQL_DB.start();
  }

  @Override
  @BeforeEach
  public void setup() throws Exception {
    environmentVariables.set(EnvVariableFeatureFlags.USE_STREAM_CAPABLE_STATE, "true");
    final String dbName = Strings.addRandomSuffix("db", "_", 10).toLowerCase();
    COLUMN_CLAUSE_WITH_PK =
        "id INTEGER, name VARCHAR(200) NOT NULL, updated_at DATE NOT NULL, wakeup_at TIMETZ NOT NULL, last_visited_at TIMESTAMPTZ NOT NULL, last_comment_at TIMESTAMP NOT NULL";
    COLUMN_CLAUSE_WITHOUT_PK =
        "id INTEGER NOT NULL, name VARCHAR(200) NOT NULL, updated_at DATE NOT NULL, wakeup_at TIMETZ NOT NULL, last_visited_at TIMESTAMPTZ NOT NULL, last_comment_at TIMESTAMP NOT NULL";
    COLUMN_CLAUSE_WITH_COMPOSITE_PK =
        "first_name VARCHAR(200) NOT NULL, last_name VARCHAR(200) NOT NULL, updated_at DATE NOT NULL, wakeup_at TIMETZ NOT NULL, last_visited_at TIMESTAMPTZ NOT NULL, last_comment_at TIMESTAMP NOT NULL";

    config = Jsons.jsonNode(ImmutableMap.builder()
        .put(JdbcUtils.HOST_KEY, PSQL_DB.getHost())
        .put(JdbcUtils.PORT_KEY, PSQL_DB.getFirstMappedPort())
        .put(JdbcUtils.DATABASE_KEY, dbName)
        .put(JdbcUtils.SCHEMAS_KEY, List.of(SCHEMA_NAME, SCHEMA_NAME2))
        .put(JdbcUtils.USERNAME_KEY, PSQL_DB.getUsername())
        .put(JdbcUtils.PASSWORD_KEY, PSQL_DB.getPassword())
        .put(JdbcUtils.SSL_KEY, false)
        .put(CtidFeatureFlags.CURSOR_VIA_CTID, "true")
        .build());

    final String initScriptName = "init_" + dbName.concat(".sql");
    final String tmpFilePath = IOs.writeFileToRandomTmpDir(initScriptName, "CREATE DATABASE " + dbName + ";");
    PostgreSQLContainerHelper.runSqlScript(MountableFile.forHostPath(tmpFilePath), PSQL_DB);

    source = getSource();
    final JsonNode jdbcConfig = getToDatabaseConfigFunction().apply(config);

    streamName = TABLE_NAME;

    dataSource = DataSourceFactory.create(
        jdbcConfig.get(JdbcUtils.USERNAME_KEY).asText(),
        jdbcConfig.has(JdbcUtils.PASSWORD_KEY) ? jdbcConfig.get(JdbcUtils.PASSWORD_KEY).asText() : null,
        getDriverClass(),
        jdbcConfig.get(JdbcUtils.JDBC_URL_KEY).asText(),
        JdbcUtils.parseJdbcParameters(jdbcConfig, JdbcUtils.CONNECTION_PROPERTIES_KEY, getJdbcParameterDelimiter()));

    database = new StreamingJdbcDatabase(dataSource,
        JdbcUtils.getDefaultSourceOperations(),
        AdaptiveStreamingQueryConfig::new);

    createSchemas();

    database.execute(connection -> {

      connection.createStatement().execute(
          createTableQuery(getFullyQualifiedTableName(TABLE_NAME), COLUMN_CLAUSE_WITH_PK,
              primaryKeyClause(Collections.singletonList("id"))));
      connection.createStatement().execute(
          String.format(
              "INSERT INTO %s(id, name, updated_at, wakeup_at, last_visited_at, last_comment_at) VALUES (1,'picard', '2004-10-19','10:10:10.123456-05:00','2004-10-19T17:23:54.123456Z','2004-01-01T17:23:54.123456')",
              getFullyQualifiedTableName(TABLE_NAME)));
      connection.createStatement().execute(
          String.format(
              "INSERT INTO %s(id, name, updated_at, wakeup_at, last_visited_at, last_comment_at) VALUES (2, 'crusher', '2005-10-19','11:11:11.123456-05:00','2005-10-19T17:23:54.123456Z','2005-01-01T17:23:54.123456')",
              getFullyQualifiedTableName(TABLE_NAME)));
      connection.createStatement().execute(
          String.format(
              "INSERT INTO %s(id, name, updated_at, wakeup_at, last_visited_at, last_comment_at) VALUES (3, 'vash', '2006-10-19','12:12:12.123456-05:00','2006-10-19T17:23:54.123456Z','2006-01-01T17:23:54.123456')",
              getFullyQualifiedTableName(TABLE_NAME)));

      connection.createStatement().execute(
          createTableQuery(getFullyQualifiedTableName(TABLE_NAME_WITHOUT_PK),
              COLUMN_CLAUSE_WITHOUT_PK, ""));
      connection.createStatement().execute(
          String.format(
              "INSERT INTO %s(id, name, updated_at, wakeup_at, last_visited_at, last_comment_at) VALUES (1,'picard', '2004-10-19','12:12:12.123456-05:00','2004-10-19T17:23:54.123456Z','2004-01-01T17:23:54.123456')",
              getFullyQualifiedTableName(TABLE_NAME_WITHOUT_PK)));
      connection.createStatement().execute(
          String.format(
              "INSERT INTO %s(id, name, updated_at, wakeup_at, last_visited_at, last_comment_at) VALUES (2, 'crusher', '2005-10-19','11:11:11.123456-05:00','2005-10-19T17:23:54.123456Z','2005-01-01T17:23:54.123456')",
              getFullyQualifiedTableName(TABLE_NAME_WITHOUT_PK)));
      connection.createStatement().execute(
          String.format(
              "INSERT INTO %s(id, name, updated_at, wakeup_at, last_visited_at, last_comment_at) VALUES (3, 'vash', '2006-10-19','10:10:10.123456-05:00','2006-10-19T17:23:54.123456Z','2006-01-01T17:23:54.123456')",
              getFullyQualifiedTableName(TABLE_NAME_WITHOUT_PK)));

      connection.createStatement().execute(
          createTableQuery(getFullyQualifiedTableName(TABLE_NAME_COMPOSITE_PK),
              COLUMN_CLAUSE_WITH_COMPOSITE_PK,
              primaryKeyClause(ImmutableList.of("first_name", "last_name"))));
      connection.createStatement().execute(
          String.format(
              "INSERT INTO %s(first_name, last_name, updated_at, wakeup_at, last_visited_at, last_comment_at) VALUES ('first' ,'picard', '2004-10-19','12:12:12.123456-05:00','2004-10-19T17:23:54.123456Z','2004-01-01T17:23:54.123456')",
              getFullyQualifiedTableName(TABLE_NAME_COMPOSITE_PK)));
      connection.createStatement().execute(
          String.format(
              "INSERT INTO %s(first_name, last_name, updated_at, wakeup_at, last_visited_at, last_comment_at) VALUES ('second', 'crusher', '2005-10-19','11:11:11.123456-05:00','2005-10-19T17:23:54.123456Z','2005-01-01T17:23:54.123456')",
              getFullyQualifiedTableName(TABLE_NAME_COMPOSITE_PK)));
      connection.createStatement().execute(
          String.format(
              "INSERT INTO %s(first_name, last_name, updated_at, wakeup_at, last_visited_at, last_comment_at) VALUES  ('third', 'vash', '2006-10-19','10:10:10.123456-05:00','2006-10-19T17:23:54.123456Z','2006-01-01T17:23:54.123456')",
              getFullyQualifiedTableName(TABLE_NAME_COMPOSITE_PK)));

    });

    CREATE_TABLE_WITHOUT_CURSOR_TYPE_QUERY = "CREATE TABLE %s (%s BIT(3) NOT NULL);";
    INSERT_TABLE_WITHOUT_CURSOR_TYPE_QUERY = "INSERT INTO %s VALUES(B'101');";
  }

  @Override
  protected List<AirbyteMessage> getAirbyteMessagesReadOneColumn() {
    return getTestMessages().stream()
        .map(Jsons::clone)
        .peek(m -> {
          ((ObjectNode) m.getRecord().getData()).remove(COL_NAME);
          ((ObjectNode) m.getRecord().getData()).remove(COL_UPDATED_AT);
          ((ObjectNode) m.getRecord().getData()).remove(COL_WAKEUP_AT);
          ((ObjectNode) m.getRecord().getData()).remove(COL_LAST_VISITED_AT);
          ((ObjectNode) m.getRecord().getData()).remove(COL_LAST_COMMENT_AT);
          ((ObjectNode) m.getRecord().getData()).replace(COL_ID,
              Jsons.jsonNode(m.getRecord().getData().get(COL_ID).asInt()));
        })
        .collect(toList());
  }

  @Override
  protected ArrayList<AirbyteMessage> getAirbyteMessagesCheckCursorSpaceInColumnName(final ConfiguredAirbyteStream streamWithSpaces) {
    final AirbyteMessage firstMessage = getTestMessages().get(0);
    firstMessage.getRecord().setStream(streamWithSpaces.getStream().getName());
    ((ObjectNode) firstMessage.getRecord().getData()).remove(COL_UPDATED_AT);
    ((ObjectNode) firstMessage.getRecord().getData()).remove(COL_WAKEUP_AT);
    ((ObjectNode) firstMessage.getRecord().getData()).remove(COL_LAST_VISITED_AT);
    ((ObjectNode) firstMessage.getRecord().getData()).remove(COL_LAST_COMMENT_AT);
    ((ObjectNode) firstMessage.getRecord().getData()).set(COL_LAST_NAME_WITH_SPACE,
        ((ObjectNode) firstMessage.getRecord().getData()).remove(COL_NAME));

    final AirbyteMessage secondMessage = getTestMessages().get(2);
    secondMessage.getRecord().setStream(streamWithSpaces.getStream().getName());
    ((ObjectNode) secondMessage.getRecord().getData()).remove(COL_UPDATED_AT);
    ((ObjectNode) secondMessage.getRecord().getData()).remove(COL_WAKEUP_AT);
    ((ObjectNode) secondMessage.getRecord().getData()).remove(COL_LAST_VISITED_AT);
    ((ObjectNode) secondMessage.getRecord().getData()).remove(COL_LAST_COMMENT_AT);
    ((ObjectNode) secondMessage.getRecord().getData()).set(COL_LAST_NAME_WITH_SPACE,
        ((ObjectNode) secondMessage.getRecord().getData()).remove(COL_NAME));

    Lists.newArrayList(getTestMessages().get(0), getTestMessages().get(2));

    return Lists.newArrayList(firstMessage, secondMessage);
  }

  @Override
  protected List<AirbyteMessage> getAirbyteMessagesSecondSync(final String streamName2) {
    return getTestMessages()
        .stream()
        .map(Jsons::clone)
        .peek(m -> {
          m.getRecord().setStream(streamName2);
          m.getRecord().setNamespace(getDefaultNamespace());
          ((ObjectNode) m.getRecord().getData()).remove(COL_UPDATED_AT);
          ((ObjectNode) m.getRecord().getData()).remove(COL_WAKEUP_AT);
          ((ObjectNode) m.getRecord().getData()).remove(COL_LAST_VISITED_AT);
          ((ObjectNode) m.getRecord().getData()).remove(COL_LAST_COMMENT_AT);
          ((ObjectNode) m.getRecord().getData()).replace(COL_ID,
              Jsons.jsonNode(m.getRecord().getData().get(COL_ID).asInt()));
        })
        .collect(toList());
  }

  @Override
  protected List<AirbyteMessage> getAirbyteMessagesSecondStreamWithNamespace(final String streamName2) {
    return getTestMessages()
        .stream()
        .map(Jsons::clone)
        .peek(m -> {
          m.getRecord().setStream(streamName2);
          ((ObjectNode) m.getRecord().getData()).remove(COL_UPDATED_AT);
          ((ObjectNode) m.getRecord().getData()).remove(COL_WAKEUP_AT);
          ((ObjectNode) m.getRecord().getData()).remove(COL_LAST_VISITED_AT);
          ((ObjectNode) m.getRecord().getData()).remove(COL_LAST_COMMENT_AT);
          ((ObjectNode) m.getRecord().getData()).replace(COL_ID,
              Jsons.jsonNode(m.getRecord().getData().get(COL_ID).asInt()));
        })
        .collect(toList());
  }

  @Override
  protected List<AirbyteMessage> getAirbyteMessagesForTablesWithQuoting(final ConfiguredAirbyteStream streamForTableWithSpaces) {
    return getTestMessages()
        .stream()
        .map(Jsons::clone)
        .peek(m -> {
          m.getRecord().setStream(streamForTableWithSpaces.getStream().getName());
          ((ObjectNode) m.getRecord().getData()).set(COL_LAST_NAME_WITH_SPACE,
              ((ObjectNode) m.getRecord().getData()).remove(COL_NAME));
          ((ObjectNode) m.getRecord().getData()).remove(COL_UPDATED_AT);
          ((ObjectNode) m.getRecord().getData()).remove(COL_LAST_VISITED_AT);
          ((ObjectNode) m.getRecord().getData()).remove(COL_LAST_COMMENT_AT);
          ((ObjectNode) m.getRecord().getData()).remove(COL_WAKEUP_AT);
          ((ObjectNode) m.getRecord().getData()).replace(COL_ID,
              Jsons.jsonNode(m.getRecord().getData().get(COL_ID).asInt()));
        })
        .collect(toList());
  }

  @Override
  public boolean supportsSchemas() {
    return true;
  }

  @Override
  public AbstractJdbcSource<PostgresType> getJdbcSource() {
    return new PostgresSource();
  }

  @Override
  public JsonNode getConfig() {
    return config;
  }

  @Override
  public String getDriverClass() {
    return PostgresSource.DRIVER_CLASS;
  }

  @AfterAll
  static void cleanUp() {
    PSQL_DB.close();
  }

  @Test
  void testSpec() throws Exception {
    final ConnectorSpecification actual = source.spec();
    final ConnectorSpecification expected = Jsons.deserialize(MoreResources.readResource("spec.json"), ConnectorSpecification.class);

    assertEquals(expected, actual);
  }

  @Override
  protected List<AirbyteMessage> getTestMessages() {
    return Lists.newArrayList(
        new AirbyteMessage().withType(AirbyteMessage.Type.RECORD)
            .withRecord(new AirbyteRecordMessage().withStream(streamName).withNamespace(getDefaultNamespace())
                .withData(Jsons.jsonNode(ImmutableMap
                    .of(COL_ID, ID_VALUE_1,
                        COL_NAME, "picard",
                        COL_UPDATED_AT, "2004-10-19",
                        COL_WAKEUP_AT, "10:10:10.123456-05:00",
                        COL_LAST_VISITED_AT, "2004-10-19T17:23:54.123456Z",
                        COL_LAST_COMMENT_AT, "2004-01-01T17:23:54.123456")))),
        new AirbyteMessage().withType(AirbyteMessage.Type.RECORD)
            .withRecord(new AirbyteRecordMessage().withStream(streamName).withNamespace(getDefaultNamespace())
                .withData(Jsons.jsonNode(ImmutableMap
                    .of(COL_ID, ID_VALUE_2,
                        COL_NAME, "crusher",
                        COL_UPDATED_AT, "2005-10-19",
                        COL_WAKEUP_AT, "11:11:11.123456-05:00",
                        COL_LAST_VISITED_AT, "2005-10-19T17:23:54.123456Z",
                        COL_LAST_COMMENT_AT, "2005-01-01T17:23:54.123456")))),
        new AirbyteMessage().withType(AirbyteMessage.Type.RECORD)
            .withRecord(new AirbyteRecordMessage().withStream(streamName).withNamespace(getDefaultNamespace())
                .withData(Jsons.jsonNode(ImmutableMap
                    .of(COL_ID, ID_VALUE_3,
                        COL_NAME, "vash",
                        COL_UPDATED_AT, "2006-10-19",
                        COL_WAKEUP_AT, "12:12:12.123456-05:00",
                        COL_LAST_VISITED_AT, "2006-10-19T17:23:54.123456Z",
                        COL_LAST_COMMENT_AT, "2006-01-01T17:23:54.123456")))));
  }

  @Override
  protected void executeStatementReadIncrementallyTwice() throws SQLException {
    database.execute(connection -> {
      connection.createStatement().execute(
          String.format(
              "INSERT INTO %s(id, name, updated_at, wakeup_at, last_visited_at, last_comment_at) VALUES (4,'riker', '2006-10-19','12:12:12.123456-05:00','2006-10-19T17:23:54.123456Z','2006-01-01T17:23:54.123456')",
              getFullyQualifiedTableName(TABLE_NAME)));
      connection.createStatement().execute(
          String.format(
              "INSERT INTO %s(id, name, updated_at, wakeup_at, last_visited_at, last_comment_at) VALUES (5, 'data', '2006-10-19','12:12:12.123456-05:00','2006-10-19T17:23:54.123456Z','2006-01-01T17:23:54.123456')",
              getFullyQualifiedTableName(TABLE_NAME)));
    });
  }

  private AirbyteStream getAirbyteStream(final String tableName, final String namespace) {
    return CatalogHelpers.createAirbyteStream(
        tableName,
        namespace,
        Field.of(COL_ID, JsonSchemaType.INTEGER),
        Field.of(COL_NAME, JsonSchemaType.STRING),
        Field.of(COL_UPDATED_AT, JsonSchemaType.STRING_DATE),
        Field.of(COL_WAKEUP_AT, JsonSchemaType.STRING_TIME_WITH_TIMEZONE),
        Field.of(COL_LAST_VISITED_AT, JsonSchemaType.STRING_TIMESTAMP_WITH_TIMEZONE),
        Field.of(COL_LAST_COMMENT_AT, JsonSchemaType.STRING_TIMESTAMP_WITHOUT_TIMEZONE))
        .withSupportedSyncModes(Lists.newArrayList(SyncMode.FULL_REFRESH, SyncMode.INCREMENTAL))
        .withSourceDefinedPrimaryKey(List.of(List.of(COL_ID)));
  }

  @Override
  protected AirbyteCatalog getCatalog(final String defaultNamespace) {
    return new AirbyteCatalog().withStreams(Lists.newArrayList(
        CatalogHelpers.createAirbyteStream(
            TABLE_NAME,
            defaultNamespace,
            Field.of(COL_ID, JsonSchemaType.INTEGER),
            Field.of(COL_NAME, JsonSchemaType.STRING),
            Field.of(COL_UPDATED_AT, JsonSchemaType.STRING_DATE),
            Field.of(COL_WAKEUP_AT, JsonSchemaType.STRING_TIME_WITH_TIMEZONE),
            Field.of(COL_LAST_VISITED_AT, JsonSchemaType.STRING_TIMESTAMP_WITH_TIMEZONE),
            Field.of(COL_LAST_COMMENT_AT, JsonSchemaType.STRING_TIMESTAMP_WITHOUT_TIMEZONE))
            .withSupportedSyncModes(Lists.newArrayList(SyncMode.FULL_REFRESH, SyncMode.INCREMENTAL))
            .withSourceDefinedPrimaryKey(List.of(List.of(COL_ID))),
        CatalogHelpers.createAirbyteStream(
            TABLE_NAME_WITHOUT_PK,
            defaultNamespace,
            Field.of(COL_ID, JsonSchemaType.INTEGER),
            Field.of(COL_NAME, JsonSchemaType.STRING),
            Field.of(COL_UPDATED_AT, JsonSchemaType.STRING_DATE),
            Field.of(COL_WAKEUP_AT, JsonSchemaType.STRING_TIME_WITH_TIMEZONE),
            Field.of(COL_LAST_VISITED_AT, JsonSchemaType.STRING_TIMESTAMP_WITH_TIMEZONE),
            Field.of(COL_LAST_COMMENT_AT, JsonSchemaType.STRING_TIMESTAMP_WITHOUT_TIMEZONE))
            .withSupportedSyncModes(Lists.newArrayList(SyncMode.FULL_REFRESH, SyncMode.INCREMENTAL))
            .withSourceDefinedPrimaryKey(Collections.emptyList()),
        CatalogHelpers.createAirbyteStream(
            TABLE_NAME_COMPOSITE_PK,
            defaultNamespace,
            Field.of(COL_FIRST_NAME, JsonSchemaType.STRING),
            Field.of(COL_LAST_NAME, JsonSchemaType.STRING),
            Field.of(COL_UPDATED_AT, JsonSchemaType.STRING_DATE),
            Field.of(COL_WAKEUP_AT, JsonSchemaType.STRING_TIME_WITH_TIMEZONE),
            Field.of(COL_LAST_VISITED_AT, JsonSchemaType.STRING_TIMESTAMP_WITH_TIMEZONE),
            Field.of(COL_LAST_COMMENT_AT, JsonSchemaType.STRING_TIMESTAMP_WITHOUT_TIMEZONE))
            .withSupportedSyncModes(Lists.newArrayList(SyncMode.FULL_REFRESH, SyncMode.INCREMENTAL))
            .withSourceDefinedPrimaryKey(
                List.of(List.of(COL_FIRST_NAME), List.of(COL_LAST_NAME)))));
  }

  @Override
  protected void incrementalDateCheck() throws Exception {
    incrementalCursorCheck(COL_UPDATED_AT,
        "2005-10-18",
        "2006-10-19",
        Lists.newArrayList(getTestMessages().get(1),
            getTestMessages().get(2)));
  }

  @Test
  void incrementalTimeTzCheck() throws Exception {
    incrementalCursorCheck(COL_WAKEUP_AT,
        "11:09:11.123456-05:00",
        "12:12:12.123456-05:00",
        Lists.newArrayList(getTestMessages().get(1),
            getTestMessages().get(2)));
  }

  @Test
  void incrementalTimestampTzCheck() throws Exception {
    incrementalCursorCheck(COL_LAST_VISITED_AT,
        "2005-10-18T17:23:54.123456Z",
        "2006-10-19T17:23:54.123456Z",
        Lists.newArrayList(getTestMessages().get(1),
            getTestMessages().get(2)));
  }

  @Test
  void incrementalTimestampCheck() throws Exception {
    incrementalCursorCheck(COL_LAST_COMMENT_AT,
        "2004-12-12T17:23:54.123456",
        "2006-01-01T17:23:54.123456",
        Lists.newArrayList(getTestMessages().get(1),
            getTestMessages().get(2)));
  }

  @Override
  protected List<AirbyteMessage> getExpectedAirbyteMessagesSecondSync(final String namespace) {
    final List<AirbyteMessage> expectedMessages = new ArrayList<>();
    expectedMessages.add(new AirbyteMessage().withType(AirbyteMessage.Type.RECORD)
        .withRecord(new AirbyteRecordMessage().withStream(streamName).withNamespace(namespace)
            .withData(Jsons.jsonNode(ImmutableMap
                .of(COL_ID, ID_VALUE_4,
                    COL_NAME, "riker",
                    COL_UPDATED_AT, "2006-10-19",
                    COL_WAKEUP_AT, "12:12:12.123456-05:00",
                    COL_LAST_VISITED_AT, "2006-10-19T17:23:54.123456Z",
                    COL_LAST_COMMENT_AT, "2006-01-01T17:23:54.123456")))));
    expectedMessages.add(new AirbyteMessage().withType(AirbyteMessage.Type.RECORD)
        .withRecord(new AirbyteRecordMessage().withStream(streamName).withNamespace(namespace)
            .withData(Jsons.jsonNode(ImmutableMap
                .of(COL_ID, ID_VALUE_5,
                    COL_NAME, "data",
                    COL_UPDATED_AT, "2006-10-19",
                    COL_WAKEUP_AT, "12:12:12.123456-05:00",
                    COL_LAST_VISITED_AT, "2006-10-19T17:23:54.123456Z",
                    COL_LAST_COMMENT_AT, "2006-01-01T17:23:54.123456")))));
    final DbStreamState state = new StandardStatus().withStateType(StateType.STANDARD).withVersion(2L)
        .withStreamName(streamName)
        .withStreamNamespace(namespace)
        .withCursorField(ImmutableList.of(COL_ID))
        .withCursor("5")
        .withCursorRecordCount(1L);
    expectedMessages.addAll(createExpectedTestMessages(List.of(state)));
    return expectedMessages;
  }
  @Test
  void testReadMultipleTablesIncrementallyWithCtid() throws Exception {
    final String namespace = getDefaultNamespace();
    final String streamOneName = streamName;
    // Create a second table
    final String streamTwoName = TABLE_NAME + 2;
    final String streamTwoFullyQualifiedName = getFullyQualifiedTableName(streamTwoName);
    // Insert records into second table
    database.execute(ctx -> {
      ctx.createStatement().execute(
          createTableQuery(streamTwoFullyQualifiedName, COLUMN_CLAUSE_WITH_PK, ""));
      ctx.createStatement().execute(
          String.format("INSERT INTO %s(id, name, updated_at, wakeup_at, last_visited_at, last_comment_at)"
              + "VALUES (40,'Jean Luc','2006-10-19','12:12:12.123456-05:00','2006-10-19T17:23:54.123456Z','2006-01-01T17:23:54.123456')",
              streamTwoFullyQualifiedName));
      ctx.createStatement().execute(
          String.format("INSERT INTO %s(id, name, updated_at, wakeup_at, last_visited_at, last_comment_at)"
              + "VALUES (41, 'Groot', '2006-10-19','12:12:12.123456-05:00','2006-10-19T17:23:54.123456Z','2006-01-01T17:23:54.123456')",
              streamTwoFullyQualifiedName));
      ctx.createStatement().execute(
          String.format("INSERT INTO %s(id, name, updated_at, wakeup_at, last_visited_at, last_comment_at)"
              + "VALUES (42, 'Thanos','2006-10-19','12:12:12.123456-05:00','2006-10-19T17:23:54.123456Z','2006-01-01T17:23:54.123456')",
              streamTwoFullyQualifiedName));
    });
    // Create records list that we expect to see in the state message
    final List<AirbyteMessage> streamTwoExpectedRecords = Arrays.asList(
        createRecord(streamTwoName, namespace, map(
            COL_ID, 40,
            COL_NAME, "Jean Luc",
            COL_UPDATED_AT, "2006-10-19",
            COL_WAKEUP_AT, "12:12:12.123456-05:00",
            COL_LAST_VISITED_AT, "2006-10-19T17:23:54.123456Z",
            COL_LAST_COMMENT_AT, "2006-01-01T17:23:54.123456")),
        createRecord(streamTwoName, namespace, map(
            COL_ID, 41,
            COL_NAME, "Groot",
            COL_UPDATED_AT, "2006-10-19",
            COL_WAKEUP_AT, "12:12:12.123456-05:00",
            COL_LAST_VISITED_AT, "2006-10-19T17:23:54.123456Z",
            COL_LAST_COMMENT_AT, "2006-01-01T17:23:54.123456")),
        createRecord(streamTwoName, namespace, map(
            COL_ID, 42,
            COL_NAME, "Thanos",
            COL_UPDATED_AT, "2006-10-19",
            COL_WAKEUP_AT, "12:12:12.123456-05:00",
            COL_LAST_VISITED_AT, "2006-10-19T17:23:54.123456Z",
            COL_LAST_COMMENT_AT, "2006-01-01T17:23:54.123456")));

    // Prep and create a configured catalog to perform sync
    final AirbyteStream streamOne = getAirbyteStream(streamName, namespace);
    final AirbyteStream streamTwo = getAirbyteStream(streamTwoName, namespace);

    final ConfiguredAirbyteCatalog configuredCatalog = CatalogHelpers.toDefaultConfiguredCatalog(
        new AirbyteCatalog().withStreams(List.of(streamOne, streamTwo)));
    configuredCatalog.getStreams().forEach(airbyteStream -> {
      airbyteStream.setSyncMode(SyncMode.INCREMENTAL);
      airbyteStream.setCursorField(List.of(COL_ID));
      airbyteStream.setDestinationSyncMode(DestinationSyncMode.APPEND);
      airbyteStream.withPrimaryKey(List.of(List.of(COL_ID)));
    });

    // Perform initial sync
    final List<AirbyteMessage> messagesFromFirstSync = MoreIterators
        .toList(source.read(config, configuredCatalog, null));

    final List<AirbyteMessage> recordsFromFirstSync = filterRecords(messagesFromFirstSync);

    setEmittedAtToNull(messagesFromFirstSync);
    // All records in the 2 configured streams should be present
    assertThat(filterRecords(recordsFromFirstSync)).containsExactlyElementsOf(
        Stream.concat(getTestMessages().stream().parallel(),
            streamTwoExpectedRecords.stream().parallel()).collect(toList()));

    final List<AirbyteStateMessage> actualFirstSyncState = extractStateMessage(messagesFromFirstSync);
    // Since we are emitting a state message after each record, we should have 1 state for each record -
    // 3 from stream1 and 3 from stream2
    assertEquals(6, actualFirstSyncState.size());

    // The expected state type should be 2 ctid's and the last one being standard
    final List<String> expectedStateTypesFromFirstSync = List.of("ctid", "ctid", "standard");
    final List<String> stateTypeOfStreamOneStatesFromFirstSync =
        extractSpecificFieldFromCombinedMessages(messagesFromFirstSync, streamName, "state_type");
    final List<String> stateTypeOfStreamTwoStatesFromFirstSync =
        extractSpecificFieldFromCombinedMessages(messagesFromFirstSync, streamTwoName, "state_type");
    // It should be the same for stream1 and stream2
    assertEquals(stateTypeOfStreamOneStatesFromFirstSync, expectedStateTypesFromFirstSync);
    assertEquals(stateTypeOfStreamTwoStatesFromFirstSync, expectedStateTypesFromFirstSync);

    // Create the expected ctids that we should see
    final List<String> expectedCtidsFromFirstSync = List.of("(0,1)", "(0,2)");
    final List<String> ctidFromStreamOneStatesFromFirstSync =
        extractSpecificFieldFromCombinedMessages(messagesFromFirstSync, streamName, "ctid");
    final List<String> ctidFromStreamTwoStatesFromFirstSync =
        extractSpecificFieldFromCombinedMessages(messagesFromFirstSync, streamName, "ctid");

    // Verifying each element and its index to match.
    // Only checking the first 2 elements since we have verified that the last state_type is "standard"
    assertEquals(ctidFromStreamOneStatesFromFirstSync.get(0), expectedCtidsFromFirstSync.get(0));
    assertEquals(ctidFromStreamOneStatesFromFirstSync.get(1), expectedCtidsFromFirstSync.get(1));
    assertEquals(ctidFromStreamTwoStatesFromFirstSync.get(0), expectedCtidsFromFirstSync.get(0));
    assertEquals(ctidFromStreamTwoStatesFromFirstSync.get(1), expectedCtidsFromFirstSync.get(1));

    // Extract only state messages for each stream
    final List<AirbyteStateMessage> streamOneStateMessagesFromFirstSync = extractStateMessage(messagesFromFirstSync, streamName);
    final List<AirbyteStateMessage> streamTwoStateMessagesFromFirstSync = extractStateMessage(messagesFromFirstSync, streamTwoName);
    // Extract the incremental states of each stream's first and second state message
    final List<JsonNode> streamOneIncrementalStatesFromFirstSync =
        List.of(streamOneStateMessagesFromFirstSync.get(0).getStream().getStreamState().get("incremental_state"),
            streamOneStateMessagesFromFirstSync.get(1).getStream().getStreamState().get("incremental_state"));
    final JsonNode streamOneFinalStreamStateFromFirstSync = streamOneStateMessagesFromFirstSync.get(2).getStream().getStreamState();

    final List<JsonNode> streamTwoIncrementalStatesFromFirstSync =
        List.of(streamTwoStateMessagesFromFirstSync.get(0).getStream().getStreamState().get("incremental_state"),
            streamTwoStateMessagesFromFirstSync.get(1).getStream().getStreamState().get("incremental_state"));
    final JsonNode streamTwoFinalStreamStateFromFirstSync = streamTwoStateMessagesFromFirstSync.get(2).getStream().getStreamState();

    // The incremental_state of each stream's first and second incremental states is expected
    // to be identical to the stream_state of the final state message for each stream
    assertEquals(streamOneIncrementalStatesFromFirstSync.get(0), streamOneFinalStreamStateFromFirstSync);
    assertEquals(streamOneIncrementalStatesFromFirstSync.get(1), streamOneFinalStreamStateFromFirstSync);
    assertEquals(streamTwoIncrementalStatesFromFirstSync.get(0), streamTwoFinalStreamStateFromFirstSync);
    assertEquals(streamTwoIncrementalStatesFromFirstSync.get(1), streamTwoFinalStreamStateFromFirstSync);

    // Sync should work with a ctid state AND a standard cursor state from each stream
    // Forcing a sync with
    // - stream one state still being the first record read via CTID.
    // - stream two state being the CTID state before the final emitted state before the cursor switch
    final List<AirbyteMessage> messagesFromSecondSyncWithMixedStates = MoreIterators
        .toList(source.read(config, configuredCatalog,
            Jsons.jsonNode(List.of(streamOneStateMessagesFromFirstSync.get(0),
                streamTwoStateMessagesFromFirstSync.get(1)))));

    // Extract only state messages for each stream after second sync
    final List<AirbyteStateMessage> streamOneStateMessagesFromSecondSync =
        extractStateMessage(messagesFromSecondSyncWithMixedStates, streamOneName);
    final List<String> stateTypeOfStreamOneStatesFromSecondSync =
        extractSpecificFieldFromCombinedMessages(messagesFromSecondSyncWithMixedStates, streamName, "state_type");

    final List<AirbyteStateMessage> streamTwoStateMessagesFromSecondSync =
        extractStateMessage(messagesFromSecondSyncWithMixedStates, streamTwoName);
    final List<String> stateTypeOfStreamTwoStatesFromSecondSync =
        extractSpecificFieldFromCombinedMessages(messagesFromSecondSyncWithMixedStates, streamTwoName, "state_type");

    // Stream One states after the second sync are expected to have 2 stream states
    // - 1 with Ctid state_type and 1 state that is of standard state type
    assertEquals(2, streamOneStateMessagesFromSecondSync.size());
    assertEquals(List.of("ctid", "standard"), stateTypeOfStreamOneStatesFromSecondSync);

    // Stream Two states after the second sync are expected to have 1 stream state
    // - The state that is of standard state type
    assertEquals(1, streamTwoStateMessagesFromSecondSync.size());
    assertEquals(List.of("standard"), stateTypeOfStreamTwoStatesFromSecondSync);

    // Add some data to each table and perform a third read.
    // Expect to see all records be synced via standard method and not ctid

    database.execute(ctx -> {
      ctx.createStatement().execute(
          String.format("INSERT INTO %s(id, name, updated_at, wakeup_at, last_visited_at, last_comment_at)"
              + "VALUES (4,'Hooper','2006-10-19','12:12:12.123456-05:00','2006-10-19T17:23:54.123456Z','2006-01-01T17:23:54.123456')",
              getFullyQualifiedTableName(streamOneName)));
      ctx.createStatement().execute(
          String.format("INSERT INTO %s(id, name, updated_at, wakeup_at, last_visited_at, last_comment_at)"
              + "VALUES (43, 'Iron Man', '2006-10-19','12:12:12.123456-05:00','2006-10-19T17:23:54.123456Z','2006-01-01T17:23:54.123456')",
              streamTwoFullyQualifiedName));
    });

    final List<AirbyteMessage> messagesFromThirdSync = MoreIterators
        .toList(source.read(config, configuredCatalog,
            Jsons.jsonNode(List.of(streamOneStateMessagesFromSecondSync.get(1),
                streamTwoStateMessagesFromSecondSync.get(0)))));

    // Extract only state messages, state type, and cursor for each stream after second sync
    final List<AirbyteStateMessage> streamOneStateMessagesFromThirdSync =
        extractStateMessage(messagesFromThirdSync, streamOneName);
    final List<String> stateTypeOfStreamOneStatesFromThirdSync =
        extractSpecificFieldFromCombinedMessages(messagesFromThirdSync, streamName, "state_type");
    final List<String> cursorOfStreamOneStatesFromThirdSync =
        extractSpecificFieldFromCombinedMessages(messagesFromThirdSync, streamName, "cursor");

    final List<AirbyteStateMessage> streamTwoStateMessagesFromThirdSync =
        extractStateMessage(messagesFromThirdSync, streamTwoName);
    final List<String> stateTypeOfStreamTwoStatesFromThirdSync =
        extractSpecificFieldFromCombinedMessages(messagesFromThirdSync, streamTwoName, "state_type");
    final List<String> cursorOfStreamTwoStatesFromThirdSync =
        extractSpecificFieldFromCombinedMessages(messagesFromThirdSync, streamTwoName, "cursor");

    // Both streams should now be synced via standard cursor and have updated max cursor values
    // cursor: 4 for stream one
    // cursor: 43 for stream two
    assertEquals(1, streamOneStateMessagesFromThirdSync.size());
    assertEquals(List.of("standard"), stateTypeOfStreamOneStatesFromThirdSync);
    assertEquals(List.of("4"), cursorOfStreamOneStatesFromThirdSync);

    assertEquals(1, streamTwoStateMessagesFromThirdSync.size());
    assertEquals(List.of("standard"), stateTypeOfStreamTwoStatesFromThirdSync);
    assertEquals(List.of("43"), cursorOfStreamTwoStatesFromThirdSync);
  }

  @Override
  protected boolean supportsPerStream() {
    return true;
  }

  /**
   * Postgres Source Error Codes:
   * <p>
   * https://www.postgresql.org/docs/current/errcodes-appendix.html
   * </p>
   *
   * @throws Exception
   */
  @Test
  void testCheckIncorrectPasswordFailure() throws Exception {
    ((ObjectNode) config).put(JdbcUtils.PASSWORD_KEY, "fake");
    final AirbyteConnectionStatus status = source.check(config);
    assertEquals(AirbyteConnectionStatus.Status.FAILED, status.getStatus());
    assertTrue(status.getMessage().contains("State code: 28P01;"));
  }

  @Test
  public void testCheckIncorrectUsernameFailure() throws Exception {
    ((ObjectNode) config).put(JdbcUtils.USERNAME_KEY, "fake");
    final AirbyteConnectionStatus status = source.check(config);
    assertEquals(AirbyteConnectionStatus.Status.FAILED, status.getStatus());
    assertTrue(status.getMessage().contains("State code: 28P01;"));
  }

  @Test
  public void testCheckIncorrectHostFailure() throws Exception {
    ((ObjectNode) config).put(JdbcUtils.HOST_KEY, "localhost2");
    final AirbyteConnectionStatus status = source.check(config);
    assertEquals(AirbyteConnectionStatus.Status.FAILED, status.getStatus());
    assertTrue(status.getMessage().contains("State code: 08001;"));
  }

  @Test
  public void testCheckIncorrectPortFailure() throws Exception {
    ((ObjectNode) config).put(JdbcUtils.PORT_KEY, "30000");
    final AirbyteConnectionStatus status = source.check(config);
    assertEquals(AirbyteConnectionStatus.Status.FAILED, status.getStatus());
    assertTrue(status.getMessage().contains("State code: 08001;"));
  }

  @Test
  public void testCheckIncorrectDataBaseFailure() throws Exception {
    ((ObjectNode) config).put(JdbcUtils.DATABASE_KEY, "wrongdatabase");
    final AirbyteConnectionStatus status = source.check(config);
    assertEquals(AirbyteConnectionStatus.Status.FAILED, status.getStatus());
    assertTrue(status.getMessage().contains("State code: 3D000;"));
  }

  @Test
  public void testUserHasNoPermissionToDataBase() throws Exception {
    database.execute(connection -> connection.createStatement()
        .execute(String.format("create user %s with password '%s';", USERNAME_WITHOUT_PERMISSION, PASSWORD_WITHOUT_PERMISSION)));
    database.execute(connection -> connection.createStatement()
        .execute(String.format("create database %s;", DATABASE)));
    // deny access for database for all users from group public
    database.execute(connection -> connection.createStatement()
        .execute(String.format("revoke all on database %s from public;", DATABASE)));
    ((ObjectNode) config).put("username", USERNAME_WITHOUT_PERMISSION);
    ((ObjectNode) config).put("password", PASSWORD_WITHOUT_PERMISSION);
    ((ObjectNode) config).put("database", DATABASE);
    final AirbyteConnectionStatus status = source.check(config);
    Assertions.assertEquals(AirbyteConnectionStatus.Status.FAILED, status.getStatus());
    assertTrue(status.getMessage().contains("State code: 42501;"));
  }

  @Override
  protected void incrementalCursorCheck(
                                        final String initialCursorField,
                                        final String cursorField,
                                        final String initialCursorValue,
                                        final String endCursorValue,
                                        final List<AirbyteMessage> expectedRecordMessages,
                                        final ConfiguredAirbyteStream airbyteStream)
      throws Exception {
    airbyteStream.setSyncMode(SyncMode.INCREMENTAL);
    airbyteStream.setCursorField(List.of(cursorField));
    airbyteStream.setDestinationSyncMode(DestinationSyncMode.APPEND);

    final ConfiguredAirbyteCatalog configuredCatalog = new ConfiguredAirbyteCatalog()
        .withStreams(List.of(airbyteStream));

    final DbStreamState dbStreamState = new StandardStatus()
        .withStateType(StateType.STANDARD)
        .withVersion(2L)
        .withStreamName(airbyteStream.getStream().getName())
        .withStreamNamespace(airbyteStream.getStream().getNamespace())
        .withCursorField(List.of(initialCursorField))
        .withCursor(initialCursorValue)
        .withCursorRecordCount(1L);

    final JsonNode streamStates = Jsons.jsonNode(createState(List.of(dbStreamState)));

    final List<AirbyteMessage> actualMessages = MoreIterators
        .toList(source.read(config, configuredCatalog, streamStates));

    setEmittedAtToNull(actualMessages);

    final List<DbStreamState> expectedStreams = List.of(
        new StandardStatus().withStateType(StateType.STANDARD).withVersion(2L)
            .withStreamName(airbyteStream.getStream().getName())
            .withStreamNamespace(airbyteStream.getStream().getNamespace())
            .withCursorField(List.of(cursorField))
            .withCursor(endCursorValue)
            .withCursorRecordCount(1L));

    final List<AirbyteMessage> expectedMessages = new ArrayList<>(expectedRecordMessages);
    expectedMessages.addAll(createExpectedTestMessages(expectedStreams));

    assertEquals(expectedMessages.size(), actualMessages.size());
    assertTrue(expectedMessages.containsAll(actualMessages));
    assertTrue(actualMessages.containsAll(expectedMessages));
  }

  @Override
  protected JsonNode getStateData(final AirbyteMessage airbyteMessage, final String streamName) {
    final JsonNode streamState = airbyteMessage.getState().getStream().getStreamState();
    if (streamState.get("stream_name").asText().equals(streamName)) {
      return streamState;
    }

    throw new IllegalArgumentException("Stream not found in state message: " + streamName);
  }

  /**
   * {@inheritDoc}
   *
   * @param syncState
   */
  @Override
  protected void addStandardStateTypeToSyncState(final JsonNode syncState) {
    ((ObjectNode) syncState).put("state_type", "standard");
  }

  /**
   * {ine}
   *
   * @param configuredCatalog catalog of DB source
   * @return
   * @throws Exception
   */
  @Override
  protected List<AirbyteMessage> readCatalogForInitialSync(final ConfiguredAirbyteCatalog configuredCatalog) throws Exception {
    return MoreIterators
        .toList(source.read(config, configuredCatalog, null));
  }

  /*
   * Similar to parent's method but does not include the Data key as CTID, Xmin, and Standard sync no
   * longer emits this information
   */
  @Override
  protected AirbyteMessage createStateMessage(final DbStreamState dbStreamState, final List<DbStreamState> legacyStates) {
    final CtidFeatureFlags ctidFeatureFlags = new CtidFeatureFlags(config);
    if (!ctidFeatureFlags.isCursorSyncEnabled()) {
      return super.createStateMessage(dbStreamState, legacyStates);
    }

    if (supportsPerStream()) {
      return new AirbyteMessage().withType(Type.STATE)
          .withState(
              new AirbyteStateMessage().withType(AirbyteStateType.STREAM)
                  .withStream(new AirbyteStreamState()
                      .withStreamDescriptor(new StreamDescriptor().withNamespace(dbStreamState.getStreamNamespace())
                          .withName(dbStreamState.getStreamName()))
                      .withStreamState(Jsons.jsonNode(dbStreamState))));
    } else {
      return new AirbyteMessage().withType(Type.STATE).withState(new AirbyteStateMessage().withType(AirbyteStateType.LEGACY));
    }
  }

  /*
   * Identical to parent method but have a state_type attach to the DBStreamState making it
   * effectively StandardStatus
   */
  @Override
  protected DbStreamState buildExpectedStreamState(final String streamName,
                                                   final String nameSpace,
                                                   final List<String> cursorFields,
                                                   final String cursorValue,
                                                   final Long cursorRecordCount) {
    final CtidFeatureFlags ctidFeatureFlags = new CtidFeatureFlags(config);
    if (!ctidFeatureFlags.isCursorSyncEnabled()) {
      return super.buildExpectedStreamState(streamName, nameSpace, cursorFields, cursorValue, cursorRecordCount);
    }

    final DbStreamState streamState = new StandardStatus().withStateType(StateType.STANDARD).withVersion(2L);
    streamState.setStreamName(streamName);
    streamState.setStreamNamespace(nameSpace);
    streamState.setCursorField(cursorFields);
    if (cursorValue != null) {
      streamState.setCursor(cursorValue);
    }

    if (cursorRecordCount != null) {
      streamState.setCursorRecordCount(cursorRecordCount);
    }

    return streamState;
  }

}
