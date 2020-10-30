/*
 * MIT License
 *
 * Copyright (c) 2020 Airbyte
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package io.airbyte.integrations.source.mysql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.io.Resources;
import io.airbyte.commons.io.IOs;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.resources.MoreResources;
import io.airbyte.db.DatabaseHelper;
import io.airbyte.protocol.models.AirbyteCatalog;
import io.airbyte.protocol.models.AirbyteConnectionStatus;
import io.airbyte.protocol.models.AirbyteConnectionStatus.Status;
import io.airbyte.protocol.models.AirbyteMessage;
import io.airbyte.protocol.models.AirbyteMessage.Type;
import io.airbyte.protocol.models.AirbyteRecordMessage;
import io.airbyte.protocol.models.AirbyteStream;
import io.airbyte.protocol.models.CatalogHelpers;
import io.airbyte.protocol.models.ConnectorSpecification;
import io.airbyte.protocol.models.Field;
import io.airbyte.protocol.models.Field.JsonSchemaPrimitive;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.lang3.RandomStringUtils;
import org.jooq.SQLDialect;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.utility.MountableFile;

class MySqlSourceTest {

  private static final String TEST_USER = "test";
  private static final String TEST_PASSWORD = "test";
  private static final String STREAM_NAME = "id_and_name";
  private static final AirbyteCatalog CATALOG = CatalogHelpers.createAirbyteCatalog(
      STREAM_NAME,
      Field.of("id", JsonSchemaPrimitive.NUMBER),
      Field.of("name", JsonSchemaPrimitive.STRING));
  private static final Set<AirbyteMessage> MESSAGES = Sets.newHashSet(
      new AirbyteMessage().withType(Type.RECORD)
          .withRecord(new AirbyteRecordMessage().withStream(STREAM_NAME).withData(Jsons.jsonNode(ImmutableMap.of("id", 1, "name", "picard")))),
      new AirbyteMessage().withType(Type.RECORD)
          .withRecord(new AirbyteRecordMessage().withStream(STREAM_NAME).withData(Jsons.jsonNode(ImmutableMap.of("id", 2, "name", "crusher")))),
      new AirbyteMessage().withType(Type.RECORD)
          .withRecord(new AirbyteRecordMessage().withStream(STREAM_NAME).withData(Jsons.jsonNode(ImmutableMap.of("id", 3, "name", "vash")))));

  private JsonNode config;

  private static MySQLContainer<?> db;

  @BeforeAll
  static void init() {
    MoreResources.writeResource("init.sql",
            "CREATE USER '"+ TEST_USER + "'@'%' IDENTIFIED BY '" + TEST_PASSWORD + "';\n"
                + "GRANT ALL PRIVILEGES ON *.* TO '" + TEST_USER + "'@'%';\n"
    );
    db = new MySQLContainer<>("mysql:8.0").withInitScript("init.sql").withUsername("root").withPassword("");
    db.start();
  }

  @BeforeEach
  void setup() throws SQLException {
    config = Jsons.jsonNode(ImmutableMap.builder()
        .put("host", db.getHost())
        .put("port", db.getFirstMappedPort())
        .put("database", "db_" + RandomStringUtils.randomAlphabetic(10))
        .put("username", TEST_USER)
        .put("password", TEST_PASSWORD)
        .build());

    final BasicDataSource connectionPool = DatabaseHelper.getConnectionPool(
        config.get("username").asText(),
        config.get("password").asText(),
        String.format("jdbc:mysql://%s:%s",
            config.get("host").asText(),
            config.get("port").asText()),
        "com.mysql.cj.jdbc.Driver");

    DatabaseHelper.query(connectionPool, ctx -> {
      ctx.fetch("CREATE DATABASE " + config.get("database").asText());
      ctx.fetch("USE " + config.get("database").asText());
      ctx.fetch("CREATE TABLE id_and_name(id INTEGER, name VARCHAR(200));");
      ctx.fetch("INSERT INTO id_and_name (id, name) VALUES (1,'picard'),  (2, 'crusher'), (3, 'vash');");

      return null;
    }, SQLDialect.MYSQL);
  }

  @AfterAll
  static void cleanUp() {
    db.stop();
    db.close();
  }

  @Test
  void testSpec() throws Exception {
    final ConnectorSpecification actual = new MySqlSource().spec();
    final String resourceString = MoreResources.readResource("spec.json");
    final ConnectorSpecification expected = Jsons.deserialize(resourceString, ConnectorSpecification.class);

    assertEquals(expected, actual);
  }

  @Test
  void testCheckSuccess() throws Exception {
    final AirbyteConnectionStatus actual = new MySqlSource().check(config);
    final AirbyteConnectionStatus expected = new AirbyteConnectionStatus().withStatus(Status.SUCCEEDED);
    assertEquals(expected, actual);
  }

  @Test
  void testCheckFailure() throws Exception {
    ((ObjectNode) config).put("password", "fake");
    final AirbyteConnectionStatus actual = new MySqlSource().check(config);
    final AirbyteConnectionStatus expected = new AirbyteConnectionStatus().withStatus(Status.FAILED)
        .withMessage("Cannot create PoolableConnectionFactory (Access denied for user 'test'@'172.17.0.1' (using password: YES))");
    assertEquals(expected, actual);
  }

  @Test
  void testDiscover() throws Exception {
    final AirbyteCatalog actual = new MySqlSource().discover(config);
    assertEquals(CATALOG, actual);
  }

  @Test
  void testReadSuccess() throws Exception {
    final Set<AirbyteMessage> actualMessages = new MySqlSource().read(config, CATALOG, null).collect(Collectors.toSet());

    actualMessages.forEach(r -> {
      if (r.getRecord() != null) {
        r.getRecord().setEmittedAt(null);
      }
    });

    assertEquals(MESSAGES, actualMessages);
  }

  @SuppressWarnings("ResultOfMethodCallIgnored")
  @Test
  void testReadFailure() throws Exception {
    final AirbyteStream spiedAbStream = spy(CATALOG.getStreams().get(0));
    final AirbyteCatalog catalog = new AirbyteCatalog().withStreams(Lists.newArrayList(spiedAbStream));
    doThrow(new RuntimeException()).when(spiedAbStream).getName();

    final Stream<AirbyteMessage> stream = new MySqlSource().read(config, catalog, null);

    assertThrows(RuntimeException.class, () -> stream.collect(Collectors.toList()));
  }

}
