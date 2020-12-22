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

package io.airbyte.integrations.destination.postgres;

import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.table;
import static org.jooq.impl.DSL.unquotedName;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.lang.CloseableQueue;
import io.airbyte.integrations.base.Destination;
import io.airbyte.integrations.base.IntegrationRunner;
import io.airbyte.integrations.destination.jdbc.AbstractJdbcDestination;
import io.airbyte.protocol.models.AirbyteRecordMessage;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.UUID;
import org.jooq.InsertValuesStep3;
import org.jooq.JSONB;
import org.jooq.Record;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PostgresDestination extends AbstractJdbcDestination implements Destination {

  private static final Logger LOGGER = LoggerFactory.getLogger(PostgresDestination.class);
  protected static final String COLUMN_NAME = AbstractJdbcDestination.COLUMN_NAME;

  public PostgresDestination() {
    super("org.postgresql.Driver", SQLDialect.POSTGRES, new PostgresSQLNameTransformer());
  }

  @Override
  public JsonNode toJdbcConfig(JsonNode config) {
    ImmutableMap.Builder<Object, Object> configBuilder = ImmutableMap.builder()
        .put("username", config.get("username").asText())
        .put("jdbc_url", String.format("jdbc:postgresql://%s:%s/%s",
            config.get("host").asText(),
            config.get("port").asText(),
            config.get("database").asText()))
        .put("schema", getDefaultSchemaName(config));

    if (config.has("password")) {
      configBuilder.put("password", config.get("password").asText());
    }
    return Jsons.jsonNode(configBuilder.build());
  }

  @Override
  protected String createDestinationTableQuery(String schemaName, String tableName) {
    return String.format(
        "CREATE TABLE IF NOT EXISTS %s.%s ( \n"
            + "ab_id VARCHAR PRIMARY KEY,\n"
            + "%s JSONB,\n"
            + "emitted_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP\n"
            + ");\n",
        schemaName, tableName, COLUMN_NAME);
  }

  @Override
  protected String insertBufferedRecordsQuery(int batchSize, CloseableQueue<byte[]> writeBuffer, String schemaName, String tableName) {
    InsertValuesStep3<Record, String, JSONB, OffsetDateTime> step =
        DSL.insertInto(
            table(unquotedName(schemaName, tableName)),
            field("ab_id", String.class),
            field(COLUMN_NAME, JSONB.class),
            field("emitted_at", OffsetDateTime.class));

    for (int i = 0; i < batchSize; i++) {
      final byte[] record = writeBuffer.poll();
      if (record == null) {
        break;
      }
      final AirbyteRecordMessage message = Jsons.deserialize(new String(record, Charsets.UTF_8), AirbyteRecordMessage.class);

      step = step.values(
          UUID.randomUUID().toString(),
          JSONB.valueOf(Jsons.serialize(message.getData())),
          OffsetDateTime.of(LocalDateTime.ofEpochSecond(message.getEmittedAt() / 1000, 0, ZoneOffset.UTC), ZoneOffset.UTC));
    }
    return step.toString();
  }

  public static void main(String[] args) throws Exception {
    final Destination destination = new PostgresDestination();
    LOGGER.info("starting destination: {}", PostgresDestination.class);
    new IntegrationRunner(destination).run(args);
    LOGGER.info("completed destination: {}", PostgresDestination.class);
  }

}
