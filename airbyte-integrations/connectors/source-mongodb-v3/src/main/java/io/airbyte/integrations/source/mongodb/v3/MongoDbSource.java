/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.source.mongodb.v3;

import com.fasterxml.jackson.databind.JsonNode;
import io.airbyte.commons.util.AutoCloseableIterator;
import io.airbyte.integrations.base.IntegrationRunner;
import io.airbyte.integrations.base.Source;
import io.airbyte.protocol.models.v0.AirbyteCatalog;
import io.airbyte.protocol.models.v0.AirbyteConnectionStatus;
import io.airbyte.protocol.models.v0.AirbyteMessage;
import io.airbyte.protocol.models.v0.ConfiguredAirbyteCatalog;
import io.airbyte.protocol.models.v0.ConnectorSpecification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MongoDbSource implements Source, AutoCloseable {

  private static final Logger LOGGER = LoggerFactory.getLogger(MongoDbSource.class);

  public static void main(final String[] args) throws Exception {
    final Source source = new MongoDbSource();
    LOGGER.info("starting source: {}", MongoDbSource.class);
    new IntegrationRunner(source).run(args);
    LOGGER.info("completed source: {}", MongoDbSource.class);
  }


  @Override
  public AirbyteConnectionStatus check(JsonNode config) throws Exception {
    return null;
  }

  @Override
  public AirbyteCatalog discover(JsonNode config) throws Exception {
    return null;
  }

  @Override
  public AutoCloseableIterator<AirbyteMessage> read(JsonNode config, ConfiguredAirbyteCatalog catalog, JsonNode state) throws Exception {
    return null;
  }

  @Override
  public ConnectorSpecification spec() throws Exception {
    return null;
  }

  @Override
  public void close() throws Exception {

  }

}
