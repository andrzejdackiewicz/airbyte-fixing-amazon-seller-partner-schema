/*
 * Copyright (c) 2022 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.source.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import io.airbyte.commons.util.AutoCloseableIterator;
import io.airbyte.integrations.BaseConnector;
import io.airbyte.integrations.base.IntegrationRunner;
import io.airbyte.integrations.base.Source;
import io.airbyte.integrations.source.kafka.format.KafkaFormat;
import io.airbyte.protocol.models.*;
import io.airbyte.protocol.models.AirbyteConnectionStatus.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class KafkaSource extends BaseConnector implements Source {

  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaSource.class);

  public KafkaSource() {}

  @Override
  public AirbyteConnectionStatus check(final JsonNode config) {
    KafkaFormat  kafkaFormat =KafkaFormatFactory.getFormat(config);
    if(kafkaFormat.isAccessible()){
      return new AirbyteConnectionStatus().withStatus(Status.SUCCEEDED);
    }
      return new AirbyteConnectionStatus()
          .withStatus(Status.FAILED)
          .withMessage("Could not connect to the Kafka brokers with provided configuration. \n" );
  }

  @Override
  public AirbyteCatalog discover(final JsonNode config) {
    KafkaFormat  kafkaFormat =KafkaFormatFactory.getFormat(config);
    final List<AirbyteStream> streams = kafkaFormat.getStreams();
    return new AirbyteCatalog().withStreams(streams);
  }

  @Override
  public AutoCloseableIterator<AirbyteMessage> read(final JsonNode config, final ConfiguredAirbyteCatalog catalog, final JsonNode state)
      throws Exception {
    final AirbyteConnectionStatus check = check(config);
    if (check.getStatus().equals(AirbyteConnectionStatus.Status.FAILED)) {
      throw new RuntimeException("Unable establish a connection: " + check.getMessage());
    }
    KafkaFormat  kafkaFormat =KafkaFormatFactory.getFormat(config);
    return kafkaFormat.read();
  }

  public static void main(final String[] args) throws Exception {
    final Source source = new KafkaSource();
    LOGGER.info("Starting source: {}", KafkaSource.class);
    new IntegrationRunner(source).run(args);
    LOGGER.info("Completed source: {}", KafkaSource.class);
  }

}
