/*
 * Copyright (c) 2021 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.e2e_test;

import com.fasterxml.jackson.databind.JsonNode;
import io.airbyte.integrations.BaseConnector;
import io.airbyte.integrations.base.AirbyteMessageConsumer;
import io.airbyte.integrations.base.Destination;
import io.airbyte.protocol.models.AirbyteConnectionStatus;
import io.airbyte.protocol.models.AirbyteConnectionStatus.Status;
import io.airbyte.protocol.models.AirbyteMessage;
import io.airbyte.protocol.models.AirbyteMessage.Type;
import io.airbyte.protocol.models.ConfiguredAirbyteCatalog;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FailAfterNDestination extends BaseConnector implements Destination {

  private static final Logger LOGGER = LoggerFactory.getLogger(FailAfterNDestination.class);

  @Override
  public AirbyteConnectionStatus check(final JsonNode config) {
    return new AirbyteConnectionStatus().withStatus(Status.SUCCEEDED);
  }

  @Override
  public AirbyteMessageConsumer getConsumer(final JsonNode config,
                                            final ConfiguredAirbyteCatalog catalog,
                                            final Consumer<AirbyteMessage> outputRecordCollector) {
    return new FailAfterNConsumer(config.get("num_messages").asLong(), outputRecordCollector);
  }

  public static class FailAfterNConsumer implements AirbyteMessageConsumer {

    private final Consumer<AirbyteMessage> outputRecordCollector;
    private final long numMessagesAfterWhichToFail;
    private long numMessagesSoFar;

    public FailAfterNConsumer(final long numMessagesAfterWhichToFail, final Consumer<AirbyteMessage> outputRecordCollector) {
      this.numMessagesAfterWhichToFail = numMessagesAfterWhichToFail;
      this.outputRecordCollector = outputRecordCollector;
      this.numMessagesSoFar = 0;
    }

    @Override
    public void start() {}

    @Override
    public void accept(final AirbyteMessage message) throws Exception {
      LOGGER.info("received record: {}", message);
      numMessagesSoFar += 1;
      LOGGER.info("received {} messages so far", numMessagesSoFar);

      if (numMessagesSoFar > numMessagesAfterWhichToFail) {
        throw new IllegalStateException("Forcing a fail after processing " + numMessagesAfterWhichToFail + " messages.");
      }

      if (message.getType() == Type.STATE) {
        LOGGER.info("emitting state: {}", message);
        outputRecordCollector.accept(message);
      }
    }

    @Override
    public void close() {}

  }

}
