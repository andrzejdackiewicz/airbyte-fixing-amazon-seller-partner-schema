/*
 * Copyright (c) 2021 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.kinesis;

import io.airbyte.commons.json.Jsons;
import io.airbyte.integrations.base.AirbyteStreamNameNamespacePair;
import io.airbyte.integrations.base.FailureTrackingAirbyteMessageConsumer;
import io.airbyte.protocol.models.AirbyteMessage;
import io.airbyte.protocol.models.ConfiguredAirbyteCatalog;
import java.time.Instant;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * KinesisMessageConsumer class for handling incoming Airbyte messages.
 */
public class KinesisMessageConsumer extends FailureTrackingAirbyteMessageConsumer {

  private static final Logger LOGGER = LoggerFactory.getLogger(KinesisMessageConsumer.class);

  private final Consumer<AirbyteMessage> outputRecordCollector;

  private final KinesisStream kinesisStream;

  private final Map<AirbyteStreamNameNamespacePair, KinesisStreamConfig> kinesisStreams;

  private AirbyteMessage lastMessage = null;

  public KinesisMessageConsumer(KinesisConfig kinesisConfig,
                                ConfiguredAirbyteCatalog configuredCatalog,
                                Consumer<AirbyteMessage> outputRecordCollector) {
    this.outputRecordCollector = outputRecordCollector;
    this.kinesisStream = new KinesisStream(kinesisConfig);
    this.kinesisStreams = configuredCatalog.getStreams().stream()
        .collect(Collectors.toUnmodifiableMap(
            AirbyteStreamNameNamespacePair::fromConfiguredAirbyteSteam,
            k -> new KinesisStreamConfig(
                k.getStream().getName(), k.getStream().getNamespace(), k.getDestinationSyncMode())));
  }

  /**
   * Start tracking the incoming Airbyte streams by creating the needed Kinesis streams.
   */
  @Override
  protected void startTracked() {
    kinesisStreams.forEach((k, v) -> kinesisStream.createStream(v.getStreamName()));
  }

  /**
   * Handle an incoming Airbyte message by serializing it to the appropriate Kinesis structure and
   * sending it to the stream.
   *
   * @param message received from the Airbyte source.
   */
  @Override
  protected void acceptTracked(AirbyteMessage message) {
    if (message.getType() == AirbyteMessage.Type.RECORD) {
      var messageRecord = message.getRecord();

      var streamConfig =
          kinesisStreams.get(AirbyteStreamNameNamespacePair.fromRecordMessage(messageRecord));

      if (streamConfig == null) {
        throw new IllegalArgumentException("Unrecognized destination stream");
      }

      var partitionKey = KinesisUtils.buildPartitionKey();

      var data = Jsons.jsonNode(Map.of(
          KinesisRecord.COLUMN_NAME_AB_ID, partitionKey,
          KinesisRecord.COLUMN_NAME_DATA, Jsons.serialize(messageRecord.getData()),
          KinesisRecord.COLUMN_NAME_EMITTED_AT, Instant.now(),
          KinesisRecord.DATA_SOURCE_IDENTIFIER, streamConfig.getNamespace()));

      var streamName = streamConfig.getStreamName();
      kinesisStream.putRecord(streamName, partitionKey, Jsons.serialize(data), e -> {
        LOGGER.error("Error while streaming data to Kinesis", e);
        // throw exception and end sync?
      });
    } else if (message.getType() == AirbyteMessage.Type.STATE) {
      this.lastMessage = message;
    } else {
      LOGGER.warn("Unsupported airbyte message type: {}", message.getType());
    }
  }

  /**
   * Flush the Kinesis stream if there are any remaining messages to be sent and close the client as a
   * terminal operation.
   *
   * @param hasFailed flag for indicating if the operation has failed.
   */
  @Override
  protected void close(boolean hasFailed) {
    try {
      if (!hasFailed) {
        kinesisStream.flush(e -> {
          LOGGER.error("Error while streaming data to Kinesis", e);
        });
        this.outputRecordCollector.accept(lastMessage);
      }
    } finally {
      kinesisStream.close();
    }
  }

}
