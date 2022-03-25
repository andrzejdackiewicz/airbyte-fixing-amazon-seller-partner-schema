/*
 * Copyright (c) 2021 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.s3;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Preconditions;
import io.airbyte.commons.functional.CheckedBiConsumer;
import io.airbyte.commons.functional.CheckedBiFunction;
import io.airbyte.commons.json.Jsons;
import io.airbyte.integrations.base.AirbyteMessageConsumer;
import io.airbyte.integrations.base.AirbyteStreamNameNamespacePair;
import io.airbyte.integrations.base.sentry.AirbyteSentry;
import io.airbyte.integrations.destination.NamingConventionTransformer;
import io.airbyte.integrations.destination.buffered_stream_consumer.BufferedStreamConsumer;
import io.airbyte.integrations.destination.buffered_stream_consumer.OnCloseFunction;
import io.airbyte.integrations.destination.buffered_stream_consumer.OnStartFunction;
import io.airbyte.integrations.destination.record_buffer.SerializableBuffer;
import io.airbyte.integrations.destination.record_buffer.SerializedBufferingStrategy;
import io.airbyte.protocol.models.AirbyteMessage;
import io.airbyte.protocol.models.AirbyteStream;
import io.airbyte.protocol.models.ConfiguredAirbyteCatalog;
import io.airbyte.protocol.models.ConfiguredAirbyteStream;
import io.airbyte.protocol.models.DestinationSyncMode;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.commons.io.FileUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class S3ConsumerFactory {

  private static final Logger LOGGER = LoggerFactory.getLogger(S3ConsumerFactory.class);
  private static final DateTime SYNC_DATETIME = DateTime.now(DateTimeZone.UTC);

  public AirbyteMessageConsumer create(final Consumer<AirbyteMessage> outputRecordCollector,
                                       final BlobStorageOperations storageOperations,
                                       final NamingConventionTransformer namingResolver,
                                       final CheckedBiFunction<AirbyteStreamNameNamespacePair, ConfiguredAirbyteCatalog, SerializableBuffer, Exception> onCreateBuffer,
                                       final JsonNode config,
                                       final ConfiguredAirbyteCatalog catalog) {
    final List<WriteConfig> writeConfigs = createWriteConfigs(storageOperations, namingResolver, config, catalog);
    return new BufferedStreamConsumer(
        outputRecordCollector,
        onStartFunction(storageOperations, writeConfigs),
        new SerializedBufferingStrategy(
            onCreateBuffer,
            catalog,
            flushBufferFunction(storageOperations, writeConfigs, catalog)),
        onCloseFunction(storageOperations, writeConfigs),
        catalog,
        storageOperations::isValidData);
  }

  private static List<WriteConfig> createWriteConfigs(final BlobStorageOperations storageOperations,
                                                      final NamingConventionTransformer namingResolver,
                                                      final JsonNode config,
                                                      final ConfiguredAirbyteCatalog catalog) {
    return catalog.getStreams()
        .stream()
        .map(toWriteConfig(storageOperations, namingResolver, config))
        .collect(Collectors.toList());
  }

  private static Function<ConfiguredAirbyteStream, WriteConfig> toWriteConfig(
                                                                              final BlobStorageOperations storageOperations,
                                                                              final NamingConventionTransformer namingResolver,
                                                                              final JsonNode config) {
    return stream -> {
      Preconditions.checkNotNull(stream.getDestinationSyncMode(), "Undefined destination sync mode");
      final AirbyteStream abStream = stream.getStream();
      final String namespace = abStream.getNamespace();
      final String streamName = abStream.getName();
      final String outputNamespace = getOutputNamespace(abStream, config.get("s3_bucket_path").asText(), namingResolver);
      final String customOutputFormat =
          config.has("s3_path_format") && !config.get("s3_path_format").asText().isBlank() ? config.get("s3_path_format").asText()
              : S3DestinationConstants.DEFAULT_PATH_FORMAT;
      final String outputBucketPath = storageOperations.getBucketObjectPath(outputNamespace, streamName, SYNC_DATETIME, customOutputFormat);
      final DestinationSyncMode syncMode = stream.getDestinationSyncMode();
      final WriteConfig writeConfig = new WriteConfig(namespace, streamName, outputNamespace, outputBucketPath, syncMode);
      LOGGER.info("Write config: {}", writeConfig);
      return writeConfig;
    };
  }

  private static String getOutputNamespace(final AirbyteStream stream,
                                           final String defaultDestNamespace,
                                           final NamingConventionTransformer namingResolver) {
    return stream.getNamespace() != null
        ? namingResolver.getNamespace(stream.getNamespace())
        : namingResolver.getNamespace(defaultDestNamespace);
  }

  private OnStartFunction onStartFunction(final BlobStorageOperations storageOperations, final List<WriteConfig> writeConfigs) {
    return () -> {
      LOGGER.info("Preparing bucket in destination started for {} streams", writeConfigs.size());
      for (final WriteConfig writeConfig : writeConfigs) {
        final String namespace = writeConfig.getNamespace();
        final String stream = writeConfig.getStreamName();
        final String outputBucketPath = writeConfig.getOutputBucketPath();
        if (writeConfig.getSyncMode().equals(DestinationSyncMode.OVERWRITE)) {
          LOGGER.info("Clearing storage area in destination started for namespace {} stream {} bucketObject {}", namespace, stream, outputBucketPath);
          AirbyteSentry.executeWithTracing("PrepareStreamStorage",
              () -> storageOperations.dropBucketObject(outputBucketPath),
              Map.of("namespace", Objects.requireNonNullElse(namespace, "null"), "stream", stream, "storage", outputBucketPath));
          LOGGER.info("Clearing storage area in destination completed for namespace {} stream {} bucketObject {}", namespace, stream,
              outputBucketPath);
        }
      }
      LOGGER.info("Preparing storage area in destination completed.");
    };
  }

  private static AirbyteStreamNameNamespacePair toNameNamespacePair(final WriteConfig config) {
    return new AirbyteStreamNameNamespacePair(config.getStreamName(), config.getNamespace());
  }

  private CheckedBiConsumer<AirbyteStreamNameNamespacePair, SerializableBuffer, Exception> flushBufferFunction(final BlobStorageOperations storageOperations,
                                                                                                               final List<WriteConfig> writeConfigs,
                                                                                                               final ConfiguredAirbyteCatalog catalog) {
    final Map<AirbyteStreamNameNamespacePair, WriteConfig> pairToWriteConfig =
        writeConfigs.stream()
            .collect(Collectors.toUnmodifiableMap(
                S3ConsumerFactory::toNameNamespacePair, Function.identity()));

    return (pair, writer) -> {
      LOGGER.info("Flushing buffer for stream {} ({}) to storage", pair.getName(), FileUtils.byteCountToDisplaySize(writer.getByteCount()));
      if (!pairToWriteConfig.containsKey(pair)) {
        throw new IllegalArgumentException(
            String.format("Message contained record from a stream %s that was not in the catalog. \ncatalog: %s", pair, Jsons.serialize(catalog)));
      }

      final WriteConfig writeConfig = pairToWriteConfig.get(pair);
      try (writer) {
        writer.flush();
        writeConfig.addStoredFile(storageOperations.uploadRecordsToBucket(
            writer,
            writeConfig.getOutputNamespace(),
            writeConfig.getStreamName(),
            writeConfig.getOutputBucketPath()));
      } catch (final Exception e) {
        LOGGER.error("Failed to flush and upload buffer to storage:", e);
        throw new RuntimeException("Failed to upload buffer to storage", e);
      }
    };
  }

  private OnCloseFunction onCloseFunction(final BlobStorageOperations storageOperations,
                                          final List<WriteConfig> writeConfigs) {
    return (hasFailed) -> {
      if (hasFailed) {
        LOGGER.info("Cleaning up destination started for {} streams", writeConfigs.size());
        for (final WriteConfig writeConfig : writeConfigs) {
          storageOperations.cleanUpBucketObject(writeConfig.getOutputBucketPath(), writeConfig.getStoredFiles());
          writeConfig.clearStoredFiles();
        }
        LOGGER.info("Cleaning up destination completed.");
      }
    };
  }

}
