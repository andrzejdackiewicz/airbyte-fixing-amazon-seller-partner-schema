package io.airbyte.integrations.destination.elasticsearch;

import co.elastic.clients.elasticsearch._core.BulkResponse;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.airbyte.commons.concurrency.VoidCallable;
import io.airbyte.commons.functional.CheckedConsumer;
import io.airbyte.commons.functional.CheckedFunction;
import io.airbyte.integrations.base.AirbyteMessageConsumer;
import io.airbyte.integrations.destination.StandardNameTransformer;
import io.airbyte.integrations.destination.buffered_stream_consumer.BufferedStreamConsumer;
import io.airbyte.integrations.destination.buffered_stream_consumer.RecordWriter;
import io.airbyte.protocol.models.AirbyteMessage;
import io.airbyte.protocol.models.ConfiguredAirbyteCatalog;
import io.airbyte.protocol.models.DestinationSyncMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

public class ElasticsearchAirbyteMessageConsumerFactory {

    private static final Logger log = LoggerFactory.getLogger(ElasticsearchAirbyteMessageConsumerFactory.class);
    private static final StandardNameTransformer namingResolver = new StandardNameTransformer();
    private static final int MAX_BATCH_SIZE = 10000;
    private static final ObjectMapper mapper = new ObjectMapper();

    private static AtomicLong recordsWritten = new AtomicLong(0);

    /**
     * Holds a mapping of temp to target indices.
     * After closing a sync job, the target index is removed if it already exists, and the temp index is copied to replace it.
     */
    private static final Map<String, String> tempIndices = new HashMap<>();

    public static AirbyteMessageConsumer create(Consumer<AirbyteMessage> outputRecordCollector,
                                                ElasticsearchConnection connection,
                                                Map<String, ElasticsearchWriteConfig> writeConfigs,
                                                ConfiguredAirbyteCatalog catalog) {

        return new BufferedStreamConsumer(
                outputRecordCollector,
                onStartFunction(connection, writeConfigs),
                recordWriterFunction(connection, writeConfigs),
                onCloseFunction(connection),
                catalog,
                isValidFunction(connection),
                MAX_BATCH_SIZE);
    }

    // is there any json node that wont fit in the index?
    private static CheckedFunction<JsonNode, Boolean, Exception> isValidFunction(ElasticsearchConnection connection) {
        return jsonNode -> {
            return true;
        };
    }

    private static CheckedConsumer<Boolean, Exception> onCloseFunction(ElasticsearchConnection connection) {

        return (hasFailed) -> {
            if (!tempIndices.isEmpty() && !hasFailed) {
                tempIndices.entrySet().stream().forEach(m -> {
                    connection.replaceIndex(m.getKey(), m.getValue());
                });
            }
            connection.close();
        };
    }

    private static RecordWriter recordWriterFunction(
            ElasticsearchConnection connection, Map<String, ElasticsearchWriteConfig> writeConfigs) {

        return (pair, records) -> {
            log.info("writing {} records in bulk operation", records.size());
            var config = writeConfigs.get(pair.getName());
            BulkResponse response = null;
            switch (config.getSyncMode()) {
                case APPEND, APPEND_DEDUP -> {
                    response = connection.indexDocuments(streamToIndexName(pair.getNamespace(), pair.getName()), records, config);
                }
                case OVERWRITE -> {
                    response = connection.indexDocuments(streamToTempIndexName(pair.getNamespace(), pair.getName()), records, config);
                }
            }
            if (response.errors()) {
                var items = mapper.valueToTree(response.items());
                String msg = String.format("failed to write bulk records: %s", items);
                throw new Exception(msg);
            } else {
                log.info("bulk write took: {}ms", response.took());
            }
        };
    }

    private static VoidCallable onStartFunction(ElasticsearchConnection connection, Map<String, ElasticsearchWriteConfig> writeConfigs) {
        return () -> {
            for (var config :
                    writeConfigs.entrySet()) {
                var targetIndex = streamToIndexName(config.getValue().getNamespace(), config.getKey());
                if (config.getValue().getSyncMode() == DestinationSyncMode.OVERWRITE) {
                    var tempIndex = streamToTempIndexName(config.getValue().getNamespace(), config.getKey());
                    tempIndices.put(tempIndex, targetIndex);
                    connection.deleteIndexIfPresent(tempIndex);
                }
                connection.createIndexIfMissing(targetIndex);
            }
        };
    }

    protected static String streamToIndexName(String namespace, String streamName) {
        String prefix = "";
        if (Objects.nonNull(namespace) && !namespace.isEmpty()) {
            prefix = String.format("%s_", namespace);
        }
        return String.format("%s%s", prefix, namingResolver.getIdentifier(streamName));
    }

    protected static String streamToTempIndexName(String namespace, String streamName) {
        return String.format("tmp_%s", streamToIndexName(namespace, streamName));
    }
}
