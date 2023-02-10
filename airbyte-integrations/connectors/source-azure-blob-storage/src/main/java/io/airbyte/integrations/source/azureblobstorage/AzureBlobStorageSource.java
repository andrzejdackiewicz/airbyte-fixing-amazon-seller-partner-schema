package io.airbyte.integrations.source.azureblobstorage;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.airbyte.commons.features.EnvVariableFeatureFlags;
import io.airbyte.commons.features.FeatureFlags;
import io.airbyte.commons.util.AutoCloseableIterator;
import io.airbyte.commons.util.AutoCloseableIterators;
import io.airbyte.integrations.BaseConnector;
import io.airbyte.integrations.base.IntegrationRunner;
import io.airbyte.integrations.base.Source;
import io.airbyte.integrations.source.relationaldb.CursorInfo;
import io.airbyte.integrations.source.relationaldb.StateDecoratingIterator;
import io.airbyte.integrations.source.relationaldb.state.StateManager;
import io.airbyte.integrations.source.relationaldb.state.StateManagerFactory;
import io.airbyte.protocol.models.JsonSchemaPrimitiveUtil;
import io.airbyte.protocol.models.v0.AirbyteCatalog;
import io.airbyte.protocol.models.v0.AirbyteConnectionStatus;
import io.airbyte.protocol.models.v0.AirbyteMessage;
import io.airbyte.protocol.models.v0.AirbyteRecordMessage;
import io.airbyte.protocol.models.v0.AirbyteStream;
import io.airbyte.protocol.models.v0.AirbyteStreamNameNamespacePair;
import io.airbyte.protocol.models.v0.ConfiguredAirbyteCatalog;
import io.airbyte.protocol.models.v0.SyncMode;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Optional;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AzureBlobStorageSource extends BaseConnector implements Source {

    private static final Logger LOGGER = LoggerFactory.getLogger(AzureBlobStorageSource.class);

    private final FeatureFlags featureFlags = new EnvVariableFeatureFlags();

    private final ObjectMapper objectMapper = new ObjectMapper();

    public static void main(final String[] args) throws Exception {
        final Source source = new AzureBlobStorageSource();
        LOGGER.info("starting Source: {}", AzureBlobStorageSource.class);
        new IntegrationRunner(source).run(args);
        LOGGER.info("completed Source: {}", AzureBlobStorageSource.class);
    }

    @Override
    public AirbyteConnectionStatus check(JsonNode config) throws Exception {
        var azureBlobStorageConfig = AzureBlobStorageConfig.createAzureBlobStorageConfig(config);
        try {
            var azureBlobStorageOperations = new AzureBlobStorageOperations(azureBlobStorageConfig);
            azureBlobStorageOperations.listBlobs();

            return new AirbyteConnectionStatus()
                .withStatus(AirbyteConnectionStatus.Status.SUCCEEDED);
        } catch (Exception e) {
            LOGGER.error("Error while listing Azure Blob Storage blobs with reason: ", e);
            return new AirbyteConnectionStatus()
                .withStatus(AirbyteConnectionStatus.Status.FAILED);
        }

    }

    @Override
    public AirbyteCatalog discover(JsonNode config) throws Exception {
        var azureBlobStorageConfig = AzureBlobStorageConfig.createAzureBlobStorageConfig(config);

        JsonNode schema;
        if (!StringUtils.isBlank(azureBlobStorageConfig.schema())) {
            schema = objectMapper.readTree(azureBlobStorageConfig.schema());
        } else {
            var azureBlobStorageOperations = new AzureBlobStorageOperations(azureBlobStorageConfig);
            schema = azureBlobStorageOperations.inferSchema();
        }

        return new AirbyteCatalog()
            .withStreams(List.of(new AirbyteStream()
                .withName(azureBlobStorageConfig.containerName())
                .withJsonSchema(schema)
                .withSourceDefinedCursor(true)
                .withDefaultCursorField(List.of(AzureBlobAdditionalProperties.LAST_MODIFIED))
                .withSupportedSyncModes(List.of(SyncMode.INCREMENTAL, SyncMode.FULL_REFRESH))));
    }

    @Override
    public AutoCloseableIterator<AirbyteMessage> read(JsonNode config, ConfiguredAirbyteCatalog catalog, JsonNode state)
        throws Exception {

        final var streamState =
            AzureBlobStorageStateManager.deserializeStreamState(state, featureFlags.useStreamCapableState());

        final StateManager stateManager = StateManagerFactory
            .createStateManager(streamState.airbyteStateType(), streamState.airbyteStateMessages(), catalog);

        var azureBlobStorageConfig = AzureBlobStorageConfig.createAzureBlobStorageConfig(config);
        var azureBlobStorageOperations = new AzureBlobStorageOperations(azureBlobStorageConfig);

        // only one stream per connection
        var streamIterators = catalog.getStreams().stream()
            .map(cas -> switch (cas.getSyncMode()) {
                case INCREMENTAL ->
                    readIncremental(azureBlobStorageOperations, cas.getStream(), cas.getCursorField().get(0),
                        stateManager);
                case FULL_REFRESH -> readFullRefresh(azureBlobStorageOperations, cas.getStream());
            })
            .toList();

        return AutoCloseableIterators.concatWithEagerClose(streamIterators);

    }

    private AutoCloseableIterator<AirbyteMessage> readIncremental(AzureBlobStorageOperations azureBlobStorageOperations,
                                                                  AirbyteStream airbyteStream,
                                                                  String cursorField,
                                                                  StateManager stateManager) {
        var streamPair = new AirbyteStreamNameNamespacePair(airbyteStream.getName(), airbyteStream.getNamespace());

        Optional<CursorInfo> cursorInfo = stateManager.getCursorInfo(streamPair);

        var messageStream = cursorInfo
            .map(cursor -> azureBlobStorageOperations.readBlobs(OffsetDateTime.parse(cursor.getCursor())))
            .orElse(azureBlobStorageOperations.readBlobs(null))
            .stream()
            .map(jn -> new AirbyteMessage()
                .withType(AirbyteMessage.Type.RECORD)
                .withRecord(new AirbyteRecordMessage()
                    .withStream(airbyteStream.getName())
                    .withEmittedAt(Instant.now().toEpochMilli())
                    .withData(jn)));

        return AutoCloseableIterators.transform(autoCloseableIterator -> new StateDecoratingIterator(
                autoCloseableIterator,
                stateManager,
                streamPair,
                cursorField,
                cursorInfo.map(CursorInfo::getCursor).orElse(null),
                JsonSchemaPrimitiveUtil.JsonSchemaPrimitive.TIMESTAMP_WITH_TIMEZONE_V1,
                // TODO (itaseski) emit state after every record since they are sorted in increasing order
                0),
            AutoCloseableIterators.fromStream(messageStream));
    }

    private AutoCloseableIterator<AirbyteMessage> readFullRefresh(AzureBlobStorageOperations azureBlobStorageOperations,
                                                                  AirbyteStream airbyteStream) {
        var messageStream = azureBlobStorageOperations
            .readBlobs(null)
            .stream()
            .map(jn -> new AirbyteMessage()
                .withType(AirbyteMessage.Type.RECORD)
                .withRecord(new AirbyteRecordMessage()
                    .withStream(airbyteStream.getName())
                    .withEmittedAt(Instant.now().toEpochMilli())
                    .withData(jn)));

        return AutoCloseableIterators.fromStream(messageStream);
    }
}
