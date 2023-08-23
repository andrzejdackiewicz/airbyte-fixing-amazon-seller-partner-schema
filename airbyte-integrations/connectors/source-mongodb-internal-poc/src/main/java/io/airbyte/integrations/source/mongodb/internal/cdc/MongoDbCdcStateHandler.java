package io.airbyte.integrations.source.mongodb.internal.cdc;

import io.airbyte.integrations.debezium.CdcStateHandler;
import io.airbyte.integrations.source.mongodb.internal.state.MongoDbStateManager;
import io.airbyte.protocol.models.v0.AirbyteMessage;
import io.airbyte.protocol.models.v0.AirbyteStateMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static io.airbyte.integrations.debezium.internals.mongodb.MongoDbDebeziumConstants.ChangeEvent.SOURCE_ORDER;
import static io.airbyte.integrations.debezium.internals.mongodb.MongoDbDebeziumConstants.ChangeEvent.SOURCE_RESUME_TOKEN;
import static io.airbyte.integrations.debezium.internals.mongodb.MongoDbDebeziumConstants.ChangeEvent.SOURCE_SECONDS;

/**
 * Implementation of the {@link CdcStateHandler} that handles saving the CDC offset as Airbyte state for MongoDB.
 */
public class MongoDbCdcStateHandler implements CdcStateHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(MongoDbCdcStateHandler.class);

    private final MongoDbStateManager stateManager;

    public MongoDbCdcStateHandler(MongoDbStateManager stateManager) {
        this.stateManager = stateManager;
    }

    @Override
    public AirbyteMessage saveState(final Map<String, String> offset, String dbHistory) {
        final MongoDbCdcState cdcState = new MongoDbCdcState(
                Integer.valueOf(offset.getOrDefault(SOURCE_SECONDS, "0")),
                Integer.valueOf(offset.getOrDefault(SOURCE_ORDER, "0")),
                offset.get(SOURCE_RESUME_TOKEN));

        LOGGER.info("Saving Debezium state {}...", cdcState);
        stateManager.updateCdcState(cdcState);

        final AirbyteStateMessage stateMessage = stateManager.toState();
        return new AirbyteMessage().withType(AirbyteMessage.Type.STATE).withState(stateMessage);
    }

    @Override
    public AirbyteMessage saveStateAfterCompletionOfSnapshotOfNewStreams() {
        LOGGER.info("Snapshot of new collections is complete, saving state...");

        final AirbyteStateMessage stateMessage = stateManager.toState();
        return new AirbyteMessage().withType(AirbyteMessage.Type.STATE).withState(stateMessage);
    }

    @Override
    public boolean isCdcCheckpointEnabled() {
        return true;
    }
}
