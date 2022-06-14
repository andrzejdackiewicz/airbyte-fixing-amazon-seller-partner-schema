/*
 * Copyright (c) 2022 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.source.relationaldb.state;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.annotations.VisibleForTesting;
import io.airbyte.commons.json.Jsons;
import io.airbyte.integrations.source.relationaldb.models.DbState;
import io.airbyte.protocol.models.AirbyteStateMessage;
import io.airbyte.protocol.models.AirbyteStateMessage.AirbyteStateType;
import io.airbyte.protocol.models.ConfiguredAirbyteCatalog;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factory class that creates {@link StateManager} instances based on the provided state.
 */
public class StateManagerFactory {

  private static final Logger LOGGER = LoggerFactory.getLogger(StateManagerFactory.class);

  /**
   * Private constructor to prevent direct instantiation.
   */
  private StateManagerFactory() {}

  /**
   * Creates a {@link StateManager} based on the provided state object and catalog.
   *
   * @param state The deserialized state.
   * @param catalog The {@link ConfiguredAirbyteCatalog} for the connector that will utilize the state
   *        manager.
   * @param config The connector configuration.
   * @return A newly created {@link StateManager} implementation based on the provided state.
   */
  public static StateManager createStateManager(final List<AirbyteStateMessage> state,
                                                final ConfiguredAirbyteCatalog catalog,
                                                final JsonNode config) {
    if (state != null && !state.isEmpty()) {
      final AirbyteStateMessage airbyteStateMessage = state.get(0);
      if (isCdc(config) || airbyteStateMessage.getStateType() == AirbyteStateType.GLOBAL) {
        LOGGER.info("Global state manager selected to manage state object with type {}.", airbyteStateMessage.getStateType());
        return new GlobalStateManager(airbyteStateMessage, catalog);
      } else if (airbyteStateMessage.getData() != null && airbyteStateMessage.getStream() == null) {
        LOGGER.info("Legacy state manager selected to manage state object with type {}.", airbyteStateMessage.getStateType());
        return new LegacyStateManager(Jsons.object(airbyteStateMessage.getData(), DbState.class), catalog);
      } else {
        LOGGER.info("Stream state manager selected to manage state object with type {}.", airbyteStateMessage.getStateType());
        return new StreamStateManager(state, catalog);
      }
    } else {
      throw new IllegalArgumentException("Failed to create state manager due to empty state list.");
    }
  }

  /**
   * Test whether the connector is configured to use change data capture (CDC) for replication.
   *
   * @param config The connector configuration.
   * @return {@code true} if the connector utilizes CDC or {@code false} otherwise.
   */
  @VisibleForTesting
  protected static boolean isCdc(final JsonNode config) {
    return config.hasNonNull("replication_method")
        && config.get("replication_method").hasNonNull("replication_slot")
        && config.get("replication_method").hasNonNull("publication");
  }

}
