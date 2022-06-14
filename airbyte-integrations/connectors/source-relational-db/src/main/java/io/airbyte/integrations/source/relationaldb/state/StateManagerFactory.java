/*
 * Copyright (c) 2022 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.source.relationaldb.state;

import io.airbyte.commons.json.Jsons;
import io.airbyte.integrations.source.relationaldb.models.DbState;
import io.airbyte.protocol.models.AirbyteStateMessage;
import io.airbyte.protocol.models.AirbyteStateMessage.AirbyteStateType;
import io.airbyte.protocol.models.ConfiguredAirbyteCatalog;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;
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
   * Creates a {@link StateManager} based on the provided state object and catalog.  This method will handle the
   * conversion of the provided state to match the requested state manager based on the provided {@link AirbyteStateType}.
   *
   * @param state The deserialized state.
   * @param catalog The {@link ConfiguredAirbyteCatalog} for the connector that will utilize the state
   *        manager.
   * @param stateTypeSupplier {@link Supplier} that provides the {@link AirbyteStateType} that will be
   *        used to select the correct state manager.
   * @return A newly created {@link StateManager} implementation based on the provided state.
   */
  public static StateManager createStateManager(final List<AirbyteStateMessage> state,
                                                final ConfiguredAirbyteCatalog catalog,
                                                final Supplier<AirbyteStateType> stateTypeSupplier) {
    if (state != null && !state.isEmpty()) {
      final AirbyteStateMessage airbyteStateMessage = state.get(0);
      switch (stateTypeSupplier.get()) {
        case LEGACY:
          LOGGER.info("Legacy state manager selected to manage state object with type {}.", airbyteStateMessage.getStateType());
          return new LegacyStateManager(Jsons.object(airbyteStateMessage.getData(), DbState.class), catalog);
        case GLOBAL:
          LOGGER.info("Global state manager selected to manage state object with type {}.", airbyteStateMessage.getStateType());
          return new GlobalStateManager(generateGlobalState(airbyteStateMessage), catalog);
        case STREAM:
        default:
          LOGGER.info("Stream state manager selected to manage state object with type {}.", airbyteStateMessage.getStateType());
          return new StreamStateManager(generateStreamState(state), catalog);
      }
    } else {
      throw new IllegalArgumentException("Failed to create state manager due to empty state list.");
    }
  }

  /**
   * Handles the conversion between a different state type and the global state. This method handles
   * the following transitions:
   * <ul>
   * <li>Stream -> Global (not supported, results in {@link IllegalArgumentException}</li>
   * <li>Legacy -> Global (supported)</li>
   * <li>Global -> Glboal (supported/no conversion required)</li>
   * </ul>
   *
   * @param airbyteStateMessage The current state that is to be converted to global state.
   * @return The converted state message.
   * @throws IllegalArgumentException if unable to convert between the given state type and global.
   */
  private static AirbyteStateMessage generateGlobalState(final AirbyteStateMessage airbyteStateMessage) {
    AirbyteStateMessage globalStateMessage = airbyteStateMessage;

    switch (airbyteStateMessage.getStateType()) {
      case STREAM:
        throw new IllegalArgumentException("Unable to convert connector state from per-stream to global.  Please reset the connection to continue.");
      case LEGACY:
        globalStateMessage = StateGeneratorUtils.convertLegacyStateToGlobalState(airbyteStateMessage);
        LOGGER.info("Legacy state converted to global state.", airbyteStateMessage.getStateType());
        break;
      case GLOBAL:
      default:
        break;
    }

    return globalStateMessage;
  }

  /**
   * Handles the conversion between a different state type and the stream state. This method handles
   * the following transitions:
   * <ul>
   * <li>Global -> Stream (supported/shared state discarded)</li>
   * <li>Legacy -> Stream (supported)</li>
   * <li>Stream -> Stream (supported/no conversion required)</li>
   * </ul>
   *
   * @param states The list of current states.
   * @return The converted state messages.
   */
  private static List<AirbyteStateMessage> generateStreamState(final List<AirbyteStateMessage> states) {
    final AirbyteStateMessage airbyteStateMessage = states.get(0);
    final List<AirbyteStateMessage> streamStates = new ArrayList<>();
    switch (airbyteStateMessage.getStateType()) {
      case GLOBAL:
        streamStates.addAll(StateGeneratorUtils.convertGlobalStateToStreamState(airbyteStateMessage));
        LOGGER.info("Global state converted to stream state.", airbyteStateMessage.getStateType());
        break;
      case LEGACY:
        streamStates.addAll(StateGeneratorUtils.convertLegacyStateToStreamState(airbyteStateMessage));
        break;
      case STREAM:
      default:
        streamStates.addAll(states);
        break;
    }

    return streamStates;
  }

}
