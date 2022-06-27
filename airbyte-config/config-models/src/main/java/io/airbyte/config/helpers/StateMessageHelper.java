/*
 * Copyright (c) 2022 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.helpers;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.Iterables;
import io.airbyte.commons.json.Jsons;
import io.airbyte.config.StateType;
import io.airbyte.config.StateWrapper;
import io.airbyte.protocol.models.AirbyteStateMessage;
import io.airbyte.protocol.models.AirbyteStateMessage.AirbyteStateType;
import java.util.List;
import java.util.Optional;

public class StateMessageHelper {

  public static class AirbyteStateMessageListTypeReference extends TypeReference<List<AirbyteStateMessage>> {}

  /**
   * This a takes a json blob state and tries return either a legacy state in the format of a json
   * object or a state message with the new format which is a list of airbyte state message.
   *
   * @param state - a blob representing the state
   * @return An optional state wrapper, if there is no state an empty optional will be returned
   */
  public static Optional<StateWrapper> getTypedState(final JsonNode state, final boolean useStreamCapableState) {
    if (state == null) {
      return Optional.empty();
    } else {
      final List<AirbyteStateMessage> stateMessages;
      try {
        stateMessages = Jsons.object(state, new AirbyteStateMessageListTypeReference());
      } catch (final IllegalArgumentException e) {
        return Optional.of(getLegacyStateWrapper(state));
      }
      if (stateMessages.size() == 1) {
        if (stateMessages.get(0).getType() == AirbyteStateType.GLOBAL) {
          return Optional.of(provideGlobalState(stateMessages, useStreamCapableState));
        } else if (stateMessages.get(0).getType() == null || stateMessages.get(0).getType() == AirbyteStateType.LEGACY) {
          return Optional.of(getLegacyStateWrapper(state));
        } else {
          throw new IllegalStateException("Unexpected state blob, the");
        }
      } else if (stateMessages.size() >= 1) {
        if (stateMessages.stream().allMatch(stateMessage -> stateMessage.getType() == AirbyteStateType.STREAM)) {
          return Optional.of(provideStreamState(stateMessages, useStreamCapableState));
        }
        if (stateMessages.stream().allMatch(stateMessage -> stateMessage.getType() == null)) {
          return Optional.of(getLegacyStateWrapper(state));
        } else {
          throw new IllegalStateException("Unexpected state blob, the");
        }
      } else {
        return Optional.of(getLegacyStateWrapper(state));
      }
    }
    // return Optional.empty();
  }

  private static StateWrapper provideGlobalState(final List<AirbyteStateMessage> stateMessages, final boolean useStreamCapableState) {
    if (useStreamCapableState) {
      return new StateWrapper()
          .withStateType(StateType.GLOBAL)
          .withGlobal(stateMessages.get(0));
    } else {
      return new StateWrapper()
          .withStateType(StateType.LEGACY)
          .withLegacyState(stateMessages.get(0).getData());
    }
  }

  /**
   * This is returning a wrapped state, it assumes that the state messages are ordered.
   *
   * @param stateMessages - an ordered list of state message
   * @param useStreamCapableState - a flag that hallow to return the new format
   * @return a wrapped state
   */
  private static StateWrapper provideStreamState(final List<AirbyteStateMessage> stateMessages, final boolean useStreamCapableState) {
    if (useStreamCapableState) {
      return new StateWrapper()
          .withStateType(StateType.STREAM)
          .withStateMessages(stateMessages);
    } else {
      return new StateWrapper()
          .withStateType(StateType.LEGACY)
          .withLegacyState(Iterables.getLast(stateMessages).getData());
    }
  }

  private static StateWrapper getLegacyStateWrapper(final JsonNode state) {
    return new StateWrapper()
        .withStateType(StateType.LEGACY)
        .withLegacyState(state);
  }

}
