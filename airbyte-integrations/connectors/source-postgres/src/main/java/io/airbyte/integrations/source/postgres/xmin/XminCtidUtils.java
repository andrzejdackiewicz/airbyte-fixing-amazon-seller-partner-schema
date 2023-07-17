/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.source.postgres.xmin;

import static io.airbyte.integrations.source.postgres.ctid.CtidStateManager.STATE_TYPE_KEY;
import static io.airbyte.integrations.source.postgres.ctid.CtidUtils.identifyNewlyAddedStreams;
import static io.airbyte.integrations.source.postgres.xmin.PostgresXminHandler.shouldPerformFullSync;

import com.fasterxml.jackson.databind.JsonNode;
import io.airbyte.commons.json.Jsons;
import io.airbyte.integrations.source.postgres.ctid.CtidUtils.CtidStreams;
import io.airbyte.integrations.source.postgres.ctid.CtidUtils.StreamsCategorised;
import io.airbyte.integrations.source.postgres.internal.models.XminStatus;
import io.airbyte.integrations.source.relationaldb.state.StateManager;
import io.airbyte.protocol.models.v0.AirbyteStateMessage;
import io.airbyte.protocol.models.v0.AirbyteStreamNameNamespacePair;
import io.airbyte.protocol.models.v0.ConfiguredAirbyteCatalog;
import io.airbyte.protocol.models.v0.ConfiguredAirbyteStream;
import io.airbyte.protocol.models.v0.StreamDescriptor;
import io.airbyte.protocol.models.v0.SyncMode;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The class mainly categorises the streams based on the state type into two categories:
 * 1. Streams that need to be synced via ctid iterator: These are streams that are either newly added or did
 * not complete their initial sync. 2. Streams that need to be synced via xmin iterator: These are
 * streams that have completed their initial sync and are not syncing data incrementally.
 */
public class XminCtidUtils {

  private static final Logger LOGGER = LoggerFactory.getLogger(XminCtidUtils.class);

  public static StreamsCategorised<XminStreams> categoriseStreams(final StateManager stateManager,
                                                                  final ConfiguredAirbyteCatalog fullCatalog,
                                                                  final XminStatus currentXminStatus) {
    final List<AirbyteStateMessage> rawStateMessages = stateManager.getRawStateMessages();
    final List<AirbyteStateMessage> statesFromCtidSync = new ArrayList<>();
    final List<AirbyteStateMessage> statesFromXminSync = new ArrayList<>();

    final Set<AirbyteStreamNameNamespacePair> alreadySeenStreams = new HashSet<>();
    final Set<AirbyteStreamNameNamespacePair> streamsStillInCtidSync = new HashSet<>();

    if (rawStateMessages != null) {
      rawStateMessages.forEach(stateMessage -> {
        final JsonNode streamState = stateMessage.getStream().getStreamState();
        final StreamDescriptor streamDescriptor = stateMessage.getStream().getStreamDescriptor();
        if (streamState == null || streamDescriptor == null) {
          return;
        }

        if (streamState.has(STATE_TYPE_KEY)) {
          if (streamState.get(STATE_TYPE_KEY).asText().equalsIgnoreCase("ctid")) {
            statesFromCtidSync.add(stateMessage);
            streamsStillInCtidSync.add(new AirbyteStreamNameNamespacePair(streamDescriptor.getName(), streamDescriptor.getNamespace()));
          } else if (streamState.get(STATE_TYPE_KEY).asText().equalsIgnoreCase("xmin")) {
            if (shouldPerformFullSync(currentXminStatus, streamState)) {
              final AirbyteStreamNameNamespacePair pair = new AirbyteStreamNameNamespacePair(streamDescriptor.getName(),
                  streamDescriptor.getNamespace());
              LOGGER.info("Detected multiple wraparounds. Will perform a full sync for {}", pair);
              streamsStillInCtidSync.add(pair);
            } else {
              statesFromXminSync.add(stateMessage);
            }
          } else {
            throw new RuntimeException("Unknown state type: " + streamState.get(STATE_TYPE_KEY).asText());
          }
        } else {
          throw new RuntimeException("State type not present");
        }
        alreadySeenStreams.add(new AirbyteStreamNameNamespacePair(streamDescriptor.getName(), streamDescriptor.getNamespace()));
      });
    }

    final List<ConfiguredAirbyteStream> newlyAddedStreams = identifyNewlyAddedStreams(fullCatalog, alreadySeenStreams);
    final List<ConfiguredAirbyteStream> streamsForCtidSync = new ArrayList<>();
    fullCatalog.getStreams().stream()
        .filter(stream -> streamsStillInCtidSync.contains(AirbyteStreamNameNamespacePair.fromAirbyteStream(stream.getStream())))
        .map(Jsons::clone)
        .forEach(streamsForCtidSync::add);

    //When FULL_REFRESH streams are enabled in xmin, ctid will only go over INCREMENTAL ones for now
    newlyAddedStreams.stream().filter(c -> c.getSyncMode() == SyncMode.INCREMENTAL).forEach(streamsForCtidSync::add);

    final List<ConfiguredAirbyteStream> streamsForXminOrFullRefreshSync = fullCatalog.getStreams().stream()
        .filter(stream -> !streamsForCtidSync.contains(stream))
        .map(Jsons::clone)
        .toList();

    return new StreamsCategorised<>(new CtidStreams(streamsForCtidSync, statesFromCtidSync), new XminStreams(streamsForXminOrFullRefreshSync, statesFromXminSync));
  }

  public record XminStreams(List<ConfiguredAirbyteStream> streamsForXminSync,
                            List<AirbyteStateMessage> statesFromXminSync) {

  }

}
