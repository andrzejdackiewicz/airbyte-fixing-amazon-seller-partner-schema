/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.dest_state_lifecycle_manager;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.airbyte.protocol.models.v0.AirbyteMessage;
import io.airbyte.protocol.models.v0.AirbyteStateMessage.AirbyteStateType;
import io.airbyte.protocol.models.v0.AirbyteStreamNameNamespacePair;
import io.airbyte.protocol.models.v0.StreamDescriptor;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Queue;
import java.util.stream.Collectors;

/**
 * This {@link DestStateLifecycleManager} handles any state where the state messages are scoped by
 * stream. In these cases, at each state of the process, it tracks the LAST state message for EACH
 * stream (no duplicates!).
 *
 * <p>
 * Guaranteed to output state messages in order relative to other messages of the SAME state. Does
 * NOT guarantee that state messages of different streams will be output in the order in which they
 * were received. State messages across streams will be emitted in alphabetical order (primary sort
 * on namespace, secondary on name).
 * </p>
 */
public class DestStreamStateLifecycleManager implements DestStateLifecycleManager {

  private final Map<StreamDescriptor, AirbyteMessage> streamToLastPendingState;
  private final Map<StreamDescriptor, AirbyteMessage> streamToLastFlushedState;
  private final Map<StreamDescriptor, AirbyteMessage> streamToLastCommittedState;

  public DestStreamStateLifecycleManager() {
    streamToLastPendingState = new HashMap<>();
    streamToLastFlushedState = new HashMap<>();
    streamToLastCommittedState = new HashMap<>();
  }

  @Override
  public void addState(final AirbyteMessage message) {
    Preconditions.checkArgument(message.getState().getType() == AirbyteStateType.STREAM);
    streamToLastPendingState.put(message.getState().getStream().getStreamDescriptor(), message);
  }

  @VisibleForTesting
  Queue<AirbyteMessage> listPending() {
    return listStatesInOrder(streamToLastPendingState);
  }

  /*
   * Similar to #markFlushedAsCommmitted, this method should no longer be used to align with the
   * changes to destination checkpointing where flush/commit operations will be bundled
   */
  @Deprecated
  @Override
  public void markPendingAsFlushed() {
    moveToNextPhase(streamToLastPendingState, streamToLastFlushedState);
  }

  @Override
  public Queue<AirbyteMessage> listFlushed() {
    return listStatesInOrder(streamToLastFlushedState);
  }

  /*
   * During the process of migration to destination checkpointing, this method should no longer be in
   * use in favor of #markPendingAsCommitted where states will be flushed/committed as a singular
   * transaction
   */
  @Deprecated
  @Override
  public void markFlushedAsCommitted() {
    moveToNextPhase(streamToLastFlushedState, streamToLastCommittedState);
  }

  @Override
  public void clearCommitted() {
    streamToLastCommittedState.clear();
  }

  @Override
  public void markPendingAsCommitted() {
    moveToNextPhase(streamToLastPendingState, streamToLastCommittedState);
  }

  @Override
  public void markPendingAsCommitted(final AirbyteStreamNameNamespacePair stream) {
    // Handle the case where the connection is configured to use a custom namespace. In this case, the records' namespace
    // will be set to that override, but the streams' namespace will still be the source's original namespace.
    // We need to match purely on the stream name.
    // TODO handle stream name prefixes (i.e. use endsWith instead of equals)
    // This is a hack to work around https://github.com/airbytehq/airbyte-platform-internal/pull/8365.
    final List<Entry<StreamDescriptor, AirbyteMessage>> matchingStates = streamToLastPendingState.entrySet().stream()
        .filter(entry -> entry.getKey().getName().equals(stream.getName()))
        .toList();
    final StreamDescriptor streamToCommit;
    if (matchingStates.size() > 1) {
      // Multiple streams have this name. Check if any of them have the right namespace.
      final Optional<Entry<StreamDescriptor, AirbyteMessage>> matchingState = matchingStates.stream()
          .filter(entry -> entry.getKey().getNamespace().equals(stream.getNamespace()))
          .findFirst();
      streamToCommit = matchingState.map(Entry::getKey).orElse(null);
    } else if (matchingStates.isEmpty()) {
      // None of the states match this stream. We can't commit anything.
      streamToCommit = null;
    } else {
      // Exactly one stream has this name. Assume that we can commit it.
      // This is technically wrong: it's possible for a source to emit these messages:
      // 1. record(name=foo, namespace=bar)
      // 2. state(name=foo, namespace=baz)
      // But in practice, sources will emit records and states belonging to the same stream. And having multiple streams
      // with the same but different namespace is relatively uncommon...
      streamToCommit = matchingStates.get(0).getKey();
    }

    if (streamToCommit != null) {
      final AirbyteMessage lastPendingState = streamToLastPendingState.remove(streamToCommit);
      if (lastPendingState != null) {
        streamToLastCommittedState.put(streamToCommit, lastPendingState);
      }
    }
  }

  @Override
  public Queue<AirbyteMessage> listCommitted() {
    return listStatesInOrder(streamToLastCommittedState);
  }

  @Override
  public boolean supportsPerStreamFlush() {
    return true;
  }

  /**
   * Lists out the states in the stream to state maps. Guarantees a deterministic sort order, which is
   * handy because we are going from a map (unsorted) to a queue. The sort order primary sort on
   * namespace (with null at the top) followed by secondary sort on name. This maps onto the pretty
   * common order that we list streams elsewhere.
   *
   * @param streamToState - map of stream descriptor to its last state
   * @return queue with the states ordered per the sort mentioned above
   */
  private static Queue<AirbyteMessage> listStatesInOrder(final Map<StreamDescriptor, AirbyteMessage> streamToState) {
    return streamToState
        .entrySet()
        .stream()
        // typically, we support by namespace and then stream name, so we retain that pattern here.
        .sorted(Comparator
            .<Entry<StreamDescriptor, AirbyteMessage>, String>comparing(
                entry -> entry.getKey().getNamespace(),
                Comparator.nullsFirst(Comparator.naturalOrder())) // namespace is allowed to be null
            .thenComparing(entry -> entry.getKey().getName()))
        .map(Entry::getValue)
        .collect(Collectors.toCollection(LinkedList::new));
  }

  /**
   * Moves all state messages from previous phase into next phase.
   *
   * @param prevPhase - map of stream to state messages for previous phase that will be moved to next
   *        phase. when this method returns this map will be empty.
   * @param nextPhase - map into which state messages from prevPhase will be added.
   */
  private static void moveToNextPhase(final Map<StreamDescriptor, AirbyteMessage> prevPhase, final Map<StreamDescriptor, AirbyteMessage> nextPhase) {
    if (!prevPhase.isEmpty()) {
      nextPhase.putAll(prevPhase);
      prevPhase.clear();
    }
  }

}
