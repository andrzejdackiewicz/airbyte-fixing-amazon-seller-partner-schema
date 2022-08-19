/*
 * Copyright (c) 2022 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.airbyte.protocol.models.AirbyteMessage;
import io.airbyte.protocol.models.AirbyteStateMessage;
import io.airbyte.workers.internal.StateMetricsTracker.StateMetricsTrackerException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class StateMetricsTrackerTest {

  private StateMetricsTracker stateMetricsTracker;
  private static final String STREAM_1 = "stream1";

  @BeforeEach
  void setup() {
    this.stateMetricsTracker = new StateMetricsTracker(10L * 1024L * 1024L);
  }

  @Test
  void testCalculateMean() throws Exception {
    // Mean for 3 state messages is 5, 4th state message is 9, new mean should be 6
    assertEquals(6L, stateMetricsTracker.calculateMean(5L, 4L, 9L));

    // Mean for 5 state messages is 10, 4th state message is 12, new mean is 10.33 rounded down to 10
    assertEquals(10L, stateMetricsTracker.calculateMean(10L, 6L, 12L));
  }

  @Test
  void testStreamMaxandMeanSecondsBetweenStateMessageEmittedandCommitted() throws StateMetricsTrackerException {
    final AirbyteStateMessage s1 = AirbyteMessageUtils.createStreamStateMessage(STREAM_1, 1);
    final AirbyteStateMessage s2 = AirbyteMessageUtils.createStreamStateMessage(STREAM_1, 2);
    final AirbyteStateMessage s3 = AirbyteMessageUtils.createStreamStateMessage(STREAM_1, 3);

    // 3 per-stream state messages emitted for the same stream
    final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    stateMetricsTracker.addState(s1, 0, LocalDateTime.parse("2022-01-01 12:00:00", formatter));
    stateMetricsTracker.addState(s2, 1, LocalDateTime.parse("2022-01-01 12:00:01", formatter));
    stateMetricsTracker.addState(s3, 2, LocalDateTime.parse("2022-01-01 12:00:02", formatter));

    // Committed up to 2nd state message - time to commit is 5 seconds (second 00 to second 05)
    stateMetricsTracker.incrementTotalDestinationEmittedStateMessages();
    stateMetricsTracker.updateStates(s2, 1, LocalDateTime.parse("2022-01-01 12:00:05", formatter));

    // Committed final state message - time to commit is 7 seconds (second 02 to second 09)
    stateMetricsTracker.incrementTotalDestinationEmittedStateMessages();
    stateMetricsTracker.updateStates(s3, 2, LocalDateTime.parse("2022-01-01 12:00:09", formatter));

    assertEquals(7L, stateMetricsTracker.getMaxSecondsBetweenStateMessageEmittedAndCommitted());
    assertEquals(6L, stateMetricsTracker.getMeanSecondsBetweenStateMessageEmittedAndCommitted());
  }

  @Test
  void testGlobalMaxandMeanSecondsBetweenStateMessageEmittedandCommitted() throws StateMetricsTrackerException {
    final AirbyteMessage s1 = AirbyteMessageUtils.createGlobalStateMessage(1, STREAM_1);
    final AirbyteMessage s2 = AirbyteMessageUtils.createGlobalStateMessage(2, STREAM_1);
    final AirbyteMessage s3 = AirbyteMessageUtils.createGlobalStateMessage(3, STREAM_1);

    // 3 global state messages emitted
    final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    stateMetricsTracker.addState(s1.getState(), 0, LocalDateTime.parse("2022-01-01 12:00:00", formatter));
    stateMetricsTracker.addState(s2.getState(), 1, LocalDateTime.parse("2022-01-01 12:00:01", formatter));
    stateMetricsTracker.addState(s3.getState(), 2, LocalDateTime.parse("2022-01-01 12:00:02", formatter));

    // Committed up to 2nd state message - time to commit is 5 seconds (second 00 to second 05)
    stateMetricsTracker.incrementTotalDestinationEmittedStateMessages();
    stateMetricsTracker.updateStates(s2.getState(), 1, LocalDateTime.parse("2022-01-01 12:00:05", formatter));

    // Committed final state message - time to commit is 7 seconds (second 02 to second 09)
    stateMetricsTracker.incrementTotalDestinationEmittedStateMessages();
    stateMetricsTracker.updateStates(s3.getState(), 2, LocalDateTime.parse("2022-01-01 12:00:09", formatter));

    assertEquals(7L, stateMetricsTracker.getMaxSecondsBetweenStateMessageEmittedAndCommitted());
    assertEquals(6L, stateMetricsTracker.getMeanSecondsBetweenStateMessageEmittedAndCommitted());
  }

}
