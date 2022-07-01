package io.airbyte.workers.internal;

import io.airbyte.config.State;
import io.airbyte.protocol.models.AirbyteStateMessage;

public interface StateAggregator {

  void ingest(AirbyteStateMessage stateMessage);

  State getAggregated();
}
