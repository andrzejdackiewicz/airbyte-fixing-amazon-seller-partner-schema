/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination_async.buffers;

import io.airbyte.integrations.destination_async.GlobalMemoryManager;
import io.airbyte.integrations.destination_async.buffers.StreamAwareQueue.MessageWithMeta;
import io.airbyte.integrations.destination_async.state.AsyncStateManager;
import io.airbyte.protocol.models.v0.AirbyteMessage;
import java.util.List;
import java.util.Map;

/**
 * POJO abstraction representing one discrete buffer read. This allows ergonomics dequeues by
 * {@link io.airbyte.integrations.destination_async.FlushWorkers}.
 * <p>
 * The contained stream **IS EXPECTED to be a BOUNDED** stream. Returning a boundless stream has
 * undefined behaviour.
 * <p>
 * Once done, consumers **MUST** invoke {@link #close()}. As the {@link #batch} has already been
 * retrieved from in-memory buffers, we need to update {@link GlobalMemoryManager} to reflect the
 * freed up memory and avoid memory leaks.
 */
public class MemoryAwareMessageBatch implements AutoCloseable {

  private List<MessageWithMeta> batch;
  private final long sizeInBytes;
  private final GlobalMemoryManager memoryManager;
  private final AsyncStateManager stateManager;

  public MemoryAwareMessageBatch(final List<MessageWithMeta> batch,
                                 final long sizeInBytes,
                                 final GlobalMemoryManager memoryManager,
                                 final AsyncStateManager stateManager) {
    this.batch = batch;
    this.sizeInBytes = sizeInBytes;
    this.memoryManager = memoryManager;
    this.stateManager = stateManager;
  }

  public List<MessageWithMeta> getData() {
    return batch;
  }

  @Override
  public void close() throws Exception {
    batch = null;
    memoryManager.free(sizeInBytes);
  }

  /**
   * For the batch, marks all the states that have now been flushed. Also returns states that can be
   * flushed. This method is descriptrive, it assumes that whatever consumes the state messages emits
   * them, internally it purges the states it returns. message that it can.
   * <p>
   *
   * @return list of states that can be flushed
   */
  public List<AirbyteMessage> flushStates(final Map<Long, Long> stateIdToCount) {
    stateIdToCount.forEach(stateManager::decrement);
    return stateManager.flushStates();
  }

}
