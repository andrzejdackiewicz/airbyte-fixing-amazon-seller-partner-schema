/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination_async;

import io.airbyte.integrations.base.AirbyteMessageConsumer;
import io.airbyte.protocol.models.v0.AirbyteMessage;
import io.airbyte.protocol.models.v0.AirbyteMessage.Type;
import io.airbyte.protocol.models.v0.AirbyteStateMessage.AirbyteStateType;
import io.airbyte.protocol.models.v0.StreamDescriptor;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AsyncStreamConsumer implements AirbyteMessageConsumer {

  private static final String NON_STREAM_STATE_IDENTIFIER = "GLOBAL";
  private final BufferManagerEnqueue bufferManagerEnqueue;
  private final UploadWorkers uploadWorkers;

  public AsyncStreamConsumer(final BufferManager bufferManager, StreamDestinationFlusher flusher) {
    bufferManagerEnqueue = bufferManager.getBufferManagerEnqueue();
    uploadWorkers = new UploadWorkers(bufferManager.getBufferManagerDequeue(), flusher);
  }

  @Override
  public void start() throws Exception {
    uploadWorkers.start();
  }

  @Override
  public void accept(final AirbyteMessage message) throws Exception {
    /*
     * intentionally putting extractStream outside the buffer manager so that if in the future we want
     * to try to use a threadpool to partial deserialize to get record type and stream name, we can do
     * it without touching buffer manager.
     */
    extractStream(message)
        .ifPresent(streamDescriptor -> bufferManagerEnqueue.addRecord(streamDescriptor, message));
  }

  @Override
  public void close() throws Exception {
    // assume the closing upload workers will flush all accepted records.
    uploadWorkers.close();
  }

  // todo (cgardens) - handle global state.
  /**
   * Extract the stream from the message, ff the message is a record or state. Otherwise, we don't
   * care.
   *
   * @param message message to extract stream from
   * @return stream descriptor if the message is a record or state, otherwise empty. In the case of
   *         global state messages the stream descriptor is hardcoded
   */
  private static Optional<StreamDescriptor> extractStream(final AirbyteMessage message) {
    if (message.getType() == Type.RECORD) {
      return Optional.of(new StreamDescriptor().withNamespace(message.getRecord().getNamespace()).withName(message.getRecord().getStream()));
    } else if (message.getType() == Type.STATE) {
      if (message.getState().getType() == AirbyteStateType.STREAM) {
        return Optional.of(message.getState().getStream().getStreamDescriptor());
      } else {
        return Optional.of(new StreamDescriptor().withNamespace(NON_STREAM_STATE_IDENTIFIER).withNamespace(NON_STREAM_STATE_IDENTIFIER));
      }
    } else {
      return Optional.empty();
    }
  }

  static class BufferManager {

    Map<StreamDescriptor, LinkedBlockingQueue<AirbyteMessage>> buffers;

    BufferManagerEnqueue bufferManagerEnqueue;
    BufferManagerDequeue bufferManagerDequeue;

    public BufferManager() {
      buffers = new HashMap<>();
      bufferManagerEnqueue = new BufferManagerEnqueue(buffers);
      bufferManagerDequeue = new BufferManagerDequeue(buffers);
    }

    public BufferManagerEnqueue getBufferManagerEnqueue() {
      return bufferManagerEnqueue;
    }

    public BufferManagerDequeue getBufferManagerDequeue() {
      return bufferManagerDequeue;
    }

  }

  static class BufferManagerEnqueue {

    Map<StreamDescriptor, LinkedBlockingQueue<AirbyteMessage>> buffers;

    public BufferManagerEnqueue(final Map<StreamDescriptor, LinkedBlockingQueue<AirbyteMessage>> buffers) {
      this.buffers = buffers;
    }

    public void addRecord(final StreamDescriptor streamDescriptor, final AirbyteMessage message) {
      // todo (cgardens) - replace this with fancy logic to make sure we don't oom.
      if (!buffers.containsKey(streamDescriptor)) {
        buffers.put(streamDescriptor, new LinkedBlockingQueue<>());
      }
      buffers.get(streamDescriptor).add(message);
    }

  }

  static class BufferManagerDequeue {

    Map<StreamDescriptor, LinkedBlockingQueue<AirbyteMessage>> buffers;

    public BufferManagerDequeue(final Map<StreamDescriptor, LinkedBlockingQueue<AirbyteMessage>> buffers) {
      this.buffers = buffers;
    }

    public Map<StreamDescriptor, LinkedBlockingQueue<AirbyteMessage>> getBuffers() {
      return new HashMap<>(buffers);
    }

    public LinkedBlockingQueue<AirbyteMessage> getBuffer(final StreamDescriptor streamDescriptor) {
      return buffers.get(streamDescriptor);
    }

    public int getTotalGlobalQueueSizeInMb() {
      return 0;
    }

    public int getQueueSizeInMb(final StreamDescriptor streamDescriptor) {
      return 0;
    }

    public Instant getTimeOfLastRecord(final StreamDescriptor streamDescriptor) {
      return null;
    }

  }

  /**
   * In charge of looking for records in queues and efficiently getting those records uploaded.
   */
  static class UploadWorkers implements AutoCloseable {

    private static final double TOTAL_QUEUES_MAX_SIZE_LIMIT_BYTES = Runtime.getRuntime().maxMemory() * 0.8;
    private static final double MAX_CONCURRENT_QUEUES = 10.0;
    private static final double MAX_QUEUE_SIZE_BYTES = TOTAL_QUEUES_MAX_SIZE_LIMIT_BYTES / MAX_CONCURRENT_QUEUES;
    private static final long MAX_TIME_BETWEEN_REC_MINS = 15L;

    private static final long SUPERVISOR_INITIAL_DELAY_SECS = 0L;
    private static final long SUPERVISOR_PERIOD_SECS = 1L;
    private final ScheduledExecutorService supervisorThread = Executors.newScheduledThreadPool(1);
    // note: this queue size is unbounded.
    private final Executor workerPool = Executors.newFixedThreadPool(5);
    private final BufferManagerDequeue bufferManagerDequeue;
    private final StreamDestinationFlusher flusher;

    public UploadWorkers(final BufferManagerDequeue bufferManagerDequeue, final StreamDestinationFlusher flusher1) {
      this.bufferManagerDequeue = bufferManagerDequeue;
      this.flusher = flusher1;
    }

    public void start() {
      supervisorThread.scheduleAtFixedRate(this::retrieveWork, SUPERVISOR_INITIAL_DELAY_SECS, SUPERVISOR_PERIOD_SECS,
          TimeUnit.SECONDS);
    }

    private void retrieveWork() {
      // if the total size is > n, flush all buffers
      if (bufferManagerDequeue.getTotalGlobalQueueSizeInMb() > TOTAL_QUEUES_MAX_SIZE_LIMIT_BYTES) {
        flushAll();
      }

      // otherwise, if each individual stream has crossed a specific threshold, flush
      for (Map.Entry<StreamDescriptor, LinkedBlockingQueue<AirbyteMessage>> entry : bufferManagerDequeue.getBuffers().entrySet()) {
        final var stream = entry.getKey();
        final var exceedSize = bufferManagerDequeue.getQueueSizeInMb(stream) >= MAX_QUEUE_SIZE_BYTES;
        final var tooLongSinceLastRecord = bufferManagerDequeue.getTimeOfLastRecord(stream)
            .isBefore(Instant.now().minus(MAX_TIME_BETWEEN_REC_MINS, ChronoUnit.MINUTES));
        if (exceedSize || tooLongSinceLastRecord) {
          flush(stream);
        }
      }
    }

    private void flushAll() {
      for (final StreamDescriptor desc : bufferManagerDequeue.getBuffers().keySet()) {
        flush(desc);
      }
    }

    private void flush(final StreamDescriptor desc) {
      workerPool.execute(() -> {
        final var queue = bufferManagerDequeue.getBuffer(desc);
        flusher.flush(desc, queue.stream());
      });
    }

    @Override
    public void close() throws Exception {

    }

  }

  interface StreamDestinationFlusher {

    void flush(StreamDescriptor decs, Stream<AirbyteMessage> stream);

  }

}
