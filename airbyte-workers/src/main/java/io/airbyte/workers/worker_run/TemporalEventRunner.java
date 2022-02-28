package io.airbyte.workers.worker_run;

import io.airbyte.api.model.ConnectionUpdate;
import io.airbyte.config.persistence.ConfigNotFoundException;
import io.airbyte.validation.json.JsonValidationException;
import io.airbyte.workers.temporal.TemporalClient;
import io.airbyte.workers.temporal.TemporalClient.ManualSyncSubmissionResult;
import java.io.IOException;
import java.util.Set;
import java.util.UUID;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class TemporalEventRunner implements EventRunner {
  private final TemporalClient temporalClient;

  public void createNewSchedulerWorkflow(final UUID connectionId) {
    temporalClient.submitConnectionUpdaterAsync(connectionId);
  }

  public ManualSyncSubmissionResult startNewManualSync(final UUID connectionId) {
    return temporalClient.startNewManualSync(connectionId);
  }

  public ManualSyncSubmissionResult startNewCancelation(final UUID connectionId) {
    return temporalClient.startNewCancelation(connectionId);
  }

  public ManualSyncSubmissionResult resetConnection(final UUID connectionId) {
    return temporalClient.resetConnection(connectionId);
  }

  public ManualSyncSubmissionResult synchronousResetConnection(final UUID connectionId) {
    return temporalClient.synchronousResetConnection(connectionId);
  }

  public void deleteConnection(final UUID connectionId) {
    temporalClient.deleteConnection(connectionId);
  }

  public void migrateSyncIfNeeded(final Set<UUID> connectionIds) {
    temporalClient.migrateSyncIfNeeded(connectionIds);
  }

  public void update(final ConnectionUpdate connectionUpdate) throws JsonValidationException, ConfigNotFoundException, IOException {
    temporalClient.update(connectionUpdate);
  }
}
