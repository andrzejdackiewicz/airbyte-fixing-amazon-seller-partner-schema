/*
 * Copyright (c) 2021 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.sync;

import io.airbyte.config.StandardSyncInput;
import io.airbyte.config.StandardSyncOutput;
import io.airbyte.scheduler.models.IntegrationLauncherConfig;
import io.airbyte.scheduler.models.JobRunConfig;
import io.temporal.workflow.Workflow;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class EmptySyncWorkflow implements SyncWorkflow {

  @Override
  public StandardSyncOutput run(final JobRunConfig jobRunConfig,
                                final IntegrationLauncherConfig sourceLauncherConfig,
                                final IntegrationLauncherConfig destinationLauncherConfig,
                                final StandardSyncInput syncInput,
                                final UUID connectionId) {
    int count = 0;
    while (count < 1) {
      log.error("" + count++);

      Workflow.sleep(1000);
    }

    return new StandardSyncOutput();
  }

}
