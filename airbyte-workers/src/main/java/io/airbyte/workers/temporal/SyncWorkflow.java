/*
 * MIT License
 *
 * Copyright (c) 2020 Airbyte
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package io.airbyte.workers.temporal;

import io.airbyte.config.StandardSyncInput;
import io.airbyte.config.StandardSyncOutput;
import io.airbyte.scheduler.models.IntegrationLauncherConfig;
import io.airbyte.scheduler.models.JobRunConfig;
import io.airbyte.workers.DefaultSyncWorker;
import io.airbyte.workers.SyncWorker;
import io.airbyte.workers.WorkerConstants;
import io.airbyte.workers.WorkerException;
import io.airbyte.workers.normalization.NormalizationRunnerFactory;
import io.airbyte.workers.process.AirbyteIntegrationLauncher;
import io.airbyte.workers.process.IntegrationLauncher;
import io.airbyte.workers.process.ProcessBuilderFactory;
import io.airbyte.workers.protocols.airbyte.AirbyteMessageTracker;
import io.airbyte.workers.protocols.airbyte.AirbyteSource;
import io.airbyte.workers.protocols.airbyte.DefaultAirbyteDestination;
import io.airbyte.workers.protocols.airbyte.DefaultAirbyteSource;
import io.airbyte.workers.protocols.airbyte.EmptyAirbyteSource;
import io.airbyte.workers.protocols.airbyte.NamespacingMapper;
import io.temporal.activity.ActivityCancellationType;
import io.temporal.activity.ActivityInterface;
import io.temporal.activity.ActivityMethod;
import io.temporal.activity.ActivityOptions;
import io.temporal.common.RetryOptions;
import io.temporal.workflow.Workflow;
import io.temporal.workflow.WorkflowInterface;
import io.temporal.workflow.WorkflowMethod;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@WorkflowInterface
public interface SyncWorkflow {

  @WorkflowMethod
  StandardSyncOutput run(JobRunConfig jobRunConfig,
                         IntegrationLauncherConfig sourceLauncherConfig,
                         IntegrationLauncherConfig destinationLauncherConfig,
                         StandardSyncInput syncInput)
      throws TemporalJobException;

  class WorkflowImpl implements SyncWorkflow {

    private final ActivityOptions options = ActivityOptions.newBuilder()
        .setScheduleToCloseTimeout(Duration.ofDays(3))
        .setCancellationType(ActivityCancellationType.WAIT_CANCELLATION_COMPLETED)
        .build();

    private final SyncActivity activity = Workflow.newActivityStub(SyncActivity.class, options);

    @Override
    public StandardSyncOutput run(JobRunConfig jobRunConfig,
                                  IntegrationLauncherConfig sourceLauncherConfig,
                                  IntegrationLauncherConfig destinationLauncherConfig,
                                  StandardSyncInput syncInput)
        throws TemporalJobException {
      return activity.run(jobRunConfig, sourceLauncherConfig, destinationLauncherConfig, syncInput);
    }

  }

  @ActivityInterface
  interface SyncActivity {

    @ActivityMethod
    StandardSyncOutput run(JobRunConfig jobRunConfig,
                           IntegrationLauncherConfig sourceLauncherConfig,
                           IntegrationLauncherConfig destinationLauncherConfig,
                           StandardSyncInput syncInput)
        throws TemporalJobException;

  }

  class SyncActivityImpl implements SyncActivity {

    private static final Logger LOGGER = LoggerFactory.getLogger(SyncActivityImpl.class);

    private final ProcessBuilderFactory pbf;
    private final Path workspaceRoot;

    public SyncActivityImpl(ProcessBuilderFactory pbf, Path workspaceRoot) {
      this.pbf = pbf;
      this.workspaceRoot = workspaceRoot;
    }

    public StandardSyncOutput run(JobRunConfig jobRunConfig,
                                  IntegrationLauncherConfig sourceLauncherConfig,
                                  IntegrationLauncherConfig destinationLauncherConfig,
                                  StandardSyncInput syncInput)
        throws TemporalJobException {

      return new TemporalAttemptExecution<>(workspaceRoot, jobRunConfig, (jobRoot) -> {
        final IntegrationLauncher sourceLauncher = new AirbyteIntegrationLauncher(
            sourceLauncherConfig.getJobId(),
            Math.toIntExact(sourceLauncherConfig.getAttemptId()),
            sourceLauncherConfig.getDockerImage(),
            pbf);
        final IntegrationLauncher destinationLauncher = new AirbyteIntegrationLauncher(
            destinationLauncherConfig.getJobId(),
            Math.toIntExact(destinationLauncherConfig.getAttemptId()),
            destinationLauncherConfig.getDockerImage(),
            pbf);

        // reset jobs use an empty source to induce resetting all data in destination.
        final AirbyteSource airbyteSource =
            sourceLauncherConfig.getDockerImage().equals(WorkerConstants.RESET_JOB_SOURCE_DOCKER_IMAGE_STUB) ? new EmptyAirbyteSource()
                : new DefaultAirbyteSource(sourceLauncher);

        final SyncWorker syncWorker = new DefaultSyncWorker(
            jobRunConfig.getJobId(),
            Math.toIntExact(jobRunConfig.getAttemptId()),
            airbyteSource,
            new NamespacingMapper(syncInput.getPrefix()),
            new DefaultAirbyteDestination(destinationLauncher),
            new AirbyteMessageTracker(),
            NormalizationRunnerFactory.create(
                destinationLauncherConfig.getDockerImage(),
                pbf,
                syncInput.getDestinationConfiguration()));

        final CancellationHandler cancellationHandler = new CancellationHandler.TemporalCancellationHandler();

        final Duration interval = Duration.ofSeconds(5);

        CompletableFuture<StandardSyncOutput> completableFuture = new CompletableFuture<>();

        // todo: set mdc in each place

        final Thread workerThread = new Thread(() -> {
          try {
            StandardSyncOutput output = syncWorker.run(syncInput, jobRoot);
            completableFuture.complete(output);
          } catch (Throwable e) {
            LOGGER.info("completing future exceptionally...", e);
            completableFuture.completeExceptionally(e);
          }
        });

        workerThread.start();

        ScheduledExecutorService scheduledExecutor = Executors.newSingleThreadScheduledExecutor();
        scheduledExecutor.scheduleAtFixedRate(() -> {
          try {
            cancellationHandler.heartbeat(() -> {
              LOGGER.info("Running sync worker cancellation...");
              syncWorker.cancel();

              LOGGER.info("Interrupting worker thread...");
              workerThread.interrupt();

              LOGGER.info("Cancelling completable future...");
              completableFuture.cancel(false);
            });
          } catch (WorkerException e) {
            LOGGER.error("heartbeat exception", e);
          }
        }, interval.toSeconds(), interval.toSeconds(), TimeUnit.SECONDS);

        try {
          return completableFuture.get();
        } finally {
          LOGGER.info("Stopping heartbeat scheduling...");
          scheduledExecutor.shutdown();
        }
      }).get();
    }

  }

}
