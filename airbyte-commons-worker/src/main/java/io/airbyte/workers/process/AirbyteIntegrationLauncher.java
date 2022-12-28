/*
 * Copyright (c) 2022 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.process;

import static io.airbyte.metrics.lib.ApmTraceConstants.Tags.DOCKER_IMAGE_KEY;
import static io.airbyte.metrics.lib.ApmTraceConstants.Tags.JOB_ID_KEY;
import static io.airbyte.metrics.lib.ApmTraceConstants.Tags.JOB_ROOT_KEY;
import static io.airbyte.metrics.lib.ApmTraceConstants.WORKER_OPERATION_NAME;
import static io.airbyte.workers.process.Metadata.CHECK_JOB;
import static io.airbyte.workers.process.Metadata.DISCOVER_JOB;
import static io.airbyte.workers.process.Metadata.JOB_TYPE;
import static io.airbyte.workers.process.Metadata.READ_STEP;
import static io.airbyte.workers.process.Metadata.SPEC_JOB;
import static io.airbyte.workers.process.Metadata.SYNC_JOB;
import static io.airbyte.workers.process.Metadata.SYNC_STEP;
import static io.airbyte.workers.process.Metadata.WRITE_STEP;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import datadog.trace.api.Trace;
import io.airbyte.commons.features.EnvVariableFeatureFlags;
import io.airbyte.commons.features.FeatureFlags;
import io.airbyte.config.ResourceRequirements;
import io.airbyte.config.WorkerEnvConstants;
import io.airbyte.metrics.lib.ApmTraceUtils;
import io.airbyte.workers.exception.WorkerException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AirbyteIntegrationLauncher implements IntegrationLauncher {

  private static final String CONFIG = "--config";

  private final String jobId;
  private final int attempt;
  private final String imageName;
  private final ProcessFactory processFactory;
  private final ResourceRequirements resourceRequirement;
  private final FeatureFlags featureFlags;

  /**
   * If true, launcher will use a separated isolated pool to run the job.
   *
   * At this moment, we put custom connector jobs into an isolated pool.
   */
  private final boolean useIsolatedPool;

  public AirbyteIntegrationLauncher(final String jobId,
                                    final int attempt,
                                    final String imageName,
                                    final ProcessFactory processFactory,
                                    final ResourceRequirements resourceRequirement,
                                    final boolean useIsolatedPool) {
    this.jobId = jobId;
    this.attempt = attempt;
    this.imageName = imageName;
    this.processFactory = processFactory;
    this.resourceRequirement = resourceRequirement;
    this.featureFlags = new EnvVariableFeatureFlags();
    this.useIsolatedPool = useIsolatedPool;
  }

  @Trace(operationName = WORKER_OPERATION_NAME)
  @Override
  public Process spec(final Path jobRoot) throws WorkerException {
    ApmTraceUtils.addTagsToTrace(Map.of(JOB_ID_KEY, jobId, JOB_ROOT_KEY, jobRoot, DOCKER_IMAGE_KEY, imageName));
    return processFactory.create(
        SPEC_JOB,
        jobId,
        attempt,
        jobRoot,
        imageName,
        useIsolatedPool,
        false,
        Collections.emptyMap(),
        null,
        resourceRequirement,
        Map.of(JOB_TYPE, SPEC_JOB),
        getWorkerMetadata(),
        Collections.emptyMap(),
        "spec");
  }

  @Trace(operationName = WORKER_OPERATION_NAME)
  @Override
  public Process check(final Path jobRoot, final String configFilename, final String configContents) throws WorkerException {
    ApmTraceUtils.addTagsToTrace(Map.of(JOB_ID_KEY, jobId, JOB_ROOT_KEY, jobRoot, DOCKER_IMAGE_KEY, imageName));
    return processFactory.create(
        CHECK_JOB,
        jobId,
        attempt,
        jobRoot,
        imageName,
        useIsolatedPool,
        false,
        ImmutableMap.of(configFilename, configContents),
        null,
        resourceRequirement,
        Map.of(JOB_TYPE, CHECK_JOB),
        getWorkerMetadata(),
        Collections.emptyMap(),
        "check",
        CONFIG, configFilename);
  }

  @Trace(operationName = WORKER_OPERATION_NAME)
  @Override
  public Process discover(final Path jobRoot, final String configFilename, final String configContents) throws WorkerException {
    ApmTraceUtils.addTagsToTrace(Map.of(JOB_ID_KEY, jobId, JOB_ROOT_KEY, jobRoot, DOCKER_IMAGE_KEY, imageName));
    return processFactory.create(
        DISCOVER_JOB,
        jobId,
        attempt,
        jobRoot,
        imageName,
        useIsolatedPool,
        false,
        ImmutableMap.of(configFilename, configContents),
        null,
        resourceRequirement,
        Map.of(JOB_TYPE, DISCOVER_JOB),
        getWorkerMetadata(),
        Collections.emptyMap(),
        "discover",
        CONFIG, configFilename);
  }

  @Trace(operationName = WORKER_OPERATION_NAME)
  @Override
  public Process read(final Path jobRoot,
                      final String configFilename,
                      final String configContents,
                      final String catalogFilename,
                      final String catalogContents,
                      final String stateFilename,
                      final String stateContents)
      throws WorkerException {
    ApmTraceUtils.addTagsToTrace(Map.of(JOB_ID_KEY, jobId, JOB_ROOT_KEY, jobRoot, DOCKER_IMAGE_KEY, imageName));
    final List<String> arguments = Lists.newArrayList(
        "read",
        CONFIG, configFilename,
        "--catalog", catalogFilename);

    final Map<String, String> files = new HashMap<>();
    files.put(configFilename, configContents);
    files.put(catalogFilename, catalogContents);

    if (stateFilename != null) {
      arguments.add("--state");
      arguments.add(stateFilename);

      Preconditions.checkNotNull(stateContents);
      files.put(stateFilename, stateContents);
    }

    return processFactory.create(
        READ_STEP,
        jobId,
        attempt,
        jobRoot,
        imageName,
        useIsolatedPool,
        false,
        files,
        null,
        resourceRequirement,
        Map.of(JOB_TYPE, SYNC_JOB, SYNC_STEP, READ_STEP),
        getWorkerMetadata(),
        Collections.emptyMap(),
        arguments.toArray(new String[arguments.size()]));
  }

  @Trace(operationName = WORKER_OPERATION_NAME)
  @Override
  public Process write(final Path jobRoot,
                       final String configFilename,
                       final String configContents,
                       final String catalogFilename,
                       final String catalogContents)
      throws WorkerException {
    ApmTraceUtils.addTagsToTrace(Map.of(JOB_ID_KEY, jobId, JOB_ROOT_KEY, jobRoot, DOCKER_IMAGE_KEY, imageName));
    final Map<String, String> files = ImmutableMap.of(
        configFilename, configContents,
        catalogFilename, catalogContents);

    return processFactory.create(
        WRITE_STEP,
        jobId,
        attempt,
        jobRoot,
        imageName,
        useIsolatedPool,
        true,
        files,
        null,
        resourceRequirement,
        Map.of(JOB_TYPE, SYNC_JOB, SYNC_STEP, WRITE_STEP),
        getWorkerMetadata(),
        Collections.emptyMap(),
        "write",
        CONFIG, configFilename,
        "--catalog", catalogFilename);
  }

  private Map<String, String> getWorkerMetadata() {
    return Map.of(
        WorkerEnvConstants.WORKER_CONNECTOR_IMAGE, imageName,
        WorkerEnvConstants.WORKER_JOB_ID, jobId,
        WorkerEnvConstants.WORKER_JOB_ATTEMPT, String.valueOf(attempt),
        EnvVariableFeatureFlags.USE_STREAM_CAPABLE_STATE, String.valueOf(featureFlags.useStreamCapableState()),
        EnvVariableFeatureFlags.AUTO_DETECT_SCHEMA, String.valueOf(featureFlags.autoDetectSchema()));
  }

}
