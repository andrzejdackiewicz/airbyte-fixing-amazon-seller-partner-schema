/*
 * Copyright (c) 2022 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.spec;

import static io.airbyte.workers.temporal.TemporalTraceConstants.DOCKER_IMAGE_TAG_KEY;
import static io.airbyte.workers.temporal.TemporalTraceConstants.JOB_ID_TAG_KEY;
import static io.airbyte.workers.temporal.TemporalTraceConstants.WORKFLOW_TRACE_OPERATION_NAME;

import datadog.trace.api.Trace;
import io.airbyte.commons.temporal.scheduling.SpecWorkflow;
import io.airbyte.config.ConnectorJobOutput;
import io.airbyte.metrics.lib.ApmTraceUtils;
import io.airbyte.persistence.job.models.IntegrationLauncherConfig;
import io.airbyte.persistence.job.models.JobRunConfig;
import io.airbyte.workers.temporal.annotations.TemporalActivityStub;
import java.util.Map;

public class SpecWorkflowImpl implements SpecWorkflow {

  @TemporalActivityStub(activityOptionsBeanName = "specActivityOptions")
  private SpecActivity activity;

  @Trace(operationName = WORKFLOW_TRACE_OPERATION_NAME)
  @Override
  public ConnectorJobOutput run(final JobRunConfig jobRunConfig, final IntegrationLauncherConfig launcherConfig) {
    ApmTraceUtils.addTagsToTrace(Map.of(DOCKER_IMAGE_TAG_KEY, launcherConfig.getDockerImage(), JOB_ID_TAG_KEY, jobRunConfig.getJobId()));
    return activity.run(jobRunConfig, launcherConfig);
  }

}
