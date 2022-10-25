/*
 * Copyright (c) 2022 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.metrics.lib;

/**
 * Collection of constants for APM tracing.
 */
public final class ApmTraceConstants {

  /**
   * Operation name for an APM trace of a Temporal activity.
   */
  public static final String ACTIVITY_TRACE_OPERATION_NAME = "activity";

  /**
   * Operation name for an APM trace of a job orchestrator.
   */
  public static final String JOB_ORCHESTRATOR_OPERATION_NAME = "job.orchestrator";

  /**
   * Operation name for an APM trace of a worker implementation.
   */
  public static final String WORKER_OPERATION_NAME = "worker";

  /**
   * Operation name for an APM trace of a Temporal workflow.
   */
  public static final String WORKFLOW_TRACE_OPERATION_NAME = "workflow";

  private ApmTraceConstants() {}

  /**
   * Trace tag constants.
   */
  public static final class Tags {

    /**
     * Name of the APM trace tag that holds the destination Docker image value associated with the
     * trace.
     */
    public static final String CONNECTION_ID_KEY = "connection_id";

    /**
     * Name of the APM trace tag that holds the connector version value associated with the trace.
     */
    public static final String CONNECTOR_VERSION_KEY = "connector_version";

    /**
     * Name of the APM trace tag that holds the destination Docker image value associated with the
     * trace.
     */
    public static final String DESTINATION_DOCKER_IMAGE_KEY = "destination.docker_image";

    /**
     * Name of the APM trace tag that holds the Docker image value associated with the trace.
     */
    public static final String DOCKER_IMAGE_KEY = "docker_image";

    /**
     * Name of the APM trace tag that holds the job ID value associated with the trace.
     */
    public static final String JOB_ID_KEY = "job_id";

    /**
     * Name of the APM trace tag that holds the job root value associated with the trace.
     */
    public static final String JOB_ROOT_KEY = "job_root";

    /**
     * Name of the APM trace tag that holds the source Docker image value associated with the trace.
     */
    public static final String SOURCE_DOCKER_IMAGE_KEY = "source.docker_image";

    /**
     * Name of the APM trace tag that holds the source ID value associated with the trace.
     */
    public static final String SOURCE_ID_KEY = "source.id";

    /**
     * Name of the APM trace tag that holds the webhook config ID value associated with the trace.
     */
    public static final String WEBHOOK_CONFIG_ID_KEY = "webhook.config_id";

    private Tags() {}

  }

}
