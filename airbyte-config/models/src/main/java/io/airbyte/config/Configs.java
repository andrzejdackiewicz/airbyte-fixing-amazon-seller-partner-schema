/*
 * Copyright (c) 2021 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config;

import io.airbyte.commons.version.AirbyteVersion;
import io.airbyte.config.helpers.LogConfigs;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;

public interface Configs {

  // CORE
  // General
  String getAirbyteRole();

  AirbyteVersion getAirbyteVersion();

  String getAirbyteVersionOrWarning();

  String getSpecCacheBucket();

  DeploymentMode getDeploymentMode();

  WorkerEnvironment getWorkerEnvironment();

  Path getConfigRoot();

  Path getWorkspaceRoot();

  // Docker Only
  String getWorkspaceDockerMount();

  String getLocalDockerMount();

  String getDockerNetwork();

  Path getLocalRoot();

  // Secrets
  String getSecretStoreGcpProjectId();

  String getSecretStoreGcpCredentials();

  SecretPersistenceType getSecretPersistenceType();

  // Database
  String getDatabaseUser();

  String getDatabasePassword();

  String getDatabaseUrl();

  String getConfigDatabaseUser();

  String getConfigDatabasePassword();

  String getConfigDatabaseUrl();

  boolean runDatabaseMigrationOnStartup();

  // Airbyte Services
  String getTemporalHost();

  String getAirbyteApiHost();

  int getAirbyteApiPort();

  String getWebappUrl();

  // Jobs
  int getMaxSyncJobAttempts();

  int getMaxSyncTimeoutDays();

  String getJobImagePullPolicy();

  List<WorkerPodToleration> getWorkerPodTolerations();

  Map<String, String> getWorkerNodeSelectors();

  String getJobsImagePullSecret();

  String getJobSocatImage();

  String getJobBusyboxImage();

  String getJobCurlImage();

  String getKubeNamespace();

  String getCpuRequest();

  String getCpuLimit();

  String getMemoryRequest();

  String getMemoryLimit();

  // Logging/Monitoring/Tracking
  LogConfigs getLogConfigs();

  boolean getPublishMetrics();

  TrackingStrategy getTrackingStrategy();

  // APPLICATIONS
  // Worker
  MaxWorkersConfig getMaxWorkers();

  Set<Integer> getTemporalWorkerPorts();

  // Scheduler
  WorkspaceRetentionConfig getWorkspaceRetentionConfig();

  String getSubmitterNumThreads();

  enum TrackingStrategy {
    SEGMENT,
    LOGGING
  }

  enum WorkerEnvironment {
    DOCKER,
    KUBERNETES
  }

  enum DeploymentMode {
    OSS,
    CLOUD
  }

  enum SecretPersistenceType {
    NONE,
    TESTING_CONFIG_DB_TABLE,
    GOOGLE_SECRET_MANAGER
  }

}
