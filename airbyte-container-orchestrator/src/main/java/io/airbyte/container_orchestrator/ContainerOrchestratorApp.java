/*
 * Copyright (c) 2021 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container_orchestrator;

import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.logging.LoggingHelper;
import io.airbyte.commons.logging.MdcScope;
import io.airbyte.config.Configs;
import io.airbyte.config.EnvConfigs;
import io.airbyte.config.helpers.LogClientSingleton;
import io.airbyte.workers.WorkerApp;
import io.airbyte.workers.WorkerConfigs;
import io.airbyte.workers.WorkerUtils;
import io.airbyte.workers.process.AsyncKubePodStatus;
import io.airbyte.workers.process.AsyncOrchestratorPodProcess;
import io.airbyte.workers.process.DockerProcessFactory;
import io.airbyte.workers.process.KubePodInfo;
import io.airbyte.workers.process.KubePodProcess;
import io.airbyte.workers.process.KubePortManagerSingleton;
import io.airbyte.workers.process.KubeProcessFactory;
import io.airbyte.workers.process.ProcessFactory;
import io.airbyte.workers.process.WorkerHeartbeatServer;
import io.airbyte.workers.storage.DocumentStoreClient;
import io.airbyte.workers.storage.StateClients;
import io.airbyte.workers.temporal.sync.DbtLauncherWorker;
import io.airbyte.workers.temporal.sync.NormalizationLauncherWorker;
import io.airbyte.workers.temporal.sync.OrchestratorConstants;
import io.airbyte.workers.temporal.sync.ReplicationLauncherWorker;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;

/**
 * Entrypoint for the application responsible for launching containers and handling all message
 * passing for replication, normalization, and dbt. Also, the current version relies on a heartbeat
 * from a Temporal worker. This will also be removed in the future so this can run fully async.
 *
 * This application retrieves most of its configuration from copied files from the calling Temporal
 * worker.
 *
 * This app uses default logging which is directly captured by the calling Temporal worker. In the
 * future this will need to independently interact with cloud storage.
 */
@Slf4j
public class ContainerOrchestratorApp {

  private static final MdcScope.Builder LOG_MDC_BUILDER = new MdcScope.Builder()
      .setLogPrefix("container-orchestrator")
      .setPrefixColor(LoggingHelper.Color.CYAN_BACKGROUND);

  public static void main(final String[] args) throws Exception {
    WorkerHeartbeatServer heartbeatServer = null;
    DocumentStoreClient documentStoreClient = null;
    KubePodInfo kubePodInfo = null;
    AsyncStateManager asyncStateManager = null;

    try {
      // wait for config files to be copied
      final var successFile = Path.of(KubePodProcess.CONFIG_DIR, KubePodProcess.SUCCESS_FILE_NAME);

      while (!successFile.toFile().exists()) {
        log.info("Waiting for config file transfers to complete...");
        Thread.sleep(1000);
      }

      // read files that contain all necessary configuration
      final String application = Files.readString(Path.of(KubePodProcess.CONFIG_DIR, OrchestratorConstants.INIT_FILE_APPLICATION));
      final Map<String, String> envMap =
          (Map<String, String>) Jsons.deserialize(Files.readString(Path.of(KubePodProcess.CONFIG_DIR, OrchestratorConstants.INIT_FILE_ENV_MAP)),
              Map.class);

      kubePodInfo =
          Jsons.deserialize(Files.readString(Path.of(KubePodProcess.CONFIG_DIR, AsyncOrchestratorPodProcess.KUBE_POD_INFO)), KubePodInfo.class);

      final Configs configs = new EnvConfigs(envMap);

      for (String envVar : OrchestratorConstants.ENV_VARS_TO_TRANSFER) {
        if (envMap.containsKey(envVar)) {
          System.setProperty(envVar, envMap.get(envVar));
        }
      }

      final var logClient = LogClientSingleton.getInstance();
      final var jobRunConfig = JobOrchestrator.readJobRunConfig();
      final var jobRoot = WorkerUtils.getJobRoot(configs.getWorkspaceRoot(), jobRunConfig.getJobId(), jobRunConfig.getAttemptId());

      logClient.setJobMdc(
          configs.getWorkerEnvironment(),
          configs.getLogConfigs(),
          jobRoot);

      try (final var mdcScope = LOG_MDC_BUILDER.build()) {
        documentStoreClient = StateClients.create(configs.getStateStorageCloudConfigs(), WorkerApp.STATE_STORAGE_PREFIX);
        asyncStateManager = new DefaultAsyncStateManager(documentStoreClient);

        asyncStateManager.write(kubePodInfo, AsyncKubePodStatus.INITIALIZING);

        heartbeatServer = new WorkerHeartbeatServer(WorkerApp.KUBE_HEARTBEAT_PORT);
        heartbeatServer.startBackground();

        final WorkerConfigs workerConfigs = new WorkerConfigs(configs);
        final ProcessFactory processFactory = getProcessBuilderFactory(configs, workerConfigs);
        final JobOrchestrator<?> jobOrchestrator = getJobOrchestrator(configs, workerConfigs, processFactory, application);

        log.info("Starting {} orchestrator...", jobOrchestrator.getOrchestratorName());
        asyncStateManager.write(kubePodInfo, AsyncKubePodStatus.RUNNING);
        final Optional<String> output = jobOrchestrator.runJob();
        asyncStateManager.write(kubePodInfo, AsyncKubePodStatus.SUCCEEDED, output.orElse(""));
        log.info("{} orchestrator complete!", jobOrchestrator.getOrchestratorName());
      } catch (Throwable t) {
        if (asyncStateManager != null && kubePodInfo != null) {
          asyncStateManager.write(kubePodInfo, AsyncKubePodStatus.FAILED);
        }

        log.error("Orchestrator failed", t);
      } finally {
        if (heartbeatServer != null) {
          log.info("Shutting down heartbeat server...");
          heartbeatServer.stop();
        }

        // required to kill kube client
        log.info("Runner closing...");
        System.exit(0);
      }
      // todo: catch throwing pre-log configuration in a better way
    } catch (Throwable t) {
      // not catchable by cloud logging
      if (asyncStateManager != null && kubePodInfo != null) {
        asyncStateManager.write(kubePodInfo, AsyncKubePodStatus.FAILED);
      }

      log.error("Orchestrator failed", t);
    } finally {
      // not catchable by cloud logging
      if (heartbeatServer != null) {
        log.info("Shutting down heartbeat server...");
        heartbeatServer.stop();
      }

      // required to kill kube client
      log.info("Runner closing...");
      System.exit(0);
    }
  }

  private static JobOrchestrator<?> getJobOrchestrator(final Configs configs,
                                                       final WorkerConfigs workerConfigs,
                                                       final ProcessFactory processFactory,
                                                       final String application) {

    return switch (application) {
      case ReplicationLauncherWorker.REPLICATION -> new ReplicationJobOrchestrator(configs, workerConfigs, processFactory);
      case NormalizationLauncherWorker.NORMALIZATION -> new NormalizationJobOrchestrator(configs, workerConfigs, processFactory);
      case DbtLauncherWorker.DBT -> new DbtJobOrchestrator(configs, workerConfigs, processFactory);
      case AsyncOrchestratorPodProcess.NO_OP -> new NoOpOrchestrator();
      default -> {
        log.error("Runner failed", new IllegalStateException("Unexpected value: " + application));
        System.exit(1);
      }
    };
  }

  /**
   * Creates a process builder factory that will be used to create connector containers/pods.
   */
  private static ProcessFactory getProcessBuilderFactory(final Configs configs, final WorkerConfigs workerConfigs) throws IOException {
    if (configs.getWorkerEnvironment() == Configs.WorkerEnvironment.KUBERNETES) {
      final KubernetesClient fabricClient = new DefaultKubernetesClient();
      final String localIp = InetAddress.getLocalHost().getHostAddress();
      final String kubeHeartbeatUrl = localIp + ":" + WorkerApp.KUBE_HEARTBEAT_PORT;
      log.info("Using Kubernetes namespace: {}", configs.getJobKubeNamespace());

      // this needs to have two ports for the source and two ports for the destination (all four must be
      // exposed)
      KubePortManagerSingleton.init(OrchestratorConstants.PORTS);

      return new KubeProcessFactory(workerConfigs, configs.getJobKubeNamespace(), fabricClient, kubeHeartbeatUrl, false);
    } else {
      return new DockerProcessFactory(
          workerConfigs,
          configs.getWorkspaceRoot(),
          configs.getWorkspaceDockerMount(),
          configs.getLocalDockerMount(),
          configs.getDockerNetwork(),
          false);
    }
  }

}
