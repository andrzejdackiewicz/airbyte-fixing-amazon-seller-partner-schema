/*
 * Copyright (c) 2022 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.config;

import io.airbyte.analytics.Deployment;
import io.airbyte.analytics.TrackingClient;
import io.airbyte.analytics.TrackingClientSingleton;
import io.airbyte.commons.temporal.TemporalClient;
import io.airbyte.commons.temporal.config.WorkerMode;
import io.airbyte.commons.version.AirbyteVersion;
import io.airbyte.config.Configs.DeploymentMode;
import io.airbyte.config.Configs.TrackingStrategy;
import io.airbyte.config.Configs.WorkerEnvironment;
import io.airbyte.config.persistence.ConfigRepository;
import io.airbyte.persistence.job.JobPersistence;
import io.airbyte.persistence.job.errorreporter.JobErrorReporter;
import io.airbyte.persistence.job.factory.OAuthConfigSupplier;
import io.airbyte.persistence.job.tracker.JobTracker;
import io.airbyte.server.scheduler.DefaultSynchronousSchedulerClient;
import io.airbyte.server.scheduler.SynchronousSchedulerClient;
import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Requires;
import io.micronaut.context.annotation.Value;
import jakarta.inject.Singleton;
import java.io.IOException;

/**
 * Micronaut bean factory for Temporal-related singletons.
 */
@Factory
public class TemporalBeanFactory {

  @Singleton
  @Requires(env = WorkerMode.CONTROL_PLANE)
  public TrackingClient trackingClient(final TrackingStrategy trackingStrategy,
                                       final DeploymentMode deploymentMode,
                                       final JobPersistence jobPersistence,
                                       final WorkerEnvironment workerEnvironment,
                                       @Value("${airbyte.role}") final String airbyteRole,
                                       final AirbyteVersion airbyteVersion,
                                       final ConfigRepository configRepository)
      throws IOException {

    TrackingClientSingleton.initialize(
        trackingStrategy,
        new Deployment(deploymentMode, jobPersistence.getDeployment().orElseThrow(),
            workerEnvironment),
        airbyteRole,
        airbyteVersion,
        configRepository);

    return TrackingClientSingleton.get();
  }

  @Singleton
  @Requires(env = WorkerMode.CONTROL_PLANE)
  public OAuthConfigSupplier oAuthConfigSupplier(final ConfigRepository configRepository, final TrackingClient trackingClient) {
    return new OAuthConfigSupplier(configRepository, trackingClient);
  }

  @Singleton
  @Requires(env = WorkerMode.CONTROL_PLANE)
  public SynchronousSchedulerClient synchronousSchedulerClient(final TemporalClient temporalClient,
                                                               final JobTracker jobTracker,
                                                               final JobErrorReporter jobErrorReporter,
                                                               final OAuthConfigSupplier oAuthConfigSupplier) {
    return new DefaultSynchronousSchedulerClient(temporalClient, jobTracker, jobErrorReporter, oAuthConfigSupplier);
  }

}
