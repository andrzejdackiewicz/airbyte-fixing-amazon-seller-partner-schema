/*
 * Copyright (c) 2022 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server;

import io.airbyte.analytics.TrackingClient;
import io.airbyte.commons.version.AirbyteVersion;
import io.airbyte.config.Configs.WorkerEnvironment;
import io.airbyte.config.helpers.LogConfigs;
import io.airbyte.config.persistence.ConfigRepository;
import io.airbyte.config.persistence.SecretsRepositoryReader;
import io.airbyte.config.persistence.SecretsRepositoryWriter;
import io.airbyte.config.persistence.StatePersistence;
import io.airbyte.db.Database;
import io.airbyte.persistence.job.JobPersistence;
import io.airbyte.server.apis.AttemptApiController;
import io.airbyte.server.apis.ConfigurationApi;
import io.airbyte.server.apis.binders.AttemptApiBinder;
import io.airbyte.server.apis.factories.AttemptApiFactory;
import io.airbyte.server.scheduler.EventRunner;
import io.airbyte.server.scheduler.SynchronousSchedulerClient;
import java.net.http.HttpClient;
import java.nio.file.Path;
import java.util.Set;
import org.flywaydb.core.Flyway;
import org.slf4j.MDC;

public interface ServerFactory {

  ServerRunnable create(SynchronousSchedulerClient cachingSchedulerClient,
                        ConfigRepository configRepository,
                        SecretsRepositoryReader secretsRepositoryReader,
                        SecretsRepositoryWriter secretsRepositoryWriter,
                        JobPersistence jobPersistence,
                        Database configsDatabase,
                        Database jobsDatabase,
                        TrackingClient trackingClient,
                        WorkerEnvironment workerEnvironment,
                        LogConfigs logConfigs,
                        AirbyteVersion airbyteVersion,
                        Path workspaceRoot,
                        HttpClient httpClient,
                        EventRunner eventRunner,
                        Flyway configsFlyway,
                        Flyway jobsFlyway);

  class Api implements ServerFactory {

    @Override
    public ServerRunnable create(final SynchronousSchedulerClient synchronousSchedulerClient,
                                 final ConfigRepository configRepository,
                                 final SecretsRepositoryReader secretsRepositoryReader,
                                 final SecretsRepositoryWriter secretsRepositoryWriter,
                                 final JobPersistence jobPersistence,
                                 final Database configsDatabase,
                                 final Database jobsDatabase,
                                 final TrackingClient trackingClient,
                                 final WorkerEnvironment workerEnvironment,
                                 final LogConfigs logConfigs,
                                 final AirbyteVersion airbyteVersion,
                                 final Path workspaceRoot,
                                 final HttpClient httpClient,
                                 final EventRunner eventRunner,
                                 final Flyway configsFlyway,
                                 final Flyway jobsFlyway) {
      // set static values for factory
      ConfigurationApiFactory.setValues(
          configRepository,
          secretsRepositoryReader,
          secretsRepositoryWriter,
          jobPersistence,
          synchronousSchedulerClient,
          new StatePersistence(configsDatabase),
          MDC.getCopyOfContextMap(),
          configsDatabase,
          jobsDatabase,
          trackingClient,
          workerEnvironment,
          logConfigs,
          airbyteVersion,
          workspaceRoot,
          httpClient,
          eventRunner,
          configsFlyway,
          jobsFlyway);

      AttemptApiFactory.setValues(jobPersistence, MDC.getCopyOfContextMap());

      // server configurations
      final Set<Class<?>> componentClasses = Set.of(ConfigurationApi.class, AttemptApiController.class);
      final Set<Object> components = Set.of(new CorsFilter(), new ConfigurationApiBinder(), new AttemptApiBinder());

      // construct server
      return new ServerApp(airbyteVersion, componentClasses, components);
    }

  }

}
