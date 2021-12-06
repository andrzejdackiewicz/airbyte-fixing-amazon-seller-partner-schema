package io.airbyte.bootloader;

import io.airbyte.commons.resources.MoreResources;
import io.airbyte.commons.version.AirbyteVersion;
import io.airbyte.config.Configs;
import io.airbyte.config.EnvConfigs;
import io.airbyte.config.StandardWorkspace;
import io.airbyte.config.init.YamlSeedConfigPersistence;
import io.airbyte.config.persistence.ConfigRepository;
import io.airbyte.config.persistence.DatabaseConfigPersistence;
import io.airbyte.db.Database;
import io.airbyte.db.instance.DatabaseMigrator;
import io.airbyte.db.instance.configs.ConfigsDatabaseInstance;
import io.airbyte.db.instance.configs.ConfigsDatabaseMigrator;
import io.airbyte.db.instance.jobs.JobsDatabaseInstance;
import io.airbyte.db.instance.jobs.JobsDatabaseMigrator;
import io.airbyte.scheduler.persistence.DefaultJobPersistence;
import io.airbyte.scheduler.persistence.JobPersistence;
import io.airbyte.validation.json.JsonValidationException;
import java.io.IOException;
import java.util.Optional;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BootloaderApp {
  private static final Logger LOGGER = LoggerFactory.getLogger(BootloaderApp.class);
  
  private static final AirbyteVersion VERSION_BREAK = new AirbyteVersion("0.32.0-alpha");

  public static void main(String[] args) throws Exception {
    final Configs configs = new EnvConfigs();

    final Database configDatabase = new ConfigsDatabaseInstance(
        configs.getConfigDatabaseUser(),
        configs.getConfigDatabasePassword(),
        configs.getConfigDatabaseUrl())
        .getAndInitialize();
    final DatabaseConfigPersistence configPersistence = new DatabaseConfigPersistence(configDatabase);
    final ConfigRepository configRepository =
        new ConfigRepository(configPersistence.withValidation(), null, Optional.empty(), Optional.empty());
    createWorkspaceIfNoneExists(configRepository);
    LOGGER.info("Set up config database and default workspace..");

    final Database jobDatabase = new JobsDatabaseInstance(
        configs.getDatabaseUser(),
        configs.getDatabasePassword(),
        configs.getDatabaseUrl())
        .getAndInitialize();
    final JobPersistence jobPersistence = new DefaultJobPersistence(jobDatabase);
    createDeploymentIfNoneExists(jobPersistence);
    LOGGER.info("Set up job database and default deployment..");

    final AirbyteVersion currAirbyteVersion = configs.getAirbyteVersion();
    assertNonBreakingMigration(jobPersistence, currAirbyteVersion);

    runFlywayMigration(configs, configDatabase, jobDatabase);
    LOGGER.info("Ran Flyway migrations...");

    jobPersistence.setVersion(currAirbyteVersion.serialize());
    LOGGER.info("Set version to ");

    configPersistence.loadData(YamlSeedConfigPersistence.getDefault());
    LOGGER.info("Loaded seed data...");

    LOGGER.info("Finished bootstrapping Airbyte environment..");
  }

  private static void createDeploymentIfNoneExists(final JobPersistence jobPersistence) throws IOException {
    final Optional<UUID> deploymentOptional = jobPersistence.getDeployment();
    if (deploymentOptional.isPresent()) {
      LOGGER.info("running deployment: {}", deploymentOptional.get());
    } else {
      final UUID deploymentId = UUID.randomUUID();
      jobPersistence.setDeployment(deploymentId);
      LOGGER.info("created deployment: {}", deploymentId);
    }
  }

  private static void createWorkspaceIfNoneExists(final ConfigRepository configRepository) throws JsonValidationException, IOException {
    if (!configRepository.listStandardWorkspaces(true).isEmpty()) {
      LOGGER.info("workspace already exists for the deployment.");
      return;
    }

    final UUID workspaceId = UUID.randomUUID();
    final StandardWorkspace workspace = new StandardWorkspace()
        .withWorkspaceId(workspaceId)
        .withCustomerId(UUID.randomUUID())
        .withName(workspaceId.toString())
        .withSlug(workspaceId.toString())
        .withInitialSetupComplete(false)
        .withDisplaySetupWizard(true)
        .withTombstone(false);
    configRepository.writeStandardWorkspace(workspace);
  }

  private static void assertNonBreakingMigration(JobPersistence jobPersistence, AirbyteVersion airbyteVersion) throws IOException {
    // version in the database when the server main method is called. may be empty if this is the first
    // time the server is started.
    final Optional<AirbyteVersion> initialAirbyteDatabaseVersion = jobPersistence.getVersion().map(AirbyteVersion::new);
    if (!isLegalUpgrade(initialAirbyteDatabaseVersion.orElse(null), airbyteVersion)) {
      final String attentionBanner = MoreResources.readResource("banner/attention-banner.txt");
      LOGGER.error(attentionBanner);
      final String message = String.format(
          "Cannot upgrade from version %s to version %s directly. First you must upgrade to version %s. After that upgrade is complete, you may upgrade to version %s",
          initialAirbyteDatabaseVersion.get().serialize(),
          airbyteVersion.serialize(),
          VERSION_BREAK.serialize(),
          airbyteVersion.serialize());

      LOGGER.error(message);
      throw new RuntimeException(message);
    }
  }

  static boolean isLegalUpgrade(final AirbyteVersion airbyteDatabaseVersion, final AirbyteVersion airbyteVersion) {
    // means there was no previous version so upgrade even needs to happen. always legal.
    if (airbyteDatabaseVersion == null) {
      return true;
    }

    final var isUpgradingThroughVersionBreak = !airbyteDatabaseVersion.lessThan(VERSION_BREAK) && airbyteVersion.greaterThan(VERSION_BREAK);
    return !isUpgradingThroughVersionBreak;
  }

  private static void runFlywayMigration(final Configs configs, final Database configDatabase, final Database jobDatabase) {
    final DatabaseMigrator configDbMigrator = new ConfigsDatabaseMigrator(configDatabase, BootloaderApp.class.getSimpleName());
    final DatabaseMigrator jobDbMigrator = new JobsDatabaseMigrator(jobDatabase, BootloaderApp.class.getSimpleName());

    configDbMigrator.createBaseline();
    jobDbMigrator.createBaseline();

    if (configs.runDatabaseMigrationOnStartup()) {
      LOGGER.info("Migrating configs database");
      configDbMigrator.migrate();
      LOGGER.info("Migrating jobs database");
      jobDbMigrator.migrate();
    } else {
      LOGGER.info("Auto database migration is skipped");
    }
  }
}
