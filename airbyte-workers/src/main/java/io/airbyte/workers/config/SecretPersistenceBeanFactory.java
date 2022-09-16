/*
 * Copyright (c) 2022 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.config;

import io.airbyte.config.persistence.split_secrets.GoogleSecretManagerPersistence;
import io.airbyte.config.persistence.split_secrets.LocalTestingSecretPersistence;
import io.airbyte.config.persistence.split_secrets.NoOpSecretsHydrator;
import io.airbyte.config.persistence.split_secrets.RealSecretsHydrator;
import io.airbyte.config.persistence.split_secrets.SecretPersistence;
import io.airbyte.config.persistence.split_secrets.SecretsHydrator;
import io.airbyte.config.persistence.split_secrets.VaultSecretPersistence;
import io.airbyte.db.Database;
import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Requires;
import io.micronaut.context.annotation.Value;
import javax.inject.Named;
import javax.inject.Singleton;

/**
 * Micronaut bean factory for secret persistence-related singletons.
 */
@Factory
@SuppressWarnings("PMD.AvoidDuplicateLiterals")
public class SecretPersistenceBeanFactory {

  @Singleton
  @Requires(property = "airbyte.secret.persistence",
            notEquals = "TESTING_CONFIG_DB_TABLE")
  @Requires(property = "airbyte.secret.persistence",
            notEquals = "GOOGLE_SECRET_MANAGER")
  @Requires(property = "airbyte.secret.persistence",
            notEquals = "VAULT")
  @Requires(property = "airbyte.worker.plane",
            notEquals = "DATA_PLANE")
  @Named("secretPersistence")
  public SecretPersistence defaultSecretPersistence(@Named("configDatabase") final Database configDatabase) {
    return localTestingSecretPersistence(configDatabase);
  }

  @Singleton
  @Requires(property = "airbyte.secret.persistence",
            value = "TESTING_CONFIG_DB_TABLE")
  @Requires(property = "airbyte.worker.plane",
            notEquals = "DATA_PLANE")
  @Named("secretPersistence")
  public SecretPersistence localTestingSecretPersistence(@Named("configDatabase") final Database configDatabase) {
    return new LocalTestingSecretPersistence(configDatabase);
  }

  @Singleton
  @Requires(property = "airbyte.secret.persistence",
            value = "GOOGLE_SECRET_MANAGER")
  @Requires(property = "airbyte.worker.plane",
            notEquals = "DATA_PLANE")
  @Named("secretPersistence")
  public SecretPersistence googleSecretPersistence(@Value("${airbyte.secret.store.gcp.credentials}") final String credentials,
                                                   @Value("${airbyte.secret.store.gcp.project-id}") final String projectId) {
    return GoogleSecretManagerPersistence.getLongLived(projectId, credentials);
  }

  @Singleton
  @Requires(property = "airbyte.secret.persistence",
            value = "VAULT")
  @Requires(property = "airbyte.worker.plane",
            notEquals = "DATA_PLANE")
  @Named("secretPersistence")
  public SecretPersistence vaultSecretPersistence(@Value("${airbyte.secret.store.vault.address}") final String address,
                                                  @Value("${airbyte.secret.store.vault.prefix}") final String prefix,
                                                  @Value("${airbyte.secret.store.vault.token}") final String token) {
    return new VaultSecretPersistence(address, prefix, token);
  }

  @Singleton
  @Requires(property = "airbyte.worker.plane",
            notEquals = "DATA_PLANE")
  public SecretsHydrator secretsHydrator(@Named("secretPersistence") final SecretPersistence secretPersistence) {
    return new RealSecretsHydrator(secretPersistence);
  }

  @Singleton
  @Requires(env = "data")
  public SecretsHydrator secretsHydrator() {
    return new NoOpSecretsHydrator();
  }

}
