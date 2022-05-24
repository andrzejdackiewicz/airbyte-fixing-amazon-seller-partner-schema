/*
 * Copyright (c) 2021 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.init.impl;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

import io.airbyte.commons.resources.MoreResources;
import io.airbyte.db.check.DatabaseAvailabilityCheck;
import io.airbyte.db.check.DatabaseCheckException;
import io.airbyte.db.init.DatabaseInitializationException;
import io.airbyte.db.instance.DatabaseConstants;
import java.io.IOException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Test suite for the {@link ConfigsDatabaseInitializer} class.
 */
public class ConfigsDatabaseInitializerTest extends AbstractDatabaseInitializerTest {

  @Test
  void testInitializingSchema() throws IOException {
    final var databaseAvailabilityCheck = mock(DatabaseAvailabilityCheck.class);
    final var initialSchema = MoreResources.readResource(DatabaseConstants.CONFIGS_SCHEMA_PATH);
    final var initializer = new ConfigsDatabaseInitializer(databaseAvailabilityCheck, dslContext, initialSchema);

    Assertions.assertDoesNotThrow(() -> initializer.init());
    assertTrue(initializer.hasTable(dslContext, initializer.getTableNames().get().stream().findFirst().get()));
  }

  @Test
  void testInitializingSchemaAlreadyExists() throws IOException {
    final var databaseAvailabilityCheck = mock(DatabaseAvailabilityCheck.class);
    final var initialSchema = MoreResources.readResource(DatabaseConstants.CONFIGS_SCHEMA_PATH);
    dslContext.execute(initialSchema);
    final var initializer = new ConfigsDatabaseInitializer(databaseAvailabilityCheck, dslContext, initialSchema);

    Assertions.assertDoesNotThrow(() -> initializer.init());
    assertTrue(initializer.hasTable(dslContext, initializer.getTableNames().get().stream().findFirst().get()));
  }

  @Test
  void testInitializationException() throws IOException, DatabaseCheckException {
    final var databaseAvailabilityCheck = mock(DatabaseAvailabilityCheck.class);
    final var initialSchema = MoreResources.readResource(DatabaseConstants.CONFIGS_SCHEMA_PATH);

    doThrow(new DatabaseCheckException("test")).when(databaseAvailabilityCheck).check();

    final var initializer = new ConfigsDatabaseInitializer(databaseAvailabilityCheck, dslContext, initialSchema);
    Assertions.assertThrows(DatabaseInitializationException.class, () -> initializer.init());
  }

}
