/*
 * Copyright (c) 2022 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.sync;

import io.airbyte.config.ActorCatalogFetchEvent;
import io.airbyte.config.persistence.ConfigRepository;
import java.io.IOException;
import java.time.OffsetDateTime;
import java.util.Optional;
import java.util.UUID;

public class RefreshSchemaActivityImpl implements RefreshSchemaActivity {

  private final Optional<ConfigRepository> configRepository;

  public RefreshSchemaActivityImpl(Optional<ConfigRepository> configRepository) {
    this.configRepository = configRepository;
  }

  @Override
  public boolean shouldRefreshSchema(UUID sourceCatalogId) throws IOException {
    // if job persistence is unavailable, default to skipping the schema refresh
    if (configRepository.isEmpty()) {
      return false;
    }

    if (!schemaRefreshRanRecently(sourceCatalogId)) {
      return true;
    }

    return false;
  }

  private boolean schemaRefreshRanRecently(UUID sourceCatalogId) throws IOException {
    Optional<ActorCatalogFetchEvent> mostRecentFetchEvent = configRepository.get().getMostRecentActorCatalogFetchEventForSource(sourceCatalogId);

    if (mostRecentFetchEvent.isEmpty()) {
      return false;
    }

    return mostRecentFetchEvent.get().getCreatedAt() > OffsetDateTime.now().minusHours(24l).toEpochSecond();
  }

}
