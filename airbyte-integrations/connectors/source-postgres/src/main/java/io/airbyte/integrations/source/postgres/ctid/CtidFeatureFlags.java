/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.source.postgres.ctid;

import com.fasterxml.jackson.databind.JsonNode;

// Feature flags to gate CTID syncs
// One for each type: CDC and standard cursor based
public class CtidFeatureFlags {

  private final JsonNode sourceConfig;

  public CtidFeatureFlags(final JsonNode sourceConfig) {
    this.sourceConfig = sourceConfig;
  }

  private boolean getFlagValue(final String flag) {
    return sourceConfig.has(flag) && sourceConfig.get(flag).asBoolean();
  }

}
