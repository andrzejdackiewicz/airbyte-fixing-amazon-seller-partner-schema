/*
 * Copyright (c) 2022 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations;

import org.flywaydb.core.api.migration.BaseJavaMigration;
import org.flywaydb.core.api.migration.Context;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO: update migration description in the class name
public class V0_40_22_001__AddIntegrationTypeToActorDefinition extends BaseJavaMigration {

  private static final Logger LOGGER = LoggerFactory.getLogger(
      V0_40_22_001__AddIntegrationTypeToActorDefinition.class);

  @Override
  public void migrate(final Context context) throws Exception {
    LOGGER.info("Running migration: {}", this.getClass().getSimpleName());
    final DSLContext ctx = DSL.using(context.getConnection());
    addIntegrationTypeColumn(ctx);
  }

  static void addIntegrationTypeColumn(final DSLContext ctx) {
    ctx.alterTable("actor_definition")
        .addColumnIfNotExists(DSL.field(
            "integration_type",
            SQLDataType.VARCHAR(255).nullable(true)))
        .execute();
  }

}
