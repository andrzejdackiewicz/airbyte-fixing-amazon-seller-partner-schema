
package io.airbyte.db.instance.configs.migrations;

import io.airbyte.db.instance.configs.AbstractConfigsDatabaseTest;
import java.util.UUID;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class V0_40_3_003__AddTokenToWorkspaceTest extends AbstractConfigsDatabaseTest {

  @Test
  void test() {
    final DSLContext context = getDslContext();

    // necessary to add workspace table
    V0_32_8_001__AirbyteConfigDatabaseDenormalization.migrate(context);

    final UUID id = UUID.randomUUID();
    context.insertInto(DSL.table("actor_definition"))
        .columns(
            DSL.field("id"),
            DSL.field("name"),
            DSL.field("slug"),
            DSL.field("initial_setup_complete"))
        .values(
            id,
            "name",
            "slug",
            Boolean.TRUE)
        .execute();

    Assertions.assertFalse(tokenColumnExists(context));

    V0_40_3_003__AddTokenToWorkspace.addTokenColumn(context);

    Assertions.assertTrue(tokenColumnExists(context));

  }

  protected static boolean tokenColumnExists(final DSLContext ctx) {
    return ctx.fetchExists(DSL.select()
        .from("information_schema.columns")
        .where(DSL.field("table_name").eq("workspace")
            .and(DSL.field("column_name").eq("token"))));
  }

}
