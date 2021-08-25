/*
 * MIT License
 *
 * Copyright (c) 2020 Airbyte
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package io.airbyte.db.instance.toys.migrations;

import static org.jooq.impl.DSL.currentTimestamp;
import static org.jooq.impl.DSL.field;

import io.airbyte.db.instance.toys.ToysDatabaseInstance;
import org.flywaydb.core.api.migration.BaseJavaMigration;
import org.flywaydb.core.api.migration.Context;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;

public class V0_30_4_001__Add_timestamp_columns extends BaseJavaMigration {

  @Override
  public void migrate(Context context) {
    DSLContext dsl = DSL.using(context.getConnection());
    dsl.alterTable(ToysDatabaseInstance.TABLE_NAME)
        .addColumn(field("created_at", SQLDataType.TIMESTAMP.defaultValue(currentTimestamp()).nullable(false)))
        .execute();
    dsl.alterTable(ToysDatabaseInstance.TABLE_NAME)
        .addColumn(field("updated_at", SQLDataType.TIMESTAMP.defaultValue(currentTimestamp()).nullable(false)))
        .execute();
  }

}
