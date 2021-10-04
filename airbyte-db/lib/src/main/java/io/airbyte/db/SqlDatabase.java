/*
 * Copyright (c) 2021 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.stream.Stream;

public abstract class SqlDatabase extends AbstractDatabase {

  public abstract void execute(String sql) throws Exception;

  public abstract Stream<JsonNode> query(String sql, String... params) throws Exception;

}
