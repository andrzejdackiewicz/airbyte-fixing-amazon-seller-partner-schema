/*
 * Copyright (c) 2022 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.source.relationaldb;

import com.fasterxml.jackson.databind.JsonNode;
import io.airbyte.commons.util.AutoCloseableIterator;
import io.airbyte.commons.util.AutoCloseableIterators;
import io.airbyte.db.SqlDatabase;
import java.util.List;
import java.util.StringJoiner;
import java.util.stream.Stream;

/**
 * Utility class for methods to query a relational db.
 */
public class RelationalDbQueryUtils {

  public static String getIdentifierWithQuoting(final String identifier, final String quoteString) {
    return quoteString + identifier + quoteString;
  }

  public static String enquoteIdentifierList(final List<String> identifiers, final String quoteString) {
    final StringJoiner joiner = new StringJoiner(",");
    for (final String identifier : identifiers) {
      joiner.add(getIdentifierWithQuoting(identifier, quoteString));
    }
    return joiner.toString();
  }

  public static String getFullTableName(final String nameSpace, final String tableName, final String quoteString) {
    return (nameSpace == null || nameSpace.isEmpty() ? getIdentifierWithQuoting(tableName, quoteString)
        : getIdentifierWithQuoting(nameSpace, quoteString) + "." + getIdentifierWithQuoting(tableName, quoteString));
  }

  public static <Database extends SqlDatabase> AutoCloseableIterator<JsonNode> queryTable(final Database database, final String sqlQuery) {
    return AutoCloseableIterators.lazyIterator(() -> {
      try {
        final Stream<JsonNode> stream = database.unsafeQuery(sqlQuery);
        return AutoCloseableIterators.fromStream(stream);
      } catch (final Exception e) {
        throw new RuntimeException(e);
      }
    });
  }

}
