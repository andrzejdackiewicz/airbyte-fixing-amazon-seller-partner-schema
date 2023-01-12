/*
 * Copyright (c) 2022 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.exasol;

import io.airbyte.commons.text.Names;
import io.airbyte.integrations.destination.ExtendedNameTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class ExasolSQLNameTransformer extends ExtendedNameTransformer {
  @Override
  public String applyDefaultCase(final String input) {
    return input.toUpperCase();
  }

  @Override
  public String getRawTableName(final String streamName) {
    // Exasol identifiers starting with _ must be quoted
    return Names.doubleQuote(super.getRawTableName(streamName));
  }

  @Override
  public String getTmpTableName(final String streamName) {
    // Exasol identifiers starting with _ must be quoted
    return Names.doubleQuote(super.getTmpTableName(streamName));
  }

  @Override
  public String convertStreamName(final String input) {
    // Sometimes the stream name is already quoted, so remove quotes before converting.
    // Exasol identifiers starting with _ must be quoted.
    return Names.doubleQuote(super.convertStreamName(unquote(input)));
  }

  private static String unquote(final String input) {
    String result = input;
    if(result.startsWith("\"")) {
      result = result.substring(1);
    }
    if(result.endsWith("\"")) {
      result = result.substring(0, result.length()-1);
    }
    return result;
  }
}
