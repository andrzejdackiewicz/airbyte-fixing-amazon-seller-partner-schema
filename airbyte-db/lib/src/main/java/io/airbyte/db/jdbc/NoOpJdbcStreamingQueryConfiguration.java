/*
 * Copyright (c) 2021 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class NoOpJdbcStreamingQueryConfiguration implements JdbcStreamingQueryConfiguration {

  @Override
  public void accept(final Connection connection, final PreparedStatement preparedStatement)
      throws SQLException {

  }

  @Override
  public void setFetchSize(final int fetchSize) {}

}
