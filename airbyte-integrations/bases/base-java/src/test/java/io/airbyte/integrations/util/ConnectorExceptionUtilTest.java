/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.util;

import static io.airbyte.integrations.util.ConnectorExceptionUtil.*;
import static org.junit.jupiter.api.Assertions.*;

import io.airbyte.commons.exceptions.ConfigErrorException;
import io.airbyte.commons.exceptions.ConnectionErrorException;
import java.io.EOFException;
import java.io.IOException;
import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;
import org.junit.jupiter.api.Test;

class ConnectorExceptionUtilTest {

  public static final String CONFIG_EXCEPTION_MESSAGE = "test message";
  public static final String RECOVERY_EXCEPTION_MESSAGE = "FATAL: terminating connection due to conflict with recovery";
  public static final String COMMON_EXCEPTION_MESSAGE = "something happens with connection";
  public static final String EOF_EXCEPTION_MESSAGE = "Can not read response from server. Expected to read";
  public static final String EOF_EXCEPTION_IN_CDC_MESSAGE = "Failed to read remaining";
  public static final String CONNECTION_ERROR_MESSAGE_TEMPLATE = "State code: %s; Error code: %s; Message: %s";
  public static final String UNKNOWN_COLUMN_SQL_EXCEPTION_MESSAGE = "Unknown column 'table.column' in 'field list'";

  @Test()
  void isConfigErrorForConfigException() {
    ConfigErrorException configErrorException = new ConfigErrorException(CONFIG_EXCEPTION_MESSAGE);
    assertTrue(ConnectorExceptionUtil.isConfigError(configErrorException));

  }

  @Test
  void isConfigErrorForConnectionException() {
    ConnectionErrorException connectionErrorException = new ConnectionErrorException(CONFIG_EXCEPTION_MESSAGE);
    assertTrue(ConnectorExceptionUtil.isConfigError(connectionErrorException));
  }

  @Test
  void isConfigErrorForRecoveryPSQLException() {
    SQLException recoveryPSQLException = new SQLException(RECOVERY_EXCEPTION_MESSAGE);
    assertTrue(ConnectorExceptionUtil.isConfigError(recoveryPSQLException));
  }

  @Test
  void isConfigErrorForUnknownColumnSQLSyntaxErrorException() {
    SQLSyntaxErrorException unknownColumnSQLSyntaxErrorException = new SQLSyntaxErrorException(UNKNOWN_COLUMN_SQL_EXCEPTION_MESSAGE);
    assertTrue(ConnectorExceptionUtil.isConfigError(unknownColumnSQLSyntaxErrorException));
  }

  @Test
  void isConfigErrorForEOFException() {
    EOFException eofException = new EOFException(EOF_EXCEPTION_MESSAGE);
    assertTrue(ConnectorExceptionUtil.isConfigError(eofException));
  }

  @Test
  void isConfigErrorForEOFExceptionCdc() {
    EOFException eofException = new EOFException(EOF_EXCEPTION_IN_CDC_MESSAGE);
    assertTrue(ConnectorExceptionUtil.isConfigError(eofException));
  }

  @Test
  void isConfigErrorForCommonSQLException() {
    SQLException commonSQLException = new SQLException(COMMON_EXCEPTION_MESSAGE);
    assertFalse(ConnectorExceptionUtil.isConfigError(commonSQLException));
  }

  @Test
  void isConfigErrorForCommonException() {
    assertFalse(ConnectorExceptionUtil.isConfigError(new Exception()));
  }

  @Test
  void getDisplayMessageForConfigException() {
    ConfigErrorException configErrorException = new ConfigErrorException(CONFIG_EXCEPTION_MESSAGE);
    String actualDisplayMessage = ConnectorExceptionUtil.getDisplayMessage(configErrorException);
    assertEquals(CONFIG_EXCEPTION_MESSAGE, actualDisplayMessage);
  }

  @Test
  void getDisplayMessageForConnectionError() {
    String testCode = "test code";
    int errorCode = -1;
    ConnectionErrorException connectionErrorException = new ConnectionErrorException(testCode, errorCode, CONFIG_EXCEPTION_MESSAGE, new Exception());
    String actualDisplayMessage = ConnectorExceptionUtil.getDisplayMessage(connectionErrorException);
    assertEquals(String.format(CONNECTION_ERROR_MESSAGE_TEMPLATE, testCode, errorCode, CONFIG_EXCEPTION_MESSAGE), actualDisplayMessage);
  }

  @Test
  void getDisplayMessageForRecoveryException() {
    SQLException recoveryException = new SQLException(RECOVERY_EXCEPTION_MESSAGE);
    String actualDisplayMessage = ConnectorExceptionUtil.getDisplayMessage(recoveryException);
    assertEquals(RECOVERY_CONNECTION_ERROR_MESSAGE, actualDisplayMessage);
  }

  @Test
  void getDisplayMessageForUnknownSQLErrorException() {
    SQLSyntaxErrorException unknownColumnSQLSyntaxErrorException = new SQLSyntaxErrorException(UNKNOWN_COLUMN_SQL_EXCEPTION_MESSAGE);
    String actualDisplayMessage = ConnectorExceptionUtil.getDisplayMessage(unknownColumnSQLSyntaxErrorException);
    assertEquals(UNKNOWN_COLUMN_SQL_EXCEPTION_MESSAGE, actualDisplayMessage);
  }

  @Test
  void getDisplayMessageForCommonException() {
    Exception exception = new SQLException(COMMON_EXCEPTION_MESSAGE);
    String actualDisplayMessage = ConnectorExceptionUtil.getDisplayMessage(exception);
    assertEquals(String.format(COMMON_EXCEPTION_MESSAGE_TEMPLATE, COMMON_EXCEPTION_MESSAGE), actualDisplayMessage);
  }

  @Test
  void getDisplayMessageForEOFException() {
    Exception exception = new EOFException(EOF_EXCEPTION_MESSAGE);
    String actualDisplayMessage = ConnectorExceptionUtil.getDisplayMessage(exception);
    assertEquals(ConnectorExceptionUtil.EOF_EXCEPTION_MESSAGE, actualDisplayMessage);
  }

  @Test
  void getRootConfigErrorFromConfigException() {
    ConfigErrorException configErrorException = new ConfigErrorException(CONFIG_EXCEPTION_MESSAGE);
    Exception exception = new Exception(COMMON_EXCEPTION_MESSAGE, configErrorException);

    Throwable actualRootConfigError = ConnectorExceptionUtil.getRootConfigError(exception);
    assertEquals(configErrorException, actualRootConfigError);
  }

  @Test
  void getRootConfigErrorFromRecoverySQLException() {
    SQLException recoveryException = new SQLException(RECOVERY_EXCEPTION_MESSAGE);
    RuntimeException runtimeException = new RuntimeException(COMMON_EXCEPTION_MESSAGE, recoveryException);
    Exception exception = new Exception(runtimeException);

    Throwable actualRootConfigError = ConnectorExceptionUtil.getRootConfigError(exception);
    assertEquals(recoveryException, actualRootConfigError);
  }

  @Test
  void getRootConfigErrorFromUnknownSQLErrorException() {
    SQLException unknownSQLErrorException = new SQLSyntaxErrorException(UNKNOWN_COLUMN_SQL_EXCEPTION_MESSAGE);
    RuntimeException runtimeException = new RuntimeException(COMMON_EXCEPTION_MESSAGE, unknownSQLErrorException);
    Exception exception = new Exception(runtimeException);

    Throwable actualRootConfigError = ConnectorExceptionUtil.getRootConfigError(exception);
    assertEquals(unknownSQLErrorException, actualRootConfigError);
  }

  @Test
  void getRootConfigErrorFromEOFException() {
    EOFException eofException = new EOFException(EOF_EXCEPTION_MESSAGE);
    IOException ioException = new IOException(COMMON_EXCEPTION_MESSAGE, eofException);
    Exception exception = new Exception(ioException);

    Throwable actualRootConfigError = ConnectorExceptionUtil.getRootConfigError(exception);
    assertEquals(eofException, actualRootConfigError);
  }

  @Test
  void getRootConfigErrorFromEOFExceptionInCDC() {
    EOFException eofException = new EOFException(EOF_EXCEPTION_IN_CDC_MESSAGE);
    IOException ioException = new IOException(COMMON_EXCEPTION_MESSAGE, eofException);
    Exception exception = new Exception(ioException);

    Throwable actualRootConfigError = ConnectorExceptionUtil.getRootConfigError(exception);
    assertEquals(eofException, actualRootConfigError);
  }

  @Test
  void getRootConfigErrorFromNonConfigException() {
    SQLException configErrorException = new SQLException(CONFIG_EXCEPTION_MESSAGE);
    Exception exception = new Exception(COMMON_EXCEPTION_MESSAGE, configErrorException);

    Throwable actualRootConfigError = ConnectorExceptionUtil.getRootConfigError(exception);
    assertEquals(exception, actualRootConfigError);
  }

}
