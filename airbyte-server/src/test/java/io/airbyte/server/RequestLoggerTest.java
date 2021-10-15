/*
 * Copyright (c) 2021 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server;

import com.google.common.collect.Lists;
import io.airbyte.commons.io.IOs;
import io.airbyte.config.helpers.LogClientSingleton;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerResponseContext;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.MDC;

@ExtendWith(MockitoExtension.class)
public class RequestLoggerTest {

  private static final String VALID_JSON_OBJECT = "{\"valid\":1}";
  private static final String INVALID_JSON_OBJECT = "invalid";
  private static final String ACCEPTED_CONTENT_TYPE = "application/json";
  private static final String NON_ACCEPTED_CONTENT_TYPE = "application/gzip";

  private static final String method = "POST";
  private static final String remoteAddr = "123.456.789.101";
  private static final String url = "/api/v1/test";

  @Mock
  private HttpServletRequest mServletRequest;

  @Mock
  private ContainerRequestContext mRequestContext;

  @Mock
  private ContainerResponseContext mResponseContext;

  private RequestLogger requestLogger;

  @BeforeEach
  public void init() throws Exception {
    Mockito.when(mRequestContext.getMethod())
        .thenReturn(method);

    Mockito.when(mServletRequest.getMethod())
        .thenReturn(method);
    Mockito.when(mServletRequest.getRemoteAddr())
        .thenReturn(remoteAddr);
    Mockito.when(mServletRequest.getRequestURI())
        .thenReturn(url);
  }

  private static final int ERROR_CODE = 401;
  private static final int SUCCESS_CODE = 200;

  private static final String errorPrefix = RequestLogger
      .createLogPrefix(remoteAddr, method, ERROR_CODE, url)
      .toString();

  private static final String successPrefix = RequestLogger
      .createLogPrefix(remoteAddr, method, SUCCESS_CODE, url)
      .toString();


  static Stream<Arguments> logScenarios() {
    return Stream.of(
        Arguments.of(INVALID_JSON_OBJECT, NON_ACCEPTED_CONTENT_TYPE, ERROR_CODE, errorPrefix),
        Arguments.of(INVALID_JSON_OBJECT, ACCEPTED_CONTENT_TYPE, ERROR_CODE, errorPrefix),
        Arguments.of(VALID_JSON_OBJECT, NON_ACCEPTED_CONTENT_TYPE, ERROR_CODE, errorPrefix),
        Arguments.of(VALID_JSON_OBJECT, ACCEPTED_CONTENT_TYPE, ERROR_CODE, errorPrefix + " - " + VALID_JSON_OBJECT),
        Arguments.of(INVALID_JSON_OBJECT, NON_ACCEPTED_CONTENT_TYPE, SUCCESS_CODE, successPrefix),
        Arguments.of(INVALID_JSON_OBJECT, ACCEPTED_CONTENT_TYPE, SUCCESS_CODE, successPrefix),
        Arguments.of(VALID_JSON_OBJECT, NON_ACCEPTED_CONTENT_TYPE, SUCCESS_CODE, successPrefix),
        Arguments.of(VALID_JSON_OBJECT, ACCEPTED_CONTENT_TYPE, SUCCESS_CODE, successPrefix + " - " + VALID_JSON_OBJECT)
    );
  }

  @ParameterizedTest
  @MethodSource("logScenarios")
  @DisplayName("Check that the proper log is produced based on the scenario")
  public void test(String inputByteBuffer, String contentType, int status, String expectedLog) throws IOException {
    // set up the mdc so that actually log to a file, so that we can verify that file logging captures
    // threads.
    final Path jobRoot = Files.createTempDirectory(Path.of("/tmp"), "mdc_test");
    LogClientSingleton.setJobMdc(jobRoot);

    // We have to instanciate the logger here, because the MDC config has been changed to log in a temporary file.
    requestLogger = new RequestLogger(MDC.getCopyOfContextMap(), mServletRequest);

    Mockito.when(mRequestContext.getEntityStream())
        .thenReturn(new ByteArrayInputStream(inputByteBuffer.getBytes()));

    Mockito.when(mResponseContext.getStatus())
        .thenReturn(status);

    Mockito.when(mServletRequest.getHeader("Content-Type"))
        .thenReturn(contentType);

    // This is call to set the requestBody variable in the RequestLogger
    requestLogger.filter(mRequestContext);
    requestLogger.filter(mRequestContext, mResponseContext);

    String expectedLogLevel = status == SUCCESS_CODE ? "INFO" : "ERROR";

    final Path logPath = jobRoot.resolve(LogClientSingleton.LOG_FILENAME);
    final String logs = IOs.readFile(logPath);
    final List<String> splitLogs = Lists.newArrayList(logs.split(System.lineSeparator()));
    Assertions.assertThat(splitLogs)
        .extracting((line) -> line.endsWith(expectedLog) && line.contains(expectedLogLevel))
        .containsOnlyOnce(true);
  }
}
