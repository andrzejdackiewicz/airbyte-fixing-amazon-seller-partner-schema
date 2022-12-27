/*
 * Copyright (c) 2022 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.general;

/*
 * Copyright (c) 2022 Airbyte, Inc., all rights reserved.
 */

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.airbyte.commons.io.IOs;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.resources.MoreResources;
import io.airbyte.config.ConnectorJobOutput;
import io.airbyte.config.ConnectorJobOutput.OutputType;
import io.airbyte.config.FailureReason;
import io.airbyte.config.StandardDiscoverCatalogInput;
import io.airbyte.config.persistence.ConfigRepository;
import io.airbyte.protocol.models.AirbyteCatalog;
import io.airbyte.protocol.models.AirbyteMessage;
import io.airbyte.protocol.models.AirbyteMessage.Type;
import io.airbyte.protocol.models.CatalogHelpers;
import io.airbyte.protocol.models.Field;
import io.airbyte.protocol.models.JsonSchemaType;
import io.airbyte.workers.WorkerConstants;
import io.airbyte.workers.exception.WorkerException;
import io.airbyte.workers.helper.ConnectorConfigUpdater;
import io.airbyte.workers.internal.AirbyteStreamFactory;
import io.airbyte.workers.process.IntegrationLauncher;
import io.airbyte.workers.test_utils.AirbyteMessageUtils;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.UUID;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;

class DefaultDiscoverCatalogWorkerTest {

  @Mock
  private ConfigRepository mConfigRepository;

  private static final JsonNode CREDENTIALS = Jsons.jsonNode(ImmutableMap.builder().put("apiKey", "123").build());

  private static final UUID SOURCE_ID = UUID.randomUUID();
  private static final StandardDiscoverCatalogInput INPUT =
      new StandardDiscoverCatalogInput().withConnectionConfiguration(CREDENTIALS).withSourceId(SOURCE_ID.toString());

  private static final Path TEST_ROOT = Path.of("/tmp/airbyte_tests");
  private static final String STREAM = "users";
  private static final String COLUMN_NAME = "name";
  private static final String COLUMN_AGE = "age";
  private static final AirbyteCatalog CATALOG = new AirbyteCatalog()
      .withStreams(Lists.newArrayList(CatalogHelpers.createAirbyteStream(
          STREAM,
          Field.of(COLUMN_NAME, JsonSchemaType.STRING),
          Field.of(COLUMN_AGE, JsonSchemaType.NUMBER))));

  private Path jobRoot;
  private IntegrationLauncher integrationLauncher;
  private Process process;
  private AirbyteStreamFactory streamFactory;
  private ConnectorConfigUpdater connectorConfigUpdater;

  private UUID CATALOG_ID;

  @BeforeEach
  void setup() throws Exception {
    jobRoot = Files.createTempDirectory(Files.createDirectories(TEST_ROOT), "");
    integrationLauncher = mock(IntegrationLauncher.class, RETURNS_DEEP_STUBS);
    process = mock(Process.class);
    mConfigRepository = mock(ConfigRepository.class);
    connectorConfigUpdater = mock(ConnectorConfigUpdater.class);

    CATALOG_ID = UUID.randomUUID();
    when(mConfigRepository.writeActorCatalogFetchEvent(any(), any(), any(), any())).thenReturn(CATALOG_ID);

    when(integrationLauncher.discover(jobRoot, WorkerConstants.SOURCE_CONFIG_JSON_FILENAME, Jsons.serialize(CREDENTIALS))).thenReturn(process);
    final InputStream inputStream = mock(InputStream.class);
    when(process.getInputStream()).thenReturn(inputStream);
    when(process.getErrorStream()).thenReturn(new ByteArrayInputStream(new byte[0]));

    IOs.writeFile(jobRoot, WorkerConstants.SOURCE_CATALOG_JSON_FILENAME, MoreResources.readResource("airbyte_postgres_catalog.json"));

    streamFactory = noop -> Lists.newArrayList(new AirbyteMessage().withType(Type.CATALOG).withCatalog(CATALOG)).stream();
  }

  @SuppressWarnings("BusyWait")
  @Test
  void testDiscoverSchema() throws Exception {
    final DefaultDiscoverCatalogWorker worker =
        new DefaultDiscoverCatalogWorker(mConfigRepository, integrationLauncher, connectorConfigUpdater, streamFactory);
    final ConnectorJobOutput output = worker.run(INPUT, jobRoot);

    assertNull(output.getFailureReason());
    assertEquals(OutputType.DISCOVER_CATALOG_ID, output.getOutputType());
    assertEquals(CATALOG_ID, output.getDiscoverCatalogId());
    verify(mConfigRepository).writeActorCatalogFetchEvent(eq(CATALOG), eq(SOURCE_ID), any(), any());

    Assertions.assertTimeout(Duration.ofSeconds(5), () -> {
      while (process.getErrorStream().available() != 0) {
        Thread.sleep(50);
      }
    });

    verify(process).exitValue();
  }

  @SuppressWarnings("BusyWait")
  @Test
  void testDiscoverSchemaProcessFail() throws Exception {
    when(process.exitValue()).thenReturn(1);

    final DefaultDiscoverCatalogWorker worker =
        new DefaultDiscoverCatalogWorker(mConfigRepository, integrationLauncher, connectorConfigUpdater, streamFactory);
    assertThrows(WorkerException.class, () -> worker.run(INPUT, jobRoot));

    Assertions.assertTimeout(Duration.ofSeconds(5), () -> {
      while (process.getErrorStream().available() != 0) {
        Thread.sleep(50);
      }
    });

    verify(process).exitValue();
  }

  @SuppressWarnings("BusyWait")
  @Test
  void testDiscoverSchemaProcessFailWithTraceMessage() throws Exception {
    final AirbyteStreamFactory traceStreamFactory = noop -> Lists.newArrayList(
        AirbyteMessageUtils.createErrorMessage("some error from the connector", 123.0)).stream();

    when(process.exitValue()).thenReturn(1);

    final DefaultDiscoverCatalogWorker worker =
        new DefaultDiscoverCatalogWorker(mConfigRepository, integrationLauncher, connectorConfigUpdater, traceStreamFactory);
    final ConnectorJobOutput output = worker.run(INPUT, jobRoot);
    // assertEquals(OutputType.DISCOVER_CATALOG, output.getOutputType());
    // assertNull(output.getDiscoverCatalog());
    assertNotNull(output.getFailureReason());

    final FailureReason failureReason = output.getFailureReason();
    assertEquals("some error from the connector", failureReason.getExternalMessage());

    Assertions.assertTimeout(Duration.ofSeconds(5), () -> {
      while (process.getErrorStream().available() != 0) {
        Thread.sleep(50);
      }
    });

    verify(process).exitValue();
  }

  @Test
  void testDiscoverSchemaException() throws WorkerException {
    when(integrationLauncher.discover(jobRoot, WorkerConstants.SOURCE_CONFIG_JSON_FILENAME, Jsons.serialize(CREDENTIALS)))
        .thenThrow(new RuntimeException());

    final DefaultDiscoverCatalogWorker worker =
        new DefaultDiscoverCatalogWorker(mConfigRepository, integrationLauncher, connectorConfigUpdater, streamFactory);
    assertThrows(WorkerException.class, () -> worker.run(INPUT, jobRoot));
  }

  @Test
  void testCancel() throws WorkerException {
    final DefaultDiscoverCatalogWorker worker =
        new DefaultDiscoverCatalogWorker(mConfigRepository, integrationLauncher, connectorConfigUpdater, streamFactory);
    worker.run(INPUT, jobRoot);

    worker.cancel();

    verify(process).destroy();
  }

}
