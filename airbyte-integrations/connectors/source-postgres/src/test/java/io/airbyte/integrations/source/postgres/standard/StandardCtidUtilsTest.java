/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.source.postgres.standard;

import static io.airbyte.integrations.source.postgres.standard.StandardCtidUtils.categoriseStreams;
import static io.airbyte.integrations.source.postgres.utils.PostgresUnitTestsUtil.generateStateMessage;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.Lists;
import io.airbyte.commons.json.Jsons;
import io.airbyte.integrations.source.postgres.ctid.CtidUtils.StreamsCategorised;
import io.airbyte.integrations.source.postgres.internal.models.CtidStatus;
import io.airbyte.integrations.source.postgres.internal.models.InternalModels.StateType;
import io.airbyte.integrations.source.postgres.internal.models.StandardStatus;
import io.airbyte.integrations.source.postgres.standard.StandardCtidUtils.StandardStreams;
import io.airbyte.integrations.source.relationaldb.state.StreamStateManager;
import io.airbyte.protocol.models.Field;
import io.airbyte.protocol.models.JsonSchemaType;
import io.airbyte.protocol.models.v0.AirbyteStateMessage;
import io.airbyte.protocol.models.v0.CatalogHelpers;
import io.airbyte.protocol.models.v0.ConfiguredAirbyteCatalog;
import io.airbyte.protocol.models.v0.ConfiguredAirbyteStream;
import io.airbyte.protocol.models.v0.SyncMode;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;

public class StandardCtidUtilsTest {

  @Test
  public void emptyStateTest() {
    final ConfiguredAirbyteCatalog configuredCatalog = new ConfiguredAirbyteCatalog().withStreams(Arrays.asList(STREAM_1, STREAM_2));
    final StreamStateManager streamStateManager = new StreamStateManager(Collections.emptyList(), configuredCatalog);
    final StreamsCategorised<StandardStreams> streamsCategorised = categoriseStreams(streamStateManager, configuredCatalog);

    assertEquals(2, streamsCategorised.ctidStreams().streamsForCtidSync().size());
    assertEquals(0, streamsCategorised.remainingStreams().streamsForStandardSync().size());
    assertTrue(streamsCategorised.remainingStreams().streamsForStandardSync().isEmpty());
    assertThat(streamsCategorised.ctidStreams().streamsForCtidSync()).containsExactlyInAnyOrder(STREAM_1, STREAM_2);
  }

  @Test
  public void correctOneCtidOneStandardTest() {
    final ConfiguredAirbyteCatalog configuredCatalog = new ConfiguredAirbyteCatalog().withStreams(Arrays.asList(STREAM_1, STREAM_2));
    final JsonNode stream1CtidStatus = Jsons.jsonNode(new CtidStatus()
        .withStateType(StateType.CTID)
        .withCtid("(0,0)")
        .withRelationFilenode(456L));

    final JsonNode stream2StandardStatus = Jsons.jsonNode(new StandardStatus()
        .withStateType(StateType.STANDARD)
        .withStreamName(STREAM_2.getStream().getName())
        .withStreamNamespace(STREAM_2.getStream().getNamespace())
        .withCursorField(List.of("COL_ID"))
        .withCursor("1")
        .withCursorRecordCount(1L));

    final JsonNode stream2StandardStatusJson = Jsons.jsonNode(stream2StandardStatus);
    final AirbyteStateMessage stream1CtidState = generateStateMessage(STREAM_1.getStream().getName(), STREAM_1.getStream().getNamespace(),
        stream1CtidStatus);
    final AirbyteStateMessage stream2StandardState = generateStateMessage(STREAM_2.getStream().getName(), STREAM_2.getStream().getNamespace(),
        stream2StandardStatusJson);
    final StreamStateManager streamStateManager = new StreamStateManager(List.of(stream1CtidState, stream2StandardState), configuredCatalog);
    final StreamsCategorised<StandardStreams> streamsCategorised = categoriseStreams(streamStateManager, configuredCatalog);

    assertEquals(streamsCategorised.ctidStreams().streamsForCtidSync().size(), 1);
    assertEquals(streamsCategorised.remainingStreams().streamsForStandardSync().size(), 1);
    assertEquals(streamsCategorised.ctidStreams().streamsForCtidSync().stream().findFirst().get(), STREAM_1);
    assertEquals(streamsCategorised.remainingStreams().streamsForStandardSync().stream().findFirst().get(), STREAM_2);
  }

  @Test
  public void correctEmptyCtidTest() {
    final ConfiguredAirbyteCatalog configuredCatalog = new ConfiguredAirbyteCatalog().withStreams(Arrays.asList(STREAM_1, STREAM_2));
    final JsonNode standardStatus = Jsons.jsonNode(new StandardStatus()
                                                          .withStateType(StateType.STANDARD)
                                                          .withStreamName(STREAM_2.getStream().getName())
                                                          .withStreamNamespace(STREAM_2.getStream().getNamespace())
                                                          .withCursorField(List.of("COL_ID"))
                                                          .withCursor("1")
                                                          .withCursorRecordCount(1L));

    final AirbyteStateMessage stream1CtidState = generateStateMessage(STREAM_1.getStream().getName(), STREAM_1.getStream().getNamespace(),
                                                                      standardStatus);
    final AirbyteStateMessage stream2StandardState = generateStateMessage(STREAM_2.getStream().getName(), STREAM_2.getStream().getNamespace(),
                                                                          standardStatus);
    final StreamStateManager streamStateManager = new StreamStateManager(List.of(stream1CtidState, stream2StandardState), configuredCatalog);
    final StreamsCategorised<StandardStreams> streamsCategorised = categoriseStreams(streamStateManager, configuredCatalog);

    assertEquals(streamsCategorised.ctidStreams().streamsForCtidSync().size(), 0);
    assertEquals(streamsCategorised.remainingStreams().streamsForStandardSync().size(), 2);
    assertThat(streamsCategorised.remainingStreams().streamsForStandardSync()).containsExactlyInAnyOrder(STREAM_1, STREAM_2);

  }

  private static final ConfiguredAirbyteStream STREAM_1 = CatalogHelpers.toDefaultConfiguredStream(CatalogHelpers.createAirbyteStream(
      "STREAM_1",
      "SCHEMA",
      Field.of("COL_ID", JsonSchemaType.INTEGER),
      Field.of("COL_MAKE_ID", JsonSchemaType.INTEGER),
      Field.of("COL_MODEL", JsonSchemaType.STRING))
      .withSupportedSyncModes(Lists.newArrayList(SyncMode.FULL_REFRESH, SyncMode.INCREMENTAL))
      .withSourceDefinedPrimaryKey(List.of(List.of("COL_ID"))));

  private static final ConfiguredAirbyteStream STREAM_2 = CatalogHelpers.toDefaultConfiguredStream(CatalogHelpers.createAirbyteStream(
      "STREAM_2",
      "SCHEMA",
      Field.of("COL_ID", JsonSchemaType.INTEGER),
      Field.of("COL_MAKE_ID", JsonSchemaType.INTEGER),
      Field.of("COL_MODEL", JsonSchemaType.STRING))
      .withSupportedSyncModes(Lists.newArrayList(SyncMode.FULL_REFRESH, SyncMode.INCREMENTAL))
      .withSourceDefinedPrimaryKey(List.of(List.of("COL_ID"))));

}
