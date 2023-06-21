package io.airbyte.integrations.source.postgres.xmin;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import io.airbyte.commons.json.Jsons;
import io.airbyte.integrations.source.postgres.internal.models.XminStatus;
import org.junit.jupiter.api.Test;

public class PostgresXminHandlerTest {

  @Test
  void testWraparound() {
    final XminStatus initialStatus =
        new XminStatus()
            .withNumWraparound(0L)
            .withXminRawValue(5555L)
            .withXminRawValue(5555L);
    final JsonNode initialStatusAsJson = Jsons.jsonNode(initialStatus);

    final XminStatus noWrapAroundStatus =
        new XminStatus()
            .withNumWraparound(0L)
            .withXminRawValue(5588L)
            .withXminRawValue(5588L);
    assertFalse(PostgresXminHandler.isSingleWraparound(initialStatus, noWrapAroundStatus));
    assertFalse(PostgresXminHandler.shouldPerformFullSync(noWrapAroundStatus, initialStatusAsJson));

    final XminStatus singleWrapAroundStatus =
        new XminStatus()
            .withNumWraparound(1L)
            .withXminRawValue(5588L)
            .withXminRawValue(4294972884L);

    assertTrue(PostgresXminHandler.isSingleWraparound(initialStatus, singleWrapAroundStatus));
    assertFalse(PostgresXminHandler.shouldPerformFullSync(singleWrapAroundStatus, initialStatusAsJson));

    final XminStatus doubleWrapAroundStatus =
        new XminStatus()
            .withNumWraparound(2L)
            .withXminRawValue(5588L)
            .withXminRawValue(8589940180L);

    assertFalse(PostgresXminHandler.isSingleWraparound(initialStatus, doubleWrapAroundStatus));
    assertTrue(PostgresXminHandler.shouldPerformFullSync(doubleWrapAroundStatus, initialStatusAsJson));
  }
//
//
//  @Test
//  void testWraparound() {
//    final JsonNode initialStatus =
//        Jsons.jsonNode(new XminStatus()
//            .withNumWraparound(0L)
//            .withXminRawValue(5555L)
//            .withXminRawValue(5555L));
//
//    final XminStatus noWrapAroundStatus =
//        new XminStatus()
//            .withNumWraparound(0L)
//            .withXminRawValue(5588L)
//            .withXminRawValue(5588L);
//    assertFalse(PostgresXminHandler.shouldPerformFullSync(noWrapAroundStatus, initialStatus));
//
//    final XminStatus singleWrapAroundStatus =
//        new XminStatus()
//            .withNumWraparound(1L)
//            .withXminRawValue(5588L)
//            .withXminRawValue(4294972884L);
//
//    assertFalse(PostgresXminHandler.shouldPerformFullSync(singleWrapAroundStatus, initialStatus));
//
//    final XminStatus doubleWrapAroundStatus =
//        new XminStatus()
//            .withNumWraparound(2L)
//            .withXminRawValue(5588L)
//            .withXminRawValue(8589940180L);
//
//    assertTrue(PostgresXminHandler.shouldPerformFullSync(doubleWrapAroundStatus, initialStatus));
//  }
}
