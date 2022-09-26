/*
 * Copyright (c) 2022 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.protocol;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.inject.Inject;
import org.junit.jupiter.api.Test;

@MicronautTest
class AirbyteSerDeProviderMicronautTest {

  @Inject
  AirbyteSerDeProvider serDeProvider;

  @Test
  void testSerDeInjection() {
    // This should contain the list of all the supported majors of the airbyte protocol
    final Set<String> expectedVersions = new HashSet<>(List.of("0"));

    assertEquals(expectedVersions, serDeProvider.getDeserializerKeys());
    assertEquals(expectedVersions, serDeProvider.getSerializerKeys());
  }

}
