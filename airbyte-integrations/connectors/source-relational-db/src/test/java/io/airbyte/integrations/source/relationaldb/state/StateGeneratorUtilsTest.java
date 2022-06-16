/*
 * Copyright (c) 2022 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.source.relationaldb.state;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.airbyte.protocol.models.StreamDescriptor;
import org.junit.jupiter.api.Test;

/**
 * Test suite for the {@link StateGeneratorUtils} class.
 */
public class StateGeneratorUtilsTest {

  @Test
  void testValidStreamDescriptor() {
    final StreamDescriptor streamDescriptor1 = null;
    final StreamDescriptor streamDescriptor2 = new StreamDescriptor();
    final StreamDescriptor streamDescriptor3 = new StreamDescriptor().withName("name");
    final StreamDescriptor streamDescriptor4 = new StreamDescriptor().withNamespace("namespace");
    final StreamDescriptor streamDescriptor5 = new StreamDescriptor().withName("name").withNamespace("namespace");

    assertFalse(StateGeneratorUtils.isValidStreamDescriptor(streamDescriptor1));
    assertFalse(StateGeneratorUtils.isValidStreamDescriptor(streamDescriptor2));
    assertTrue(StateGeneratorUtils.isValidStreamDescriptor(streamDescriptor3));
    assertTrue(StateGeneratorUtils.isValidStreamDescriptor(streamDescriptor4));
    assertTrue(StateGeneratorUtils.isValidStreamDescriptor(streamDescriptor5));
  }

}
