/*
 * MIT License
 *
 * Copyright (c) 2020 Dataline
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package io.dataline.analytics;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.dataline.config.Configs;
import io.dataline.config.StandardWorkspace;
import io.dataline.config.persistence.ConfigNotFoundException;
import io.dataline.config.persistence.ConfigRepository;
import io.dataline.commons.json.JsonValidationException;
import io.dataline.config.persistence.PersistenceConstants;
import java.io.IOException;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TrackingClientSingletonTest {

  private ConfigRepository configRepository;

  @BeforeEach
  void setup() {
    configRepository = mock(ConfigRepository.class);
    // equivalent of resetting TrackingClientSingleton to uninitialized state.
    TrackingClientSingleton.initialize(null);
  }

  @Test
  void testCreateTrackingClientLogging() {
    assertTrue(
        TrackingClientSingleton.createTrackingClient(Configs.TrackingStrategy.LOGGING, TrackingIdentity::empty) instanceof LoggingTrackingClient);
  }

  @Test
  void testCreateTrackingClientSegment() {
    assertTrue(
        TrackingClientSingleton.createTrackingClient(Configs.TrackingStrategy.SEGMENT, TrackingIdentity::empty) instanceof SegmentTrackingClient);
  }

  @Test
  void testGet() {
    TrackingClient client = mock(TrackingClient.class);
    TrackingClientSingleton.initialize(client);
    assertEquals(client, TrackingClientSingleton.get());
  }

  @Test
  void testGetUninitialized() {
    assertTrue(TrackingClientSingleton.get() instanceof LoggingTrackingClient);
  }

  @Test
  void testGetTrackingIdentityInitialSetupNotComplete() throws JsonValidationException, IOException, ConfigNotFoundException {
    final StandardWorkspace workspace = new StandardWorkspace().withCustomerId(UUID.randomUUID());

    when(configRepository.getStandardWorkspace(PersistenceConstants.DEFAULT_WORKSPACE_ID)).thenReturn(workspace);

    final TrackingIdentity actual = TrackingClientSingleton.getTrackingIdentity(configRepository);
    final TrackingIdentity expected = new TrackingIdentity(workspace.getCustomerId(), null, null, null, null);

    assertEquals(expected, actual);
  }

  @Test
  void testGetTrackingIdentityNonAnonymous() throws JsonValidationException, IOException, ConfigNotFoundException {
    final StandardWorkspace workspace = new StandardWorkspace()
        .withCustomerId(UUID.randomUUID())
        .withEmail("a@dataline.io")
        .withAnonymousDataCollection(false)
        .withNews(true)
        .withSecurityUpdates(true);

    when(configRepository.getStandardWorkspace(PersistenceConstants.DEFAULT_WORKSPACE_ID)).thenReturn(workspace);

    final TrackingIdentity actual = TrackingClientSingleton.getTrackingIdentity(configRepository);
    final TrackingIdentity expected = new TrackingIdentity(workspace.getCustomerId(), workspace.getEmail(), false, true, true);

    assertEquals(expected, actual);
  }

  @Test
  void testGetTrackingIdentityAnonymous() throws JsonValidationException, IOException, ConfigNotFoundException {
    final StandardWorkspace workspace = new StandardWorkspace()
        .withCustomerId(UUID.randomUUID())
        .withEmail("a@dataline.io")
        .withAnonymousDataCollection(true)
        .withNews(true)
        .withSecurityUpdates(true);

    when(configRepository.getStandardWorkspace(PersistenceConstants.DEFAULT_WORKSPACE_ID)).thenReturn(workspace);

    final TrackingIdentity actual = TrackingClientSingleton.getTrackingIdentity(configRepository);
    final TrackingIdentity expected = new TrackingIdentity(workspace.getCustomerId(), null, true, true, true);

    assertEquals(expected, actual);
  }

}
