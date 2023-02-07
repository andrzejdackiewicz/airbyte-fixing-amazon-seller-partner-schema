/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.scheduling.activities;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import io.airbyte.config.SlackNotificationConfiguration;
import io.airbyte.notification.SlackNotificationClient;
import java.io.IOException;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class NotifySchemaChangeActivityTest {

  static private SlackNotificationClient mNotificationClient;
  static private NotifySchemaChangeActivityImpl notifySchemaChangeActivity;

  @BeforeEach
  void setUp() {
    mNotificationClient = mock(SlackNotificationClient.class);
    notifySchemaChangeActivity = new NotifySchemaChangeActivityImpl();
  }

  @Test
  void testNotifySchemaChange() throws IOException, InterruptedException {
    UUID connectionId = UUID.randomUUID();
    String connectionUrl = "connection_url";
    boolean isBreaking = false;
    SlackNotificationConfiguration config = new SlackNotificationConfiguration();
    notifySchemaChangeActivity.notifySchemaChange(connectionId, isBreaking, config, connectionUrl);
    verify(mNotificationClient, times(1)).notifySchemaChange(connectionId, isBreaking, config, connectionUrl);
  }

}
