/*
 * Copyright (c) 2022 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.scheduling;

import io.airbyte.api.client.invoker.generated.ApiException;
import io.airbyte.commons.temporal.scheduling.ConnectionNotificationWorkflow;
import io.airbyte.config.Notification;
import io.airbyte.config.Notification.NotificationType;
import io.airbyte.config.SlackNotificationConfiguration;
import io.airbyte.notification.SlackNotificationClient;
import io.airbyte.workers.temporal.annotations.TemporalActivityStub;
import io.airbyte.workers.temporal.scheduling.activities.NotifySchemaChangeActivity;
import io.airbyte.workers.temporal.scheduling.activities.SlackConfigActivity;
import java.io.IOException;
import java.util.Optional;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ConnectionNotificationWorkflowImpl implements ConnectionNotificationWorkflow {

  @TemporalActivityStub(activityOptionsBeanName = "shortActivityOptions")
  private NotifySchemaChangeActivity notifySchemaChangeActivity;
  @TemporalActivityStub(activityOptionsBeanName = "shortActivityOptions")
  private SlackConfigActivity slackConfigActivity;

  @Override
  public boolean sendSchemaChangeNotification(UUID connectionId, boolean isBreaking) throws IOException, InterruptedException, ApiException {
    Optional<SlackNotificationConfiguration> slackConfig = slackConfigActivity.fetchSlackConfiguration(connectionId);
    if (slackConfig.isPresent()) {
      Notification notification = new Notification().withNotificationType(NotificationType.SLACK).withSendOnFailure(false).withSendOnSuccess(false)
          .withSlackConfiguration(slackConfig.get());
      SlackNotificationClient notificationClient = new SlackNotificationClient(notification);
      return notifySchemaChangeActivity.notifySchemaChange(notificationClient, connectionId, isBreaking);
    } else {
      return false;
    }
  }

}
