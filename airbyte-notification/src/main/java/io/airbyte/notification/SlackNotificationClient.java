/*
 * MIT License
 *
 * Copyright (c) 2020 Airbyte
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

package io.airbyte.notification;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.map.MoreMaps;
import io.airbyte.commons.resources.MoreResources;
import io.airbyte.commons.yaml.Yamls;
import io.airbyte.config.SlackNotificationConfiguration;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.logging.log4j.util.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Notification client that uses Slack API for Incoming Webhook to send messages.
 *
 * This class also reads a resource YAML file that defines the template message to send.
 *
 * It is stored as a YAML so that we can easily change the structure of the JSON data expected by
 * the API that we are posting to (and we can write multi-line strings more easily).
 *
 * For example, slack API expects some text message in the { "text" : "Hello World" } field...
 *
 * It is also a template as the content of the message can be customized with basic variables that
 * are built in the metadata map. The templating is very basic for now: it is just looking for keys
 * from the metadata map surrounded by '<' '>' characters in the string
 */
public class SlackNotificationClient implements NotificationClient {

  private static final Logger LOGGER = LoggerFactory.getLogger(SlackNotificationClient.class);

  private final HttpClient httpClient = HttpClient.newBuilder()
      .version(HttpClient.Version.HTTP_2)
      .build();
  private final SlackNotificationConfiguration config;
  private final String connectionPageUrl;

  public SlackNotificationClient(final String connectionPageUrl, final SlackNotificationConfiguration config) {
    this.connectionPageUrl = connectionPageUrl;
    this.config = config;
  }

  @Override
  public boolean notifyJobFailure(final Map<String, Object> jobData) throws IOException, InterruptedException {
    if (config.getOnFailure()) {
      return notify(renderJobData(jobData, "failure_slack_notification_template.yml"));
    }
    return true;
  }

  private String renderJobData(final Map<String, Object> jobData, final String templateFile) throws IOException {
    final Map<String, Object> metadata = MoreMaps.merge(jobData, generateMetadata(jobData));
    final JsonNode template = Yamls.deserialize(MoreResources.readResource(templateFile));
    String data = Jsons.toPrettyString(template);
    for (Entry<String, Object> entry : metadata.entrySet()) {
      final String key = "<" + entry.getKey() + ">";
      if (data.contains(key)) {
        data = data.replaceAll(key, entry.getValue().toString());
      }
    }
    return data;
  }

  private Map<String, Object> generateMetadata(final Map<String, Object> jobData) {
    final Builder<String, Object> metadata = ImmutableMap.builder();
    metadata.put("log_url", connectionPageUrl + jobData.get("connection_id"));
    return metadata.build();
  }

  private boolean notify(final String data) throws IOException, InterruptedException {
    final String webhookUrl = config.getWebhook();
    if (!Strings.isEmpty(webhookUrl)) {
      final HttpRequest request = HttpRequest.newBuilder()
          .POST(HttpRequest.BodyPublishers.ofString(data))
          .uri(URI.create(webhookUrl))
          .header("Content-Type", "application/json")
          .build();
      final HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
      LOGGER.info("Successful notification ({}): {}", response.statusCode(), response.body());
      return isSuccessfulHttpResponse(response.statusCode());
    }
    return false;
  }

  /**
   * Use an integer division to check successful HTTP status codes (i.e., those from 200-299), not
   * just 200. https://www.w3.org/Protocols/rfc2616/rfc2616-sec10.html
   */
  private static boolean isSuccessfulHttpResponse(int httpStatusCode) {
    return httpStatusCode / 100 != 2;
  }

}
