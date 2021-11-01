/*
 * Copyright (c) 2021 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.oauth.flows;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import io.airbyte.commons.json.Jsons;
import io.airbyte.config.DestinationOAuthParameter;
import io.airbyte.config.SourceOAuthParameter;
import io.airbyte.config.persistence.ConfigNotFoundException;
import io.airbyte.config.persistence.ConfigRepository;
import io.airbyte.validation.json.JsonValidationException;
import java.io.IOException;
import java.net.http.HttpClient;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class SurveymonkeyOAuthFlowTest {

  private static final String REDIRECT_URL = "https://airbyte.io";

  private HttpClient httpClient;
  private ConfigRepository configRepository;
  private SurveymonkeyOAuthFlow surveymonkeyOAuthFlow;

  private UUID workspaceId;
  private UUID definitionId;

  @BeforeEach
  public void setup() throws JsonValidationException, IOException {
    httpClient = mock(HttpClient.class);
    configRepository = mock(ConfigRepository.class);
    surveymonkeyOAuthFlow = new SurveymonkeyOAuthFlow(configRepository, httpClient, SurveymonkeyOAuthFlowTest::getConstantState);

    workspaceId = UUID.randomUUID();
    definitionId = UUID.randomUUID();
    when(configRepository.listSourceOAuthParam()).thenReturn(List.of(new SourceOAuthParameter()
        .withOauthParameterId(UUID.randomUUID())
        .withSourceDefinitionId(definitionId)
        .withWorkspaceId(workspaceId)
        .withConfiguration(Jsons.jsonNode(ImmutableMap.builder()
            .put("client_id", "test_client_id")
            .put("client_secret", "test_client_secret")
            .build()))));
    when(configRepository.listDestinationOAuthParam()).thenReturn(List.of(new DestinationOAuthParameter()
        .withOauthParameterId(UUID.randomUUID())
        .withDestinationDefinitionId(definitionId)
        .withWorkspaceId(workspaceId)
        .withConfiguration(Jsons.jsonNode(ImmutableMap.builder()
            .put("client_id", "test_client_id")
            .put("client_secret", "test_client_secret")
            .build()))));
  }

  private static String getConstantState() {
    return "state";
  }

  @Test
  public void testGetSourceConcentUrl() throws IOException, InterruptedException, ConfigNotFoundException {
    final String concentUrl =
        surveymonkeyOAuthFlow.getSourceConsentUrl(workspaceId, definitionId, REDIRECT_URL);
    assertEquals(
        "https://api.surveymonkey.com/oauth/authorize?client_id=test_client_id&redirect_uri=https%3A%2F%2Fairbyte.io&response_type=code&state=state",
        concentUrl);
  }

  @Test
  public void testGetDestinationConcentUrl() throws IOException, InterruptedException, ConfigNotFoundException {
    final String concentUrl =
        surveymonkeyOAuthFlow.getDestinationConsentUrl(workspaceId, definitionId, REDIRECT_URL);
    assertEquals(
        "https://api.surveymonkey.com/oauth/authorize?client_id=test_client_id&redirect_uri=https%3A%2F%2Fairbyte.io&response_type=code&state=state",
        concentUrl);
  }

  @Test
  public void testCompleteSourceOAuth() throws IOException, InterruptedException, ConfigNotFoundException {

    final Map<String, String> returnedCredentials = Map.of("access_token", "access_token_response");
    final HttpResponse response = mock(HttpResponse.class);
    when(response.body()).thenReturn(Jsons.serialize(returnedCredentials));
    when(httpClient.send(any(), any())).thenReturn(response);
    final Map<String, Object> queryParams = Map.of("code", "test_code");
    final Map<String, Object> actualQueryParams =
        surveymonkeyOAuthFlow.completeSourceOAuth(workspaceId, definitionId, queryParams, REDIRECT_URL);

    assertEquals(Jsons.serialize(Map.of("credentials", returnedCredentials)), Jsons.serialize(actualQueryParams));
  }

  @Test
  public void testCompleteDestinationOAuth() throws IOException, ConfigNotFoundException, InterruptedException {

    final Map<String, String> returnedCredentials = Map.of("access_token", "access_token_response");
    final HttpResponse response = mock(HttpResponse.class);
    when(response.body()).thenReturn(Jsons.serialize(returnedCredentials));
    when(httpClient.send(any(), any())).thenReturn(response);
    final Map<String, Object> queryParams = Map.of("code", "test_code");
    final Map<String, Object> actualQueryParams =
        surveymonkeyOAuthFlow.completeDestinationOAuth(workspaceId, definitionId, queryParams, REDIRECT_URL);

    assertEquals(Jsons.serialize(Map.of("credentials", returnedCredentials)), Jsons.serialize(actualQueryParams));
  }

}
