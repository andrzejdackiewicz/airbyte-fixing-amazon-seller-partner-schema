/*
 * Copyright (c) 2021 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.oauth.flows;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import io.airbyte.config.persistence.ConfigRepository;
import io.airbyte.oauth.BaseOAuthFlow;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.function.Supplier;
import org.apache.http.client.utils.URIBuilder;

/**
 * Following docs from
 * https://devdocs.drift.com/docs/authentication-and-scopes#1-direct-the-user-to-the-drift-oauth-url-
 */
public class DriftOAuthFlow extends BaseOAuthFlow {

  private static final String ACCESS_TOKEN_URL = "https://driftapi.com/oauth2/token";

  public DriftOAuthFlow(ConfigRepository configRepository) {
    super(configRepository);
  }

  @VisibleForTesting
  DriftOAuthFlow(ConfigRepository configRepository, HttpClient httpClient, Supplier<String> stateSupplier) {
    super(configRepository, httpClient, stateSupplier);
  }

  @Override
  protected String formatConsentUrl(UUID definitionId, String clientId, String redirectUrl) throws IOException {
    final URIBuilder builder = new URIBuilder()
        .setScheme("https")
        .setHost("dev.drift.com")
        .setPath("authorize")
        .addParameter("response_type", "code")
        .addParameter("client_id", clientId)
        .addParameter("redirect_uri", redirectUrl)
        .addParameter("state", getState());
    try {
      return builder.build().toString();
    } catch (URISyntaxException e) {
      throw new IOException("Failed to format Consent URL for OAuth flow", e);
    }
  }

  @Override
  protected String extractCodeParameter(Map<String, Object> queryParams) throws IOException {
    if (queryParams.containsKey("code")) {
      return (String) queryParams.get("code");
    } else {
      throw new IOException("Undefined 'code' from consent redirected url.");
    }
  }

  @Override
  protected String getClientIdUnsafe(JsonNode config) {
    // the config object containing client ID is nested inside the "credentials" object
    Preconditions.checkArgument(config.hasNonNull("credentials"));
    return super.getClientIdUnsafe(config.get("credentials"));
  }

  @Override
  protected String getClientSecretUnsafe(JsonNode config) {
    // the config object containing client SECRET is nested inside the "credentials" object
    Preconditions.checkArgument(config.hasNonNull("credentials"));
    return super.getClientSecretUnsafe(config.get("credentials"));
  }

  @Override
  protected String getAccessTokenUrl() {
    return ACCESS_TOKEN_URL;
  }

  @Override
  protected Map<String, String> getAccessTokenQueryParameters(String clientId, String clientSecret, String authCode, String redirectUrl) {
    return ImmutableMap.<String, String>builder()
        .put("client_id", clientId)
        .put("client_secret", clientSecret)
        .put("code", authCode)
        .put("grant_type", "authorization_code")
        .build();
  }

  protected Map<String, Object> extractRefreshToken(final JsonNode data, String accessTokenUrl) throws IOException {
    final Map<String, Object> result = new HashMap<>();
    if (data.has("access_token")) {
      result.put("access_token", data.get("access_token").asText());
    } else {
      throw new IOException(String.format("Missing 'access_token' in query params from %s", accessTokenUrl));
    }
    return Map.of("credentials", result);
  }

}
