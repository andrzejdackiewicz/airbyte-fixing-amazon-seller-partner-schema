/*
 * Copyright (c) 2021 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.oauth.flows;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableMap;
import io.airbyte.config.persistence.ConfigNotFoundException;
import io.airbyte.config.persistence.ConfigRepository;
import io.airbyte.oauth.BaseOAuth2Flow;
import io.airbyte.protocol.models.OAuthConfigSpecification;
import io.airbyte.validation.json.JsonValidationException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.util.Map;
import java.util.UUID;
import java.util.function.Supplier;
import org.apache.http.client.utils.URIBuilder;

public class MicrosoftTeamsOAuthFlow extends BaseOAuth2Flow {

  private static final String fieldName = "tenant_id";
  private static String tenantId;

  public MicrosoftTeamsOAuthFlow(final ConfigRepository configRepository, final HttpClient httpClient) {
    super(configRepository, httpClient);
  }

  public MicrosoftTeamsOAuthFlow(final ConfigRepository configRepository, final HttpClient httpClient, final Supplier<String> stateSupplier) {
    super(configRepository, httpClient, stateSupplier, TOKEN_REQUEST_CONTENT_TYPE.JSON);
  }

  /**
   * Depending on the OAuth flow implementation, the URL to grant user's consent may differ,
   * especially in the query parameters to be provided. This function should generate such consent URL
   * accordingly.
   *
   * @param definitionId The configured definition ID of this client
   * @param clientId The configured client ID
   * @param redirectUrl the redirect URL
   */
  @Override
  protected String formatConsentUrl(final UUID definitionId,
                                    final String clientId,
                                    final String redirectUrl,
                                    final JsonNode inputOAuthConfiguration)
      throws IOException {

    try {
      tenantId = getConfigValueUnsafe(inputOAuthConfiguration, fieldName);
    } catch (final IllegalArgumentException e) {
      throw new IOException("Failed to format Consent URL for OAuth flow", e);
    }

    try {
      return new URIBuilder()
          .setScheme("https")
          .setHost("login.microsoftonline.com")
          .setPath(tenantId + "/oauth2/v2.0/authorize")
          .addParameter("client_id", clientId)
          .addParameter("redirect_uri", redirectUrl)
          .addParameter("state", getState())
          .addParameter("scope", getScopes())
          .addParameter("response_type", "code")
          .build().toString();
    } catch (final URISyntaxException e) {
      throw new IOException("Failed to format Consent URL for OAuth flow", e);
    }
  }

  @Override
  protected Map<String, String> getAccessTokenQueryParameters(final String clientId,
                                                              final String clientSecret,
                                                              final String authCode,
                                                              final String redirectUrl) {
    return ImmutableMap.<String, String>builder()
        // required
        .put("client_id", clientId)
        .put("redirect_uri", redirectUrl)
        .put("client_secret", clientSecret)
        .put("code", authCode)
        .put("grant_type", "authorization_code")
        .build();
  }

  private String getScopes() {
    return String.join(" ", "offline_access",
        "Application.Read.All",
        "Channel.ReadBasic.All",
        "ChannelMember.Read.All",
        "ChannelMember.ReadWrite.All",
        "ChannelSettings.Read.All",
        "ChannelSettings.ReadWrite.All",
        "Directory.Read.All",
        "Directory.ReadWrite.All",
        "Files.Read.All",
        "Files.ReadWrite.All",
        "Group.Read.All",
        "Group.ReadWrite.All",
        "GroupMember.Read.All",
        "Reports.Read.All",
        "Sites.Read.All",
        "Sites.ReadWrite.All",
        "TeamsTab.Read.All",
        "TeamsTab.ReadWrite.All",
        "User.Read.All",
        "User.ReadWrite.All");
  }

  @Override
  @Deprecated
  public Map<String, Object> completeSourceOAuth(final UUID workspaceId,
                                                 final UUID sourceDefinitionId,
                                                 final Map<String, Object> queryParams,
                                                 final String redirectUrl)
      throws IOException {
    throw new IOException("not supported");
  }

  @Override
  @Deprecated
  public Map<String, Object> completeDestinationOAuth(final UUID workspaceId,
                                                      final UUID destinationDefinitionId,
                                                      final Map<String, Object> queryParams,
                                                      final String redirectUrl)
      throws IOException {
    throw new IOException("not supported");
  }

  @Override
  public Map<String, Object> completeSourceOAuth(final UUID workspaceId,
                                                 final UUID sourceDefinitionId,
                                                 final Map<String, Object> queryParams,
                                                 final String redirectUrl,
                                                 final JsonNode inputOAuthConfiguration,
                                                 final OAuthConfigSpecification oAuthConfigSpecification)
      throws IOException, ConfigNotFoundException, JsonValidationException {
    tenantId = null;
    try {
      tenantId = getConfigValueUnsafe(inputOAuthConfiguration, fieldName);
    } catch (final IllegalArgumentException e) {}
    return super.completeSourceOAuth(workspaceId, sourceDefinitionId, queryParams, redirectUrl, inputOAuthConfiguration, oAuthConfigSpecification);
  }

  @Override
  public Map<String, Object> completeDestinationOAuth(final UUID workspaceId,
                                                      final UUID destinationDefinitionId,
                                                      final Map<String, Object> queryParams,
                                                      final String redirectUrl,
                                                      final JsonNode inputOAuthConfiguration,
                                                      final OAuthConfigSpecification oAuthConfigSpecification)
      throws IOException, ConfigNotFoundException, JsonValidationException {
    tenantId = null;
    try {
      tenantId = getConfigValueUnsafe(inputOAuthConfiguration, fieldName);
    } catch (final IllegalArgumentException e) {}
    return super.completeDestinationOAuth(workspaceId, destinationDefinitionId, queryParams, redirectUrl, inputOAuthConfiguration,
        oAuthConfigSpecification);
  }

  /**
   * Returns the URL where to retrieve the access token from.
   *
   */
  @Override
  protected String getAccessTokenUrl() {
    if (tenantId == null) {
      throw new IllegalArgumentException(String.format("Undefined parameter '%s' necessary for the OAuth Flow.", fieldName));
    }
    return "https://login.microsoftonline.com/" + tenantId + "/oauth2/v2.0/token";
  }

}
