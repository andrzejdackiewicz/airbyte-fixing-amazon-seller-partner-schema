/*
 * Copyright (c) 2022 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.oauth.flows;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableMap;
import io.airbyte.config.persistence.ConfigRepository;
import io.airbyte.oauth.BaseOAuth2Flow;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.util.Map;
import java.util.UUID;
import java.util.function.Supplier;
import org.apache.http.client.utils.URIBuilder;

public class AmazonAdsOAuthFlow extends BaseOAuth2Flow {

  enum RegionHost {

    /**
     * North America (NA) —— United States (US), Canada (CA), Mexico (MX), Brazil (BR)
     */
    NA("https://www.amazon.com/ap/oa","https://api.amazon.com/auth/o2/token"),

    /**
     * Europe (EU) —— United Kingdom (UK), France (FR), Italy (IT), Spain (ES), Germany (DE), Netherlands (NL), United Arab Emirates (AE), Poland (PL), Turkey (TR), Egypt (EG), Saudi Arabia (SA), Sweden (SE), Belgium (BE), India (IN)
     */
    EU("https://eu.account.amazon.com/ap/oa","https://api.amazon.co.uk/auth/o2/token"),

    /**
     * Far East (FE) —— Japan (JP), Australia (AU), Singapore (SG)
     */
    FE("https://apac.account.amazon.com/ap/oa","https://api.amazon.co.jp/auth/o2/token"),
    ;
    private final String host;
    private final String tokenUrl;


    RegionHost(String host,String tokenUrl) {
      this.host = host;
      this.tokenUrl = tokenUrl;
    }

    public String getHost(){
      return host;
    }

    public String getTokenUrl(){
      return tokenUrl;
    }

  }


  private static final String AUTHORIZE_URL = "https://www.amazon.com/ap/oa";
  private static final String ACCESS_TOKEN_URL = "https://api.amazon.com/auth/o2/token";

  public AmazonAdsOAuthFlow(final ConfigRepository configRepository, final HttpClient httpClient) {
    super(configRepository, httpClient);
  }

  public AmazonAdsOAuthFlow(final ConfigRepository configRepository, final HttpClient httpClient, final Supplier<String> stateSupplier) {
    super(configRepository, httpClient, stateSupplier);
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

    final String regionCountry = getConfigValueUnsafe(inputOAuthConfiguration, "region");
    String authUrl = RegionHost.valueOf(regionCountry).getHost();


    try {
      return new URIBuilder(authUrl)
          .addParameter("client_id", clientId)
          .addParameter("scope", "advertising::campaign_management")
          .addParameter("response_type", "code")
          .addParameter("redirect_uri", redirectUrl)
          .addParameter("state", getState())
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

  /**
   * Returns the URL where to retrieve the access token from.
   *
   */
  @Override
  protected String getAccessTokenUrl(final JsonNode inputOAuthConfiguration) {

    final String regionCountry = getConfigValueUnsafe(inputOAuthConfiguration, "region");

    return RegionHost.valueOf(regionCountry).getTokenUrl();
  }

}
