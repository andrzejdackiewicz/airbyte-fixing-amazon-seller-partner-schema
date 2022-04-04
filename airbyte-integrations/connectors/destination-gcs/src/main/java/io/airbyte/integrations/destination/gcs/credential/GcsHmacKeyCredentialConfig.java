/*
 * Copyright (c) 2021 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.gcs.credential;

import com.fasterxml.jackson.databind.JsonNode;
import io.airbyte.integrations.destination.s3.credential.S3AccessKeyCredentialConfig;
import io.airbyte.integrations.destination.s3.credential.S3CredentialConfig;
import java.util.Optional;

public class GcsHmacKeyCredentialConfig implements GcsCredentialConfig {

  private final String hmacKeyAccessId;
  private final String hmacKeySecret;

  public GcsHmacKeyCredentialConfig(final JsonNode credentialConfig) {
    this.hmacKeyAccessId = credentialConfig.get("hmac_key_access_id").asText();
    this.hmacKeySecret = credentialConfig.get("hmac_key_secret").asText();
  }

  public String getHmacKeyAccessId() {
    return hmacKeyAccessId;
  }

  public String getHmacKeySecret() {
    return hmacKeySecret;
  }

  @Override
  public GcsCredentialType getCredentialType() {
    return GcsCredentialType.HMAC_KEY;
  }

  @Override
  public Optional<S3CredentialConfig> getS3CredentialConfig() {
    return Optional.of(new S3AccessKeyCredentialConfig(hmacKeyAccessId, hmacKeySecret));
  }

}
