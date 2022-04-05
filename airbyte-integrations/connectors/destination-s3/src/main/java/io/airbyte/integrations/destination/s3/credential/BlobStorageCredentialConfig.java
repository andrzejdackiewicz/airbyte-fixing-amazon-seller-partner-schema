/*
 * Copyright (c) 2021 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.s3.credential;

public interface BlobStorageCredentialConfig<CredentialType> {

  CredentialType getCredentialType();

}
