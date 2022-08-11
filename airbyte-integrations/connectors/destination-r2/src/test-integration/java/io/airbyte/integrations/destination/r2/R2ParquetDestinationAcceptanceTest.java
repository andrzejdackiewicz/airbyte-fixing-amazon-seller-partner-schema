package io.airbyte.integrations.destination.r2;

import io.airbyte.integrations.destination.s3.S3BaseParquetDestinationAcceptanceTest;
import io.airbyte.integrations.destination.s3.StorageProvider;

public class R2ParquetDestinationAcceptanceTest extends S3BaseParquetDestinationAcceptanceTest {

  @Override
  protected String getImageName() {
    return "airbyte/destination-r2:dev";
  }

  @Override
  public StorageProvider storageProvider() {
    return StorageProvider.CF_R2;
  }
}
