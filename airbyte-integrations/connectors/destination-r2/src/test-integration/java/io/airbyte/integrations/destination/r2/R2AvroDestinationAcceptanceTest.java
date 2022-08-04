package io.airbyte.integrations.destination.r2;

import io.airbyte.integrations.destination.s3.S3BaseAvroDestinationAcceptanceTest;

public class R2AvroDestinationAcceptanceTest extends S3BaseAvroDestinationAcceptanceTest {

  @Override
  protected String getImageName() {
    return "airbyte/destination-r2:dev";
  }
}
