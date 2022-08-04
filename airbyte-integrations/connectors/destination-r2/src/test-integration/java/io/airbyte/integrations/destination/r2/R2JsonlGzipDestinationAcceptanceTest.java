package io.airbyte.integrations.destination.r2;

import io.airbyte.integrations.destination.s3.S3BaseJsonlGzipDestinationAcceptanceTest;

public class R2JsonlGzipDestinationAcceptanceTest extends S3BaseJsonlGzipDestinationAcceptanceTest {

  @Override
  protected String getImageName() {
    return "airbyte/destination-r2:dev";
  }
}
