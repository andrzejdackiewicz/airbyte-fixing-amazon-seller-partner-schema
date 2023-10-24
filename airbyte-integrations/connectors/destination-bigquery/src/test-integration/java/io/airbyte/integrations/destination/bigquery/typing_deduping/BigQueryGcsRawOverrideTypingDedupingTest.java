/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.bigquery.typing_deduping;

public class BigQueryGcsRawOverrideTypingDedupingTest extends AbstractBigQueryTypingDedupingTest {

  @Override
  public String getConfigPath() {
    return "secrets/credentials-1s1t-gcs-raw-override.json";
  }

  @Override
  protected String getRawDataset() {
    return "overridden_raw_dataset";
  }

  @Override
  public void testRemovingPKNonNullIndexes() throws Exception {
    // Do nothing.
  }

  @Override
  public void identicalNameSimultaneousSync() throws Exception {
    // TODO: create fixtures to verify how raw tables are affected. Base tests check for final tables.
  }

}
