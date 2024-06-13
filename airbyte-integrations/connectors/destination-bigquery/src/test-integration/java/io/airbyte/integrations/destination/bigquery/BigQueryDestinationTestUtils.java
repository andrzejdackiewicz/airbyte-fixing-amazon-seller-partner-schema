/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.bigquery;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.DeleteObjectsRequest.KeyVersion;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.DatasetInfo;
import io.airbyte.commons.json.Jsons;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BigQueryDestinationTestUtils {

  private static final Logger LOGGER = LoggerFactory.getLogger(BigQueryDestinationTestUtils.class);

  /**
   * Parse the config file and replace dataset with rawNamespace and stagingPath randomly generated by
   * the test.
   *
   * @param configFile Path to the config file
   * @param datasetId Dataset id to use in the test. Should be randomized per test case.
   * @param stagingPath Staging GCS path to use in the test, or null if the test is running in
   *        standard inserts mode. Should be randomized per test case.
   */
  public static ObjectNode createConfig(Path configFile, String datasetId, String stagingPath) throws IOException {
    LOGGER.info("Setting default dataset to {}", datasetId);
    final String tmpConfigAsString = Files.readString(configFile);
    final ObjectNode config = (ObjectNode) Jsons.deserialize(tmpConfigAsString);
    config.put(BigQueryConsts.CONFIG_DATASET_ID, datasetId);

    // This is sort of a hack. Ideally tests shouldn't interfere with each other even when using the
    // same staging path.
    // Most likely there's a real bug in the connector - but we should investigate that and write a real
    // test case,
    // rather than relying on tests randomly failing to indicate that bug.
    // See https://github.com/airbytehq/airbyte/issues/28372.
    if (stagingPath != null && BigQueryUtils.getLoadingMethod(config) == UploadingMethod.GCS) {
      ObjectNode loadingMethodNode = (ObjectNode) config.get(BigQueryConsts.LOADING_METHOD);
      loadingMethodNode.put(BigQueryConsts.GCS_BUCKET_PATH, stagingPath);
    }
    return config;
  }

  /**
   * Get a handle for the BigQuery dataset instance used by the test. This dataset instance will be
   * used to verify results of test operations and for cleaning up after the test runs
   *
   * @param config
   * @param bigquery
   * @param datasetId
   * @return
   */
  public static Dataset initDataSet(JsonNode config, BigQuery bigquery, String datasetId) {
    final DatasetInfo datasetInfo = DatasetInfo.newBuilder(datasetId)
        .setLocation(config.get(BigQueryConsts.CONFIG_DATASET_LOCATION).asText()).build();
    try {
      return bigquery.create(datasetInfo);
    } catch (Exception ex) {
      if (ex.getMessage().indexOf("Already Exists") > -1) {
        return bigquery.getDataset(datasetId);
      }
    }
    return null;
  }

  /**
   * Initialized bigQuery instance that will be used for verifying results of test operations and for
   * cleaning up BigQuery dataset after the test
   *
   * @param config
   * @param projectId
   * @return
   * @throws IOException
   */
  public static BigQuery initBigQuery(JsonNode config, String projectId) throws IOException {
    final GoogleCredentials credentials = BigQueryDestination.getServiceAccountCredentials(config);
    return BigQueryOptions.newBuilder()
        .setProjectId(projectId)
        .setCredentials(credentials)
        .build()
        .getService();
  }

  /**
   * Deletes bigquery data set created during the test
   *
   * @param bigquery
   * @param dataset
   * @param LOGGER
   */
  public static void tearDownBigQuery(BigQuery bigquery, Dataset dataset, Logger LOGGER) {
    // allows deletion of a dataset that has contents
    final BigQuery.DatasetDeleteOption option = BigQuery.DatasetDeleteOption.deleteContents();
    if (bigquery == null || dataset == null) {
      return;
    }
    try {
      final boolean success = bigquery.delete(dataset.getDatasetId(), option);
      if (success) {
        LOGGER.info("BQ Dataset " + dataset + " deleted...");
      } else {
        LOGGER.info("BQ Dataset cleanup for " + dataset + " failed!");
      }
    } catch (Exception ex) {
      LOGGER.error("Failed to remove BigQuery resources after the test", ex);
    }
  }

  /**
   * Remove all the GCS output from the tests.
   */
  public static void tearDownGcs(AmazonS3 s3Client, JsonNode config, Logger LOGGER) {
    if (s3Client == null) {
      return;
    }
    if (BigQueryUtils.getLoadingMethod(config) != UploadingMethod.GCS) {
      return;
    }
    final JsonNode properties = config.get(BigQueryConsts.LOADING_METHOD);
    final String gcsBucketName = properties.get(BigQueryConsts.GCS_BUCKET_NAME).asText();
    final String gcs_bucket_path = properties.get(BigQueryConsts.GCS_BUCKET_PATH).asText();
    try {
      final List<KeyVersion> keysToDelete = new LinkedList<>();
      final List<S3ObjectSummary> objects = s3Client
          .listObjects(gcsBucketName, gcs_bucket_path)
          .getObjectSummaries();
      for (final S3ObjectSummary object : objects) {
        keysToDelete.add(new KeyVersion(object.getKey()));
      }

      if (keysToDelete.size() > 0) {
        LOGGER.info("Tearing down test bucket path: {}/{}", gcsBucketName, gcs_bucket_path);
        // Google Cloud Storage doesn't accept request to delete multiple objects
        for (final KeyVersion keyToDelete : keysToDelete) {
          s3Client.deleteObject(gcsBucketName, keyToDelete.getKey());
        }
        LOGGER.info("Deleted {} file(s).", keysToDelete.size());
      }
    } catch (Exception ex) {
      LOGGER.error("Failed to remove GCS resources after the test", ex);
    }
  }

}
