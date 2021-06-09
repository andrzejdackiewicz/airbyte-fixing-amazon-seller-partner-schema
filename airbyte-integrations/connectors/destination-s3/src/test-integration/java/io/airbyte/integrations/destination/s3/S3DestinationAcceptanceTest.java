package io.airbyte.integrations.destination.s3;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.DeleteObjectsRequest.KeyVersion;
import com.amazonaws.services.s3.model.DeleteObjectsResult;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.airbyte.commons.io.IOs;
import io.airbyte.commons.json.Jsons;
import io.airbyte.integrations.destination.s3.util.S3OutputPathHelper;
import io.airbyte.integrations.standardtest.destination.DestinationAcceptanceTest;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.lang3.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class S3DestinationAcceptanceTest extends DestinationAcceptanceTest {

  protected static final Logger LOGGER = LoggerFactory.getLogger(S3DestinationAcceptanceTest.class);
  protected static final ObjectMapper MAPPER = new ObjectMapper();

  protected final String secretFilePath;
  protected JsonNode configJson;
  protected S3DestinationConfig config;
  protected AmazonS3 s3Client;

  protected JsonNode getBaseConfigJson() {
    return Jsons.deserialize(IOs.readFile(Path.of(secretFilePath)));
  }

  protected S3DestinationAcceptanceTest(String secretFilePath) {
    this.secretFilePath = secretFilePath;
  }

  @Override
  protected String getImageName() {
    return "airbyte/destination-s3:dev";
  }

  @Override
  protected JsonNode getConfig() {
    return configJson;
  }

  @Override
  protected JsonNode getFailCheckConfig() {
    JsonNode baseJson = getBaseConfigJson();
    JsonNode failCheckJson = Jsons.clone(baseJson);
    // invalid credential
    ((ObjectNode) failCheckJson).put("access_key_id", "fake-key");
    ((ObjectNode) failCheckJson).put("secret_access_key", "fake-secret");
    return failCheckJson;
  }

  /**
   * Helper method to retrieve all synced objects inside the configured bucket path.
   */
  protected List<S3ObjectSummary> getAllSyncedObjects(String streamName, String namespace) {
    String outputPrefix = S3OutputPathHelper
        .getOutputPrefix(config.getBucketPath(), namespace, streamName);
    List<S3ObjectSummary> objectSummaries = s3Client
        .listObjects(config.getBucketName(), outputPrefix)
        .getObjectSummaries()
        .stream()
        .sorted(Comparator.comparingLong(o -> o.getLastModified().getTime()))
        .collect(Collectors.toList());
    LOGGER.info(
        "All objects: {}",
        objectSummaries.stream().map(o -> String.format("%s/%s", o.getBucketName(), o.getKey())).collect(Collectors.toList()));
    return objectSummaries;
  }

  @Override
  protected void setup(TestDestinationEnv testEnv) {
    JsonNode baseConfigJson = getBaseConfigJson();
    // Set a random s3 bucket path for each integration test
    JsonNode configJson = Jsons.clone(baseConfigJson);
    String testBucketPath = String.format(
        "%s_%s",
        configJson.get("s3_bucket_path").asText(),
        RandomStringUtils.randomAlphanumeric(5));
    ((ObjectNode) configJson).put("s3_bucket_path", testBucketPath);
    this.configJson = configJson;
    this.config = S3DestinationConfig.getS3DestinationConfig(configJson);
    LOGGER.info("Test full path: {}/{}", config.getBucketName(), config.getBucketPath());

    AWSCredentials awsCreds = new BasicAWSCredentials(config.getAccessKeyId(),
        config.getSecretAccessKey());
    this.s3Client = AmazonS3ClientBuilder.standard()
        .withCredentials(new AWSStaticCredentialsProvider(awsCreds))
        .withRegion(config.getBucketRegion())
        .build();
  }

  @Override
  protected void tearDown(TestDestinationEnv testEnv) {
    List<KeyVersion> keysToDelete = new LinkedList<>();
    List<S3ObjectSummary> objects = s3Client
        .listObjects(config.getBucketName(), config.getBucketPath())
        .getObjectSummaries();
    for (S3ObjectSummary object : objects) {
      keysToDelete.add(new KeyVersion(object.getKey()));
    }

    if (keysToDelete.size() > 0) {
      LOGGER.info("Tearing down test bucket path: {}/{}", config.getBucketName(),
          config.getBucketPath());
      DeleteObjectsResult result = s3Client
          .deleteObjects(new DeleteObjectsRequest(config.getBucketName()).withKeys(keysToDelete));
      LOGGER.info("Deleted {} file(s).", result.getDeletedObjects().size());
    }
  }

}
