/*
 * Copyright (c) 2021 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.s3;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import io.airbyte.integrations.destination.NamingConventionTransformer;
import java.util.List;
import java.util.regex.Pattern;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

public class S3StorageOperationsTest {

  private static final String BUCKET_NAME = "fake-bucket";
  private static final String FAKE_BUCKET_PATH = "fake-bucketPath";
  private static final String NAMESPACE = "namespace";
  private static final String STREAM_NAME = "stream_name1";
  private static final String OBJECT_TO_DELETE = NAMESPACE + "/" + STREAM_NAME + "/2022_04_04_123456789_0.csv.gz";
  private AmazonS3 s3Client;
  private S3StorageOperations s3StorageOperations;

  @BeforeEach
  public void setup() {
    final NamingConventionTransformer nameTransformer = mock(NamingConventionTransformer.class);
    s3Client = mock(AmazonS3.class);

    final S3ObjectSummary objectSummary1 = mock(S3ObjectSummary.class);
    final S3ObjectSummary objectSummary2 = mock(S3ObjectSummary.class);
    final S3ObjectSummary objectSummary3 = mock(S3ObjectSummary.class);
    when(objectSummary1.getKey()).thenReturn(OBJECT_TO_DELETE);
    when(objectSummary2.getKey()).thenReturn(NAMESPACE + "/stream_name2/2022_04_04_123456789_0.csv.gz");
    when(objectSummary3.getKey()).thenReturn("other_files.txt");

    final ObjectListing results = mock(ObjectListing.class);
    when(results.isTruncated()).thenReturn(false);
    when(results.getObjectSummaries()).thenReturn(List.of(objectSummary1, objectSummary2, objectSummary3));
    when(s3Client.listObjects(any(ListObjectsRequest.class))).thenReturn(results);

    final S3DestinationConfig s3Config = S3DestinationConfig.create(BUCKET_NAME, FAKE_BUCKET_PATH, "fake-region")
        .withEndpoint("fake-endpoint")
        .withAccessKeyCredential("fake-accessKeyId", "fake-secretAccessKey")
        .withS3Client(s3Client)
        .get();
    s3StorageOperations = new S3StorageOperations(nameTransformer, s3Client, s3Config);
  }

  @Test
  void testRegexMatch() {
    final String regexFormat = S3StorageOperations.getRegexFormat(NAMESPACE, STREAM_NAME, S3DestinationConstants.DEFAULT_PATH_FORMAT);
    assertTrue(Pattern.matches(regexFormat, OBJECT_TO_DELETE));
    assertFalse(Pattern.matches(regexFormat, NAMESPACE + "/" + STREAM_NAME + "/some_random_file_0.doc"));
    assertFalse(Pattern.matches(regexFormat, NAMESPACE + "/stream_name2/2022_04_04_123456789_0.csv.gz"));
  }

  @Test
  void testCleanUpBucketObject() {
    final String pathFormat = S3DestinationConstants.DEFAULT_PATH_FORMAT;
    s3StorageOperations.cleanUpBucketObject(NAMESPACE, STREAM_NAME, FAKE_BUCKET_PATH, pathFormat);
    final ArgumentCaptor<DeleteObjectsRequest> deleteRequest = ArgumentCaptor.forClass(DeleteObjectsRequest.class);
    verify(s3Client).deleteObjects(deleteRequest.capture());
    assertEquals(1, deleteRequest.getValue().getKeys().size());
    assertEquals(OBJECT_TO_DELETE, deleteRequest.getValue().getKeys().get(0).getKey());
  }

}
