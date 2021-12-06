/*
 * Copyright (c) 2021 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.helpers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.airbyte.config.EnvConfigs;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

@Tag("logger-client")
public class S3LogsTest {

  private static final LogConfigs logConfigs = (new EnvConfigs()).getLogConfigs();

  /**
   * The test files here were generated by {@link #generatePaginateTestFiles()}.
   *
   * Generate enough files to force pagination and confirm all data is read.
   */
  @Test
  public void testRetrieveAllLogs() throws IOException {
    final var s3 = S3Client.builder().region(Region.of("us-west-2")).build();
    final var data = S3Logs.getFile(s3, logConfigs, "paginate", 6);

    final var retrieved = new ArrayList<String>();
    Files.lines(data.toPath()).forEach(retrieved::add);

    final var expected = List.of("Line 0", "Line 1", "Line 2", "Line 3", "Line 4", "Line 5", "Line 6", "Line 7", "Line 8");

    assertEquals(expected, retrieved);
  }

  /**
   * The test files for this test have been pre-generated and uploaded into the bucket folder. The
   * folder contains the following files with these contents:
   * <li>first-file.txt - Line 1, Line 2, Line 3</li>
   * <li>second-file.txt - Line 4, Line 5, Line 6</li>
   * <li>third-file.txt - Line 7, Line 8, Line 9</li>
   */
  @Test
  public void testTail() throws IOException {
    final var s3 = S3Client.builder().region(Region.of("us-west-2")).build();
    final var data = new S3Logs(() -> s3).tailCloudLog(logConfigs, "tail", 6);
    final var expected = List.of("Line 4", "Line 5", "Line 6", "Line 7", "Line 8", "Line 9");
    assertEquals(data, expected);
  }

  public static void main(final String[] args) {
    generatePaginateTestFiles();
  }

  private static void generatePaginateTestFiles() {
    final var s3 = S3Client.builder().region(Region.of("us-west-2")).build();

    for (int i = 0; i < 9; i++) {
      final var fileName = i + "-file";
      final var line = "Line " + i + "\n";
      final PutObjectRequest objectRequest = PutObjectRequest.builder()
          .bucket("airbyte-kube-integration-logging-test")
          .key("paginate/" + fileName)
          .build();

      s3.putObject(objectRequest, RequestBody.fromBytes(line.getBytes(StandardCharsets.UTF_8)));
    }
  }

}
