
/*
 * MIT License
 *
 * Copyright (c) 2020 Airbyte
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package io.airbyte.integrations.destination.redshift;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.annotations.VisibleForTesting;
import io.airbyte.commons.json.Jsons;
import io.airbyte.db.Databases;
import io.airbyte.db.jdbc.JdbcDatabase;
import io.airbyte.integrations.base.DestinationConsumer;
import io.airbyte.integrations.base.FailureTrackingConsumer;
import io.airbyte.integrations.destination.StandardNameTransformer;
import io.airbyte.integrations.destination.jdbc.AbstractJdbcDestination;
import io.airbyte.protocol.models.AirbyteConnectionStatus;
import io.airbyte.protocol.models.AirbyteConnectionStatus.Status;
import io.airbyte.protocol.models.AirbyteMessage;
import io.airbyte.protocol.models.ConfiguredAirbyteCatalog;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A more efficient Redshift Destination than the sql-based {@link RedshiftDestination}. Instead of
 * inserting data as batched SQL INSERTs, we follow Redshift best practices and, 1) Stream the data
 * to S3. One compressed file is created per table. 2) Copy the S3 file to Redshift. See
 * https://docs.aws.amazon.com/redshift/latest/dg/c_best-practices-use-copy.html for more info.
 *
 * Although Redshift recommends splitting the file for more efficient copying, this introduces
 * complexity around file partitioning that should be handled by a file destination connector. The
 * single file approach is orders of magnitude faster than batch inserting and 'good-enough' for
 * now.
 */
public class RedshiftCopyDestination {

  // TODO: figure out a way to consistently randomise this bucket
  public static String DEFAULT_AIRBYTE_STAGING_S3_BUCKET = "airbyte.staging";
  private static final StandardNameTransformer namingResolver = new StandardNameTransformer();
  private static final Logger LOGGER = LoggerFactory.getLogger(RedshiftCopyDestination.class);

  public DestinationConsumer<AirbyteMessage> write(JsonNode config, ConfiguredAirbyteCatalog catalog) {
    return new RedshiftCopyDestinationConsumer(config, catalog);
  }

  public AirbyteConnectionStatus check(JsonNode config) {
    try {
      var outputTableName = "_airbyte_connection_test_" + UUID.randomUUID().toString().replaceAll("-", "");
      attemptWriteAndDeleteS3Object(config, outputTableName);

      var outputSchema = namingResolver.getIdentifier(config.get("schema").asText());
      JdbcDatabase database = getRedshift(config);
      AbstractJdbcDestination.attemptSQLCreateAndDropTableOperations(outputSchema, database, namingResolver, new RedshiftSqlOperations());

      return new AirbyteConnectionStatus().withStatus(Status.SUCCEEDED);
    } catch (Exception e) {
      LOGGER.debug("Exception while checking connection: ", e);
      return new AirbyteConnectionStatus()
          .withStatus(Status.FAILED)
          .withMessage("Could not connect with provided configuration. \n" + e.getMessage());
    }
  }

  private void attemptWriteAndDeleteS3Object(JsonNode config, String outputTableName) {
    var s3 = getAmazonS3(config);
    var s3Bucket = config.get("s3_staging_bucket").asText();
    s3.putObject(s3Bucket, outputTableName, "check-content");
    s3.deleteObject(s3Bucket, outputTableName);
  }

  private static AmazonS3 getAmazonS3(JsonNode config) {
    var s3Region = config.get("s3_region").asText();
    var accessKeyId = config.get("access_key_id").asText();
    var secretAccessKey = config.get("secret_access_key").asText();
    var awsCreds = new BasicAWSCredentials(accessKeyId, secretAccessKey);
    return AmazonS3ClientBuilder.standard()
        .withCredentials(new AWSStaticCredentialsProvider(awsCreds)).withRegion(s3Region)
        .build();
  }

  private static JdbcDatabase getRedshift(JsonNode config) {
    var jdbcConfig = RedshiftInsertDestination.getJdbcConfig(config);
    return Databases.createJdbcDatabase(
        jdbcConfig.get("username").asText(),
        jdbcConfig.has("password") ? jdbcConfig.get("password").asText() : null,
        jdbcConfig.get("jdbc_url").asText(),
        RedshiftInsertDestination.DRIVER_CLASS);
  }

  /**
   * Redshift urls are of the form <cluster-name>.<cluster-id>.<region>.redshift.amazon.com.
   * Extracting region from the url is currently the simplest way to figure out a cluster's region.
   * Although unlikely, might break if the url schema changes.
   */
  @VisibleForTesting
  static String extractRegionFromRedshiftUrl(String url) {
    // TODO: validate the url?
    var split = url.split("\\.");
    return split[2];
  }

  private static class RedshiftCopyDestinationConsumer extends FailureTrackingConsumer<AirbyteMessage> {

    private final JsonNode config;
    private final ConfiguredAirbyteCatalog catalog;
    private final JdbcDatabase redshiftDb;
    private final AmazonS3 s3Client;
    private final Map<String, RedshiftCopier> streamNameToCopier;

    public RedshiftCopyDestinationConsumer(JsonNode config, ConfiguredAirbyteCatalog catalog) {
      this.config = config;
      this.catalog = catalog;
      this.redshiftDb = getRedshift(config);
      this.s3Client = getAmazonS3(config);
      this.streamNameToCopier = new HashMap<>();
    }

    @Override
    protected void startTracked() throws Exception {
      var runFolder = UUID.randomUUID().toString();
      for (var stream : catalog.getStreams()) {
        var streamName = stream.getStream().getName();
        var syncMode = stream.getSyncMode();
        var schema = config.get("schema").asText();
        var s3Region = config.get("s3_region").asText();
        var accessKeyId = config.get("access_key_id").asText();
        var secretAccessKey = config.get("secret_access_key").asText();
        var copier = new RedshiftCopier(runFolder, syncMode, schema, streamName, s3Client, redshiftDb, accessKeyId, secretAccessKey, s3Region);

        streamNameToCopier.put(streamName, copier);
      }
    }

    @Override
    protected void acceptTracked(AirbyteMessage message) throws Exception {
      if (message.getType() == AirbyteMessage.Type.RECORD) {
        var streamName = message.getRecord().getStream();
        if (!streamNameToCopier.containsKey(streamName)) {
          throw new IllegalArgumentException(
              String.format("Message contained record from a stream that was not in the catalog. \ncatalog: %s , \nmessage: %s",
                  Jsons.serialize(catalog), Jsons.serialize(message)));
        }

        streamNameToCopier.get(streamName).uploadToS3(message.getRecord());
      }
    }

    /**
     * Although 'close' suggests a focus on clean up, this method also loads S3 files into Redshift.
     * First, move the files into temporary table, then merge the temporary tables with the final
     * destination tables. Lastly, do actual clean up and best-effort remove the S3 files and temporary
     * tables.
     */
    @Override
    protected void close(boolean hasFailed) throws Exception {
      RedshiftCopier.closeAsOneTransaction(new ArrayList<>(streamNameToCopier.values()), hasFailed, redshiftDb);
    }

  }

}
