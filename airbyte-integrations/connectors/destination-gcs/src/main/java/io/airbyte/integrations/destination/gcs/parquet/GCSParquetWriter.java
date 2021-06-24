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

package io.airbyte.integrations.destination.gcs.parquet;

import com.amazonaws.services.s3.AmazonS3;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.airbyte.integrations.base.JavaBaseConstants;
import io.airbyte.integrations.destination.gcs.GCSDestinationConfig;
import io.airbyte.integrations.destination.gcs.GCSFormat;
import io.airbyte.integrations.destination.gcs.writer.BaseGCSWriter;
import io.airbyte.integrations.destination.gcs.writer.GCSWriter;
import io.airbyte.protocol.models.AirbyteRecordMessage;
import io.airbyte.protocol.models.ConfiguredAirbyteStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.Timestamp;
import java.util.UUID;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Record;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.Constants;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.allegro.schema.json2avro.converter.JsonAvroConverter;

public class GCSParquetWriter extends BaseGCSWriter implements GCSWriter {

  private static final Logger LOGGER = LoggerFactory.getLogger(GCSParquetWriter.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final ObjectWriter WRITER = MAPPER.writer();

  private final Schema schema;
  private final JsonFieldNameUpdater nameUpdater;
  private final ParquetWriter<Record> parquetWriter;
  private final JsonAvroConverter converter = new JsonAvroConverter();

  public GCSParquetWriter(GCSDestinationConfig config,
                         AmazonS3 s3Client,
                         ConfiguredAirbyteStream configuredStream,
                         Timestamp uploadTimestamp,
                         Schema schema,
                         JsonFieldNameUpdater nameUpdater)
      throws URISyntaxException, IOException {
    super(config, s3Client, configuredStream);
    this.schema = schema;
    this.nameUpdater = nameUpdater;

    String outputFilename = BaseGCSWriter.getOutputFilename(uploadTimestamp, GCSFormat.PARQUET);
    String objectKey = String.join("/", outputPrefix, outputFilename);

    LOGGER.info("Full GCS path for stream '{}': {}/{}", stream.getName(), config.getBucketName(),
        objectKey);

    URI uri = new URI(
        String.format("s3a://%s/%s/%s", config.getBucketName(), outputPrefix, outputFilename));    // <----- CHECK
    Path path = new Path(uri);

    GCSParquetFormatConfig formatConfig = (GCSParquetFormatConfig) config.getFormatConfig();
    Configuration hadoopConfig = getHadoopConfig(config);
    this.parquetWriter = AvroParquetWriter.<GenericData.Record>builder(HadoopOutputFile.fromPath(path, hadoopConfig))
        .withSchema(schema)
        .withCompressionCodec(formatConfig.getCompressionCodec())
        .withRowGroupSize(formatConfig.getBlockSize())
        .withMaxPaddingSize(formatConfig.getMaxPaddingSize())
        .withPageSize(formatConfig.getPageSize())
        .withDictionaryPageSize(formatConfig.getDictionaryPageSize())
        .withDictionaryEncoding(formatConfig.isDictionaryEncoding())
        .build();
  }

  public static Configuration getHadoopConfig(GCSDestinationConfig config) {
    Configuration hadoopConfig = new Configuration();

    // hadoopConfig.set("fs.gs.project.id", config.getProjectID());
    // hadoopConfig.set("google.cloud.auth.service.account.email", "simphony-elt@data-247212.iam.gserviceaccount.com");
    // hadoopConfig.set("fs.gs.auth.service.account.private.key.id", config.getAccessKeyId());
    // hadoopConfig.set("fs.gs.auth.service.account.private.key", config.getSecretAccessKey());
    // hadoopConfig.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem"); 
    // hadoopConfig.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS");
    // hadoopConfig.set("google.cloud.auth.service.account.enable", "true");

    hadoopConfig.set("fs.s3a.access.key", config.getAccessKeyId());
    hadoopConfig.set("fs.s3a.secret.key", config.getSecretAccessKey());
    hadoopConfig.setBoolean("fs.s3a.path.style.access", true);
    hadoopConfig.set("fs.s3a.endpoint", "storage.googleapis.com");
    hadoopConfig.setInt("fs.s3a.list.version", 1);

    // hadoopConfig
    //     .set(Constants.ENDPOINT, String.format("s3.%s.amazonaws.com", config.getBucketRegion())); // <----- CHECK
    // hadoopConfig.set(Constants.AWS_CREDENTIALS_PROVIDER,
    //     "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider");
    return hadoopConfig;
  }

  @Override
  public void write(UUID id, AirbyteRecordMessage recordMessage) throws IOException {
    JsonNode inputData = recordMessage.getData();
    // LOGGER.info("InputData -> {}",inputData.toString());
    inputData = nameUpdater.getJsonWithStandardizedFieldNames(inputData);

    ObjectNode jsonRecord = MAPPER.createObjectNode();
    jsonRecord.put(JavaBaseConstants.COLUMN_NAME_AB_ID, UUID.randomUUID().toString());
    jsonRecord.put(JavaBaseConstants.COLUMN_NAME_EMITTED_AT, recordMessage.getEmittedAt());
    jsonRecord.setAll((ObjectNode) inputData);

    GenericData.Record avroRecord = converter.convertToGenericDataRecord(WRITER.writeValueAsBytes(jsonRecord), schema);
    parquetWriter.write(avroRecord);
  }

  @Override
  public void close(boolean hasFailed) throws IOException {
    if (hasFailed) {
      LOGGER.warn("Failure detected. Aborting upload of stream '{}'...", stream.getName());
      parquetWriter.close();
      LOGGER.warn("Upload of stream '{}' aborted.", stream.getName());
    } else {
      LOGGER.info("Uploading remaining data for stream '{}'.", stream.getName());
      parquetWriter.close();
      LOGGER.info("Upload completed for stream '{}'.", stream.getName());
    }
  }

}
