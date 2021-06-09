package io.airbyte.integrations.destination.s3.parquet;

import static io.airbyte.integrations.destination.s3.S3DestinationConstants.YYYY_MM_DD_FORMAT;

import com.amazonaws.services.s3.AmazonS3;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.airbyte.integrations.base.JavaBaseConstants;
import io.airbyte.integrations.destination.s3.S3DestinationConfig;
import io.airbyte.integrations.destination.s3.writer.BaseS3Writer;
import io.airbyte.integrations.destination.s3.writer.S3Writer;
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

public class S3ParquetWriter extends BaseS3Writer implements S3Writer {

  private static final Logger LOGGER = LoggerFactory.getLogger(S3ParquetWriter.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final ObjectWriter WRITER = MAPPER.writer();

  private final Schema schema;
  private final ParquetWriter<Record> parquetWriter;
  private final JsonAvroConverter converter = new JsonAvroConverter();

  public S3ParquetWriter(S3DestinationConfig config,
                         AmazonS3 s3Client,
                         ConfiguredAirbyteStream configuredStream,
                         Timestamp uploadTimestamp,
                         Schema schema) throws URISyntaxException, IOException {
    super(config, s3Client, configuredStream);
    this.schema = schema;

    String outputFilename = getOutputFilename(uploadTimestamp);
    String objectKey = String.join("/", outputPrefix, outputFilename);

    LOGGER.info("Full S3 path for stream '{}': {}/{}", stream.getName(), config.getBucketName(),
        objectKey);

    URI uri = new URI(
        String.format("s3a://%s/%s/%s", config.getBucketName(), outputPrefix, outputFilename));
    Path path = new Path(uri);

    Configuration hadoopConfig = getHadoopConfig(config);
    this.parquetWriter = AvroParquetWriter
        .<GenericData.Record>builder(HadoopOutputFile.fromPath(path, hadoopConfig))
        .withSchema(schema)
        .build();
  }

  public static Configuration getHadoopConfig(S3DestinationConfig config) {
    Configuration hadoopConfig = new Configuration();
    hadoopConfig.set(Constants.ACCESS_KEY, config.getAccessKeyId());
    hadoopConfig.set(Constants.SECRET_KEY, config.getSecretAccessKey());
    hadoopConfig
        .set(Constants.ENDPOINT, String.format("s3.%s.amazonaws.com", config.getBucketRegion()));
    hadoopConfig.set(Constants.AWS_CREDENTIALS_PROVIDER,
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider");
    return hadoopConfig;
  }

  static String getOutputFilename(Timestamp timestamp) {
    return String
        .format("%s_%d_0.parquet", YYYY_MM_DD_FORMAT.format(timestamp), timestamp.getTime());
  }

  @Override
  public void write(UUID id, AirbyteRecordMessage recordMessage) throws IOException {
    ObjectNode jsonRecord = MAPPER.createObjectNode();
    jsonRecord.put(JavaBaseConstants.COLUMN_NAME_AB_ID, UUID.randomUUID().toString());
    jsonRecord.put(JavaBaseConstants.COLUMN_NAME_EMITTED_AT, recordMessage.getEmittedAt());
    jsonRecord.setAll((ObjectNode) recordMessage.getData());

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
