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

package io.airbyte.integrations.destination.gcs.writer;

import com.amazonaws.services.s3.AmazonS3;
import io.airbyte.integrations.destination.gcs.GCSDestinationConfig;
import io.airbyte.integrations.destination.gcs.GCSFormat;
import io.airbyte.integrations.destination.gcs.csv.GCSCsvWriter;
import io.airbyte.integrations.destination.gcs.parquet.JsonFieldNameUpdater;
import io.airbyte.integrations.destination.gcs.parquet.JsonToAvroSchemaConverter;
import io.airbyte.integrations.destination.gcs.parquet.GCSParquetWriter;
import io.airbyte.protocol.models.AirbyteStream;
import io.airbyte.protocol.models.ConfiguredAirbyteStream;
import java.sql.Timestamp;
import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProductionWriterFactory implements GCSWriterFactory {

  protected static final Logger LOGGER = LoggerFactory.getLogger(ProductionWriterFactory.class);

  @Override
  public GCSWriter create(GCSDestinationConfig config,
                         AmazonS3 s3Client,
                         ConfiguredAirbyteStream configuredStream,
                         Timestamp uploadTimestamp)
      throws Exception {
    GCSFormat format = config.getFormatConfig().getFormat();
    if (format == GCSFormat.CSV) {
      return new GCSCsvWriter(config, s3Client, configuredStream, uploadTimestamp);
    }
    if (format == GCSFormat.PARQUET) {
      AirbyteStream stream = configuredStream.getStream();

      JsonToAvroSchemaConverter schemaConverter = new JsonToAvroSchemaConverter();
      Schema avroSchema = schemaConverter.getAvroSchema(stream.getJsonSchema(), stream.getName(), stream.getNamespace(), true);
      JsonFieldNameUpdater nameUpdater = new JsonFieldNameUpdater(schemaConverter.getStandardizedNames());

      LOGGER.info("Avro schema for stream {}: {}", stream.getName(), avroSchema.toString(false));
      if (nameUpdater.hasNameUpdate()) {
        LOGGER.info("The following field names will be standardized: {}", nameUpdater);
      }

      return new GCSParquetWriter(config, s3Client, configuredStream, uploadTimestamp, avroSchema, nameUpdater);
    }

    throw new RuntimeException("Unexpected S3 destination format: " + format);
  }

}
