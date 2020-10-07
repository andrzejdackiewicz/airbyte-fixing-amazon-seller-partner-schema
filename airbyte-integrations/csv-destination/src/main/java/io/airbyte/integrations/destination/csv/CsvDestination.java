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

package io.airbyte.integrations.destination.csv;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Preconditions;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.resources.MoreResources;
import io.airbyte.config.DestinationConnectionSpecification;
import io.airbyte.config.Schema;
import io.airbyte.config.StandardCheckConnectionOutput;
import io.airbyte.config.StandardCheckConnectionOutput.Status;
import io.airbyte.config.StandardDiscoverSchemaOutput;
import io.airbyte.config.Stream;
import io.airbyte.integrations.base.Destination;
import io.airbyte.integrations.base.DestinationConsumer;
import io.airbyte.integrations.base.FailureTrackingConsumer;
import io.airbyte.integrations.base.IntegrationRunner;
import io.airbyte.integrations.base.JavaBaseConstants;
import io.airbyte.singer.SingerMessage;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CsvDestination implements Destination {

  private static final String COLUMN_NAME = "data";

  private static final Logger LOGGER = LoggerFactory.getLogger(CsvDestination.class);

  private static final String DESTINATION_PATH_FIELD = "destination_path";

  @Override
  public DestinationConnectionSpecification spec() throws IOException {
    final String resourceString = MoreResources.readResource("spec.json");
    return Jsons.deserialize(resourceString, DestinationConnectionSpecification.class);
  }

  @Override
  public StandardCheckConnectionOutput check(JsonNode config) {
    try {
      FileUtils.forceMkdir(getDestinationPath(config).toFile());
    } catch (IOException e) {
      return new StandardCheckConnectionOutput().withStatus(Status.FAILURE).withMessage(e.getMessage());
    }
    return new StandardCheckConnectionOutput().withStatus(Status.SUCCESS);
  }

  @Override
  public StandardDiscoverSchemaOutput discover(JsonNode config) {
    throw new RuntimeException("Not Implemented");
  }

  @Override
  public DestinationConsumer<SingerMessage> write(JsonNode config, Schema schema) throws IOException {
    final Path destinationDir = getDestinationPath(config);

    FileUtils.forceMkdir(destinationDir.toFile());

    final long now = Instant.now().toEpochMilli();
    final Map<String, WriteConfig> writeConfigs = new HashMap<>();
    for (final Stream stream : schema.getStreams()) {
      final Path tmpPath = destinationDir.resolve(stream.getName() + "_" + now + ".csv");
      final Path finalPath = destinationDir.resolve(stream.getName() + ".csv");
      final FileWriter fileWriter = new FileWriter(tmpPath.toFile());
      final CSVPrinter printer = new CSVPrinter(fileWriter, CSVFormat.DEFAULT.withHeader(COLUMN_NAME));
      writeConfigs.put(stream.getName(), new WriteConfig(printer, tmpPath, finalPath));
    }

    return new CsvConsumer(writeConfigs, schema);
  }

  /**
   * Extract provided relative path from csv config object and append to local mount path.
   *
   * @param config - csv config object
   * @return absolute path with the relative path appended to the local volume mount.
   */
  private Path getDestinationPath(JsonNode config) {
    final String destinationRelativePath = config.get(DESTINATION_PATH_FIELD).asText();
    Preconditions.checkNotNull(destinationRelativePath);

    // append destination path to the local mount.
    return JavaBaseConstants.LOCAL_MOUNT.resolve(destinationRelativePath);
  }

  public static class WriteConfig {

    private final CSVPrinter writer;
    private final Path tmpPath;
    private final Path finalPath;

    public WriteConfig(CSVPrinter writer, Path tmpPath, Path finalPath) {
      this.writer = writer;
      this.tmpPath = tmpPath;
      this.finalPath = finalPath;
    }

    public CSVPrinter getWriter() {
      return writer;
    }

    public Path getTmpPath() {
      return tmpPath;
    }

    public Path getFinalPath() {
      return finalPath;
    }

  }

  public static class CsvConsumer extends FailureTrackingConsumer<SingerMessage> {

    private final Map<String, WriteConfig> writeConfigs;
    private final Schema schema;

    public CsvConsumer(Map<String, WriteConfig> writeConfigs, Schema schema) {
      this.schema = schema;
      LOGGER.info("initializing consumer.");

      this.writeConfigs = writeConfigs;
    }

    @Override
    protected void acceptInternal(SingerMessage singerMessage) throws Exception {
      if (writeConfigs.containsKey(singerMessage.getStream())) {
        writeConfigs.get(singerMessage.getStream()).getWriter().printRecord(Jsons.serialize(singerMessage.getRecord()));
      } else {
        throw new IllegalArgumentException(
            String.format("Message contained record from a stream that was not in the catalog. \ncatalog: %s , \nmessage: %s",
                Jsons.serialize(schema), Jsons.serialize(singerMessage)));
      }
    }

    @Override
    protected void close(boolean hasFailed) throws IOException {
      LOGGER.info("finalizing consumer.");

      for (final Map.Entry<String, WriteConfig> entries : writeConfigs.entrySet()) {
        try {
          entries.getValue().getWriter().flush();
          entries.getValue().getWriter().close();
        } catch (Exception e) {
          hasFailed = true;
          LOGGER.error("failed to close writer for: {}.", entries.getKey());
        }
      }
      if (!hasFailed) {
        for (final WriteConfig writeConfig : writeConfigs.values()) {
          Files.move(writeConfig.getTmpPath(), writeConfig.getFinalPath(), StandardCopyOption.REPLACE_EXISTING);
        }
      }
    }

  }

  public static void main(String[] args) throws Exception {
    new IntegrationRunner(new CsvDestination()).run(args);
  }

}
