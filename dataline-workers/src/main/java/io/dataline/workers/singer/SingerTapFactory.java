/*
 * MIT License
 *
 * Copyright (c) 2020 Dataline
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

package io.dataline.workers.singer;

import io.dataline.commons.json.Jsons;
import io.dataline.config.SingerCatalog;
import io.dataline.config.SingerMessage;
import io.dataline.config.StandardDiscoverSchemaInput;
import io.dataline.config.StandardTapConfig;
import io.dataline.workers.DefaultSyncWorker;
import io.dataline.workers.InvalidCredentialsException;
import io.dataline.workers.OutputAndStatus;
import io.dataline.workers.TapFactory;
import io.dataline.workers.WorkerUtils;
import io.dataline.workers.process.ProcessBuilderFactory;
import io.dataline.workers.protocol.singer.SingerJsonStreamFactory;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SingerTapFactory implements TapFactory<SingerMessage> {

  private static final Logger LOGGER = LoggerFactory.getLogger(SingerTapFactory.class);

  private static final String CONFIG_JSON_FILENAME = "tap_config.json";
  private static final String CATALOG_JSON_FILENAME = "catalog.json";
  private static final String STATE_JSON_FILENAME = "input_state.json";

  private final String imageName;
  private final ProcessBuilderFactory pbf;

  private Process tapProcess = null;
  private BufferedReader bufferedReader = null;

  public SingerTapFactory(final String imageName, final ProcessBuilderFactory pbf) {
    this.imageName = imageName;
    this.pbf = pbf;
  }

  @Override
  public Stream<SingerMessage> create(StandardTapConfig input, Path jobRoot)
      throws InvalidCredentialsException {
    OutputAndStatus<SingerCatalog> discoveryOutput = runDiscovery(input, jobRoot);

    final String configDotJson = input.getSourceConnectionImplementation().getConfigurationJson();

    final SingerCatalog selectedCatalog =
        SingerCatalogConverters.applySchemaToDiscoveredCatalog(
            discoveryOutput.getOutput().get(), input.getStandardSync().getSchema());
    final String catalogDotJson = Jsons.serialize(selectedCatalog);
    final String stateDotJson = Jsons.serialize(input.getState());

    WorkerUtils.writeFileToWorkspace(jobRoot, CONFIG_JSON_FILENAME, configDotJson);
    WorkerUtils.writeFileToWorkspace(jobRoot, CATALOG_JSON_FILENAME, catalogDotJson);
    WorkerUtils.writeFileToWorkspace(jobRoot, STATE_JSON_FILENAME, stateDotJson);

    try {
      tapProcess =
          pbf.create(
              jobRoot,
              imageName,
              "--config",
              CONFIG_JSON_FILENAME,
              // TODO support both --properties and --catalog depending on integration
              "--properties",
              CATALOG_JSON_FILENAME,
              "--state",
              STATE_JSON_FILENAME)
              .redirectError(jobRoot.resolve(DefaultSyncWorker.TAP_ERR_LOG).toFile())
              .start();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    bufferedReader = new BufferedReader(new InputStreamReader(tapProcess.getInputStream()));

    return new SingerJsonStreamFactory().create(bufferedReader).onClose(getCloseFunction());
  }

  public Runnable getCloseFunction() {
    return () -> {
      if (bufferedReader != null) {
        try {
          bufferedReader.close();
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }

      WorkerUtils.cancelProcess(tapProcess);
    };
  }

  private OutputAndStatus<SingerCatalog> runDiscovery(StandardTapConfig input, Path workspaceRoot)
      throws InvalidCredentialsException {
    StandardDiscoverSchemaInput discoveryInput = new StandardDiscoverSchemaInput();
    discoveryInput.setConnectionConfigurationJson(
        input.getSourceConnectionImplementation().getConfigurationJson());
    Path scopedWorkspace = workspaceRoot.resolve("discovery");
    return new SingerDiscoverSchemaWorker(imageName, pbf)
        .runInternal(discoveryInput, scopedWorkspace);
  }

}
