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

package io.airbyte.workers.protocols.singer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.resources.MoreResources;
import io.airbyte.config.ConnectorSpecification;
import io.airbyte.config.JobGetSpecConfig;
import io.airbyte.config.StandardGetSpecOutput;
import io.airbyte.workers.JobStatus;
import io.airbyte.workers.OutputAndStatus;
import io.airbyte.workers.WorkerException;
import io.airbyte.workers.process.IntegrationLauncher;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SingerGetSpecWorkerTest {

  private static final String DUMMY_IMAGE_NAME = "airbyte/notarealimage:1.1";

  private SingerGetSpecWorker worker;
  private IntegrationLauncher integrationLauncher;
  private Process process;
  private Path jobRoot;
  private JobGetSpecConfig config;

  @BeforeEach
  public void setup() throws IOException, WorkerException {
    jobRoot = Files.createTempDirectory("");
    config = new JobGetSpecConfig().withDockerImage(DUMMY_IMAGE_NAME);
    integrationLauncher = mock(IntegrationLauncher.class, RETURNS_DEEP_STUBS);
    process = mock(Process.class);
    when(process.getErrorStream()).thenReturn(new ByteArrayInputStream(new byte[0]));
    when(integrationLauncher.spec(jobRoot).start()).thenReturn(process);

    worker = new SingerGetSpecWorker(integrationLauncher);
  }

  @Test
  public void testSuccessfulRun() throws WorkerException, IOException, InterruptedException {
    String expectedSpecString = MoreResources.readResource("valid_spec.json");
    when(process.getInputStream()).thenReturn(new ByteArrayInputStream(expectedSpecString.getBytes()));
    when(process.waitFor(anyLong(), any())).thenReturn(true);
    when(process.exitValue()).thenReturn(0);

    OutputAndStatus<StandardGetSpecOutput> actualOutput = worker.run(config, jobRoot);
    OutputAndStatus<StandardGetSpecOutput> expectedOutput =
        new OutputAndStatus<>(JobStatus.SUCCEEDED,
            new StandardGetSpecOutput().withSpecification(Jsons.deserialize(expectedSpecString, ConnectorSpecification.class)));

    assertEquals(expectedOutput, actualOutput);
  }

  @Test
  public void testFailureOnInvalidSpec() throws InterruptedException, WorkerException, IOException {
    String expectedSpecString = "{\"key\":\"value\"}";
    when(process.getInputStream()).thenReturn(new ByteArrayInputStream(expectedSpecString.getBytes()));
    when(process.waitFor(anyLong(), any())).thenReturn(true);
    when(process.exitValue()).thenReturn(0);

    OutputAndStatus<StandardGetSpecOutput> actualOutput = worker.run(config, jobRoot);
    OutputAndStatus<StandardGetSpecOutput> expectedOutput = new OutputAndStatus<>(JobStatus.FAILED);

    assertEquals(expectedOutput, actualOutput);
  }

  @Test
  public void testFailureOnNonzeroExitCode() throws InterruptedException, WorkerException, IOException {
    when(process.waitFor(anyLong(), any())).thenReturn(true);
    when(process.exitValue()).thenReturn(1);

    OutputAndStatus<StandardGetSpecOutput> actualOutput = worker.run(config, jobRoot);
    OutputAndStatus<StandardGetSpecOutput> expectedOutput = new OutputAndStatus<>(JobStatus.FAILED);

    assertEquals(expectedOutput, actualOutput);
  }

}
