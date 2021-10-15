/*
 * Copyright (c) 2021 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.process;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import com.google.common.collect.ImmutableMap;
import io.airbyte.commons.lang.Exceptions;
import io.airbyte.workers.WorkerException;
import io.airbyte.workers.WorkerUtils;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.util.Config;
import java.io.IOException;
import java.net.Inet4Address;
import java.net.ServerSocket;
import java.net.UnknownHostException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang.RandomStringUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// requires kube running locally to run. If using Minikube it requires MINIKUBE=true
// Must have a timeout on this class because it tests child processes that may misbehave; otherwise this can hang forever during failures.
@Timeout(value = 5, unit = TimeUnit.MINUTES)
public class KubePodProcessIntegrationTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(KubePodProcessIntegrationTest.class);

  private static final boolean IS_MINIKUBE = Boolean.parseBoolean(Optional.ofNullable(System.getenv("IS_MINIKUBE")).orElse("false"));
  private static List<Integer> openPorts;
  private static int heartbeatPort;
  private static String heartbeatUrl;
  private static ApiClient officialClient;
  private static KubernetesClient fabricClient;
  private static KubeProcessFactory processFactory;

  private WorkerHeartbeatServer server;

  @BeforeAll
  public static void init() throws Exception {
    openPorts = new ArrayList<>(getOpenPorts(5));

    heartbeatPort = openPorts.get(0);
    heartbeatUrl = getHost() + ":" + heartbeatPort;

    officialClient = Config.defaultClient();
    fabricClient = new DefaultKubernetesClient();

    processFactory = new KubeProcessFactory("default", officialClient, fabricClient, heartbeatUrl, getHost(),
        new HashSet<>(openPorts.subList(1, openPorts.size() - 1)));
  }

  @BeforeEach
  public void setup() throws Exception {
    server = new WorkerHeartbeatServer(heartbeatPort);
    server.startBackground();
  }

  @AfterEach
  public void teardown() throws Exception {
    server.stop();
  }

  @Test
  public void testSuccessfulSpawning() throws Exception {
    // start a finite process
    var availablePortsBefore = KubePortManagerSingleton.getInstance().getNumAvailablePorts();
    final Process process = getProcess("echo hi; sleep 1; echo hi2");
    LOGGER.info("Got process with simple echo commands: " + process.info());
    process.waitFor();

    // the pod should be dead and in a good state
    LOGGER.info("After waitFor on process with simple echo commands: " + process.info());
    assertFalse(process.isAlive());
    assertEquals(availablePortsBefore, KubePortManagerSingleton.getInstance().getNumAvailablePorts());
    assertEquals(0, process.exitValue());
  }

  @Test
  public void testSuccessfulSpawningWithQuotes() throws Exception {
    // start a finite process
    var availablePortsBefore = KubePortManagerSingleton.getInstance().getNumAvailablePorts();
    final Process process = getProcess("echo \"h\\\"i\"; sleep 1; echo hi2");
    LOGGER.info("Got process with simple echo commands, testing output: " + process.info());
    var output = new String(process.getInputStream().readAllBytes());
    assertEquals("h\"i\nhi2\n", output);
    process.waitFor();

    // the pod should be dead and in a good state
    LOGGER.info("After waitFor on process with simple echo commands, testing output: " + process.info());
    assertFalse(process.isAlive());
    assertEquals(availablePortsBefore, KubePortManagerSingleton.getInstance().getNumAvailablePorts());
    assertEquals(0, process.exitValue());
  }

  @Test
  public void testPipeInEntrypoint() throws Exception {
    // start a process that has a pipe in the entrypoint
    var availablePortsBefore = KubePortManagerSingleton.getInstance().getNumAvailablePorts();
    final Process process = getProcess("echo hi | cat");
    LOGGER.info("Got process with simple pipe: " + process.info());
    process.waitFor();

    // the pod should be dead and in a good state
    LOGGER.info("After process with simple pipe: " + process.info());
    assertFalse(process.isAlive());
    assertEquals(availablePortsBefore, KubePortManagerSingleton.getInstance().getNumAvailablePorts());
    assertEquals(0, process.exitValue());
  }

  @Test
  public void testExitCodeRetrieval() throws Exception {
    // start a process that requests
    var availablePortsBefore = KubePortManagerSingleton.getInstance().getNumAvailablePorts();
    final Process process = getProcess("exit 10");
    LOGGER.info("Got process with exit code: " + process.info());
    process.waitFor();

    // the pod should be dead with the correct error code
    LOGGER.info("After waitFor process with exit code: " + process.info());
    assertFalse(process.isAlive());
    assertEquals(availablePortsBefore, KubePortManagerSingleton.getInstance().getNumAvailablePorts());
    assertEquals(10, process.exitValue());
  }

  @Test
  public void testMissingEntrypoint() throws WorkerException, InterruptedException {
    // start a process with an entrypoint that doesn't exist
    var availablePortsBefore = KubePortManagerSingleton.getInstance().getNumAvailablePorts();
    final Process process = getProcess(null);
    LOGGER.info("Got process with null entrypoint: " + process.info());
    process.waitFor();

    // the pod should be dead and in an error state
    LOGGER.info("After waitFor process with null entrypoint: " + process.info());
    assertFalse(process.isAlive());
    assertEquals(availablePortsBefore, KubePortManagerSingleton.getInstance().getNumAvailablePorts());
    assertEquals(127, process.exitValue());
  }

  @Test
  public void testKillingWithoutHeartbeat() throws Exception {
    // start an infinite process
    var availablePortsBefore = KubePortManagerSingleton.getInstance().getNumAvailablePorts();
    final Process process = getProcess("while true; do echo hi; sleep 1; done");
    LOGGER.info("Got process with infinite loop: " + process.info());

    // kill the heartbeat server
    server.stop();

    // waiting for process
    process.waitFor(2, TimeUnit.MINUTES);
    LOGGER.info("After waitFor process with infinite loop: " + process.info());

    // the pod should be dead and in an error state
    assertFalse(process.isAlive());
    assertEquals(availablePortsBefore, KubePortManagerSingleton.getInstance().getNumAvailablePorts());
    assertNotEquals(0, process.exitValue());
  }

  private static String getRandomFile(int lines) {
    var sb = new StringBuilder();
    for (int i = 0; i < lines; i++) {
      sb.append(RandomStringUtils.randomAlphabetic(100));
      sb.append("\n");
    }
    return sb.toString();
  }

  private Process getProcess(String entrypoint) throws WorkerException {
    // these files aren't used for anything, it's just to check for exceptions when uploading
    var files = ImmutableMap.of(
        "file0", "fixed str",
        "file1", getRandomFile(1),
        "file2", getRandomFile(100),
        "file3", getRandomFile(1000));

    return processFactory.create(
        "some-id",
        0,
        Path.of("/tmp/job-root"),
        "busybox:latest",
        false,
        files,
        entrypoint, WorkerUtils.DEFAULT_RESOURCE_REQUIREMENTS, Map.of());
  }

  private static Set<Integer> getOpenPorts(int count) {
    final Set<ServerSocket> servers = new HashSet<>();
    final Set<Integer> ports = new HashSet<>();

    try {
      for (int i = 0; i < count; i++) {
        var server = new ServerSocket(0);
        servers.add(server);
        ports.add(server.getLocalPort());
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      for (ServerSocket server : servers) {
        Exceptions.swallow(server::close);
      }
    }

    return ports;
  }

  private static String getHost() {
    try {
      return (IS_MINIKUBE ? Inet4Address.getLocalHost().getHostAddress() : "host.docker.internal");
    } catch (UnknownHostException e) {
      throw new RuntimeException(e);
    }
  };

}

