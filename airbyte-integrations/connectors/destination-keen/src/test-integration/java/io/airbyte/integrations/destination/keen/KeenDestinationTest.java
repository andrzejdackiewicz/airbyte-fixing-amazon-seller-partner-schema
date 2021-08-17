package io.airbyte.integrations.destination.keen;

import static io.airbyte.integrations.destination.keen.KeenDestination.CONFIG_API_KEY;
import static io.airbyte.integrations.destination.keen.KeenDestination.CONFIG_PROJECT_ID;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.api.client.util.Lists;
import com.google.common.collect.Sets;
import io.airbyte.commons.io.IOs;
import io.airbyte.commons.json.Jsons;
import io.airbyte.integrations.standardtest.destination.DestinationAcceptanceTest;

import io.airbyte.protocol.models.AirbyteMessage;
import io.airbyte.protocol.models.ConfiguredAirbyteCatalog;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;

public class KeenDestinationTest extends DestinationAcceptanceTest {

  private static final String SECRET_FILE_PATH = "secrets/config.json";

  private final KeenHttpClient keenHttpClient = new KeenHttpClient();

  private String projectId;
  private String apiKey;
  private JsonNode configJson;

  @Override
  protected String getImageName() {
    return "airbyte/destination-keen:dev";
  }

  @Override
  protected JsonNode getConfig() throws Exception {
    return configJson;
  }

  @Override
  protected JsonNode getFailCheckConfig() throws Exception {
    ((ObjectNode) configJson).put(CONFIG_PROJECT_ID, "fake");
    ((ObjectNode) configJson).put(CONFIG_API_KEY, "fake");

    return configJson;
  }

  protected JsonNode getBaseConfigJson() {
    return Jsons.deserialize(IOs.readFile(Path.of(SECRET_FILE_PATH)));
  }

  @Override
  protected List<JsonNode> retrieveRecords(TestDestinationEnv testEnv, String streamName, String namespace, JsonNode streamSchema) throws Exception {
    String accentStrippedStreamName = KeenCharactersStripper.stripSpecialCharactersFromStreamName(streamName);

    ArrayNode array = keenHttpClient.extract(accentStrippedStreamName, getPropertiesToExtractString(streamSchema), projectId, apiKey);
    return Lists.newArrayList(array.elements());
  }

  // We don't want to extract 'keen' object, extract only properties included in streamSchema
  private String getPropertiesToExtractString(
      JsonNode streamSchema) {
    return Sets.newHashSet(streamSchema.get("properties").fieldNames())
        .stream()
        .map(fieldName -> "\"" + fieldName + "\"")
        .collect(Collectors.joining(",", "[", "]"));
  }

  @Override
  protected void setup(TestDestinationEnv testEnv) throws Exception {
    if (!Files.exists(Path.of(SECRET_FILE_PATH))) {
      throw new IllegalStateException(
          "Must provide path to a file containing Keen account credentials: project Id and Write Key. " +
              "By default {module-root}/" + SECRET_FILE_PATH);
    }
    configJson = getBaseConfigJson();
    projectId = configJson.get(CONFIG_PROJECT_ID).asText();
    apiKey = configJson.get(CONFIG_API_KEY).asText();

  }

  @Override
  protected void tearDown(TestDestinationEnv testEnv) throws Exception {
    List<String> keenCollections = keenHttpClient.getAllCollectionsForProject(projectId, apiKey);

    for (String keenCollection : keenCollections) {
      keenHttpClient.eraseStream(keenCollection, projectId, apiKey);
    }
  }

  @Override
  protected void runSyncAndVerifyStateOutput(JsonNode config,
                                             List<AirbyteMessage> messages,
                                             ConfiguredAirbyteCatalog catalog,
                                             boolean runNormalization) throws Exception {
    super.runSyncAndVerifyStateOutput(config, messages, catalog, runNormalization);
    Thread.sleep(10000);
  }
}
