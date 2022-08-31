/*
 * Copyright (c) 2022 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.init;

import static io.airbyte.config.init.PatchingHelpers.addMissingCustomField;
import static io.airbyte.config.init.PatchingHelpers.addMissingPublicField;
import static io.airbyte.config.init.PatchingHelpers.addMissingTombstoneField;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.io.Resources;
import io.airbyte.commons.docker.DockerUtils;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.util.MoreIterators;
import io.airbyte.commons.yaml.Yamls;
import io.airbyte.config.StandardDestinationDefinition;
import io.airbyte.config.StandardSourceDefinition;
import io.airbyte.config.persistence.ConfigNotFoundException;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * This config persistence contains all seed definitions according to the yaml files. It is
 * read-only.
 */
final public class LocalDefinitionsProvider implements DefinitionsProvider {

  private Map<UUID, StandardSourceDefinition> sourceDefinitions;
  private Map<UUID, StandardDestinationDefinition> destinationDefinitions;

  // TODO inject via dependency injection framework
  private final Class<?> seedResourceClass;

  public LocalDefinitionsProvider(final Class<?> seedResourceClass) throws IOException {
    this.seedResourceClass = seedResourceClass;

    // TODO remove this call once dependency injection framework manages object creation
    initialize();
  }

  // TODO will be called automatically by the dependency injection framework on object creation
  public void initialize() throws IOException {
    this.sourceDefinitions =
        parseDefinitions(this.seedResourceClass, SeedType.STANDARD_SOURCE_DEFINITION.getResourcePath(), SeedType.SOURCE_SPEC.getResourcePath(),
            SeedType.STANDARD_SOURCE_DEFINITION.getIdName(), SeedType.SOURCE_SPEC.getIdName(), StandardSourceDefinition.class);
    this.destinationDefinitions = parseDefinitions(this.seedResourceClass, SeedType.STANDARD_DESTINATION_DEFINITION.getResourcePath(),
        SeedType.DESTINATION_SPEC.getResourcePath(), SeedType.STANDARD_DESTINATION_DEFINITION.getIdName(), SeedType.DESTINATION_SPEC.getIdName(),
        StandardDestinationDefinition.class);
  }

  @Override
  public StandardSourceDefinition getSourceDefinition(final UUID definitionId) throws ConfigNotFoundException {
    StandardSourceDefinition definition = this.sourceDefinitions.get(definitionId);
    if (definition == null) {
      throw new ConfigNotFoundException(SeedType.STANDARD_SOURCE_DEFINITION.name(), definitionId.toString());
    }
    return definition;
  }

  @Override
  public List<StandardSourceDefinition> getSourceDefinitions() {
    return new ArrayList<>(this.sourceDefinitions.values());
  }

  @Override
  public StandardDestinationDefinition getDestinationDefinition(final UUID definitionId) throws ConfigNotFoundException {
    StandardDestinationDefinition definition = this.destinationDefinitions.get(definitionId);
    if (definition == null) {
      throw new ConfigNotFoundException(SeedType.STANDARD_DESTINATION_DEFINITION.name(), definitionId.toString());
    }
    return definition;
  }

  @Override
  public List<StandardDestinationDefinition> getDestinationDefinitions() {
    return new ArrayList<>(this.destinationDefinitions.values());
  }

  @SuppressWarnings("UnstableApiUsage")
  private static <T> Map<UUID, T> parseDefinitions(final Class<?> seedDefinitionsResourceClass,
                                                   final String definitionsYamlPath,
                                                   final String specYamlPath,
                                                   final String definitionIdField,
                                                   final String specIdField,
                                                   final Class<T> definitionModel)
      throws IOException {
    final Map<String, JsonNode> rawDefinitions = getJsonElements(seedDefinitionsResourceClass, definitionsYamlPath, definitionIdField);
    final Map<String, JsonNode> rawSpecs = getJsonElements(seedDefinitionsResourceClass, specYamlPath, specIdField);

    return rawDefinitions.entrySet().stream()
        .collect(Collectors.toMap(e -> UUID.fromString(e.getKey()), e -> {
          final JsonNode withMissingFields = addMissingFields(e.getValue());
          final JsonNode withSpec = mergeSpecIntoDefinition(withMissingFields, rawSpecs);
          return Jsons.object(withSpec, definitionModel);
        }));

  }

  private static Map<String, JsonNode> getJsonElements(final Class<?> seedDefinitionsResourceClass, final String resourcePath, final String idName)
      throws IOException {
    final URL url = Resources.getResource(seedDefinitionsResourceClass, resourcePath);
    final String yamlString = Resources.toString(url, StandardCharsets.UTF_8);
    final JsonNode configList = Yamls.deserialize(yamlString);
    return MoreIterators.toList(configList.elements()).stream().collect(Collectors.toMap(
        json -> json.get(idName).asText(),
        json -> json));
  }

  /**
   * Merges the corresponding spec JSON into the definition JSON. This is necessary because specs are
   * stored in a separate resource file from definitions.
   *
   * @param definitionJson JSON of connector definition that is missing a spec
   * @param specConfigs map of docker image to JSON of docker image/connector spec pair
   * @return JSON of connector definition including the connector spec
   */
  private static JsonNode mergeSpecIntoDefinition(final JsonNode definitionJson, final Map<String, JsonNode> specConfigs) {
    final String dockerImage = DockerUtils.getTaggedImageName(
        definitionJson.get("dockerRepository").asText(),
        definitionJson.get("dockerImageTag").asText());
    final JsonNode specConfigJson = specConfigs.get(dockerImage);
    if (specConfigJson == null || specConfigJson.get("spec") == null) {
      throw new UnsupportedOperationException(String.format("There is no seed spec for docker image %s", dockerImage));
    }
    ((ObjectNode) definitionJson).set("spec", specConfigJson.get("spec"));
    return definitionJson;
  }

  private static JsonNode addMissingFields(JsonNode element) {
    return addMissingPublicField(addMissingCustomField(addMissingTombstoneField(element)));
  }

}
