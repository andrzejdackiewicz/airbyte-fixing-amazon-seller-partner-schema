/*
 * Copyright (c) 2022 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.init;

import static io.airbyte.config.init.JsonDefinitionsHelper.addMissingTombstoneField;

import com.fasterxml.jackson.databind.JsonNode;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.util.MoreIterators;
import io.airbyte.config.StandardDestinationDefinition;
import io.airbyte.config.StandardSourceDefinition;
import io.airbyte.config.persistence.ConfigNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * This provider pulls the definitions from a remotely hosted catalog.
 */
final public class RemoteDefinitionsProvider implements DefinitionsProvider {

  private Map<UUID, StandardSourceDefinition> sourceDefinitions;
  private Map<UUID, StandardDestinationDefinition> destinationDefinitions;

  private static final HttpClient httpClient = HttpClient.newHttpClient();
  private final URI remoteDefinitionCatalogUrl;
  private static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(30);
  private final Duration timeout;

  public RemoteDefinitionsProvider(final URI remoteDefinitionCatalogUrl) throws InterruptedException, IOException {
    this(remoteDefinitionCatalogUrl, DEFAULT_TIMEOUT);
  }

  public RemoteDefinitionsProvider(final URI remoteDefinitionCatalogUrl, final Duration timeout) throws InterruptedException, IOException {
    this.remoteDefinitionCatalogUrl = remoteDefinitionCatalogUrl;
    this.timeout = timeout;
    // TODO remove this call once dependency injection framework manages object creation
    initialize();
  }

  // TODO will be called automatically by the dependency injection framework on object creation
  public void initialize() throws InterruptedException, IOException {
    final JsonNode catalog = getRemoteDefinitionCatalog(this.remoteDefinitionCatalogUrl, this.timeout);
    this.sourceDefinitions =
        parseDefinitions(catalog, "sources", StandardSourceDefinition.class, SeedType.STANDARD_SOURCE_DEFINITION.getIdName());
    this.destinationDefinitions =
        parseDefinitions(catalog, "destinations", StandardDestinationDefinition.class, SeedType.STANDARD_DESTINATION_DEFINITION.getIdName());
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

  private static JsonNode getRemoteDefinitionCatalog(URI catalogUrl, Duration timeout) throws IOException, InterruptedException {
    final HttpRequest request = HttpRequest.newBuilder(catalogUrl).timeout(timeout).header("accept", "application/json").build();

    final HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
    if (response.statusCode() >= 400) {
      throw new IOException(
          "getRemoteDefinitionCatalog request ran into status code error: " + response.statusCode() + " with message: " + response.getClass());
    }
    return Jsons.deserialize(response.body());
  }

  private static <T> Map<UUID, T> parseDefinitions(final JsonNode catalog,
                                                   final String catalogKey,
                                                   final Class<T> outputModel,
                                                   final String definitionIdField)
      throws IOException {
    final JsonNode catalogElementNode = catalog.get(catalogKey);
    if (catalogElementNode == null) {
      throw new IOException("The catalog schema is invalid: Missing \"" + catalogKey + "\" key");
    }
    final List<JsonNode> catalogElements = MoreIterators.toList(catalogElementNode.elements());
    return catalogElements.stream().collect(Collectors.toMap(
        json -> UUID.fromString(json.get(definitionIdField).asText()),
        json -> Jsons.object(addMissingFields(json), outputModel)));
  }

  private static JsonNode addMissingFields(JsonNode element) {
    return addMissingTombstoneField(element);
  }

}
