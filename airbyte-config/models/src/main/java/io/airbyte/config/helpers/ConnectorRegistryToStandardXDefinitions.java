package io.airbyte.config.helpers;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.util.ClassUtil;
import com.google.common.base.Preconditions;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.yaml.Yamls;
import io.airbyte.config.StandardDestinationDefinition;
import io.airbyte.config.StandardSourceDefinition;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * This class maps
 */
public class ConnectorRegistryToStandardXDefinitions {
  private static final Map<String, String> classNameToIdName = Map.ofEntries(
      new SimpleImmutableEntry<>(StandardDestinationDefinition.class.getCanonicalName(), "destinationDefinitionId"),
      new SimpleImmutableEntry<>(StandardSourceDefinition.class.getCanonicalName(), "sourceDefinitionId")
  );
  private static final ObjectMapper mapper = new ObjectMapper();

  private static <T> List<T> yamlToModelList(Class<T> c, String yamlStr) throws RuntimeException {
    final var jsonNode = Yamls.deserialize(yamlStr);
    final var idName = classNameToIdName.get(c.getCanonicalName());
    checkYamlIsPresentWithNoDuplicates(jsonNode, idName);
    return toStandardXDefinitions(jsonNode.elements(), c);
  }

  public static List<StandardSourceDefinition> toStandardSourceDefinitions () {
    return yamlToModelList(StandardSourceDefinition.class, "");
  }

  public static List<StandardDestinationDefinition> toStandardDestinationDefinitions(String yamlStr) {
    return yamlToModelList(StandardDestinationDefinition.class, yamlStr);
  }

  private static void checkYamlIsPresentWithNoDuplicates(JsonNode deserialize, String idName) {
    final var presentDestList = !deserialize.elements().equals(ClassUtil.emptyIterator());
    Preconditions.checkState(presentDestList, "Destination definition list is empty");
    checkNoDuplicateNames(deserialize.elements());
    checkNoDuplicateIds(deserialize.elements(), idName);
  }

  private static void checkNoDuplicateNames(final Iterator<JsonNode> iter) throws IllegalArgumentException{
    final var names = new HashSet<String>();
    while (iter.hasNext()) {
      final var element = Jsons.clone(iter.next());
      final var name = element.get("name").asText();
      if (names.contains(name)) {
        throw new IllegalArgumentException("Multiple records have the name: " + name);
      }
      names.add(name);
    }
  }

  private static void checkNoDuplicateIds(final Iterator<JsonNode> fileIterator, final String idName) throws IllegalArgumentException{
    final var ids = new HashSet<String>();
    while (fileIterator.hasNext()) {
      final var element = Jsons.clone(fileIterator.next());
      final var id = element.get(idName).asText();
      if (ids.contains(id)) {
        throw new IllegalArgumentException("Multiple records have the id: " + id);
      }
      ids.add(id);
    }
  }

  private static <T> List<T> toStandardXDefinitions(Iterator<JsonNode> iter, Class<T> c) throws RuntimeException {
    Iterable<JsonNode> iterable = () -> iter;
    var defList = new ArrayList<T>();
    for (JsonNode n : iterable) {
      try {
        var def = mapper.treeToValue(n, c);
        defList.add(def);
      } catch (JsonProcessingException e) {
        throw new RuntimeException("Unable to process latest definitions list", e);
      }
    }
    return defList;
  }

}
