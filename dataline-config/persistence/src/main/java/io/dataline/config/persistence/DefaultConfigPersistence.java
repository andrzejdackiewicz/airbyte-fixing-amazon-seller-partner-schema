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

package io.dataline.config.persistence;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.annotations.VisibleForTesting;
import io.dataline.commons.json.Jsons;
import io.dataline.config.ConfigSchema;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import me.andrz.jackson.JsonReferenceException;
import me.andrz.jackson.JsonReferenceProcessor;
import org.apache.commons.io.FileUtils;

// we force all interaction with disk storage to be effectively single threaded.
public class DefaultConfigPersistence implements ConfigPersistence {

  private static final String CONFIG_PATH_IN_JAR = "/json";
  private static final String CONFIG_DIR = "schemas";
  private static final Path configFilesRoot = getConfigFiles();
  private static final Object lock = new Object();

  private final JsonSchemaValidation jsonSchemaValidation;
  private final Path storageRoot;

  public DefaultConfigPersistence(Path storageRoot) {
    this.storageRoot = storageRoot;
    jsonSchemaValidation = new JsonSchemaValidation();
  }

  /**
   * JsonReferenceProcessor relies on all of the json in consumes being in a file system (not in a
   * jar). This method copies all of the json configs out of the jar into a temporary directory so
   * that JsonReferenceProcessor can find them.
   *
   * @return path where the config files can be found.
   */
  private static Path getConfigFiles() {
    final URI uri;
    try {
      uri = ConfigSchema.class.getResource(CONFIG_PATH_IN_JAR).toURI();
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }

    try {
      final Path configRoot = Files.createTempDirectory("").resolve(CONFIG_DIR);
      Files.createDirectories(configRoot);

      final FileSystem fileSystem = FileSystems.newFileSystem(uri, Collections.emptyMap());
      Path configPathInJar = fileSystem.getPath(CONFIG_PATH_IN_JAR);
      Files.walk(configPathInJar, 1)
          .forEach(
              path -> {
                if (path.toString().endsWith(".json")) {
                  try {
                    Files.copy(path, configRoot.resolve(path.getFileName().toString()));
                  } catch (IOException e) {
                    throw new RuntimeException(e);
                  }
                }
              });
      return configRoot;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public <T> T getConfig(PersistenceConfigType persistenceConfigType,
                         String configId,
                         Class<T> clazz)
      throws ConfigNotFoundException, JsonValidationException {
    synchronized (lock) {
      return getConfigInternal(persistenceConfigType, configId, clazz);
    }
  }

  private <T> T getConfigInternal(PersistenceConfigType persistenceConfigType,
                                  String configId,
                                  Class<T> clazz)
      throws ConfigNotFoundException, JsonValidationException {
    // validate file with schema
    try {
      final Path configPath = getFileOrThrow(persistenceConfigType, configId);
      final T config = Jsons.deserialize(Files.readString(configPath), clazz);
      validateJson(config, persistenceConfigType);

      return config;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public <T> Set<T> getConfigs(PersistenceConfigType persistenceConfigType, Class<T> clazz)
      throws JsonValidationException {
    synchronized (lock) {
      final Set<T> configs = new HashSet<>();
      for (String configId : getConfigIds(persistenceConfigType)) {
        try {
          configs.add(getConfig(persistenceConfigType, configId, clazz));
          // this should not happen, because we just looked up these ids.
        } catch (ConfigNotFoundException e) {
          throw new RuntimeException(e);
        }
      }
      return configs;
    }
  }

  @Override
  public <T> void writeConfig(PersistenceConfigType persistenceConfigType,
                              String configId,
                              T config)
      throws JsonValidationException {
    synchronized (lock) {
      // validate config with schema
      validateJson(Jsons.jsonNode(config), persistenceConfigType);

      final Path configPath = getConfigPath(persistenceConfigType, configId);
      ensureDirectory(getConfigDirectory(persistenceConfigType));
      try {
        Files.writeString(configPath, Jsons.serialize(config));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @VisibleForTesting
  JsonNode getSchema(PersistenceConfigType persistenceConfigType) {
    final String configSchemaFilename =
        standardConfigTypeToConfigSchema(persistenceConfigType).getSchemaFilename();
    final Path configFilePath = configFilesRoot.resolve(configSchemaFilename);

    try {
      // JsonReferenceProcessor follows $ref in json objects. Jackson does not natively support
      // this.
      final JsonReferenceProcessor jsonReferenceProcessor = new JsonReferenceProcessor();
      jsonReferenceProcessor.setMaxDepth(-1); // no max.
      return jsonReferenceProcessor.process(configFilePath.toFile());
    } catch (IOException | JsonReferenceException e) {
      throw new RuntimeException(e);
    }
  }

  private Set<Path> getFiles(PersistenceConfigType persistenceConfigType) {
    Path configDirPath = getConfigDirectory(persistenceConfigType);
    ensureDirectory(configDirPath);
    try {
      return Files.list(configDirPath).collect(Collectors.toSet());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private Path getConfigDirectory(PersistenceConfigType persistenceConfigType) {
    return storageRoot.resolve(persistenceConfigType.toString());
  }

  private Path getConfigPath(PersistenceConfigType persistenceConfigType, String configId) {
    return getConfigDirectory(persistenceConfigType).resolve(getFilename(configId));
  }

  private Set<String> getConfigIds(PersistenceConfigType persistenceConfigType) {
    return getFiles(persistenceConfigType).stream()
        .map(path -> path.getFileName().toString().replace(".json", ""))
        .collect(Collectors.toSet());
  }

  private Optional<Path> getFile(PersistenceConfigType persistenceConfigType, String id) {
    ensureDirectory(getConfigDirectory(persistenceConfigType));
    final Path path = getConfigPath(persistenceConfigType, id);
    if (Files.exists(path)) {
      return Optional.of(path);
    } else {
      return Optional.empty();
    }
  }

  private String getFilename(String id) {
    return String.format("%s.json", id);
  }

  private ConfigSchema standardConfigTypeToConfigSchema(PersistenceConfigType persistenceConfigType) {
    switch (persistenceConfigType) {
      case STANDARD_WORKSPACE:
        return ConfigSchema.STANDARD_WORKSPACE;
      case STANDARD_SOURCE:
        return ConfigSchema.STANDARD_SOURCE;
      case SOURCE_CONNECTION_SPECIFICATION:
        return ConfigSchema.SOURCE_CONNECTION_SPECIFICATION;
      case SOURCE_CONNECTION_IMPLEMENTATION:
        return ConfigSchema.SOURCE_CONNECTION_IMPLEMENTATION;
      case STANDARD_DESTINATION:
        return ConfigSchema.STANDARD_DESTINATION;
      case DESTINATION_CONNECTION_SPECIFICATION:
        return ConfigSchema.DESTINATION_CONNECTION_SPECIFICATION;
      case DESTINATION_CONNECTION_IMPLEMENTATION:
        return ConfigSchema.DESTINATION_CONNECTION_IMPLEMENTATION;
      case STANDARD_CONNECTION_STATUS:
        return ConfigSchema.STANDARD_CONNECTION_STATUS;
      case STANDARD_DISCOVERY_OUTPUT:
        return ConfigSchema.STANDARD_DISCOVERY_OUTPUT;
      case STANDARD_SYNC:
        return ConfigSchema.STANDARD_SYNC;
      case STANDARD_SYNC_SUMMARY:
        return ConfigSchema.STANDARD_SYNC_SUMMARY;
      case STANDARD_SYNC_SCHEDULE:
        return ConfigSchema.STANDARD_SYNC_SCHEDULE;
      case STATE:
        return ConfigSchema.STATE;
      default:
        throw new RuntimeException(
            String.format(
                "No mapping from StandardConfigType to ConfigSchema for %s",
                persistenceConfigType));
    }
  }

  private <T> void validateJson(T config, PersistenceConfigType persistenceConfigType)
      throws JsonValidationException {

    JsonNode schema = getSchema(persistenceConfigType);
    jsonSchemaValidation.validateThrow(schema, Jsons.jsonNode(config));
  }

  private Path getFileOrThrow(PersistenceConfigType persistenceConfigType, String configId)
      throws ConfigNotFoundException {
    return getFile(persistenceConfigType, configId)
        .orElseThrow(
            () -> new ConfigNotFoundException(
                String.format(
                    "config type: %s id: %s not found in path %s",
                    persistenceConfigType,
                    configId,
                    getConfigPath(persistenceConfigType, configId))));
  }

  private void ensureDirectory(Path path) {
    try {
      FileUtils.forceMkdir(path.toFile());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

}
