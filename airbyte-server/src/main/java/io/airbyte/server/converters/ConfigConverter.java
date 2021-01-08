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

package io.airbyte.server.converters;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.JsonNode;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.yaml.Yamls;
import io.airbyte.config.ConfigSchema;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class ConfigConverter {

  private static final String CONFIG_FOLDER_NAME = "AirbyteConfig";

  private final Path storageRoot;
  private final String version;

  public ConfigConverter(final Path storageRoot, String version) {
    this.storageRoot = storageRoot;
    this.version = version;
  }

  private Path buildConfigPath(ConfigSchema schemaType) {
    return storageRoot.resolve(CONFIG_FOLDER_NAME)
        .resolve(String.format("%s.yaml", schemaType.toString()));
  }

  public <T> void writeConfigList(ConfigSchema schemaType, List<T> configList) throws IOException {
    final Path configPath = buildConfigPath(schemaType);
    Files.createDirectories(configPath.getParent());
    Files.writeString(configPath, Yamls.serialize(new ConfigWrapper(version, configList)));
  }

  public <T> List<T> readConfigList(ConfigSchema schemaType, Class<T> clazz) throws IOException {
    final Path configPath = buildConfigPath(schemaType);
    final String configStr = Files.readString(configPath);
    final JsonNode node = Yamls.deserialize(configStr);
    final ConfigWrapper wrapper = Jsons.object(node, ConfigWrapper.class);
    final String readVersion = wrapper.getAirbyteVersion();
    if (!version.equals(readVersion)) {
      throw new IOException(String.format("Version of files to import %s does not match current version %s", readVersion, version));
    }
    final List<T> results = new ArrayList<>();
    final Iterator<JsonNode> it = wrapper.getConfigs().elements();
    while (it.hasNext()) {
      final JsonNode element = it.next();
      results.add(Jsons.object(element, clazz));
    }
    return results;
  }

  @JsonPropertyOrder({
    "airbyteVersion",
    "configList"
  })
  private static class ConfigWrapper {

    @JsonProperty("airbyteVersion")
    private String airbyteVersion;
    @JsonProperty("configs")
    private JsonNode configs;

    public <T> ConfigWrapper() {}

    public <T> ConfigWrapper(final String airbyteVersion, final List<T> configs) {
      this();
      this.airbyteVersion = airbyteVersion;
      this.configs = Jsons.jsonNode(configs);
    }

    @JsonProperty("airbyteVersion")
    public String getAirbyteVersion() {
      return airbyteVersion;
    }

    @JsonProperty("configs")
    public JsonNode getConfigs() {
      return configs;
    }

  }

}
