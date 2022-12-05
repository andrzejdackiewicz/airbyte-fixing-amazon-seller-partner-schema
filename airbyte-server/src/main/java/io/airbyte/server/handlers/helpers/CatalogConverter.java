/*
 * Copyright (c) 2022 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.handlers.helpers;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.airbyte.api.model.generated.AirbyteCatalog;
import io.airbyte.api.model.generated.AirbyteStream;
import io.airbyte.api.model.generated.AirbyteStreamAndConfiguration;
import io.airbyte.api.model.generated.AirbyteStreamConfiguration;
import io.airbyte.api.model.generated.SelectedFieldInfo;
import io.airbyte.commons.enums.Enums;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.text.Names;
import io.airbyte.config.FieldSelectionEnabledStreams;
import io.airbyte.protocol.models.ConfiguredAirbyteCatalog;
import io.airbyte.validation.json.JsonValidationException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Convert classes between io.airbyte.protocol.models and io.airbyte.api.model.generated
 */
public class CatalogConverter {

  private static io.airbyte.api.model.generated.AirbyteStream toApi(final io.airbyte.protocol.models.AirbyteStream stream) {
    return new io.airbyte.api.model.generated.AirbyteStream()
        .name(stream.getName())
        .jsonSchema(stream.getJsonSchema())
        .supportedSyncModes(Enums.convertListTo(stream.getSupportedSyncModes(), io.airbyte.api.model.generated.SyncMode.class))
        .sourceDefinedCursor(stream.getSourceDefinedCursor())
        .defaultCursorField(stream.getDefaultCursorField())
        .sourceDefinedPrimaryKey(stream.getSourceDefinedPrimaryKey())
        .namespace(stream.getNamespace());
  }

  private static io.airbyte.protocol.models.AirbyteStream toProtocol(final AirbyteStream stream, final AirbyteStreamConfiguration config)
      throws JsonValidationException {
    JsonNode streamSchema = stream.getJsonSchema();
    if (config.getFieldSelectionEnabled() != null && config.getFieldSelectionEnabled()) {
      // Only include the selected fields.
      List<String> selectedFieldNames = config.getSelectedFields().stream().map(SelectedFieldInfo::getFieldName).collect(Collectors.toList());
      final JsonNode properties = streamSchema.findValue("properties");
      if (properties.isObject()) {
        ((ObjectNode) properties).retain(selectedFieldNames);
      } else {
        throw new JsonValidationException("Requested field selection but no properties node found");
      }
    }
    return new io.airbyte.protocol.models.AirbyteStream()
        .withName(stream.getName())
        .withJsonSchema(streamSchema)
        .withSupportedSyncModes(Enums.convertListTo(stream.getSupportedSyncModes(), io.airbyte.protocol.models.SyncMode.class))
        .withSourceDefinedCursor(stream.getSourceDefinedCursor())
        .withDefaultCursorField(stream.getDefaultCursorField())
        .withSourceDefinedPrimaryKey(Optional.ofNullable(stream.getSourceDefinedPrimaryKey()).orElse(Collections.emptyList()))
        .withNamespace(stream.getNamespace());
  }

  public static io.airbyte.api.model.generated.AirbyteCatalog toApi(final io.airbyte.protocol.models.AirbyteCatalog catalog) {
    return new io.airbyte.api.model.generated.AirbyteCatalog()
        .streams(catalog.getStreams()
            .stream()
            .map(CatalogConverter::toApi)
            .map(s -> new io.airbyte.api.model.generated.AirbyteStreamAndConfiguration()
                .stream(s)
                .config(generateDefaultConfiguration(s)))
            .collect(Collectors.toList()));
  }

  private static io.airbyte.api.model.generated.AirbyteStreamConfiguration generateDefaultConfiguration(final io.airbyte.api.model.generated.AirbyteStream stream) {
    final io.airbyte.api.model.generated.AirbyteStreamConfiguration result = new io.airbyte.api.model.generated.AirbyteStreamConfiguration()
        .aliasName(Names.toAlphanumericAndUnderscore(stream.getName()))
        .cursorField(stream.getDefaultCursorField())
        .destinationSyncMode(io.airbyte.api.model.generated.DestinationSyncMode.APPEND)
        .primaryKey(stream.getSourceDefinedPrimaryKey())
        .selected(true);
    if (stream.getSupportedSyncModes().size() > 0) {
      result.setSyncMode(stream.getSupportedSyncModes().get(0));
    } else {
      result.setSyncMode(io.airbyte.api.model.generated.SyncMode.INCREMENTAL);
    }
    return result;
  }

  public static io.airbyte.api.model.generated.AirbyteCatalog toApi(final ConfiguredAirbyteCatalog catalog,
                                                                    FieldSelectionEnabledStreams fieldSelectionEnabledStreams) {
    final List<io.airbyte.api.model.generated.AirbyteStreamAndConfiguration> streams = catalog.getStreams()
        .stream()
        .map(configuredStream -> {
          final String streamName = configuredStream.getStream().getName();
          final io.airbyte.api.model.generated.AirbyteStreamConfiguration configuration =
              new io.airbyte.api.model.generated.AirbyteStreamConfiguration()
                  .syncMode(Enums.convertTo(configuredStream.getSyncMode(), io.airbyte.api.model.generated.SyncMode.class))
                  .cursorField(configuredStream.getCursorField())
                  .destinationSyncMode(
                      Enums.convertTo(configuredStream.getDestinationSyncMode(), io.airbyte.api.model.generated.DestinationSyncMode.class))
                  .primaryKey(configuredStream.getPrimaryKey())
                  .aliasName(Names.toAlphanumericAndUnderscore(streamName))
                  .selected(true)
                  .fieldSelectionEnabled(getStreamHasFieldSelectionEnabled(fieldSelectionEnabledStreams, streamName));
          if (configuration.getFieldSelectionEnabled()) {
            final List<String> selectedColumns = new ArrayList<>();
            configuredStream.getStream().getJsonSchema().findValue("properties").fieldNames().forEachRemaining((name) -> selectedColumns.add(name));
            configuration.setSelectedFields(
                selectedColumns.stream().map((fieldName) -> new SelectedFieldInfo().fieldName(fieldName)).collect(Collectors.toList()));
          }
          return new io.airbyte.api.model.generated.AirbyteStreamAndConfiguration()
              .stream(toApi(configuredStream.getStream()))
              .config(configuration);
        })
        .collect(Collectors.toList());
    return new io.airbyte.api.model.generated.AirbyteCatalog().streams(streams);
  }

  private static Boolean getStreamHasFieldSelectionEnabled(FieldSelectionEnabledStreams fieldSelectionEnabledStreams, final String name) {
    if (fieldSelectionEnabledStreams == null || fieldSelectionEnabledStreams.getAdditionalProperties().get(name) == null) {
      return false;
    }
    return fieldSelectionEnabledStreams.getAdditionalProperties().get(name);
  }

  /**
   * Converts the API catalog model into a protocol catalog. Note: returns all streams, regardless of
   * selected status. See
   * {@link CatalogConverter#toProtocol(AirbyteStream, AirbyteStreamConfiguration)} for context.
   *
   * @param catalog api catalog
   * @return protocol catalog
   */
  public static io.airbyte.protocol.models.ConfiguredAirbyteCatalog toProtocolKeepAllStreams(
                                                                                             final io.airbyte.api.model.generated.AirbyteCatalog catalog)
      throws JsonValidationException {
    final AirbyteCatalog clone = Jsons.clone(catalog);
    clone.getStreams().forEach(stream -> stream.getConfig().setSelected(true));
    return toProtocol(clone);
  }

  /**
   * Converts the API catalog model into a protocol catalog. Note: only streams marked as selected
   * will be returned. This is included in this converter as the API model always carries all the
   * streams it has access to and then marks the ones that should not be used as not selected, while
   * the protocol version just uses the presence of the streams as evidence that it should be
   * included.
   *
   * @param catalog api catalog
   * @return protocol catalog
   */
  public static io.airbyte.protocol.models.ConfiguredAirbyteCatalog toProtocol(final io.airbyte.api.model.generated.AirbyteCatalog catalog)
      throws JsonValidationException {
    final ArrayList<JsonValidationException> errors = new ArrayList<>();
    final List<io.airbyte.protocol.models.ConfiguredAirbyteStream> streams = catalog.getStreams()
        .stream()
        .filter(CatalogConverter::streamIsIncluded)
        .map(s -> {
          try {
            return new io.airbyte.protocol.models.ConfiguredAirbyteStream()
                .withStream(toProtocol(s.getStream(), s.getConfig()))
                .withSyncMode(Enums.convertTo(s.getConfig().getSyncMode(), io.airbyte.protocol.models.SyncMode.class))
                .withCursorField(s.getConfig().getCursorField())
                .withDestinationSyncMode(Enums.convertTo(s.getConfig().getDestinationSyncMode(),
                    io.airbyte.protocol.models.DestinationSyncMode.class))
                .withPrimaryKey(Optional.ofNullable(s.getConfig().getPrimaryKey()).orElse(Collections.emptyList()));
          } catch (JsonValidationException e) {
            errors.add(e);
          }
          return new io.airbyte.protocol.models.ConfiguredAirbyteStream();
        })
        .collect(Collectors.toList());
    if (!errors.isEmpty()) {
      throw errors.get(0);
    }
    return new io.airbyte.protocol.models.ConfiguredAirbyteCatalog()
        .withStreams(streams);
  }

  private static boolean streamIsIncluded(final AirbyteStreamAndConfiguration s) {
    return s.getConfig().getSelected() || (s.getConfig().getSelectedFields() != null && !s.getConfig().getSelectedFields().isEmpty());
  }

  public static FieldSelectionEnabledStreams getFieldSelectionEnabledStreams(final AirbyteCatalog syncCatalog) {
    if (syncCatalog == null) {
      return null;
    }
    final FieldSelectionEnabledStreams fieldSelectionEnabledStreams = new FieldSelectionEnabledStreams();
    syncCatalog.getStreams().stream().forEach((streamAndConfig) -> {
      fieldSelectionEnabledStreams.withAdditionalProperty(streamAndConfig.getStream().getName(),
          streamAndConfig.getConfig().getFieldSelectionEnabled() != null ? streamAndConfig.getConfig().getFieldSelectionEnabled() : false);
    });
    return fieldSelectionEnabledStreams;
  }

}
