/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.base.destination.typing_deduping;

import static io.airbyte.integrations.base.destination.typing_deduping.AirbyteType.AirbyteProtocolType.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airbyte.integrations.base.destination.typing_deduping.AirbyteType.AirbyteProtocolType;
import io.airbyte.integrations.base.destination.typing_deduping.AirbyteType.Array;
import io.airbyte.integrations.base.destination.typing_deduping.AirbyteType.Struct;
import io.airbyte.integrations.base.destination.typing_deduping.AirbyteType.Union;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AirbyteTypeUtils {

  private static final Logger LOGGER = LoggerFactory.getLogger(AirbyteTypeUtils.class);

  // Map from a protocol type to what other protocol types should take precedence over it if present
  // in a OneOf
  private static final Map<AirbyteProtocolType, List<AirbyteProtocolType>> EXCLUDED_PROTOCOL_TYPES_MAP = ImmutableMap.of(
      AirbyteProtocolType.BOOLEAN, ImmutableList.of(AirbyteProtocolType.STRING, AirbyteProtocolType.NUMBER, AirbyteProtocolType.INTEGER),
      AirbyteProtocolType.INTEGER, ImmutableList.of(AirbyteProtocolType.STRING, AirbyteProtocolType.NUMBER),
      AirbyteProtocolType.NUMBER, ImmutableList.of(AirbyteProtocolType.STRING));

  protected static boolean nodeMatches(final JsonNode node, final String value) {
    if (node == null || !node.isTextual()) {
      return false;
    }
    return node.equals(TextNode.valueOf(value));
  }

  // Extracts the appropriate protocol type from the representative JSON
  protected static AirbyteType getAirbyteProtocolType(final JsonNode node) {
    // JSON could be a string (ex: "number")
    if (node.isTextual()) {
      return matches(node.asText());
    }

    // or, JSON could be a node with fields
    final JsonNode propertyType = node.get("type");
    final JsonNode airbyteType = node.get("airbyte_type");
    final JsonNode format = node.get("format");

    if (nodeMatches(propertyType, "boolean")) {
      return BOOLEAN;
    } else if (nodeMatches(propertyType, "integer")) {
      return INTEGER;
    } else if (nodeMatches(propertyType, "number")) {
      return nodeMatches(airbyteType, "integer") ? INTEGER : NUMBER;
    } else if (nodeMatches(propertyType, "string")) {
      if (nodeMatches(format, "date")) {
        return DATE;
      } else if (nodeMatches(format, "time")) {
        if (nodeMatches(airbyteType, "time_without_timezone")) {
          return TIME_WITHOUT_TIMEZONE;
        } else if (nodeMatches(airbyteType, "time_with_timezone")) {
          return TIME_WITH_TIMEZONE;
        }
      } else if (nodeMatches(format, "date-time")) {
        if (nodeMatches(airbyteType, "timestamp_without_timezone")) {
          return TIMESTAMP_WITHOUT_TIMEZONE;
        } else if (airbyteType == null || nodeMatches(airbyteType, "timestamp_with_timezone")) {
          return TIMESTAMP_WITH_TIMEZONE;
        }
      } else {
        return STRING;
      }
    }

    return UNKNOWN;
  }

  // Picks which type in a Union takes precedence
  public static AirbyteType chooseUnionType(final Union u) {
    final List<AirbyteType> options = u.options();

    // record what types are present
    Array foundArrayType = null;
    Struct foundStructType = null;
    final Map<AirbyteProtocolType, Boolean> typePresenceMap = new HashMap<>();
    Arrays.stream(AirbyteProtocolType.values()).map(type -> typePresenceMap.put(type, false));

    // looping through the options only once for efficiency
    for (final AirbyteType option : options) {
      if (option instanceof final Array a) {
        foundArrayType = a;
      } else if (option instanceof final Struct s) {
        foundStructType = s;
      } else if (option instanceof final AirbyteProtocolType p) {
        typePresenceMap.put(p, true);
      }
    }

    if (foundArrayType != null) {
      return foundArrayType;
    } else if (foundStructType != null) {
      return foundStructType;
    } else {
      for (final AirbyteProtocolType protocolType : AirbyteProtocolType.values()) {
        if (typePresenceMap.getOrDefault(protocolType, false)) {
          boolean foundExcludedTypes = false;
          final List<AirbyteProtocolType> excludedTypes = 
              EXCLUDED_PROTOCOL_TYPES_MAP.getOrDefault(protocolType, Collections.emptyList());
          for (final AirbyteProtocolType excludedType : excludedTypes) {
            if (typePresenceMap.getOrDefault(excludedType, false)) {
              foundExcludedTypes = true;
              break;
            }
          }
          if (!foundExcludedTypes) {
            return protocolType;
          }
        }
      }
    }

    return UNKNOWN;
  }

}
