/*
 * Copyright (c) 2022 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.s3.avro;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.stream.Stream;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;

public class JsonSchemaTypeTest {

  @ParameterizedTest
  @ArgumentsSource(JsonSchemaTypeProvider.class)
  public void testFromJsonSchemaType(String type, String airbyteType, JsonSchemaType expectedJsonSchemaType) {
    assertEquals(
        expectedJsonSchemaType,
        JsonSchemaType.fromJsonSchemaType(type, airbyteType));
  }

  public static class JsonSchemaTypeProvider implements ArgumentsProvider {

    @Override
    public Stream<? extends Arguments> provideArguments(ExtensionContext context) throws Exception {
      return Stream.of(
          Arguments.of("number", "integer", JsonSchemaType.NUMBER_INT),
          Arguments.of("number", "big_integer", JsonSchemaType.NUMBER_LONG),
          Arguments.of("number", "float", JsonSchemaType.NUMBER_FLOAT),
          Arguments.of("number", null, JsonSchemaType.NUMBER),
          Arguments.of("string", null, JsonSchemaType.STRING),
          Arguments.of("integer", null, JsonSchemaType.INTEGER),
          Arguments.of("boolean", null, JsonSchemaType.BOOLEAN),
          Arguments.of("null", null, JsonSchemaType.NULL),
          Arguments.of("object", null, JsonSchemaType.OBJECT),
          Arguments.of("array", null, JsonSchemaType.ARRAY),
          Arguments.of("combined", null, JsonSchemaType.COMBINED));
    }

  }

}
