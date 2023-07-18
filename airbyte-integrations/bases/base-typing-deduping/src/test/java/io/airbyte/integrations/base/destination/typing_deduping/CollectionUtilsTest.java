package io.airbyte.integrations.base.destination.typing_deduping;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.Optional;
import java.util.Set;

public class CollectionUtilsTest {

    static Set<String> TEST_COLLECTION = Set.of("foo", "BAR", "fizz", "zip_ZOP");

    @ParameterizedTest
    @CsvSource({"foo,foo", "bar,BAR", "fIzZ,fizz", "ZIP_zop,zip_ZOP", "nope,"})
    public void testMatchingKey(final String input, final String output) {
        final var expected = Optional.ofNullable(output);
        Assertions.assertEquals(CollectionUtils.matchingKey(TEST_COLLECTION, input), expected);
    }
}
