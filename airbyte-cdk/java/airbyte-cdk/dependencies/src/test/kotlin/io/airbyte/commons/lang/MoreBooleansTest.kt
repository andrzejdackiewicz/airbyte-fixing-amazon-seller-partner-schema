/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */
package io.airbyte.commons.lang

import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import java.lang.Boolean

internal class MoreBooleansTest {
    @Test
    fun evaluateNullAsFalse() {
        Assertions.assertTrue(MoreBooleans.isTruthy(Boolean.TRUE))
        Assertions.assertFalse(MoreBooleans.isTruthy(Boolean.FALSE))
        Assertions.assertFalse(MoreBooleans.isTruthy(null))
    }
}
