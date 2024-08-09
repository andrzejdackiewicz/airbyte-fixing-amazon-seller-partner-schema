/* Copyright (c) 2024 Airbyte, Inc., all rights reserved. */
package io.airbyte.cdk.test.source

import io.airbyte.cdk.consumers.BufferingOutputConsumer
import io.airbyte.cdk.operation.CheckOperation
import io.airbyte.cdk.operation.Operation
import io.airbyte.protocol.models.v0.AirbyteConnectionStatus
import io.micronaut.context.annotation.Property
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import jakarta.inject.Inject
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test

@MicronautTest(environments = ["source"], rebuildContext = true)
@Property(name = Operation.PROPERTY, value = "check")
class FakeSourceCheckTest {
    @Inject lateinit var checkOperation: CheckOperation<FakeSourceConfigurationJsonObject>

    @Inject lateinit var outputConsumer: BufferingOutputConsumer

    @Test
    @Property(name = "airbyte.connector.config.host", value = "localhost")
    @Property(name = "airbyte.connector.config.port", value = "-1")
    @Property(name = "airbyte.connector.config.database", value = "testdb")
    fun testConfigBadPort() {
        assertFailed(" must have a minimum value of 0".toRegex())
    }

    @Test
    @Property(name = "airbyte.connector.config.host", value = "localhost")
    @Property(name = "airbyte.connector.config.database", value = "testdb")
    @Property(name = "metadata.resource", value = "test/source/metadata-valid.json")
    fun testSuccess() {
        assertSucceeded()
    }

    @Test
    @Property(name = "airbyte.connector.config.host", value = "localhost")
    @Property(name = "airbyte.connector.config.database", value = "testdb")
    @Property(name = "metadata.resource", value = "test/source/metadata-empty.json")
    fun testBadSchema() {
        assertFailed("Discovered zero tables".toRegex())
    }

    @Test
    @Property(name = "airbyte.connector.config.host", value = "localhost")
    @Property(name = "airbyte.connector.config.database", value = "testdb")
    @Property(name = "metadata.resource", value = "test/source/metadata-column-query-fails.json")
    fun testBadTables() {
        assertFailed("Unable to query any of the [0-9]+ discovered table".toRegex())
    }

    fun assertSucceeded() {
        assert(null)
    }

    fun assertFailed(regex: Regex) {
        assert(regex)
    }

    fun assert(failureRegex: Regex?) {
        checkOperation.execute()
        val statuses: List<AirbyteConnectionStatus> = outputConsumer.statuses()
        Assertions.assertEquals(1, statuses.size, statuses.toString())
        val actual: AirbyteConnectionStatus = statuses.first()
        if (failureRegex == null) {
            Assertions.assertEquals(
                AirbyteConnectionStatus.Status.SUCCEEDED,
                actual.status,
                actual.toString(),
            )
        } else {
            Assertions.assertEquals(
                AirbyteConnectionStatus.Status.FAILED,
                actual.status,
                actual.toString(),
            )
            Assertions.assertTrue(failureRegex.containsMatchIn(actual.message), actual.message)
        }
    }
}
