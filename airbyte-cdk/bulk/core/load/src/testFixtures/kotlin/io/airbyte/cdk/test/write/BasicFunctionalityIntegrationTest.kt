/*
 * Copyright (c) 2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.cdk.test.write

import io.airbyte.cdk.command.ConfigurationJsonObjectBase
import io.airbyte.cdk.command.DestinationCatalog
import io.airbyte.cdk.command.DestinationStream
import io.airbyte.cdk.command.DestinationStream.Descriptor
import io.airbyte.cdk.test.util.DestinationDataDumper
import io.airbyte.cdk.test.util.DestinationRecordMangler
import io.airbyte.cdk.test.util.IntegrationTest
import io.airbyte.cdk.test.util.OutputRecord
import io.airbyte.cdk.test.util.Record
import io.airbyte.cdk.test.util.StreamState
import io.airbyte.protocol.models.Jsons
import io.airbyte.protocol.models.v0.AirbyteConnectionStatus
import io.airbyte.protocol.models.v0.AirbyteMessage
import io.airbyte.protocol.models.v0.AirbyteStream
import io.airbyte.protocol.models.v0.ConfiguredAirbyteStream
import io.airbyte.protocol.models.v0.ConnectorSpecification
import io.airbyte.protocol.models.v0.DestinationSyncMode
import kotlin.test.assertEquals
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertAll

abstract class BasicFunctionalityIntegrationTest(
    val config: ConfigurationJsonObjectBase,
    dataDumper: DestinationDataDumper,
    recordMangler: DestinationRecordMangler,
    // TODO maybe pull this out of spec.json??
    val expectedSpec: ConnectorSpecification
) : IntegrationTest(dataDumper, recordMangler) {
    @Test
    open fun testSpec() {
        val process = destinationProcessFactory.createDestinationProcess("spec")
        process.run()
        val messages = process.readMessages()
        val specMessages = messages.filter { it.type == AirbyteMessage.Type.SPEC }

        assertEquals(
            specMessages.size,
            1,
            "Expected to receive exactly one spec message, but got ${specMessages.size}: $specMessages"
        )
        assertEquals(expectedSpec, specMessages.first().spec)
    }

    @Test
    open fun testCheck() {
        val process = destinationProcessFactory.createDestinationProcess("check", config = config)
        process.run()
        val messages = process.readMessages()
        val checkMessages = messages.filter { it.type == AirbyteMessage.Type.CONNECTION_STATUS }

        assertEquals(
            checkMessages.size,
            1,
            "Expected to receive exactly one connection status message, but got ${checkMessages.size}: $checkMessages"
        )
        assertEquals(
            AirbyteConnectionStatus.Status.SUCCEEDED,
            checkMessages.first().connectionStatus.status
        )
    }

    @Test
    open fun testBasicWrite() {
        val messages =
            runSync(
                config,
                // TODO switch to the convenient form
                //   (this is dependent on actually adding all the relevant fields
                //   to DestinationStream)
                // DestinationCatalog(listOf(
                //     DestinationStream(Descriptor("test_stream", randomizedNamespace))
                // )).toProtocolMessage(),
                 ConfiguredAirbyteStream()
                     .withDestinationSyncMode(DestinationSyncMode.APPEND)
                     .withGenerationId(0)
                     .withMinimumGenerationId(0)
                     .withSyncId(42)
                     .withStream(
                         AirbyteStream()
                             .withName("test_stream")
                             .withNamespace(randomizedNamespace)
                             .withJsonSchema(
                                 Jsons.deserialize(
                                     """
                                        {
                                          "type": "object",
                                          "properties": {
                                            "id": {"type": "integer"}
                                          }
                                        }
                                     """.trimIndent()
                                 )
                             )
                     ),
                listOf(
                    Record(
                        streamName = "test_stream",
                        streamNamespace = randomizedNamespace,
                        emittedAt = 1234,
                        data = """{"id": 5678}""",
                        changes = null,
                    ),
                    StreamState(
                        streamName = "test_stream",
                        streamNamespace = randomizedNamespace,
                        blob = """{"foo": "bar"}""",
                        sourceRecordCount = 1,
                    )
                )
            )

        val stateMessages = messages.filter { it.type == AirbyteMessage.Type.STATE }
        assertAll(
            {
                assertEquals(
                    1,
                    stateMessages.size,
                    "Expected to receive exactly one state message, got ${stateMessages.size} ($stateMessages)"
                )
                assertEquals(
                    StreamState(
                            streamName = "test_stream",
                            streamNamespace = randomizedNamespace,
                            blob = """{"foo": "bar"}""",
                            sourceRecordCount = 1,
                            destinationRecordCount = 1,
                        )
                        .asProtocolMessage(),
                    stateMessages.first()
                )
            },
            {
                // TODO get primary key / cursor - needs to be mangled to destination name
                dumpAndDiffRecords(
                    listOf(
                        OutputRecord(
                            extractedAt = 1234,
                            generationId = 0,
                            data = mapOf("id" to 5678),
                            airbyteMeta = """{"changes": []}"""
                        )
                    ),
                    "test_stream",
                    randomizedNamespace
                )
            },
        )
    }
}
