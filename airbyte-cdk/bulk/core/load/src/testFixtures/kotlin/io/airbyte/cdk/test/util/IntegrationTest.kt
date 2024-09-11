/*
 * Copyright (c) 2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.cdk.test.util

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.cdk.command.ConfigurationJsonObjectBase
import io.airbyte.cdk.message.DestinationMessage
import io.airbyte.protocol.models.Jsons
import io.airbyte.protocol.models.v0.AirbyteGlobalState
import io.airbyte.protocol.models.v0.AirbyteMessage
import io.airbyte.protocol.models.v0.AirbyteRecordMessage
import io.airbyte.protocol.models.v0.AirbyteRecordMessageMeta
import io.airbyte.protocol.models.v0.AirbyteStateMessage
import io.airbyte.protocol.models.v0.AirbyteStateStats
import io.airbyte.protocol.models.v0.AirbyteStreamState
import io.airbyte.protocol.models.v0.AirbyteStreamStatusTraceMessage
import io.airbyte.protocol.models.v0.AirbyteStreamStatusTraceMessage.AirbyteStreamStatus
import io.airbyte.protocol.models.v0.AirbyteTraceMessage
import io.airbyte.protocol.models.v0.ConfiguredAirbyteCatalog
import io.airbyte.protocol.models.v0.ConfiguredAirbyteStream
import io.airbyte.protocol.models.v0.StreamDescriptor
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import kotlinx.coroutines.async
import kotlinx.coroutines.runBlocking
import org.apache.commons.lang3.RandomStringUtils
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.TestInfo
import org.junit.jupiter.api.parallel.Execution
import org.junit.jupiter.api.parallel.ExecutionMode
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import javax.inject.Inject
import kotlin.test.fail

@MicronautTest
@Execution(ExecutionMode.CONCURRENT)
abstract class IntegrationTest(
    val dataDumper: DestinationDataDumper,
    val recordMangler: DestinationRecordMangler = DestinationRecordMangler { record, _ -> record },
) {
    // Intentionally don't inject the actual destination process - we need a full factory
    // because some tests want to run multiple syncs, so we need to run the destination
    // multiple times.
    @Inject lateinit var destinationProcessFactory: DestinationProcessFactory

    private val randomSuffix = RandomStringUtils.randomAlphabetic(4)
    private val timestampString =
        LocalDateTime.ofInstant(Instant.now(), ZoneOffset.UTC)
            .format(DateTimeFormatter.ofPattern("YYYYMMDD"))
    // stream name doesn't need to be randomized, only the namespace.
    val randomizedNamespace = "test$timestampString$randomSuffix"

    /** This field is not initialized until after the constructor is called. */
    lateinit var testInfo: TestInfo
    @BeforeEach
    fun setup(testInfo: TestInfo) {
        this.testInfo = testInfo
    }

    fun dumpAndDiffRecords(
        canonicalExpectedRecords: List<OutputRecord>,
        streamName: String,
        streamNamespace: String?,
        extractPrimaryKey: (OutputRecord) -> List<Any?> = { emptyList() },
        extractCursor: ((OutputRecord) -> Any?)? = null,
    ) {
        val actualRecords: List<OutputRecord> =
            dataDumper.dumpRecords(streamName, streamNamespace, testInfo)
        val expectedRecords: List<OutputRecord> =
            canonicalExpectedRecords.map { recordMangler.mangleRecord(it, testInfo) }

        RecordDiffer(extractPrimaryKey, extractCursor)
            .diffRecords(expectedRecords, actualRecords)
            ?.let(::fail)
    }

    fun runSync(
        config: ConfigurationJsonObjectBase,
        stream: ConfiguredAirbyteStream,
        messages: List<DestinationMessage>,
        streamStatus: AirbyteStreamStatus? = AirbyteStreamStatus.COMPLETE,
    ): List<AirbyteMessage> =
        runSync(
            config,
            ConfiguredAirbyteCatalog().withStreams(listOf(stream)),
            messages,
            streamStatus
        )

    fun runSync(
        config: ConfigurationJsonObjectBase,
        catalog: ConfiguredAirbyteCatalog,
        messages: List<DestinationMessage>,
        streamStatus: AirbyteStreamStatus? = AirbyteStreamStatus.COMPLETE,
    ): List<AirbyteMessage> {
        val destination =
            destinationProcessFactory.createDestinationProcess(
                "write",
                config,
                catalog,
            )
        return runBlocking {
            val destinationCompletion = async { destination.run() }
            messages.forEach { destination.sendMessage(it.asProtocolMessage()) }
            if (streamStatus != null) {
                catalog.streams.forEach {
                    destination.sendMessage(
                        AirbyteMessage()
                            .withType(AirbyteMessage.Type.TRACE)
                            .withTrace(
                                AirbyteTraceMessage()
                                    .withType(AirbyteTraceMessage.Type.STREAM_STATUS)
                                    .withEmittedAt(System.currentTimeMillis().toDouble())
                                    .withStreamStatus(
                                        AirbyteStreamStatusTraceMessage()
                                            .withStreamDescriptor(
                                                StreamDescriptor()
                                                    .withName(it.stream.name)
                                                    .withNamespace(it.stream.namespace)
                                            )
                                            .withStatus(streamStatus)
                                    )
                            )
                    )
                }
            }
            destination.shutdown()
            destinationCompletion.await()
            destination.readMessages()
        }
    }
}

// TODO make these better (+ probably move them to a separate file)
interface Message {
    fun asProtocolMessage(): AirbyteMessage
}

data class Record(
    val streamName: String,
    val streamNamespace: String? = null,
    val emittedAt: Long,
    val data: JsonNode,
    val airbyteMeta: AirbyteRecordMessageMeta?,
) : Message {
    constructor(
        streamName: String,
        streamNamespace: String? = null,
        emittedAt: Long,
        data: String,
        changes: String?,
    ) : this(
        streamName,
        streamNamespace,
        emittedAt,
        Jsons.deserialize(data),
        changes?.let { Jsons.deserialize(it, AirbyteRecordMessageMeta::class.java) }
    )

    override fun asProtocolMessage(): AirbyteMessage {
        val record =
            AirbyteRecordMessage()
                .withStream(streamName)
                .withNamespace(streamNamespace)
                .withEmittedAt(emittedAt)
                .withData(data)
                .apply {
                    if (airbyteMeta != null) {
                        meta = airbyteMeta
                    }
                }
        val message = AirbyteMessage().withType(AirbyteMessage.Type.RECORD).withRecord(record)
        return message
    }
}

sealed interface State : Message

data class StreamState(
    val streamName: String,
    val streamNamespace: String? = null,
    val blob: JsonNode,
    val sourceRecordCount: Int,
    val destinationRecordCount: Int? = null,
) : State {
    constructor(
        streamName: String,
        streamNamespace: String? = null,
        blob: String,
        sourceRecordCount: Int,
        destinationRecordCount: Int? = null,
    ) : this(
        streamName,
        streamNamespace,
        Jsons.deserialize(blob),
        sourceRecordCount,
        destinationRecordCount,
    )

    override fun asProtocolMessage(): AirbyteMessage {
        val stateMessage =
            AirbyteStateMessage()
                .withType(AirbyteStateMessage.AirbyteStateType.STREAM)
                .withStream(
                    AirbyteStreamState()
                        .withStreamDescriptor(
                            StreamDescriptor().withName(streamName).withNamespace(streamNamespace)
                        )
                        .withStreamState(blob)
                )
                .withSourceStats(AirbyteStateStats().withRecordCount(sourceRecordCount.toDouble()))
                .apply {
                    if (destinationRecordCount != null) {
                        destinationStats =
                            AirbyteStateStats().withRecordCount(destinationRecordCount.toDouble())
                    }
                }
        return AirbyteMessage().withType(AirbyteMessage.Type.STATE).withState(stateMessage)
    }
}

data class GlobalState(
    val blob: JsonNode,
    val sourceRecordCount: Int,
    val destinationRecordCount: Int? = null
) : State {
    constructor(
        blob: String,
        sourceRecordCount: Int,
        destinationRecordCount: Int? = null,
    ) : this(
        Jsons.deserialize(blob),
        sourceRecordCount,
        destinationRecordCount,
    )

    override fun asProtocolMessage(): AirbyteMessage {
        val stateMessage =
            AirbyteStateMessage()
                .withType(AirbyteStateMessage.AirbyteStateType.GLOBAL)
                .withGlobal(AirbyteGlobalState().withSharedState(blob))
                .withSourceStats(AirbyteStateStats().withRecordCount(sourceRecordCount.toDouble()))
                .apply {
                    if (destinationRecordCount != null) {
                        destinationStats =
                            AirbyteStateStats().withRecordCount(destinationRecordCount.toDouble())
                    }
                }
        return AirbyteMessage().withType(AirbyteMessage.Type.STATE).withState(stateMessage)
    }
}
