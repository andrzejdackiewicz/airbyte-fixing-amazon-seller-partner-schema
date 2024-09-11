/*
 * Copyright (c) 2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.cdk.message

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.cdk.command.DestinationCatalog
import io.airbyte.cdk.command.DestinationStream
import io.airbyte.protocol.models.v0.AirbyteMessage
import io.airbyte.protocol.models.v0.AirbyteRecordMessage
import io.airbyte.protocol.models.v0.AirbyteStateMessage
import io.airbyte.protocol.models.v0.AirbyteStreamState
import io.airbyte.protocol.models.v0.AirbyteStreamStatusTraceMessage
import io.airbyte.protocol.models.v0.AirbyteStreamStatusTraceMessage.AirbyteStreamStatus
import io.airbyte.protocol.models.v0.AirbyteTraceMessage
import io.airbyte.protocol.models.v0.StreamDescriptor
import jakarta.inject.Singleton

/**
 * Internal representation of destination messages. These are intended to be specialized for
 * usability. Data should be marshalled to these from frontline deserialized objects.
 */
sealed interface DestinationMessage {
    fun asProtocolMessage(): AirbyteMessage
}

/** Records. */
sealed interface DestinationStreamAffinedMessage : DestinationMessage {
    val stream: DestinationStream
}

data class DestinationRecord(
    override val stream: DestinationStream,
    val data: JsonNode? = null,
    val emittedAtMs: Long,
    val serialized: String
) : DestinationStreamAffinedMessage {
    override fun asProtocolMessage(): AirbyteMessage =
        AirbyteMessage()
            .withType(AirbyteMessage.Type.RECORD)
            .withRecord(
                AirbyteRecordMessage()
                    .withStream(stream.descriptor.name)
                    .withNamespace(stream.descriptor.namespace)
                    .withEmittedAt(emittedAtMs)
                    .withData(data)
            )
}

data class DestinationEndOfStream(
    override val stream: DestinationStream,
    val emittedAtMs: Long,
    val status: AirbyteStreamStatus,
) : DestinationStreamAffinedMessage {
    override fun asProtocolMessage(): AirbyteMessage =
        AirbyteMessage()
            .withType(AirbyteMessage.Type.TRACE)
            .withTrace(
                AirbyteTraceMessage()
                    .withType(AirbyteTraceMessage.Type.STREAM_STATUS)
                    .withEmittedAt(emittedAtMs.toDouble())
                    .withStreamStatus(
                        AirbyteStreamStatusTraceMessage()
                            .withStreamDescriptor(
                                StreamDescriptor()
                                    .withName(stream.descriptor.name)
                                    .withNamespace(stream.descriptor.namespace)
                            )
                            .withStatus(status)
                    )
            )
}

/** State. */
sealed class CheckpointMessage : DestinationMessage {
    data class Stats(val recordCount: Long)
    data class StreamCheckpoint(
        val stream: DestinationStream,
        val state: JsonNode,
    )

    abstract val sourceStats: Stats
    abstract val destinationStats: Stats?

    abstract fun withDestinationStats(stats: Stats): CheckpointMessage
}

data class StreamCheckpoint(
    val streamCheckpoint: StreamCheckpoint,
    override val sourceStats: Stats,
    override val destinationStats: Stats? = null
) : CheckpointMessage() {
    override fun withDestinationStats(stats: Stats) =
        StreamCheckpoint(streamCheckpoint, sourceStats, stats)
}

data class GlobalCheckpoint(
    val state: JsonNode,
    override val sourceStats: Stats,
    override val destinationStats: Stats? = null,
    val streamCheckpoints: List<StreamCheckpoint> = emptyList()
) : CheckpointMessage() {
    override fun withDestinationStats(stats: Stats) =
        GlobalCheckpoint(state, sourceStats, stats, streamCheckpoints)
}

/** Catchall for anything unimplemented. */
data object Undefined : DestinationMessage

@Singleton
class DestinationMessageFactory(private val catalog: DestinationCatalog) {
    fun fromAirbyteMessage(message: AirbyteMessage, serialized: String): DestinationMessage {
        return when (message.type) {
            AirbyteMessage.Type.RECORD ->
                DestinationRecord(
                    stream =
                        catalog.getStream(
                            namespace = message.record.namespace,
                            name = message.record.stream,
                        ),
                    // TODO: Map to AirbyteType
                    data = message.record.data,
                    emittedAtMs = message.record.emittedAt,
                    serialized = serialized
                )
            AirbyteMessage.Type.TRACE -> {
                val status = message.trace.streamStatus
                val stream =
                    catalog.getStream(
                        namespace = status.streamDescriptor.namespace,
                        name = status.streamDescriptor.name,
                    )
                if (
                    message.trace.type == AirbyteTraceMessage.Type.STREAM_STATUS &&
                        status.status == AirbyteStreamStatus.COMPLETE
                ) {
                    DestinationEndOfStream(stream, message.trace.emittedAt.toLong())
                } else {
                    DestinationStreamIncomplete(stream, message.trace.emittedAt.toLong())
                }
            }
            AirbyteMessage.Type.STATE -> {
                when (message.state.type) {
                    AirbyteStateMessage.AirbyteStateType.STREAM ->
                        StreamCheckpoint(
                            streamCheckpoint = fromAirbyteStreamState(message.state.stream),
                            sourceStats =
                                CheckpointMessage.Stats(
                                    recordCount = message.state.sourceStats.recordCount.toLong()
                                )
                        )
                    AirbyteStateMessage.AirbyteStateType.GLOBAL ->
                        GlobalCheckpoint(
                            sourceStats =
                                CheckpointMessage.Stats(
                                    recordCount = message.state.sourceStats.recordCount.toLong()
                                ),
                            state = message.state.global.sharedState,
                            streamCheckpoints =
                                message.state.global.streamStates.map { fromAirbyteStreamState(it) }
                        )
                    else -> // TODO: Do we still need to handle LEGACY?
                    Undefined
                }
            }
            else -> Undefined
        }
    }

    private fun fromAirbyteStreamState(
        streamState: AirbyteStreamState
    ): CheckpointMessage.StreamCheckpoint {
        val descriptor = streamState.streamDescriptor
        return CheckpointMessage.StreamCheckpoint(
            stream = catalog.getStream(namespace = descriptor.namespace, name = descriptor.name),
            state = streamState.streamState
        )
    }
}
