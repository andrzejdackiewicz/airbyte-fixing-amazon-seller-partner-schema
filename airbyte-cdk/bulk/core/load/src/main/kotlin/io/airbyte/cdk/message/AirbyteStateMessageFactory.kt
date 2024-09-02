/*
 * Copyright (c) 2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.cdk.message

import io.airbyte.protocol.models.v0.AirbyteGlobalState
import io.airbyte.protocol.models.v0.AirbyteStateMessage
import io.airbyte.protocol.models.v0.AirbyteStateStats
import io.airbyte.protocol.models.v0.AirbyteStreamState
import io.airbyte.protocol.models.v0.StreamDescriptor
import jakarta.inject.Singleton

/**
 * Converts the internal @[DestinationStateMessage] case class to the Protocol state messages
 * required by @[io.airbyte.cdk.output.OutputConsumer]
 */
interface AirbyteStateMessageFactory {
    fun fromDestinationStateMessage(message: DestinationStateMessage): AirbyteStateMessage
}

@Singleton
class DefaultAirbyteStateMessageFactory : AirbyteStateMessageFactory {
    override fun fromDestinationStateMessage(
        message: DestinationStateMessage
    ): AirbyteStateMessage {
        return when (message) {
            is DestinationStreamState ->
                AirbyteStateMessage()
                    .withSourceStats(
                        AirbyteStateStats()
                            .withRecordCount(message.sourceStats.recordCount.toDouble())
                    )
                    .withDestinationStats(
                        message.destinationStats?.let {
                            AirbyteStateStats().withRecordCount(it.recordCount.toDouble())
                        }
                            ?: throw IllegalStateException(
                                "Destination stats must be provided for DestinationStreamState"
                            )
                    )
                    .withType(AirbyteStateMessage.AirbyteStateType.STREAM)
                    .withStream(fromStreamState(message.streamState))
            is DestinationGlobalState ->
                AirbyteStateMessage()
                    .withSourceStats(
                        AirbyteStateStats()
                            .withRecordCount(message.sourceStats.recordCount.toDouble())
                    )
                    .withDestinationStats(
                        message.destinationStats?.let {
                            AirbyteStateStats().withRecordCount(it.recordCount.toDouble())
                        }
                    )
                    .withType(AirbyteStateMessage.AirbyteStateType.GLOBAL)
                    .withGlobal(
                        AirbyteGlobalState()
                            .withSharedState(message.state)
                            .withStreamStates(message.streamStates.map { fromStreamState(it) })
                    )
        }
    }

    private fun fromStreamState(
        streamState: DestinationStateMessage.StreamState
    ): AirbyteStreamState {
        return AirbyteStreamState()
            .withStreamDescriptor(
                StreamDescriptor()
                    .withNamespace(streamState.stream.descriptor.namespace)
                    .withName(streamState.stream.descriptor.name)
            )
            .withStreamState(streamState.state)
    }
}
