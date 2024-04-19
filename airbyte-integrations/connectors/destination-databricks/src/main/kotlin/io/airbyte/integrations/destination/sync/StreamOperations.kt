package io.airbyte.integrations.destination.sync

import io.airbyte.cdk.integrations.destination.StreamSyncSummary
import io.airbyte.cdk.integrations.destination.async.model.PartialAirbyteMessage
import io.airbyte.integrations.base.destination.typing_deduping.DestinationInitialStatus
import io.airbyte.protocol.models.v0.StreamDescriptor
import java.util.stream.Stream

/**
 * Operations on individual streams.
 */
interface StreamOperations<T> {

    fun initialize(destinationInitialStatus: DestinationInitialStatus<T>)

    fun writeRecords(descriptor: StreamDescriptor, stream: Stream<PartialAirbyteMessage>)

    fun finalizeTable(descriptor: StreamDescriptor, syncSummary: StreamSyncSummary)

}
