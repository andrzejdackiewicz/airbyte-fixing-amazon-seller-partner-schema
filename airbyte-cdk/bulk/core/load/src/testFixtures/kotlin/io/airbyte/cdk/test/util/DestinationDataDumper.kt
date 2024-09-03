package io.airbyte.cdk.test.util

import org.junit.jupiter.api.TestInfo

fun interface DestinationDataDumper {
    // TODO we probably should compact this pair into a useful class
    //   (but not StreamDescriptor/AirbyteStreamNameNamespacePair :P )
    fun dumpRecords(
        streamName: String,
        streamNamespace: String?,
        testInfo: TestInfo
    ): List<OutputRecord>
}
