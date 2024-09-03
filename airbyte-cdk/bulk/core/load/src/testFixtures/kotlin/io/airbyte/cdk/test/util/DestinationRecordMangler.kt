package io.airbyte.cdk.test.util

import org.junit.jupiter.api.TestInfo

fun interface DestinationRecordMangler {
    fun mangleRecord(expectedRecord: OutputRecord, testInfo: TestInfo): OutputRecord
}

class NoopDestinationRecordMangler : DestinationRecordMangler {
    override fun mangleRecord(expectedRecord: OutputRecord, testInfo: TestInfo): OutputRecord =
        expectedRecord
}
