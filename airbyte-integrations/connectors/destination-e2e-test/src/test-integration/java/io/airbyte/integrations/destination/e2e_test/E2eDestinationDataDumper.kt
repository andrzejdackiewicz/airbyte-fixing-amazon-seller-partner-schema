/*
 * Copyright (c) 2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.e2e_test

import io.airbyte.cdk.test.util.DestinationDataDumper
import io.airbyte.cdk.test.util.OutputRecord
import org.junit.jupiter.api.TestInfo

class E2eDestinationDataDumper : DestinationDataDumper {
    override fun dumpRecords(
        streamName: String,
        streamNamespace: String?,
        testInfo: TestInfo
    ): List<OutputRecord> {
        // TODO delete this
        return listOf(
            OutputRecord(
                extractedAt = 1234,
                generationId = 0,
                data = mapOf("id" to 5678),
                airbyteMeta = """{"changes": []}""",
            ),
        )
        // E2e destination doesn't actually write records, so we shouldn't even
        // have tests that try to read back the records
        throw NotImplementedError()
    }
}
