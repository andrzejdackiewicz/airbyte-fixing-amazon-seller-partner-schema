/*
 * Copyright (c) 2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.e2e_test

import com.fasterxml.jackson.databind.ObjectMapper
import io.airbyte.cdk.test.util.NoopDestinationRecordMangler
import io.airbyte.cdk.test.write.BasicFunctionalityIntegrationTest
import io.airbyte.protocol.models.v0.ConnectorSpecification
import java.net.URI
import org.junit.jupiter.api.Test

class E2eBasicFunctionalityIntegrationTest :
    BasicFunctionalityIntegrationTest(
        E2eTestUtils.loggingConfig,
        E2eDestinationDataDumper(),
        NoopDestinationRecordMangler(),
        // TODO don't just hardcode this
        ConnectorSpecification()
            .withDocumentationUrl(URI("https://docs.airbyte.com/integrations/destinations/e2e-test"))
            .withSupportsNormalization(false)
            .withSupportsDBT(false)
            // TODO WTF?
            .withSupportedDestinationSyncModes(emptyList())
            // TODO this definitely isn't the right way to do this, shove this into a file
            .withConnectionSpecification(
                ObjectMapper()
                    .readTree("""
                    {"${'$'}schema":"http://json-schema.org/draft-07/schema#","title":"E2E Test Destination Spec","type":"object","additionalProperties":true,"properties":{"test_destination":{"oneOf":[{"type":"object","additionalProperties":true,"properties":{"test_destination_type":{"type":"string","enum":["LOGGING"],"default":"LOGGING"},"logging_config":{"oneOf":[{"type":"object","additionalProperties":true,"properties":{"logging_type":{"type":"string","enum":["FirstN"],"default":"FirstN"},"max_entry_count":{"type":"integer"}},"title":"FirstN","required":["logging_type","max_entry_count"]},{"type":"object","additionalProperties":true,"properties":{"logging_type":{"type":"string","enum":["EveryNth"],"default":"EveryNth"},"nth_entry_to_log":{"type":"integer"},"max_entry_count":{"type":"integer"}},"title":"EveryNth","required":["logging_type","nth_entry_to_log","max_entry_count"]},{"type":"object","additionalProperties":true,"properties":{"logging_type":{"type":"string","enum":["RandomSampling"],"default":"RandomSampling"},"sampling_ratio":{"type":"number"},"seed":{"type":"integer"},"max_entry_count":{"type":"integer"}},"title":"RandomSampling","required":["logging_type","sampling_ratio","max_entry_count"]}],"type":"object"}},"title":"LOGGING","required":["test_destination_type","logging_config"]},{"type":"object","additionalProperties":true,"properties":{"test_destination_type":{"type":"string","enum":["SILENT"],"default":"SILENT"}},"title":"SILENT","required":["test_destination_type"]},{"type":"object","additionalProperties":true,"properties":{"test_destination_type":{"type":"string","enum":["THROTTLED"],"default":"THROTTLED"},"millis_per_record":{"type":"integer"}},"title":"THROTTLED","required":["test_destination_type","millis_per_record"]},{"type":"object","additionalProperties":true,"properties":{"test_destination_type":{"type":"string","enum":["FAILING"],"default":"FAILING"},"num_messages":{"type":"integer"}},"title":"FAILING","required":["test_destination_type","num_messages"]}],"description":"The type of destination to be used.","title":"Test Destination","type":"object"},"record_batch_size_bytes":{"type":"integer","description":"The maximum amount of record data to stage before processing.","title":"Record Batch Size Bytes"}},"required":["test_destination","record_batch_size_bytes"]}
                    """.trimIndent())
            )
    ) {
    @Test
    override fun testSpec() {
        super.testSpec()
    }

    @Test
    override fun testCheck() {
        super.testCheck()
    }

    //    @Disabled("this destination doesn't actually write any data, so disable the write smoke
    // test")
    @Test
    override fun testBasicWrite() {
        super.testBasicWrite()
    }
}
