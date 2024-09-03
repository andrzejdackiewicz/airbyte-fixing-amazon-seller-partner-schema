/*
 * Copyright (c) 2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.e2e_test

import io.airbyte.cdk.command.ValidatedJsonUtils

object E2eTestUtils {
    /*
     * Most destinations probably want a function to randomize the config:
     * fun getS3StagingConfig(randomizedNamespace: String) {
     *   return baseConfig.withDefaultNamespace(randomizedNamespace)
     * }
     * but destination-e2e doesn't actually _do_ anything, so we can just
     * use a constant config
     */
    // TODO why does this work but Jsons.deserialize() doesn't?
    //   and is record_batch_size_bytes actually required now?
    val loggingConfig: E2EDestinationConfigurationJsonObject =
        ValidatedJsonUtils.parseOne(
            E2EDestinationConfigurationJsonObject::class.java,
            """
            {
              "test_destination": {
                "test_destination_type": "LOGGING",
                "logging_config": {
                  "logging_type": "FirstN",
                  "max_entry_count": 100
                }
              },
              "record_batch_size_bytes": 1048576
            }
        """.trimIndent(),
        )
}
