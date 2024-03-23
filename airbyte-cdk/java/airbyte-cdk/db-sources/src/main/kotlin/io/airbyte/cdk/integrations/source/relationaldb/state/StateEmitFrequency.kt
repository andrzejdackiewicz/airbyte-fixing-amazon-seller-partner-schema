/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */
package io.airbyte.cdk.integrations.source.relationaldb.state

import java.time.Duration

class StateEmitFrequency(syncCheckpointRecords: Long, syncCheckpointDuration: Duration) {
    val syncCheckpointRecords: Long
    val syncCheckpointDuration: Duration

    init {
        this.streamName = streamName
        this.primaryKey = primaryKey
        this.keySequence = keySequence
        this.syncCheckpointRecords = syncCheckpointRecords
        this.syncCheckpointDuration = syncCheckpointDuration
    }
}
