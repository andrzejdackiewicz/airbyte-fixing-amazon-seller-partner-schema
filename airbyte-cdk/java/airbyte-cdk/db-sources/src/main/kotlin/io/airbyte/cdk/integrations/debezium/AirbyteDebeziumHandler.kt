/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */
package io.airbyte.cdk.integrations.debezium

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.cdk.db.jdbc.JdbcUtils
import io.airbyte.cdk.integrations.debezium.internals.*
import io.airbyte.commons.util.AutoCloseableIterator
import io.airbyte.commons.util.AutoCloseableIterators
import io.airbyte.protocol.models.v0.AirbyteMessage
import io.airbyte.protocol.models.v0.ConfiguredAirbyteCatalog
import io.airbyte.protocol.models.v0.ConfiguredAirbyteStream
import io.airbyte.protocol.models.v0.SyncMode
import io.debezium.engine.ChangeEvent
import io.debezium.engine.DebeziumEngine
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.time.Duration
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.*
import java.util.concurrent.LinkedBlockingQueue

/**
 * This class acts as the bridge between Airbyte DB connectors and debezium. If a DB connector wants
 * to use debezium for CDC, it should use this class
 */
class AirbyteDebeziumHandler<T>(private val config: JsonNode,
                                private val targetPosition: CdcTargetPosition<T>,
                                private val trackSchemaHistory: Boolean,
                                private val firstRecordWaitTime: Duration,
                                private val subsequentRecordWaitTime: Duration,
                                private val queueSize: Int,
                                private val addDbNameToOffsetState: Boolean) {
    internal inner class CapacityReportingBlockingQueue<E>(capacity: Int) : LinkedBlockingQueue<E>(capacity) {
        private var lastReport: Instant? = null

        private fun reportQueueUtilization() {
            if (lastReport == null || Duration.between(lastReport, Instant.now()).compareTo(Companion.REPORT_DURATION) > 0) {
                LOGGER.info("CDC events queue size: {}. remaining {}", this.size, this.remainingCapacity())
                synchronized(this) {
                    lastReport = Instant.now()
                }
            }
        }

        @Throws(InterruptedException::class)
        override fun put(e: E) {
            reportQueueUtilization()
            super.put(e)
        }

        override fun poll(): E {
            reportQueueUtilization()
            return super.poll()
        }

        companion object {
            private val REPORT_DURATION: Duration = Duration.of(10, ChronoUnit.SECONDS)
        }
    }

    fun getIncrementalIterators(debeziumPropertiesManager: DebeziumPropertiesManager,
                                eventConverter: DebeziumEventConverter,
                                cdcSavedInfoFetcher: CdcSavedInfoFetcher,
                                cdcStateHandler: CdcStateHandler): AutoCloseableIterator<AirbyteMessage> {
        LOGGER.info("Using CDC: {}", true)
        LOGGER.info("Using DBZ version: {}", DebeziumEngine::class.java.getPackage().implementationVersion)
        val offsetManager: AirbyteFileOffsetBackingStore = AirbyteFileOffsetBackingStore.Companion.initializeState(
                cdcSavedInfoFetcher.savedOffset,
                if (addDbNameToOffsetState) Optional.ofNullable<String>(config[JdbcUtils.DATABASE_KEY].asText()) else Optional.empty<String>())
        val schemaHistoryManager: Optional<AirbyteSchemaHistoryStorage?> = if (trackSchemaHistory
        ) Optional.of<AirbyteSchemaHistoryStorage?>(AirbyteSchemaHistoryStorage.Companion.initializeDBHistory(
                cdcSavedInfoFetcher.savedSchemaHistory, cdcStateHandler.compressSchemaHistoryForState()))
        else Optional.empty<AirbyteSchemaHistoryStorage>()
        val publisher = DebeziumRecordPublisher(debeziumPropertiesManager)
        val queue: CapacityReportingBlockingQueue<ChangeEvent<String?, String?>> = CapacityReportingBlockingQueue<ChangeEvent<String, String>>(queueSize)
        publisher.start(queue, offsetManager, schemaHistoryManager)
        // handle state machine around pub/sub logic.
        val eventIterator: AutoCloseableIterator<ChangeEventWithMetadata> = DebeziumRecordIterator(
                queue,
                targetPosition,
                { publisher.hasClosed() },
                DebeziumShutdownProcedure(queue, { publisher.close() }, { publisher.hasClosed() }),
                firstRecordWaitTime,
                subsequentRecordWaitTime)

        val syncCheckpointDuration = if (config.has(DebeziumIteratorConstants.SYNC_CHECKPOINT_DURATION_PROPERTY)
        ) Duration.ofSeconds(config[DebeziumIteratorConstants.SYNC_CHECKPOINT_DURATION_PROPERTY].asLong())
        else DebeziumIteratorConstants.SYNC_CHECKPOINT_DURATION
        val syncCheckpointRecords = if (config.has(DebeziumIteratorConstants.SYNC_CHECKPOINT_RECORDS_PROPERTY)
        ) config[DebeziumIteratorConstants.SYNC_CHECKPOINT_RECORDS_PROPERTY].asLong()
        else DebeziumIteratorConstants.SYNC_CHECKPOINT_RECORDS.toLong()
        return AutoCloseableIterators.fromIterator(DebeziumStateDecoratingIterator(
                eventIterator,
                cdcStateHandler,
                targetPosition,
                eventConverter,
                offsetManager,
                trackSchemaHistory,
                schemaHistoryManager.orElse(null),
                syncCheckpointDuration,
                syncCheckpointRecords))
    }

    companion object {
        private val LOGGER: Logger = LoggerFactory.getLogger(AirbyteDebeziumHandler::class.java)

        /**
         * We use 10000 as capacity cause the default queue size and batch size of debezium is :
         * [io.debezium.config.CommonConnectorConfig.DEFAULT_MAX_BATCH_SIZE]is 2048
         * [io.debezium.config.CommonConnectorConfig.DEFAULT_MAX_QUEUE_SIZE] is 8192
         */
        const val QUEUE_CAPACITY: Int = 10000

        fun isAnyStreamIncrementalSyncMode(catalog: ConfiguredAirbyteCatalog): Boolean {
            return catalog.streams.stream().map { obj: ConfiguredAirbyteStream -> obj.syncMode }
                    .anyMatch { syncMode: SyncMode -> syncMode == SyncMode.INCREMENTAL }
        }
    }
}
