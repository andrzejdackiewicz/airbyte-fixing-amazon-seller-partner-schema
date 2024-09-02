/* Copyright (c) 2024 Airbyte, Inc., all rights reserved. */
package io.airbyte.cdk.discover

import io.airbyte.cdk.data.AirbyteType
import io.airbyte.cdk.data.JsonDecoder
import io.airbyte.cdk.data.JsonEncoder
import io.airbyte.cdk.data.JsonStringCodec
import io.airbyte.cdk.data.LeafAirbyteType
import io.airbyte.cdk.data.OffsetDateTimeCodec
import java.time.OffsetDateTime

/** Internal equivalent of a [io.airbyte.protocol.models.Field]. */
sealed interface FieldOrMetaField {
    val id: String
    val type: FieldType
}

/**
 * Root of our own type hierarchy for Airbyte record fields.
 *
 * Connectors may define their own concrete implementations.
 */
interface FieldType {
    /** maps to [io.airbyte.protocol.models.Field.type] */
    val airbyteType: AirbyteType
    val jsonEncoder: JsonEncoder<*>
}

/**
 * Subtype of [FieldType] for all [FieldType]s whose Airbyte record values can be turned back into
 * their original source values. This allows these values to be persisted in an Airbyte state
 * message.
 *
 * Connectors may define their own concrete implementations.
 */
interface LosslessFieldType : FieldType {
    val jsonDecoder: JsonDecoder<*>
}

/**
 * Internal equivalent of [io.airbyte.protocol.models.Field] for values which come from the source
 * itself, instead of being generated by the connector during its operation.
 */
data class Field(
    override val id: String,
    override val type: FieldType,
) : FieldOrMetaField

/**
 * Internal equivalent of [io.airbyte.protocol.models.Field] for values which are generated by the
 * connector itself during its operation, instead of coming from the source.
 */
interface MetaField : FieldOrMetaField {
    companion object {
        const val META_PREFIX = "_ab_"

        fun isMetaFieldID(id: String): Boolean = id.startsWith("_ab_")
    }
}

/** Convenience enum listing the [MetaField]s which are generated by all connectors. */
enum class CommonMetaField(
    override val type: FieldType,
) : MetaField {
    CDC_LSN(CdcStringMetaFieldType),
    CDC_UPDATED_AT(CdcOffsetDateTimeMetaFieldType),
    CDC_DELETED_AT(CdcOffsetDateTimeMetaFieldType),
    ;

    override val id: String
        get() = MetaField.META_PREFIX + name.lowercase()
}

data object CdcStringMetaFieldType : LosslessFieldType {
    override val airbyteType: AirbyteType = LeafAirbyteType.STRING
    override val jsonEncoder: JsonEncoder<String> = JsonStringCodec
    override val jsonDecoder: JsonDecoder<String> = JsonStringCodec
}

data object CdcOffsetDateTimeMetaFieldType : LosslessFieldType {
    override val airbyteType: AirbyteType = LeafAirbyteType.TIMESTAMP_WITH_TIMEZONE
    override val jsonEncoder: JsonEncoder<OffsetDateTime> = OffsetDateTimeCodec
    override val jsonDecoder: JsonDecoder<OffsetDateTime> = OffsetDateTimeCodec
}
