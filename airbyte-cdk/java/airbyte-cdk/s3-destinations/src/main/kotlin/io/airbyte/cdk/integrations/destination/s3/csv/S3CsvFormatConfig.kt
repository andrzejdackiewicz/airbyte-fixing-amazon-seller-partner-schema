/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */
package io.airbyte.cdk.integrations.destination.s3.csv

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.cdk.integrations.destination.s3.S3DestinationConstants
import io.airbyte.cdk.integrations.destination.s3.S3Format
import io.airbyte.cdk.integrations.destination.s3.S3FormatConfig
import io.airbyte.cdk.integrations.destination.s3.util.CompressionType
import io.airbyte.cdk.integrations.destination.s3.util.CompressionTypeHelper
import io.airbyte.cdk.integrations.destination.s3.util.Flattening
import io.airbyte.cdk.integrations.destination.s3.util.Flattening.Companion.fromValue
import java.util.*

class S3CsvFormatConfig(val flattening: Flattening, val compressionType: CompressionType) :
    S3FormatConfig {
    constructor(
        formatConfig: JsonNode
    ) : this(
        fromValue(
            if (formatConfig.has("flattening")) formatConfig["flattening"].asText()
            else Flattening.NO.value
        ),
        if (formatConfig.has(S3DestinationConstants.COMPRESSION_ARG_NAME))
            CompressionTypeHelper.parseCompressionType(
                formatConfig[S3DestinationConstants.COMPRESSION_ARG_NAME]
            )
        else S3DestinationConstants.DEFAULT_COMPRESSION_TYPE
    )

    override val format: S3Format = S3Format.CSV

    override val fileExtension: String = CSV_SUFFIX + compressionType.fileExtension

    override fun toString(): String {
        return "S3CsvFormatConfig{" +
            "flattening=" +
            flattening +
            ", compression=" +
            compressionType!!.name +
            '}'
    }

    override fun equals(o: Any?): Boolean {
        if (this === o) {
            return true
        }
        if (o == null || javaClass != o.javaClass) {
            return false
        }
        val that = o as S3CsvFormatConfig
        return flattening == that.flattening && compressionType == that.compressionType
    }

    override fun hashCode(): Int {
        return Objects.hash(flattening, compressionType)
    }

    companion object {
        const val CSV_SUFFIX: String = ".csv"
    }
}
