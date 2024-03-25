/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */
package io.airbyte.cdk.integrations.destination.jdbc

import io.airbyte.integrations.base.destination.typing_deduping.StreamConfig.columns

/**
 * Jdbc destination column definition representation
 *
 * @param name
 * @param type
 * @param columnSize
 */
class ColumnDefinition(name: String, type: String, columnSize: Int, isNullable: Boolean) {
    val name: String
    val type: String
    val columnSize: Int
    val isNullable: Boolean

    init {
        this.columns = columns
        this.name = name
        this.type = type
        this.columnSize = columnSize
        this.isNullable = isNullable
    }
}
