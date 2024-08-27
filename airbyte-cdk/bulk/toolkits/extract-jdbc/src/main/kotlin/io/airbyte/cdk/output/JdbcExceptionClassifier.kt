/*
 * Copyright (c) 2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.cdk.output

import io.micronaut.context.annotation.EachProperty
import io.micronaut.context.annotation.Parameter
import io.micronaut.context.annotation.Requires
import io.micronaut.context.annotation.Value
import jakarta.inject.Singleton
import java.sql.SQLException

const val JDBC_CLASSIFIER_PREFIX = "${EXCEPTION_CLASSIFIER_PREFIX}.jdbc"

/** [ExceptionClassifier] implementation based on [SQLException] vendor error codes. */
@Singleton
@Requires(property = "${JDBC_CLASSIFIER_PREFIX}.rules")
class JdbcExceptionClassifier(
    @Value("\${$JDBC_CLASSIFIER_PREFIX.order:100}") override val orderValue: Int,
    override val rules: List<JdbcExceptionClassifierRule>,
) : RuleBasedExceptionClassifier<JdbcExceptionClassifierRule> {

    init {
        for (rule in rules) {
            rule.validate()
        }
    }

    override fun classify(e: Throwable): ConnectorError? {
        if (e !is SQLException) return null
        val decoratedMessage: String =
            listOfNotNull(
                    e.sqlState?.let { "State code: $it" },
                    e.errorCode.takeIf { it != 0 }?.let { "Error code: $it" },
                    e.message?.let { "Message: $it" },
                )
                .joinToString(separator = "; ")
        val decoratedException = SQLException(decoratedMessage, e.sqlState, e.errorCode)
        val ruleBasedMatch: ConnectorError? = super.classify(decoratedException)
        if (ruleBasedMatch != null) {
            return ruleBasedMatch
        }
        return SystemError(decoratedMessage)
    }
}

/** Micronaut configuration object for [JdbcExceptionClassifier] rules. */
@EachProperty("${JDBC_CLASSIFIER_PREFIX}.rules", list = true)
class JdbcExceptionClassifierRule(
    @param:Parameter override val ordinal: Int,
) : RuleBasedExceptionClassifier.Rule {

    // Micronaut configuration objects work better with mutable properties.
    override lateinit var error: RuleBasedExceptionClassifier.ErrorKind
    var code: Int = 0
    override var group: String? = null
    override var output: String? = null
    override var referenceLinks: List<String> = emptyList()

    override fun matches(e: Throwable): Boolean =
        when (e) {
            is SQLException -> e.errorCode == code
            else -> false
        }

    override fun validate() {
        require(runCatching { error }.isSuccess) { "error kind must be set" }
        require(code != 0) { "vendor code must be non-zero" }
    }
}
