package io.airbyte.integrations.base.destination.typing_deduping;

/**
 * Exception thrown when a destination's v2 sync is attempting to write to a table which does not have the expected
 * columns used by airbyte.
 */
public class TableNotMigratedException extends Exception { }
