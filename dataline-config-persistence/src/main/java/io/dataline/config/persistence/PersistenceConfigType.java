package io.dataline.config.persistence;

public enum PersistenceConfigType {
  // workspace
  STANDARD_WORKSPACE,

  // source
  STANDARD_SOURCE,
  SOURCE_CONNECTION_SPECIFICATION,
  SOURCE_CONNECTION_IMPLEMENTATION,

  // destination
  STANDARD_DESTINATION,
  DESTINATION_CONNECTION_SPECIFICATION,
  DESTINATION_CONNECTION_IMPLEMENTATION,

  // test connection
  STANDARD_CONNECTION_STATUS,

  // discover schema
  STANDARD_DISCOVERY_OUTPUT,

  // sync
  STANDARD_SYNC,
  STANDARD_SYNC_SUMMARY,
  STANDARD_SYNC_SCHEDULE,
  STATE,
}
