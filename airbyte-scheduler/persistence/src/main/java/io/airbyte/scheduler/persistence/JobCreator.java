/*
 * Copyright (c) 2020 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.scheduler.persistence;

import io.airbyte.config.DestinationConnection;
import io.airbyte.config.SourceConnection;
import io.airbyte.config.StandardSync;
import io.airbyte.config.StandardSyncOperation;
import java.io.IOException;
import java.util.List;
import java.util.Optional;

public interface JobCreator {

  /**
   *
   * @param source db model representing where data comes from
   * @param destination db model representing where data goes
   * @param standardSync sync options
   * @param sourceDockerImage docker image to use for the source
   * @param destinationDockerImage docker image to use for the destination
   * @return the new job if no other conflicting job was running, otherwise empty
   * @throws IOException if something wrong happens
   */
  Optional<Long> createSyncJob(SourceConnection source,
                               DestinationConnection destination,
                               StandardSync standardSync,
                               String sourceDockerImage,
                               String destinationDockerImage,
                               List<StandardSyncOperation> standardSyncOperations)
      throws IOException;

  /**
   *
   * @param destination db model representing where data goes
   * @param standardSync sync options
   * @param destinationDockerImage docker image to use for the destination
   * @return the new job if no other conflicting job was running, otherwise empty
   * @throws IOException if something wrong happens
   */
  Optional<Long> createResetConnectionJob(DestinationConnection destination,
                                          StandardSync standardSync,
                                          String destinationDockerImage,
                                          List<StandardSyncOperation> standardSyncOperations)
      throws IOException;

}
