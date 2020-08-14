package io.dataline.server.handlers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.Sets;
import io.dataline.api.model.DestinationIdRequestBody;
import io.dataline.api.model.DestinationRead;
import io.dataline.api.model.DestinationReadList;
import io.dataline.config.StandardDestination;
import io.dataline.config.persistence.ConfigNotFoundException;
import io.dataline.config.persistence.ConfigPersistence;
import io.dataline.config.persistence.JsonValidationException;
import io.dataline.config.persistence.PersistenceConfigType;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DestinationsHandlerTest {
  private ConfigPersistence configPersistence;
  private StandardDestination destination;
  private DestinationsHandler destinationHandler;

  @BeforeEach
  void setUp() {
    configPersistence = mock(ConfigPersistence.class);
    destination = generateDestination();
    destinationHandler = new DestinationsHandler(configPersistence);
  }

  private StandardDestination generateDestination() {
    final UUID destinationId = UUID.randomUUID();

    final StandardDestination standardDestination = new StandardDestination();
    standardDestination.setDestinationId(destinationId);
    standardDestination.setName("presto");

    return standardDestination;
  }

  @Test
  void testListDestinations() throws JsonValidationException {
    final StandardDestination destination2 = generateDestination();
    configPersistence.writeConfig(
        PersistenceConfigType.STANDARD_DESTINATION,
        destination2.getDestinationId().toString(),
        destination2);

    when(configPersistence.getConfigs(
            PersistenceConfigType.STANDARD_DESTINATION, StandardDestination.class))
        .thenReturn(Sets.newHashSet(destination, destination2));

    DestinationRead expectedDestinationRead1 = new DestinationRead();
    expectedDestinationRead1.setDestinationId(destination.getDestinationId());
    expectedDestinationRead1.setName(destination.getName());

    DestinationRead expectedDestinationRead2 = new DestinationRead();
    expectedDestinationRead2.setDestinationId(destination2.getDestinationId());
    expectedDestinationRead2.setName(destination2.getName());

    final DestinationReadList actualDestinationReadList = destinationHandler.listDestinations();

    final Optional<DestinationRead> actualDestinationRead1 =
        actualDestinationReadList.getDestinations().stream()
            .filter(
                destinationRead ->
                    destinationRead.getDestinationId().equals(destination.getDestinationId()))
            .findFirst();
    final Optional<DestinationRead> actualDestinationRead2 =
        actualDestinationReadList.getDestinations().stream()
            .filter(
                destinationRead ->
                    destinationRead.getDestinationId().equals(destination2.getDestinationId()))
            .findFirst();

    assertTrue(actualDestinationRead1.isPresent());
    assertEquals(expectedDestinationRead1, actualDestinationRead1.get());
    assertTrue(actualDestinationRead2.isPresent());
    assertEquals(expectedDestinationRead2, actualDestinationRead2.get());
  }

  @Test
  void testGetDestination() throws JsonValidationException, ConfigNotFoundException {
    when(configPersistence.getConfig(
            PersistenceConfigType.STANDARD_DESTINATION,
            destination.getDestinationId().toString(),
            StandardDestination.class))
        .thenReturn(destination);

    DestinationRead expectedDestinationRead = new DestinationRead();
    expectedDestinationRead.setDestinationId(destination.getDestinationId());
    expectedDestinationRead.setName(destination.getName());

    final DestinationIdRequestBody destinationIdRequestBody = new DestinationIdRequestBody();
    destinationIdRequestBody.setDestinationId(destination.getDestinationId());

    final DestinationRead actualDestinationRead =
        destinationHandler.getDestination(destinationIdRequestBody);

    assertEquals(expectedDestinationRead, actualDestinationRead);
  }
}
