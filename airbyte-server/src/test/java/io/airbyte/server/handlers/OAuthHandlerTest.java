/*
 * Copyright (c) 2021 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.handlers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

import io.airbyte.api.model.SetInstancewideDestinationOauthParamsRequestBody;
import io.airbyte.api.model.SetInstancewideSourceOauthParamsRequestBody;
import io.airbyte.commons.json.Jsons;
import io.airbyte.config.DestinationOAuthParameter;
import io.airbyte.config.SourceOAuthParameter;
import io.airbyte.config.persistence.ConfigRepository;
import io.airbyte.validation.json.JsonValidationException;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

class OAuthHandlerTest {

  ConfigRepository configRepository;
  OAuthHandler handler;

  @BeforeEach
  public void init() {
    configRepository = Mockito.mock(ConfigRepository.class);
    handler = new OAuthHandler(configRepository);
  }

  @Test
  void setSourceInstancewideOauthParams() throws JsonValidationException, IOException {
    UUID sourceDefId = UUID.randomUUID();
    Map<String, Object> params = new HashMap<>();
    params.put("client_id", "123");
    params.put("client_secret", "hunter2");

    SetInstancewideSourceOauthParamsRequestBody actualRequest = new SetInstancewideSourceOauthParamsRequestBody()
        .sourceDefinitionId(sourceDefId)
        .params(params);

    handler.setSourceInstancewideOauthParams(actualRequest);

    ArgumentCaptor<SourceOAuthParameter> argument = ArgumentCaptor.forClass(SourceOAuthParameter.class);
    Mockito.verify(configRepository).writeSourceOAuthParam(argument.capture());
    assertEquals(Jsons.jsonNode(params), argument.getValue().getConfiguration());
    assertEquals(sourceDefId, argument.getValue().getSourceDefinitionId());
  }

  @Test
  void resetSourceInstancewideOauthParams() throws JsonValidationException, IOException {
    UUID sourceDefId = UUID.randomUUID();
    Map<String, Object> firstParams = new HashMap<>();
    firstParams.put("client_id", "123");
    firstParams.put("client_secret", "hunter2");
    SetInstancewideSourceOauthParamsRequestBody firstRequest = new SetInstancewideSourceOauthParamsRequestBody()
        .sourceDefinitionId(sourceDefId)
        .params(firstParams);
    handler.setSourceInstancewideOauthParams(firstRequest);

    final UUID oauthParameterId = UUID.randomUUID();
    when(configRepository.getSourceOAuthParamByDefinitionIdOptional(null, sourceDefId))
        .thenReturn(Optional.of(new SourceOAuthParameter().withOauthParameterId(oauthParameterId)));

    Map<String, Object> secondParams = new HashMap<>();
    secondParams.put("client_id", "456");
    secondParams.put("client_secret", "hunter3");
    SetInstancewideSourceOauthParamsRequestBody secondRequest = new SetInstancewideSourceOauthParamsRequestBody()
        .sourceDefinitionId(sourceDefId)
        .params(secondParams);
    handler.setSourceInstancewideOauthParams(secondRequest);

    ArgumentCaptor<SourceOAuthParameter> argument = ArgumentCaptor.forClass(SourceOAuthParameter.class);
    Mockito.verify(configRepository, Mockito.times(2)).writeSourceOAuthParam(argument.capture());
    List<SourceOAuthParameter> capturedValues = argument.getAllValues();
    assertEquals(Jsons.jsonNode(firstParams), capturedValues.get(0).getConfiguration());
    assertEquals(Jsons.jsonNode(secondParams), capturedValues.get(1).getConfiguration());
    assertEquals(sourceDefId, capturedValues.get(0).getSourceDefinitionId());
    assertEquals(sourceDefId, capturedValues.get(1).getSourceDefinitionId());
    assertEquals(oauthParameterId, capturedValues.get(1).getOauthParameterId());
  }

  @Test
  void setDestinationInstancewideOauthParams() throws JsonValidationException, IOException {
    UUID destinationDefId = UUID.randomUUID();
    Map<String, Object> params = new HashMap<>();
    params.put("client_id", "123");
    params.put("client_secret", "hunter2");

    SetInstancewideDestinationOauthParamsRequestBody actualRequest = new SetInstancewideDestinationOauthParamsRequestBody()
        .destinationDefinitionId(destinationDefId)
        .params(params);

    handler.setDestinationInstancewideOauthParams(actualRequest);

    ArgumentCaptor<DestinationOAuthParameter> argument = ArgumentCaptor.forClass(DestinationOAuthParameter.class);
    Mockito.verify(configRepository).writeDestinationOAuthParam(argument.capture());
    assertEquals(Jsons.jsonNode(params), argument.getValue().getConfiguration());
    assertEquals(destinationDefId, argument.getValue().getDestinationDefinitionId());
  }

  @Test
  void resetDestinationInstancewideOauthParams() throws JsonValidationException, IOException {
    UUID destinationDefId = UUID.randomUUID();
    Map<String, Object> firstParams = new HashMap<>();
    firstParams.put("client_id", "123");
    firstParams.put("client_secret", "hunter2");
    SetInstancewideDestinationOauthParamsRequestBody firstRequest = new SetInstancewideDestinationOauthParamsRequestBody()
        .destinationDefinitionId(destinationDefId)
        .params(firstParams);
    handler.setDestinationInstancewideOauthParams(firstRequest);

    final UUID oauthParameterId = UUID.randomUUID();
    when(configRepository.getDestinationOAuthParamByDefinitionIdOptional(null, destinationDefId))
        .thenReturn(Optional.of(new DestinationOAuthParameter().withOauthParameterId(oauthParameterId)));

    Map<String, Object> secondParams = new HashMap<>();
    secondParams.put("client_id", "456");
    secondParams.put("client_secret", "hunter3");
    SetInstancewideDestinationOauthParamsRequestBody secondRequest = new SetInstancewideDestinationOauthParamsRequestBody()
        .destinationDefinitionId(destinationDefId)
        .params(secondParams);
    handler.setDestinationInstancewideOauthParams(secondRequest);

    ArgumentCaptor<DestinationOAuthParameter> argument = ArgumentCaptor.forClass(DestinationOAuthParameter.class);
    Mockito.verify(configRepository, Mockito.times(2)).writeDestinationOAuthParam(argument.capture());
    List<DestinationOAuthParameter> capturedValues = argument.getAllValues();
    assertEquals(Jsons.jsonNode(firstParams), capturedValues.get(0).getConfiguration());
    assertEquals(Jsons.jsonNode(secondParams), capturedValues.get(1).getConfiguration());
    assertEquals(destinationDefId, capturedValues.get(0).getDestinationDefinitionId());
    assertEquals(destinationDefId, capturedValues.get(1).getDestinationDefinitionId());
    assertEquals(oauthParameterId, capturedValues.get(1).getOauthParameterId());
  }

}
