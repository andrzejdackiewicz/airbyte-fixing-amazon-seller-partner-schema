/*
 * Copyright (c) 2022 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.controller;

import io.airbyte.commons.json.Jsons;
import io.airbyte.validation.json.JsonSchemaValidator;
import io.micronaut.http.HttpHeaders;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.MediaType;
import io.micronaut.http.MutableHttpResponse;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.Options;
import io.micronaut.http.annotation.Post;
import java.util.Map;
import java.util.Set;

/**
 * Heartbeat controller
 */
@Controller("/")
public class HeartbeatController {

  private static final Map<String, String> CORS_FILTER_MAP = Map.of(
      HttpHeaders.ACCESS_CONTROL_ALLOW_ORIGIN, "*",
      HttpHeaders.ACCESS_CONTROL_ALLOW_HEADERS, "Origin, Content-Type, Accept, Content-Encoding",
      HttpHeaders.ACCESS_CONTROL_ALLOW_METHODS, "GET, POST, PUT, DELETE, OPTIONS, HEAD");

  private final static Map<String, Boolean> DEFAULT_RESPONSE_BODY = Map.of("up", true);

  @Get(produces = MediaType.APPLICATION_JSON)
  @Post(produces = MediaType.APPLICATION_JSON)
  public HttpResponse<Map<String, Boolean>> heartbeat() {
    final MutableHttpResponse<Map<String, Boolean>> response = HttpResponse.ok(DEFAULT_RESPONSE_BODY);
    addCorsHeaders(response);
    return response;
  }

  @Options
  public HttpResponse<Map<String, Boolean>> emptyHeartbeat() {
    final MutableHttpResponse<Map<String, Boolean>> response = HttpResponse.ok();
    addCorsHeaders(response);
    return response;
  }

  private void addCorsHeaders(final MutableHttpResponse response) {
    for (final Map.Entry<String, String> entry : CORS_FILTER_MAP.entrySet()) {
      response.header(entry.getKey(), entry.getValue());
    }
  }

  public static void main(String[] args) {
//    Set<String> res = new JsonSchemaValidator().validate(
//        Jsons.deserialize("""
//            {
//              "$ref": "foo.json#/definitions/String"
//            }
//            """),
//        Jsons.deserialize("""
//            "arst"
//            """)
//    );
//    System.out.println("Validation result: " + res);
  }
}
