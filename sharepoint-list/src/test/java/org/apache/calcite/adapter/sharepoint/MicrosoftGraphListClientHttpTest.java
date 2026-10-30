/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.adapter.sharepoint;

import org.apache.calcite.adapter.sharepoint.auth.SharePointAuth;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The HTTP requests the client sends, received by a server in this JVM.
 */
@Tag("unit")
class MicrosoftGraphListClientHttpTest {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private HttpServer server;
  private String base;
  private final List<String> requests = new ArrayList<>();

  @BeforeEach void startServer() throws IOException {
    server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
    server.createContext("/", exchange -> {
      String body =
          new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
      requests.add(exchange.getRequestMethod() + " " + exchange.getRequestURI().getPath()
          + " " + exchange.getRequestHeaders().getFirst("Authorization") + " " + body);
      String path = exchange.getRequestURI().getPath();
      if (path.endsWith("/busy") && requests.size() == 1) {
        exchange.getResponseHeaders().add("Retry-After", "1");
        exchange.sendResponseHeaders(429, -1);
      } else if (path.endsWith("/busy-forever")) {
        exchange.sendResponseHeaders(429, -1);
      } else if (path.endsWith("/gone")) {
        exchange.sendResponseHeaders(204, -1);
      } else if (path.endsWith("/refused")) {
        byte[] error =
            "{\"error\":{\"message\":\"Access denied\"}}".getBytes(StandardCharsets.UTF_8);
        exchange.sendResponseHeaders(403, error.length);
        try (OutputStream out = exchange.getResponseBody()) {
          out.write(error);
        }
      } else {
        byte[] ok = "{\"id\":\"7\"}".getBytes(StandardCharsets.UTF_8);
        exchange.sendResponseHeaders(200, ok.length);
        try (OutputStream out = exchange.getResponseBody()) {
          out.write(ok);
        }
      }
      exchange.close();
    });
    server.start();
    base = "http://127.0.0.1:" + server.getAddress().getPort();
  }

  @AfterEach void stopServer() {
    server.stop(0);
  }

  private MicrosoftGraphListClient client() {
    return new MicrosoftGraphListClient("https://http-test.example", new SharePointAuth() {
      @Override public String getAccessToken() {
        return "token";
      }
    });
  }

  @Test void patchIsSentWithItsBody() throws Exception {
    JsonNode body = MAPPER.readTree("{\"fields\":{\"Title\":\"changed\"}}");
    JsonNode answer = client().executeGraphCall("PATCH", base + "/items/7", body);
    assertEquals("7", answer.get("id").asText());
    assertEquals(1, requests.size());
    assertEquals("PATCH /items/7 Bearer token {\"fields\":{\"Title\":\"changed\"}}",
        requests.get(0));
  }

  @Test void getAndDeleteCarryNoBody() throws Exception {
    assertEquals("7", client().executeGraphCall("GET", base + "/items/7", null).get("id").asText());
    assertEquals(0, client().executeGraphCall("DELETE", base + "/gone", null).size());
    assertEquals("GET /items/7 Bearer token ", requests.get(0));
    assertEquals("DELETE /gone Bearer token ", requests.get(1));
  }

  @Test void anErrorAnswerIsRaisedWithItsMessage() {
    IOException e =
        assertThrows(IOException.class,
            () -> client().executeGraphCall("GET", base + "/refused", null));
    assertTrue(e.getMessage().contains("HTTP 403"), e.getMessage());
    assertTrue(e.getMessage().contains("Access denied"), e.getMessage());
  }

  @Test void aThrottledCallIsSentAgainWhenGraphSaysTo() throws Exception {
    assertEquals("7", client().executeGraphCall("GET", base + "/busy", null).get("id").asText());
    assertEquals(2, requests.size());
  }

  @Test void aThrottledCallWithNoTimeToComeBackIsAnError() {
    IOException e =
        assertThrows(IOException.class,
            () -> client().executeGraphCall("GET", base + "/busy-forever", null));
    assertTrue(e.getMessage().contains("HTTP 429"), e.getMessage());
    assertEquals(1, requests.size());
  }
}
