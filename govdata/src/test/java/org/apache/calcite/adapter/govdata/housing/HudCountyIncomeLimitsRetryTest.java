/*
 * Copyright (c) 2026 Kenneth Stott
 *
 * This source code is licensed under the Business Source License 1.1
 * found in the LICENSE-BSL.txt file in the root directory of this source tree.
 *
 * NOTICE: Use of this software for training artificial intelligence or
 * machine learning models is strictly prohibited without explicit written
 * permission from the copyright holder.
 */
package org.apache.calcite.adapter.govdata.housing;

import org.apache.calcite.adapter.file.etl.RequestContext;

import com.sun.net.httpserver.HttpServer;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** A dropped connection to HUD is retried instead of failing the state's batch. */
@Tag("unit")
class HudCountyIncomeLimitsRetryTest {
  private HttpServer server;
  private final AtomicInteger listCalls = new AtomicInteger();

  @BeforeEach void start() throws IOException {
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.start();
  }

  @AfterEach void stop() {
    server.stop(0);
  }

  private void reply(com.sun.net.httpserver.HttpExchange ex, String body) throws IOException {
    byte[] b = body.getBytes(StandardCharsets.UTF_8);
    ex.sendResponseHeaders(200, b.length);
    OutputStream out = ex.getResponseBody();
    out.write(b);
    ex.close();
  }

  @Test void connectionDroppedOnTheAreaListIsRetried() throws Exception {
    server.createContext("/hudapi/public/il/listCounties/01", ex -> {
      if (listCalls.incrementAndGet() <= 2) {
        ex.getRequestBody().close();
        // Declare a body, send none, close: the client sees a dropped connection.
        ex.sendResponseHeaders(200, 100);
        ex.close();
        return;
      }
      reply(ex, "[{\"fips_code\":\"0100199999\"}]");
    });
    server.createContext("/hudapi/public/il/data/0100199999", ex ->
        reply(ex, "{\"data\":{\"county_name\":\"Autauga\",\"median_income\":\"90000\"}}"));
    Map<String, String> dims = new HashMap<String, String>();
    dims.put("year", "2026");
    dims.put("state", "01");
    Map<String, String> headers = new HashMap<String, String>();
    headers.put("Authorization", "Bearer t");
    RequestContext context = RequestContext.builder()
        .url("http://127.0.0.1:" + server.getAddress().getPort() + "/hudapi/public/il/listCounties/01")
        .dimensionValues(dims)
        .headers(headers)
        .build();
    Iterator<Map<String, Object>> it =
        new HudCountyIncomeLimitsStreamingTransformer().fetchAndTransform(context);
    assertTrue(it.hasNext());
    assertEquals("Autauga", it.next().get("county_name"));
    assertFalse(it.hasNext());
    assertEquals(3, listCalls.get());
  }
}
