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
package org.apache.calcite.adapter.govdata.econ;

import org.apache.calcite.adapter.file.etl.HttpSourceConfig;
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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A Comtrade response whose body is cut off mid-stream is re-fetched as a whole, within the
 * source's {@code rateLimit.maxRetries}, but only while no row has reached the caller.
 */
@Tag("unit")
class ComtradeTransformerRetryTest {

  private static final String REC1 =
      "{\"reporterISO\":\"USA\",\"partnerCode\":156,\"partnerISO\":\"CHN\",\"flowCode\":\"M\","
          + "\"cmdCode\":\"010121\",\"primaryValue\":100.5}";
  private static final String REC2 =
      "{\"reporterISO\":\"USA\",\"partnerCode\":124,\"partnerISO\":\"CAN\",\"flowCode\":\"M\","
          + "\"cmdCode\":\"020230\",\"primaryValue\":7}";
  private static final String REC3 =
      "{\"reporterISO\":\"USA\",\"partnerCode\":484,\"partnerISO\":\"MEX\",\"flowCode\":\"M\","
          + "\"cmdCode\":\"030303\",\"primaryValue\":9}";
  private static final String FULL_BODY =
      "{\"count\":3,\"data\":[" + REC1 + "," + REC2 + "," + REC3 + "]}";

  private HttpServer server;
  private final AtomicInteger requests = new AtomicInteger();

  @BeforeEach void startServer() throws IOException {
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.start();
  }

  @AfterEach void stopServer() {
    server.stop(0);
  }

  /**
   * Serves {@code cutBody} (declared longer than it is, then the connection is closed) for the
   * first {@code failures} requests and {@link #FULL_BODY} afterwards.
   */
  private String serve(final int failures, final String cutBody) {
    server.createContext("/data", exchange -> {
      int n = requests.incrementAndGet();
      byte[] body = (n <= failures ? cutBody : FULL_BODY).getBytes(StandardCharsets.UTF_8);
      exchange.getResponseHeaders().add("Content-Type", "application/json");
      exchange.sendResponseHeaders(200, n <= failures ? body.length + 4096 : body.length);
      OutputStream out = exchange.getResponseBody();
      out.write(body);
      out.flush();
      exchange.close();
    });
    return "http://127.0.0.1:" + server.getAddress().getPort() + "/data?reporterCode=842";
  }

  private static RequestContext context(String url, int maxRetries) {
    Map<String, Object> rl = new HashMap<String, Object>();
    rl.put("requestsPerSecond", 1000);
    rl.put("maxRetries", maxRetries);
    rl.put("retryBackoffMs", 1);
    return RequestContext.builder()
        .url(url)
        .rateLimit(HttpSourceConfig.RateLimitConfig.fromMap(rl))
        .build();
  }

  private static List<Map<String, Object>> drain(Iterator<Map<String, Object>> it) {
    List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
    while (it.hasNext()) {
      rows.add(it.next());
    }
    return rows;
  }

  @Test void retriesWholeFetchWhenBodyCutBeforeAnyRow() throws Exception {
    String url = serve(2, "{\"count\":3,\"data\":[" + REC1.substring(0, 30));
    List<Map<String, Object>> rows =
        drain(new ComtradeTransformer().fetchAndTransform(context(url, 3)));
    assertEquals(3, rows.size());
    assertEquals("CHN", rows.get(0).get("partner_code"));
    assertEquals("MEX", rows.get(2).get("partner_code"));
    assertEquals(3, requests.get());
  }

  @Test void exhaustedRetriesFailWithUrlAndOriginalCause() throws Exception {
    String url = serve(100, "{\"count\":3,\"data\":[" + REC1.substring(0, 30));
    RuntimeException e = assertThrows(RuntimeException.class,
        () -> drain(new ComtradeTransformer().fetchAndTransform(context(url, 2))));
    assertTrue(e.getMessage().contains(url), e.getMessage());
    assertNotNull(e.getCause());
    assertTrue(e.getCause() instanceof IOException, e.getCause().toString());
    assertEquals(3, requests.get());
  }

  @Test void doesNotRetryOnceRowsWereEmitted() throws Exception {
    String url = serve(100,
        "{\"count\":3,\"data\":[" + REC1 + "," + REC2 + "," + REC3.substring(0, 30));
    Iterator<Map<String, Object>> it =
        new ComtradeTransformer().fetchAndTransform(context(url, 3));
    List<Map<String, Object>> seen = new ArrayList<Map<String, Object>>();
    RuntimeException e = assertThrows(RuntimeException.class, () -> {
      while (it.hasNext()) {
        seen.add(it.next());
      }
    });
    // The fault surfaces while advancing past the second record, so only the first was returned.
    assertEquals(1, seen.size());
    assertTrue(e.getMessage().contains(url), e.getMessage());
    assertTrue(e.getCause() instanceof IOException, e.getCause().toString());
    assertEquals(1, requests.get());
  }
}
