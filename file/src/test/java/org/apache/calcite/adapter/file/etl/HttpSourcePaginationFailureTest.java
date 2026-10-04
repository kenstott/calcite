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
package org.apache.calcite.adapter.file.etl;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A failed page in the middle of a paginated crawl must fail the crawl, not end it as if the
 * data source were exhausted.
 */
class HttpSourcePaginationFailureTest {

  private static final int PAGE_SIZE = 100;

  private HttpServer server;
  private String baseUrl;

  @BeforeEach void startServer() throws IOException {
    server = HttpServer.create(new InetSocketAddress(0), 0);
    baseUrl = "http://localhost:" + server.getAddress().getPort();
    server.start();
  }

  @AfterEach void stopServer() {
    if (server != null) {
      server.stop(0);
    }
  }

  private static String fullPage() {
    StringBuilder sb = new StringBuilder("[");
    for (int i = 0; i < PAGE_SIZE; i++) {
      sb.append(i == 0 ? "" : ",").append("{\"id\":").append(i).append('}');
    }
    return sb.append(']').toString();
  }

  /** Serves a full first page, then answers every later request with the given failure status. */
  private AtomicInteger serveFullPageThenFail(String path, final int failureStatus) {
    final AtomicInteger requests = new AtomicInteger();
    server.createContext(path, new HttpHandler() {
      @Override public void handle(HttpExchange exchange) throws IOException {
        boolean first = requests.getAndIncrement() == 0;
        byte[] bytes = (first ? fullPage() : "{\"error\":\"denied\"}")
            .getBytes(StandardCharsets.UTF_8);
        exchange.sendResponseHeaders(first ? 200 : failureStatus, bytes.length);
        OutputStream out = exchange.getResponseBody();
        out.write(bytes);
        out.close();
      }
    });
    return requests;
  }

  private HttpSourceConfig offsetConfig(String url) {
    Map<String, Object> paginationMap = new LinkedHashMap<String, Object>();
    paginationMap.put("type", "OFFSET");
    paginationMap.put("limitParam", "limit");
    paginationMap.put("offsetParam", "offset");
    paginationMap.put("pageSize", PAGE_SIZE);
    Map<String, Object> responseMap = new LinkedHashMap<String, Object>();
    responseMap.put("format", "JSON");
    responseMap.put("pagination", paginationMap);
    return HttpSourceConfig.builder()
        .url(url)
        .response(HttpSourceConfig.ResponseConfig.fromMap(responseMap))
        .build();
  }

  @Test @Tag("unit") void failedLaterPageFailsTheCrawl() throws IOException {
    AtomicInteger requests = serveFullPageThenFail("/data", 401);
    HttpSource src = new HttpSource(offsetConfig(baseUrl + "/data"));
    Iterator<Map<String, Object>> it = src.fetch(Collections.<String, String>emptyMap());

    int yielded = 0;
    RuntimeException failure = null;
    try {
      while (it.hasNext()) {
        it.next();
        yielded++;
      }
    } catch (RuntimeException e) {
      failure = e;
    }

    assertEquals(PAGE_SIZE, yielded, "records of the page fetched before the failure");
    assertTrue(failure != null && failure.getMessage().contains("HTTP 401"),
        "a failed page must surface as an error naming the HTTP status, got: " + failure);
    assertEquals(2, requests.get(), "second page requested exactly once");
  }

  @Test @Tag("unit") void failedFirstPageFailsTheCrawl() throws IOException {
    server.createContext("/data", new HttpHandler() {
      @Override public void handle(HttpExchange exchange) throws IOException {
        exchange.sendResponseHeaders(401, -1);
        exchange.close();
      }
    });
    HttpSource src = new HttpSource(offsetConfig(baseUrl + "/data"));
    Iterator<Map<String, Object>> it = src.fetch(Collections.<String, String>emptyMap());

    RuntimeException e = assertThrows(RuntimeException.class, it::hasNext);
    assertTrue(e.getMessage().contains("HTTP 401"), e.getMessage());
  }
}
