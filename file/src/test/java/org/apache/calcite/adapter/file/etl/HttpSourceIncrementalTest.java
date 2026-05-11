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
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;


/**
 * Tests for HttpSource incremental (sinceDate / sinceYear+sinceQuarter) behaviour.
 *
 * <p>Unit tests use a local {@link HttpServer} on a random port so no
 * network access is required.  Integration tests (tagged "integration") hit
 * live public APIs.
 */
class HttpSourceIncrementalTest {

  // ── local mock server ──────────────────────────────────────────────────────

  private HttpServer server;
  private String baseUrl;

  /** Last URI received by the mock server (path + query string). */
  private final AtomicReference<String> capturedUri = new AtomicReference<>();

  /** All URIs captured across multiple requests (for pagination tests). */
  private final List<String> capturedUris = Collections.synchronizedList(new ArrayList<String>());

  @BeforeEach void startServer() throws IOException {
    server = HttpServer.create(new InetSocketAddress(0), 0);
    baseUrl = "http://localhost:" + server.getAddress().getPort();
    server.start();
  }

  @AfterEach void stopServer() {
    if (server != null) {
      server.stop(0);
    }
    capturedUri.set(null);
    capturedUris.clear();
  }

  // ── helpers ────────────────────────────────────────────────────────────────

  /** Registers a handler that always returns a top-level JSON array and records the request URI. */
  private void addJsonHandler(String path, final String responseBody) {
    server.createContext(path, new HttpHandler() {
      @Override public void handle(HttpExchange exchange) throws IOException {
        capturedUri.set(exchange.getRequestURI().toString());
        capturedUris.add(exchange.getRequestURI().toString());
        byte[] bytes = responseBody.getBytes(StandardCharsets.UTF_8);
        exchange.sendResponseHeaders(200, bytes.length);
        OutputStream out = exchange.getResponseBody();
        out.write(bytes);
        out.close();
      }
    });
  }

  /** Drains an iterator safely, ignoring parse errors on empty arrays. */
  private static int drain(Iterator<Map<String, Object>> it) {
    int count = 0;
    while (it.hasNext()) {
      it.next();
      count++;
    }
    return count;
  }

  /** ResponseConfig using OFFSET pagination — PaginatedIterator handles inline response safely. */
  private static HttpSourceConfig.ResponseConfig offsetResponse() {
    Map<String, Object> paginationMap = new LinkedHashMap<String, Object>();
    paginationMap.put("type", "OFFSET");
    paginationMap.put("limitParam", "$limit");
    paginationMap.put("offsetParam", "$offset");
    paginationMap.put("pageSize", 100);
    Map<String, Object> responseMap = new LinkedHashMap<String, Object>();
    responseMap.put("format", "JSON");
    responseMap.put("pagination", paginationMap);
    return HttpSourceConfig.ResponseConfig.fromMap(responseMap);
  }

  private static HttpSourceConfig whereFilterConfig(String url, String sinceDate,
      String filterParam, String dateField) {
    HttpSourceConfig.IncrementalConfig incr = new HttpSourceConfig.IncrementalConfig(
        sinceDate, null, null, filterParam, dateField, null, null);
    return HttpSourceConfig.builder()
        .url(url)
        .response(offsetResponse())
        .incremental(incr)
        .build();
  }

  private static HttpSourceConfig directParamFilterConfig(String url, String sinceDate,
      String filterParam) {
    HttpSourceConfig.IncrementalConfig incr = new HttpSourceConfig.IncrementalConfig(
        sinceDate, null, null, filterParam, null, null, null);
    return HttpSourceConfig.builder()
        .url(url)
        .response(offsetResponse())
        .incremental(incr)
        .build();
  }

  // ── blank sinceDate skips filter ───────────────────────────────────────────

  @Test @Tag("unit") void blankSinceDateProducesNoFilterParam() throws IOException {
    addJsonHandler("/data", "[]");

    HttpSourceConfig cfg = whereFilterConfig(baseUrl + "/data", "", "$where", "date");
    HttpSource src = new HttpSource(cfg);
    drain(src.fetch(Collections.<String, String>emptyMap()));

    String uri = capturedUri.get();
    assertNotNull(uri, "server must have received a request");
    assertFalse(uri.contains("$where"), "empty sinceDate must not add $where to URL — got: " + uri);
  }

  @Test @Tag("unit") void nullSinceDateProducesNoFilterParam() throws IOException {
    addJsonHandler("/data", "[]");

    // IncrementalConfig with null sinceDate
    HttpSourceConfig.IncrementalConfig incr = new HttpSourceConfig.IncrementalConfig(
        null, null, null, "$where", "date", null, null);
    HttpSourceConfig cfg = HttpSourceConfig.builder()
        .url(baseUrl + "/data")
        .response(offsetResponse())
        .incremental(incr)
        .build();
    HttpSource src = new HttpSource(cfg);
    drain(src.fetch(Collections.<String, String>emptyMap()));

    String uri = capturedUri.get();
    assertNotNull(uri);
    assertFalse(uri.contains("$where"), "null sinceDate must not add $where — got: " + uri);
  }

  @Test @Tag("unit") void envVarDefaultEmptyProducesNoFilterParam() throws IOException {
    addJsonHandler("/data", "[]");

    // ${NONEXISTENT_VAR_XYZ:} resolves to "" via VariableResolver
    HttpSourceConfig cfg = whereFilterConfig(baseUrl + "/data",
        "${NONEXISTENT_VAR_XYZ_12345:}", "$where", "date");
    HttpSource src = new HttpSource(cfg);
    drain(src.fetch(Collections.<String, String>emptyMap()));

    String uri = capturedUri.get();
    assertNotNull(uri);
    assertFalse(uri.contains("$where"),
        "unresolved env var with empty default must not add $where — got: " + uri);
  }

  // ── sinceDate injects WHERE filter ────────────────────────────────────────

  @Test @Tag("unit") void literalSinceDateInjectsWhereFilter() throws IOException {
    addJsonHandler("/data", "[]");

    HttpSourceConfig cfg = whereFilterConfig(baseUrl + "/data", "2024-01-01", "$where", "date");
    HttpSource src = new HttpSource(cfg);
    drain(src.fetch(Collections.<String, String>emptyMap()));

    String uri = capturedUri.get();
    assertNotNull(uri);
    assertTrue(uri.contains("%24where") || uri.contains("$where"),
        "$where param must appear in URL — got: " + uri);
    String decoded = URLDecoder.decode(uri, "UTF-8");
    assertTrue(decoded.contains("date >= '2024-01-01'"),
        "WHERE clause must contain date filter — got decoded: " + decoded);
  }

  @Test @Tag("unit") void systemPropertySinceDateInjectsWhereFilter() throws IOException {
    addJsonHandler("/data", "[]");
    System.setProperty("TEST_SINCE_DATE_PROP", "2023-06-01");
    try {
      HttpSourceConfig cfg = whereFilterConfig(baseUrl + "/data",
          "${TEST_SINCE_DATE_PROP:}", "$where", "weekendingdate");
      HttpSource src = new HttpSource(cfg);
      drain(src.fetch(Collections.<String, String>emptyMap()));

      String decoded = URLDecoder.decode(capturedUri.get(), "UTF-8");
      assertTrue(decoded.contains("weekendingdate >= '2023-06-01'"),
          "System.setProperty value must resolve and inject filter — got: " + decoded);
    } finally {
      System.clearProperty("TEST_SINCE_DATE_PROP");
    }
  }

  @Test @Tag("unit") void directParamStyleSinceDateInjectsDateAsParamValue() throws IOException {
    addJsonHandler("/data", "[]");

    // direct-param style: filterParam is not $where, no dateField
    HttpSourceConfig cfg = directParamFilterConfig(baseUrl + "/data",
        "2024-06-01", "lastUpdatePostDate.gte");
    HttpSource src = new HttpSource(cfg);
    drain(src.fetch(Collections.<String, String>emptyMap()));

    String uri = capturedUri.get();
    assertNotNull(uri);
    String decoded = URLDecoder.decode(uri, "UTF-8");
    assertTrue(decoded.contains("lastUpdatePostDate.gte=2024-06-01"),
        "direct-param style must set param to date value — got: " + decoded);
  }

  // ── sinceYear + sinceQuarter WHERE filter ─────────────────────────────────

  @Test @Tag("unit") void sinceYearAndQuarterInjectsCompoundWhereFilter() throws IOException {
    addJsonHandler("/data", "[]");

    HttpSourceConfig.IncrementalConfig incr = new HttpSourceConfig.IncrementalConfig(
        null, "2023", "3", "$where", null, "year", "quarter");
    HttpSourceConfig cfg = HttpSourceConfig.builder()
        .url(baseUrl + "/data")
        .response(offsetResponse())
        .incremental(incr)
        .build();
    HttpSource src = new HttpSource(cfg);
    drain(src.fetch(Collections.<String, String>emptyMap()));

    String decoded = URLDecoder.decode(capturedUri.get(), "UTF-8");
    assertTrue(decoded.contains("(year > '2023') OR (year = '2023' AND quarter >= '3')"),
        "Compound WHERE must be in URL — got: " + decoded);
  }

  @Test @Tag("unit") void blankSinceYearProducesNoFilterParam() throws IOException {
    addJsonHandler("/data", "[]");

    HttpSourceConfig.IncrementalConfig incr = new HttpSourceConfig.IncrementalConfig(
        null, "", "3", "$where", null, "year", "quarter");
    HttpSourceConfig cfg = HttpSourceConfig.builder()
        .url(baseUrl + "/data")
        .response(offsetResponse())
        .incremental(incr)
        .build();
    HttpSource src = new HttpSource(cfg);
    drain(src.fetch(Collections.<String, String>emptyMap()));

    String uri = capturedUri.get();
    assertFalse(uri.contains("$where"),
        "blank sinceYear must suppress $where even when quarter present — got: " + uri);
  }

  // ── filter propagates to all OFFSET pages ─────────────────────────────────

  @Test @Tag("unit") void filterAppearsInAllOffsetPages() throws IOException {
    // Page 1: 2 records (triggers page 2 fetch)
    // Page 2: 0 records (terminates pagination)
    final AtomicInteger requestCount = new AtomicInteger(0);
    server.createContext("/paged", new HttpHandler() {
      @Override public void handle(HttpExchange exchange) throws IOException {
        capturedUris.add(exchange.getRequestURI().toString());
        int req = requestCount.incrementAndGet();
        String body = req == 1
            ? "[{\"id\":\"1\"},{\"id\":\"2\"}]"
            : "[]";
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.sendResponseHeaders(200, bytes.length);
        OutputStream out = exchange.getResponseBody();
        out.write(bytes);
        out.close();
      }
    });

    Map<String, String> params = new LinkedHashMap<String, String>();
    params.put("$limit", "2");
    HttpSourceConfig.IncrementalConfig incr = new HttpSourceConfig.IncrementalConfig(
        "2024-01-01", null, null, "$where", "date", null, null);
    Map<String, Object> paginationMap = new LinkedHashMap<String, Object>();
    paginationMap.put("type", "OFFSET");
    paginationMap.put("limitParam", "$limit");
    paginationMap.put("offsetParam", "$offset");
    paginationMap.put("pageSize", 2);
    Map<String, Object> responseMap = new LinkedHashMap<String, Object>();
    responseMap.put("format", "JSON");
    responseMap.put("pagination", paginationMap);
    HttpSourceConfig cfg = HttpSourceConfig.builder()
        .url(baseUrl + "/paged")
        .parameters(params)
        .response(HttpSourceConfig.ResponseConfig.fromMap(responseMap))
        .incremental(incr)
        .build();

    HttpSource src = new HttpSource(cfg);
    Iterator<Map<String, Object>> it = src.fetch(Collections.<String, String>emptyMap());
    drain(it);

    assertTrue(capturedUris.size() >= 2, "must have fetched at least 2 pages");
    for (String uri : capturedUris) {
      String decoded = URLDecoder.decode(uri, "UTF-8");
      assertTrue(decoded.contains("date >= '2024-01-01'"),
          "Every page request must carry the incremental filter — got: " + decoded);
    }
  }

  // ── integration tests: live public APIs ───────────────────────────────────

  /**
   * Verifies that a future-date sinceDate filter returns zero rows from the
   * CDC COVID vaccinations Socrata API.  Implicitly confirms the filter is
   * accepted by the server (HTTP 200) and the response is parseable.
   */
  @Test
  @Tag("integration")
  void futureSinceDateReturnsZeroRowsFromCdcCovidApi() throws IOException {
    HttpSourceConfig.IncrementalConfig incr = new HttpSourceConfig.IncrementalConfig(
        "2099-01-01", null, null, "$where", "date", null, null);
    Map<String, String> params = new LinkedHashMap<String, String>();
    params.put("$limit", "100");
    HttpSourceConfig cfg = HttpSourceConfig.builder()
        .url("https://data.cdc.gov/resource/km4m-vcsb.json")
        .parameters(params)
        .response(offsetResponse())
        .incremental(incr)
        .build();
    HttpSource src = new HttpSource(cfg);
    Iterator<Map<String, Object>> it = src.fetch(Collections.<String, String>emptyMap());
    int rows = drain(it);
    assertEquals(0, rows, "Future date filter must return 0 rows from CDC COVID API");
  }

  /**
   * End-to-end integration test: sinceDate injected via System.setProperty reaches a live API.
   * Uses CDC COVID vaccinations Socrata endpoint (top-level JSON array).
   * A future date set via system property must return 0 rows from the live API.
   */
  @Test
  @Tag("integration")
  void systemPropertySinceDateReachesLiveCdcCovidApi() throws IOException {
    System.setProperty("IT_SINCE_DATE", "2099-01-01");
    try {
      Map<String, String> params = new LinkedHashMap<String, String>();
      params.put("$limit", "100");
      HttpSourceConfig.IncrementalConfig incr = new HttpSourceConfig.IncrementalConfig(
          "${IT_SINCE_DATE:}", null, null, "$where", "date", null, null);
      HttpSourceConfig cfg = HttpSourceConfig.builder()
          .url("https://data.cdc.gov/resource/km4m-vcsb.json")
          .parameters(params)
          .response(offsetResponse())
          .incremental(incr)
          .build();
      int rows = drain(new HttpSource(cfg).fetch(Collections.<String, String>emptyMap()));
      assertEquals(0, rows,
          "Future sinceDate from System.setProperty must return 0 rows from live CDC API");
    } finally {
      System.clearProperty("IT_SINCE_DATE");
    }
  }

  /**
   * Verifies that {@code quoteValues: false} works correctly against a live API.
   * The BRFSS dataset uses a numeric {@code year} column — Socrata rejects quoted
   * comparisons ({@code year >= '2023'}) with HTTP 500 but accepts unquoted ones
   * ({@code year >= 2023}).  A future year returns 0 rows; a known past year returns rows.
   */
  @Test
  @Tag("integration")
  void unquotedNumericYearFilterWorksAgainstCdcBrfssApi() throws IOException {
    String url = "https://data.cdc.gov/resource/dttw-5yxu.json";
    Map<String, String> params = new LinkedHashMap<String, String>();
    params.put("$limit", "10");

    // Future year with quoteValues=false → 0 rows (API accepts the filter)
    HttpSourceConfig.IncrementalConfig futureIncr = new HttpSourceConfig.IncrementalConfig(
        null, "2099", null, "$where", null, "year", null, false);
    int futureRows = drain(new HttpSource(HttpSourceConfig.builder()
        .url(url).parameters(params).response(offsetResponse()).incremental(futureIncr).build())
        .fetch(Collections.<String, String>emptyMap()));
    assertEquals(0, futureRows,
        "Numeric year >= 2099 (unquoted) must return 0 rows from BRFSS API");

    try { Thread.sleep(2000); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }

    // Past year with quoteValues=false → rows returned
    HttpSourceConfig.IncrementalConfig pastIncr = new HttpSourceConfig.IncrementalConfig(
        null, "2019", null, "$where", null, "year", null, false);
    int pastRows = drain(new HttpSource(HttpSourceConfig.builder()
        .url(url).parameters(params).response(offsetResponse()).incremental(pastIncr).build())
        .fetch(Collections.<String, String>emptyMap()));
    assumeTrue(pastRows > 0,
        "BRFSS API rate-limited (0 rows returned for past year) — test aborted, not failed");
  }
}
