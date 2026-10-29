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
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A Comtrade response that reaches the request's {@code maxRecords} is cut off by the API, so it is
 * dropped and re-requested split by partner until every slice comes back under the cap.
 */
@Tag("unit")
class ComtradeTransformerSplitTest {

  private HttpServer server;
  /** The {@code partnerCode} value of each request, or "" for none. */
  private final List<String> requests = new CopyOnWriteArrayList<String>();
  /** Source dataset: partner code to the number of records it holds. */
  private final Map<String, Integer> dataset = new LinkedHashMap<String, Integer>();

  @BeforeEach void startServer() throws IOException {
    ComtradeTransformer.resetCappedReporters();
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext("/data", exchange -> {
      String query = exchange.getRequestURI().getRawQuery();
      String partnerParam = "";
      long cap = Long.MAX_VALUE;
      for (String pair : query.split("&")) {
        if (pair.startsWith("partnerCode=")) {
          partnerParam = pair.substring("partnerCode=".length());
        } else if (pair.startsWith("maxRecords=")) {
          cap = Long.parseLong(pair.substring("maxRecords=".length()));
        }
      }
      requests.add(partnerParam);
      Set<String> wanted = partnerParam.isEmpty() ? null
          : new HashSet<String>(Arrays.asList(partnerParam.split(",")));
      List<String> records = new ArrayList<String>();
      for (Map.Entry<String, Integer> e : dataset.entrySet()) {
        if (wanted != null && !wanted.contains(e.getKey())) {
          continue;
        }
        for (int i = 0; i < e.getValue(); i++) {
          records.add("{\"reporterISO\":\"USA\",\"partnerCode\":" + e.getKey()
              + ",\"partnerISO\":\"P" + e.getKey() + "\",\"flowCode\":\"M\",\"cmdCode\":\"0101"
              + String.format("%02d", i) + "\",\"primaryValue\":" + i + "}");
        }
      }
      // What the real API does: stops at the cap and reports the records it returned.
      List<String> returned = records.subList(0, (int) Math.min(records.size(), cap));
      byte[] body = ("{\"elapsedTime\":\"1 secs\",\"count\":" + returned.size() + ",\"data\":["
          + String.join(",", returned) + "],\"error\":\"\"}").getBytes(StandardCharsets.UTF_8);
      exchange.getResponseHeaders().add("Content-Type", "application/json");
      exchange.sendResponseHeaders(200, body.length);
      OutputStream out = exchange.getResponseBody();
      out.write(body);
      exchange.close();
    });
    server.start();
  }

  @AfterEach void stopServer() {
    server.stop(0);
  }

  private String url(String extra) {
    return "http://127.0.0.1:" + server.getAddress().getPort() + "/data?reporterCode=842" + extra;
  }

  private static RequestContext context(String url) {
    Map<String, Object> rl = new HashMap<String, Object>();
    rl.put("requestsPerSecond", 1000);
    rl.put("maxRetries", 0);
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

  @Test void underTheCapIsOneRequestWithNoPartnerFilter() throws Exception {
    dataset.put("4", 2);
    dataset.put("156", 2);
    List<Map<String, Object>> rows = drain(
        new ComtradeTransformer().fetchAndTransform(context(url("&maxRecords=5"))));
    assertEquals(4, rows.size());
    assertEquals(Arrays.asList(""), requests);
  }

  @Test void aResponseAtTheCapIsSplitByPartnerAndNoRowIsLostOrRepeated() throws Exception {
    dataset.put("4", 3);
    dataset.put("8", 2);
    dataset.put("12", 3);
    dataset.put("156", 4);
    dataset.put("842", 1);
    List<Map<String, Object>> rows = drain(
        new ComtradeTransformer().fetchAndTransform(context(url("&maxRecords=5"))));
    assertEquals(13, rows.size());
    Set<String> keys = new HashSet<String>();
    for (Map<String, Object> r : rows) {
      keys.add(r.get("partner_code") + "/" + r.get("hs6"));
    }
    assertEquals(13, keys.size(), "a row came back twice or not at all");
    assertEquals("", requests.get(0), "first request asks for every partner");
    assertTrue(requests.size() > 2, requests.toString());
    // no partner was requested by two slices that both returned rows
    Set<String> seen = new HashSet<String>();
    for (String r : requests.subList(1, requests.size())) {
      if (r.indexOf(',') < 0 && !r.isEmpty()) {
        assertTrue(seen.add(r), "partner " + r + " requested twice");
      }
    }
  }

  @Test void aReporterSeenAtTheCapSkipsTheAllPartnersRequestNextTime() throws Exception {
    dataset.put("4", 3);
    dataset.put("156", 4);
    dataset.put("842", 1);
    drain(new ComtradeTransformer().fetchAndTransform(context(url("&maxRecords=5"))));
    assertEquals("", requests.get(0));
    requests.clear();
    List<Map<String, Object>> rows = drain(
        new ComtradeTransformer().fetchAndTransform(context(url("&maxRecords=5"))));
    assertEquals(8, rows.size());
    assertFalse(requests.contains(""), "all-partners request repeated: " + requests);
  }

  @Test void aSinglePartnerAtTheCapFailsLoudly() {
    dataset.put("156", 6);
    IOException e = assertThrows(IOException.class, () -> drain(
        new ComtradeTransformer().fetchAndTransform(context(url("&maxRecords=5")))));
    assertTrue(e.getMessage().contains("cannot be split further"), e.getMessage());
    assertTrue(e.getMessage().contains("156"), e.getMessage());
  }

  @Test void aUrlWithoutMaxRecordsIsRejected() {
    IllegalStateException e = assertThrows(IllegalStateException.class,
        () -> new ComtradeTransformer().fetchAndTransform(context(url(""))));
    assertTrue(e.getMessage().contains("maxRecords"), e.getMessage());
    assertEquals(0, requests.size());
  }

  @Test void aUrlThatSetsPartnerCodeIsRejected() {
    assertThrows(IllegalStateException.class, () -> new ComtradeTransformer()
        .fetchAndTransform(context(url("&maxRecords=5&partnerCode=156"))));
  }

  @Test void thePartnerListHoldsEveryCodeIncludingWorld() throws Exception {
    List<String> p = ComtradeTransformer.partners();
    assertTrue(p.contains("0"), "World");
    assertTrue(p.contains("842") && p.contains("156") && p.contains("124"));
    assertEquals(p.size(), new HashSet<String>(p).size(), "duplicate partner code");
    assertFalse(p.isEmpty());
  }

  @Test void theSchemaUrlPinsEveryBreakdownBelowTheTableGrain() throws Exception {
    String yaml;
    try (InputStream in = ComtradeTransformer.class.getResourceAsStream(
        "/econ/econ-schema.yaml")) {
      assertTrue(in != null, "econ-schema.yaml is not on the classpath");
      yaml = new String(readAll(in), StandardCharsets.UTF_8);
    }
    java.util.regex.Matcher m = java.util.regex.Pattern
        .compile("url: \"(https://comtradeapi\\.un\\.org/[^\"]*)\"").matcher(yaml);
    assertTrue(m.find(), "comtrade source url");
    String url = m.group(1);
    for (String pin : new String[] {"cmdCode=AG6", "motCode=0", "customsCode=C00",
        "partner2Code=0"}) {
      assertTrue(url.contains("&" + pin + "&"), url + " must pin " + pin);
    }
  }

  private static byte[] readAll(InputStream in) throws IOException {
    java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
    byte[] buf = new byte[8192];
    for (int n = in.read(buf); n >= 0; n = in.read(buf)) {
      out.write(buf, 0, n);
    }
    return out.toByteArray();
  }
}
