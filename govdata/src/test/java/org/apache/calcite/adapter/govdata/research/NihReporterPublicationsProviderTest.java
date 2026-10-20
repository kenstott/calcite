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
package org.apache.calcite.adapter.govdata.research;

import org.apache.calcite.adapter.file.etl.RawCache;

import com.sun.net.httpserver.HttpServer;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.LinkedHashMap;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests HTTP-500 bisection and the schema-level appl_id exclusion list. */
@Tag("unit")
class NihReporterPublicationsProviderTest {
  private static final long POISON = 11269188L;
  private static final Pattern IDS = Pattern.compile("\"appl_ids\":\\[([0-9,]*)\\]");

  private HttpServer server;
  private final List<Integer> requestSizes = new CopyOnWriteArrayList<Integer>();
  /** Ids that always return HTTP 500 when present in a request. */
  private final Set<Long> poison = ConcurrentHashMap.newKeySet();
  /** Remaining transient 500s per id (decremented on each request that contains the id alone). */
  private final Map<Long, Integer> transientFailures = new ConcurrentHashMap<Long, Integer>();
  private volatile int reportedTotal = -1;

  @BeforeEach void start() throws IOException {
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext("/pubs", ex -> {
      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      InputStream in = ex.getRequestBody();
      byte[] buf = new byte[4096];
      int n;
      while ((n = in.read(buf)) > 0) {
        bos.write(buf, 0, n);
      }
      Matcher m = IDS.matcher(new String(bos.toByteArray(), StandardCharsets.UTF_8));
      if (!m.find()) {
        throw new IOException("no appl_ids in request");
      }
      String[] ids = m.group(1).split(",");
      requestSizes.add(ids.length);
      boolean fail = false;
      StringBuilder results = new StringBuilder();
      for (String id : ids) {
        long l = Long.parseLong(id);
        if (poison.contains(l)) {
          fail = true;
        }
        Integer left = transientFailures.get(l);
        if (ids.length == 1 && left != null && left > 0) {
          transientFailures.put(l, left - 1);
          fail = true;
        }
        if (results.length() > 0) {
          results.append(',');
        }
        results.append("{\"applid\":").append(id).append(",\"coreproject\":\"C").append(id)
            .append("\",\"pmid\":").append(l + 1).append('}');
      }
      byte[] out;
      int status;
      if (fail) {
        status = 500;
        out = "{\"error\":\"boom\"}".getBytes(StandardCharsets.UTF_8);
      } else {
        status = 200;
        int total = reportedTotal >= 0 ? reportedTotal : ids.length;
        out = ("{\"meta\":{\"total\":" + total + "},\"results\":[" + results + "]}")
            .getBytes(StandardCharsets.UTF_8);
      }
      ex.sendResponseHeaders(status, out.length);
      ex.getResponseBody().write(out);
      ex.close();
    });
    server.start();
    configure(25, 2);
  }

  @AfterEach void stop() {
    server.stop(0);
  }

  private static NihReporterPublicationsProvider.Limits limits = null;

  private static void configure(int maxInvalid, int maxRetries) {
    limits = new NihReporterPublicationsProvider.Limits(maxRetries, maxInvalid);
  }

  private NihReporterPublicationsProvider provider() {
    return new NihReporterPublicationsProvider(
        "http://127.0.0.1:" + server.getAddress().getPort() + "/pubs", 1L);
  }

  private static List<Long> ids() {
    return new ArrayList<Long>(Arrays.asList(1L, 2L, 3L, POISON, 5L, 6L, 7L, 8L));
  }

  private List<Map<String, Object>> fetch(List<Long> ids, Map<Long, String> invalid)
      throws IOException {
    return provider().fetchPublicationsForBatch(ids, "2026", RawCache.passthrough(), 0, invalid, limits);
  }

  @Test void poisonIdYieldsOneInvalidRowAndOthersStayValid() throws IOException {
    poison.add(POISON);
    Map<Long, String> invalid = new LinkedHashMap<Long, String>();
    List<Map<String, Object>> rows = fetch(ids(), invalid);
    assertEquals(8, rows.size());
    int invalidRows = 0;
    for (Map<String, Object> row : rows) {
      if ((Long) row.get("appl_id") == POISON) {
        invalidRows++;
        assertEquals("invalid_source_record", row.get("record_status"));
        assertNull(row.get("pmid"));
        assertTrue(((String) row.get("record_issue")).contains("HTTP 500"));
        assertTrue(((String) row.get("record_issue")).contains("3 attempts"));
      } else {
        assertEquals("valid", row.get("record_status"));
        assertNull(row.get("record_issue"));
      }
    }
    assertEquals(1, invalidRows);
    assertEquals(Collections.singleton(POISON), invalid.keySet());
  }

  @Test void transientSingleIdFailureRetriesToAValidRow() throws IOException {
    transientFailures.put(2L, 2);
    Map<Long, String> invalid = new LinkedHashMap<Long, String>();
    List<Map<String, Object>> rows = fetch(Collections.singletonList(2L), invalid);
    assertEquals(1, rows.size());
    assertEquals("valid", rows.get(0).get("record_status"));
    assertEquals(3L, rows.get(0).get("pmid"));
    assertTrue(invalid.isEmpty());
    assertEquals(Arrays.asList(1, 1, 1), requestSizes);
  }

  @Test void moreInvalidIdsThanTheCapThrows() {
    configure(1, 0);
    poison.add(2L);
    poison.add(3L);
    IOException e = assertThrows(IOException.class,
        () -> fetch(Arrays.asList(1L, 2L, 3L, 4L), new LinkedHashMap<Long, String>()));
    assertTrue(e.getMessage().contains("maxInvalidRecordsPerSlice=1"), e.getMessage());
    assertTrue(e.getMessage().contains("2 invalid appl_ids"), e.getMessage());
    assertTrue(e.getMessage().contains("2") && e.getMessage().contains("3"), e.getMessage());
  }

  @Test void twoPoisonIdsInOneBatchAreBothFound() throws IOException {
    poison.add(POISON);
    poison.add(6L);
    Map<Long, String> invalid = new LinkedHashMap<Long, String>();
    List<Map<String, Object>> rows = fetch(ids(), invalid);
    assertEquals(8, rows.size());
    assertEquals(new java.util.HashSet<Long>(Arrays.asList(POISON, 6L)), invalid.keySet());
    int valid = 0;
    for (Map<String, Object> row : rows) {
      if ("valid".equals(row.get("record_status"))) {
        valid++;
      }
    }
    assertEquals(6, valid);
  }

  /** A lone id whose answer overflows the window is an unscoped response: marked, not thrown. */
  @Test void singleIdOverTheWindowIsMarkedInvalid() throws IOException {
    reportedTotal = 10001;
    Map<Long, String> invalid = new LinkedHashMap<Long, String>();
    List<Map<String, Object>> rows = fetch(Arrays.asList(1L, 2L, 3L), invalid);
    assertTrue(requestSizes.contains(3) && requestSizes.contains(1), requestSizes.toString());
    assertEquals(new java.util.HashSet<Long>(Arrays.asList(1L, 2L, 3L)), invalid.keySet());
    assertEquals(3, rows.size());
    for (Map<String, Object> row : rows) {
      assertEquals("invalid_source_record", row.get("record_status"));
      assertNull(row.get("pmid"));
      assertTrue(String.valueOf(row.get("record_issue")).contains("ignored the appl_id filter"),
          String.valueOf(row.get("record_issue")));
    }
  }

  private static org.apache.calcite.adapter.file.etl.EtlPipelineConfig configWith(
      Map<String, Object> extraSource) {
    Map<String, Object> source = new HashMap<String, Object>();
    source.put("type", "http");
    source.put("url", "http://127.0.0.1/pubs");
    source.putAll(extraSource);
    Map<String, Object> table = new HashMap<String, Object>();
    table.put("name", "nih_publications");
    table.put("source", source);
    Map<String, Object> output = new HashMap<String, Object>();
    output.put("pattern", "type=nih_publications/year=*/");
    Map<String, Object> materialize = new HashMap<String, Object>();
    materialize.put("format", "iceberg");
    materialize.put("output", output);
    table.put("materialize", materialize);
    return org.apache.calcite.adapter.file.etl.EtlPipelineConfig.fromMap(table);
  }

  @Test void limitsAreReadFromTheSourceBlock() {
    Map<String, Object> extra = new HashMap<String, Object>();
    extra.put("maxRetries", 3);
    extra.put("maxInvalidRecordsPerSlice", 25);
    NihReporterPublicationsProvider.Limits l =
        NihReporterPublicationsProvider.Limits.from(configWith(extra));
    assertEquals(3, l.maxRetries);
    assertEquals(25, l.maxInvalid);
  }

  @Test void missingRequiredSettingFailsLoudly() {
    assertThrows(IllegalStateException.class,
        () -> NihReporterPublicationsProvider.Limits.from(
            configWith(new HashMap<String, Object>())));
  }
}
