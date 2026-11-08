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

import org.apache.calcite.adapter.file.etl.FetchResumeStore;
import org.apache.calcite.adapter.file.etl.HttpSourceConfig;
import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.SourceExhaustedException;
import org.apache.calcite.adapter.file.storage.LocalFileStorageProvider;

import com.sun.net.httpserver.HttpServer;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A forced reprocess that the daily call quota stops part-way replays the batches it already
 * fetched from the resume store on the next run, spending calls only on the remainder, and ends
 * with the rows of one uninterrupted run.
 */
@Tag("unit")
class ComtradeTransformerResumeTest {

  private static final int CAP = 1000;

  private HttpServer server;
  private final AtomicInteger requests = new AtomicInteger();
  /** Requests served before the server answers 403; negative = never. */
  private volatile int quota = -1;
  @TempDir Path tmp;

  @BeforeEach void startServer() throws IOException {
    ComtradeTransformer.resetCappedReporters();
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext("/data", exchange -> {
      int n = requests.incrementAndGet();
      String query = exchange.getRequestURI().getQuery();
      byte[] body;
      int status = 200;
      if (quota >= 0 && n > quota) {
        status = 403;
        body = "{\"message\":\"quota\"}".getBytes(StandardCharsets.UTF_8);
      } else {
        body = bodyFor(query).getBytes(StandardCharsets.UTF_8);
      }
      exchange.getResponseHeaders().add("Content-Type", "application/json");
      exchange.sendResponseHeaders(status, body.length);
      OutputStream out = exchange.getResponseBody();
      out.write(body);
      out.flush();
      exchange.close();
    });
    server.start();
  }

  @AfterEach void stopServer() {
    server.stop(0);
  }

  private static String reporterOf(String query) {
    for (String pair : query.split("&")) {
      if (pair.startsWith("reporterCode=")) {
        return pair.substring("reporterCode=".length());
      }
    }
    throw new IllegalStateException(query);
  }

  /** Reporter 999 is over the cap unless the request is split by partner; the rest hold 1 row. */
  private static String bodyFor(String query) {
    String reporter = reporterOf(query);
    boolean split = query.contains("partnerCode=");
    if ("999".equals(reporter) && !split) {
      return "{\"count\":" + CAP + ",\"data\":[" + rec(reporter, "A") + "]}";
    }
    return "{\"count\":1,\"data\":[" + rec(reporter, split ? "S" : "P") + "]}";
  }

  private static String rec(String reporter, String tag) {
    return "{\"reporterISO\":\"R" + reporter + "\",\"partnerCode\":156,\"partnerISO\":\"" + tag
        + "\",\"flowCode\":\"M\",\"cmdCode\":\"010121\",\"primaryValue\":1}";
  }

  private String url(int reporter) {
    return "http://127.0.0.1:" + server.getAddress().getPort() + "/data?reporterCode=" + reporter
        + "&maxRecords=" + CAP;
  }

  private static RequestContext context(String url, FetchResumeStore store) {
    Map<String, Object> rl = new HashMap<String, Object>();
    rl.put("requestsPerSecond", 1000);
    rl.put("maxRetries", 0);
    rl.put("retryBackoffMs", 1);
    return RequestContext.builder()
        .url(url)
        .rateLimit(HttpSourceConfig.RateLimitConfig.fromMap(rl))
        .resumeStore(store)
        .build();
  }

  private List<String> run(int[] reporters, FetchResumeStore store) throws IOException {
    List<String> rows = new ArrayList<String>();
    for (int r : reporters) {
      Iterator<Map<String, Object>> it =
          new ComtradeTransformer().fetchAndTransform(context(url(r), store));
      while (it.hasNext()) {
        Map<String, Object> row = it.next();
        rows.add(row.get("reporter_code") + "/" + row.get("partner_code"));
      }
    }
    return rows;
  }

  private FetchResumeStore store() {
    return new FetchResumeStore(new LocalFileStorageProvider(), tmp.resolve("resume").toString());
  }

  @Test void resumedRunReplaysFinishedBatchesWithoutCallsAndMatchesAnUninterruptedRun()
      throws Exception {
    int[] reporters = {1, 2, 3, 4, 5, 6};
    List<String> uninterrupted = run(reporters, null);
    assertEquals(6, uninterrupted.size());

    requests.set(0);
    quota = 3;
    FetchResumeStore store = store();
    assertThrows(SourceExhaustedException.class, () -> run(reporters, store));
    assertEquals(4, requests.get(), "three served, the fourth refused");

    requests.set(0);
    quota = -1;
    List<String> resumed = run(reporters, store);
    assertEquals(3, requests.get(), "only reporters 4..6 reach the source");
    assertEquals(uninterrupted, resumed);
  }

  @Test void cappedProbeIsReplayedAsASplitWithoutRedownloadingTheProbe() throws Exception {
    int[] reporters = {999};
    FetchResumeStore store = store();
    List<String> first = run(reporters, store);
    // all-partners probe (capped) + two halves
    assertEquals(3, requests.get());

    ComtradeTransformer.resetCappedReporters();
    requests.set(0);
    List<String> replay = run(reporters, store);
    assertEquals(0, requests.get(), "probe and both halves replay from the store");
    assertEquals(first, replay);
  }

  @Test void clearEmptiesTheStoreSoTheNextForcedRunStartsFresh() throws Exception {
    FetchResumeStore store = store();
    run(new int[] {1, 2}, store);
    String key = FetchResumeStore.keyFor(url(1));
    assertTrue(store.has(key));

    store.clear();
    assertFalse(store.has(key));
    requests.set(0);
    run(new int[] {1, 2}, store);
    assertEquals(2, requests.get());
  }

  @Test void changedUrlNeverReadsAnOlderResponse() throws Exception {
    FetchResumeStore store = store();
    run(new int[] {1}, store);
    requests.set(0);
    new ComtradeTransformer().fetchAndTransform(
        context(url(1) + "&motCode=0", store));
    assertEquals(1, requests.get());
  }
}
