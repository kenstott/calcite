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

import org.apache.calcite.adapter.file.etl.RequestContext;

import com.sun.net.httpserver.HttpServer;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** A Census trade body cut off mid-stream is re-fetched whole, before any row reaches the caller. */
@Tag("unit")
class CensusTradeStreamingTransformerRetryTest {
  private static final String FULL =
      "[[\"STATE\",\"CTY_CODE\",\"ALL_VAL_MO\"],[\"06\",\"1220\",\"10\"],[\"06\",\"5700\",\"20\"],"
          + "[\"48\",\"2010\",\"30\"]]";

  private HttpServer server;
  private final AtomicInteger requests = new AtomicInteger();

  @BeforeEach void start() throws IOException {
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.start();
  }

  @AfterEach void stop() {
    server.stop(0);
  }

  /** Serves a body declared longer than sent (so the connection resets) for the first N requests. */
  private String serve(final int failures, final String cutBody) {
    server.createContext("/trade", exchange -> {
      int n = requests.incrementAndGet();
      byte[] body = (n <= failures ? cutBody : FULL).getBytes(StandardCharsets.UTF_8);
      exchange.sendResponseHeaders(200, n <= failures ? body.length + 4096 : body.length);
      OutputStream out = exchange.getResponseBody();
      out.write(body);
      out.flush();
      exchange.close();
    });
    return "http://127.0.0.1:" + server.getAddress().getPort() + "/trade";
  }

  private static List<Map<String, Object>> drain(Iterator<Map<String, Object>> it) {
    List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
    while (it.hasNext()) {
      rows.add(it.next());
    }
    return rows;
  }

  private static RequestContext ctx(String url) {
    return RequestContext.builder().url(url).build();
  }

  @Test void retriesWholeFetchWhenBodyCutAfterRows() throws Exception {
    String url = serve(2, FULL.substring(0, 60));
    List<Map<String, Object>> rows =
        drain(new CensusTradeStreamingTransformer().fetchAndTransform(ctx(url)));
    assertEquals(3, rows.size());
    assertEquals("5700", rows.get(1).get("CTY_CODE"));
    assertEquals(3, requests.get());
  }

  @Test void exhaustedRetriesFailWithUrlAndCause() throws Exception {
    String url = serve(100, FULL.substring(0, 60));
    IOException e = assertThrows(IOException.class,
        () -> new CensusTradeStreamingTransformer().fetchAndTransform(ctx(url)));
    assertTrue(e.getMessage().contains(url), e.getMessage());
    assertTrue(e.getCause() != null);
    assertEquals(4, requests.get());
  }

  @Test void spoolFileIsRemovedOnceTheRowsAreDrained() throws Exception {
    File tmp = new File(System.getProperty("java.io.tmpdir"));
    int before = tmp.listFiles((d, n) -> n.startsWith("census-trade-")).length;
    String url = serve(0, "");
    drain(new CensusTradeStreamingTransformer().fetchAndTransform(ctx(url)));
    assertEquals(before, tmp.listFiles((d, n) -> n.startsWith("census-trade-")).length);
  }
}
