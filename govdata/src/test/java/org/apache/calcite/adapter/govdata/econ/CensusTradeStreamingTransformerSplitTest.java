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

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** An unfiltered national HS6 month is fetched as commodity-prefix sub-requests. */
@Tag("unit")
class CensusTradeStreamingTransformerSplitTest {
  private HttpServer server;
  private final List<String> prefixes = new CopyOnWriteArrayList<String>();

  @BeforeEach void start() throws IOException {
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.start();
  }

  @AfterEach void stop() {
    server.stop(0);
  }

  private static String commodity(String query) {
    for (String kv : query.split("&")) {
      if (kv.startsWith("I_COMMODITY=")) {
        String v = kv.substring("I_COMMODITY=".length());
        return v.substring(0, v.length() - 1);
      }
    }
    throw new IllegalStateException("no I_COMMODITY in " + query);
  }

  private static String body(String code) {
    return "[[\"CTY_CODE\",\"I_COMMODITY\",\"GEN_VAL_MO\"],[\"-\",\"" + code + "\",\"1\"]]";
  }

  /** Prefixes in {@code timeouts} answer 524, {@code present} answer rows, the rest 204. */
  private String serve(final List<String> timeouts, final List<String> present) {
    server.createContext("/data/timeseries/intltrade/imports/hs", exchange -> {
      String prefix = commodity(exchange.getRequestURI().getRawQuery());
      prefixes.add(prefix);
      if (timeouts.contains(prefix)) {
        exchange.sendResponseHeaders(524, -1);
      } else if (present.contains(prefix)) {
        byte[] b = body(prefix + "000").getBytes(StandardCharsets.UTF_8);
        exchange.sendResponseHeaders(200, b.length);
        OutputStream out = exchange.getResponseBody();
        out.write(b);
      } else {
        exchange.sendResponseHeaders(204, -1);
      }
      exchange.close();
    });
    return "http://127.0.0.1:" + server.getAddress().getPort()
        + "/data/timeseries/intltrade/imports/hs?get=CTY_CODE,I_COMMODITY,GEN_VAL_MO"
        + "&COMM_LVL=HS6&YEAR=2025&MONTH=11&key=k";
  }

  private static List<String> codes(String url) throws IOException {
    Iterator<Map<String, Object>> it = new CensusTradeStreamingTransformer()
        .fetchAndTransform(RequestContext.builder().url(url).build());
    List<String> out = new ArrayList<String>();
    while (it.hasNext()) {
      out.add((String) it.next().get("I_COMMODITY"));
    }
    return out;
  }

  @Test void chaptersAreFetchedSeparatelyAndEmptyOnesSkipped() throws Exception {
    String url = serve(new ArrayList<String>(), java.util.Arrays.asList("01", "98"));
    assertEquals(java.util.Arrays.asList("01000", "98000"), codes(url));
    assertEquals(99, prefixes.size());
  }

  @Test void timedOutChapterIsSplitIntoItsDigitPrefixesInOrder() throws Exception {
    String url = serve(java.util.Arrays.asList("85"),
        java.util.Arrays.asList("84", "850", "853", "86"));
    assertEquals(java.util.Arrays.asList("84000", "850000", "853000", "86000"), codes(url));
    assertEquals(1, java.util.Collections.frequency(prefixes, "85"));
    assertTrue(prefixes.contains("859"));
  }

  @Test void timeoutAtFullCodeLengthFails() throws Exception {
    java.util.List<String> all = new ArrayList<String>();
    all.add("85");
    for (int d = 0; d < 10; d++) {
      all.add("85" + d);
      for (int e = 0; e < 10; e++) {
        all.add("85" + d + e);
        for (int f = 0; f < 10; f++) {
          all.add("85" + d + e + f);
          for (int g = 0; g < 10; g++) {
            all.add("85" + d + e + f + g);
          }
        }
      }
    }
    String url = serve(all, new ArrayList<String>());
    assertThrows(RuntimeException.class, () -> codes(url));
  }
}
