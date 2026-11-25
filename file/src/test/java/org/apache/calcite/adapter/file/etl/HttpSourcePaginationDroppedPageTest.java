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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * A transform that drops every record of a page must not end a crawl whose reported total says
 * more pages follow.
 */
class HttpSourcePaginationDroppedPageTest {

  private HttpServer server;
  private String baseUrl;

  @BeforeEach void startServer() throws IOException {
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    baseUrl = "http://127.0.0.1:" + server.getAddress().getPort();
    server.start();
  }

  @AfterEach void stopServer() {
    server.stop(0);
  }

  /** Serves the given page bodies in request order. */
  private AtomicInteger serve(final String... pages) {
    final AtomicInteger requests = new AtomicInteger();
    server.createContext("/data", new HttpHandler() {
      @Override public void handle(HttpExchange exchange) throws IOException {
        byte[] bytes = pages[requests.getAndIncrement()].getBytes(StandardCharsets.UTF_8);
        exchange.sendResponseHeaders(200, bytes.length);
        OutputStream out = exchange.getResponseBody();
        out.write(bytes);
        out.close();
      }
    });
    return requests;
  }

  private static String page(int total, String... records) {
    StringBuilder sb = new StringBuilder("{\"total\":").append(total).append(",\"results\":[");
    for (int i = 0; i < records.length; i++) {
      sb.append(i == 0 ? "" : ",").append(records[i]);
    }
    return sb.append("]}").toString();
  }

  /** Fetches with a transform that keeps only records whose keep flag is 1. */
  private List<Object> fetchKeptIds() throws IOException {
    Map<String, Object> paginationMap = new LinkedHashMap<String, Object>();
    paginationMap.put("type", "PAGE_ZERO");
    paginationMap.put("limitParam", "per_page");
    paginationMap.put("pageParam", "page");
    paginationMap.put("countPath", "total");
    paginationMap.put("pageSize", 1);
    Map<String, Object> responseMap = new LinkedHashMap<String, Object>();
    responseMap.put("format", "JSON");
    responseMap.put("dataPath", "results");
    responseMap.put("pagination", paginationMap);
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url(baseUrl + "/data")
        .response(HttpSourceConfig.ResponseConfig.fromMap(responseMap))
        .build();
    HttpSource src = new HttpSource(config, new ResponseTransformer() {
      @Override public String transform(String response, RequestContext context) {
        return response.contains("\"keep\":1")
            ? response.substring(response.indexOf('['), response.lastIndexOf(']') + 1)
            : "[]";
      }
    });
    List<Object> ids = new ArrayList<Object>();
    Iterator<Map<String, Object>> it = src.fetch(Collections.<String, String>emptyMap());
    while (it.hasNext()) {
      ids.add(it.next().get("id"));
    }
    return ids;
  }

  @Test @Tag("unit") void continuesPastPagesTheTransformEmptied() throws IOException {
    AtomicInteger requests =
        serve(page(4, "{\"id\":1,\"keep\":0}"), page(4, "{\"id\":2,\"keep\":1}"),
            page(4, "{\"id\":3,\"keep\":0}"), page(4, "{\"id\":4,\"keep\":1}"));

    assertEquals("[2, 4]", fetchKeptIds().toString());
    assertEquals(4, requests.get());
  }

  @Test @Tag("unit") void stopsAtTheTotalWhenEveryPageIsEmptied() throws IOException {
    AtomicInteger requests =
        serve(page(2, "{\"id\":1,\"keep\":0}"), page(2, "{\"id\":2,\"keep\":0}"));

    assertEquals("[]", fetchKeptIds().toString());
    assertEquals(2, requests.get());
  }

  @Test @Tag("unit") void stopsAtAnEmptySourcePage() throws IOException {
    AtomicInteger requests = serve(page(50, "{\"id\":1,\"keep\":1}"), page(50));

    assertEquals("[1]", fetchKeptIds().toString());
    assertEquals(2, requests.get());
  }
}
