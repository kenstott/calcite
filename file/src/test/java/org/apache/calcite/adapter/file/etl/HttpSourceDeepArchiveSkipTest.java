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

import org.apache.calcite.adapter.file.storage.LocalFileStorageProvider;
import org.apache.calcite.adapter.file.storage.StorageProvider;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Regression for kenstott/govdata-ops#230: a CSV_STREAM source's {@code skipOn} must not treat
 * an S3 {@code InvalidObjectState} 403 (object archived to Glacier/Deep Archive — real data,
 * temporarily unretrievable) the same as a genuine, cleanly-skippable gap (weekend/holiday/future
 * date). Both arrive as HTTP 403 with nothing but the response body to tell them apart.
 */
@Tag("unit")
class HttpSourceDeepArchiveSkipTest {

  private HttpServer server;
  private String baseUrl;

  @TempDir File tempDir;

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

  private void addResponseHandler(String path, int status, String body) {
    server.createContext(path, new HttpHandler() {
      @Override public void handle(HttpExchange exchange) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.sendResponseHeaders(status, bytes.length);
        try (OutputStream out = exchange.getResponseBody()) {
          out.write(bytes);
        }
      }
    });
  }

  private HttpSourceConfig csvStreamConfig(String url, int... skipOn) {
    Map<String, Object> paginationMap = new LinkedHashMap<>();
    paginationMap.put("type", "CSV_STREAM");
    paginationMap.put("pageSize", 2000);
    Map<String, Object> responseMap = new LinkedHashMap<>();
    responseMap.put("format", "CSV");
    responseMap.put("pagination", paginationMap);

    Map<String, Object> rateLimitMap = new LinkedHashMap<>();
    java.util.List<Integer> skipOnList = new java.util.ArrayList<>();
    for (int code : skipOn) {
      skipOnList.add(code);
    }
    rateLimitMap.put("skipOn", skipOnList);

    return HttpSourceConfig.builder()
        .url(url)
        .response(HttpSourceConfig.ResponseConfig.fromMap(responseMap))
        .rateLimit(HttpSourceConfig.RateLimitConfig.fromMap(rateLimitMap))
        .rawCache(HttpSourceConfig.RawCacheConfig.enabled())
        .extractPattern("*.csv")
        .build();
  }

  private HttpSource sourceWithRawCache(HttpSourceConfig config, String cacheKey) {
    StorageProvider storageProvider = new LocalFileStorageProvider();
    String rawCachePath = new File(tempDir, "raw-cache-" + cacheKey).getAbsolutePath();
    return new HttpSource(config, (HooksConfig) null, storageProvider, rawCachePath);
  }

  @Test void archivedObjectSurfacesAsFailureNotCleanSkip() throws IOException {
    // The exact body S3 returns for a Glacier/Deep Archive object — real data, not a gap.
    addResponseHandler("/archived.zip", 403,
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
            + "<Error><Code>InvalidObjectState</Code>"
            + "<Message>The operation is not valid for the object's storage class</Message>"
            + "<StorageClass>DEEP_ARCHIVE</StorageClass></Error>");

    HttpSourceConfig config = csvStreamConfig(baseUrl + "/archived.zip", 403, 404);
    HttpSource src = sourceWithRawCache(config, "archived");

    IOException thrown = assertThrows(IOException.class,
        () -> drain(src.fetch(Collections.<String, String>emptyMap())),
        "an archived object must not be silently treated as an empty period");
    assertTrue(thrown.getMessage().contains("InvalidObjectState"),
        "the failure must name the real cause: " + thrown.getMessage());
  }

  @Test void genuineGapStillSkipsCleanly() throws IOException {
    // A plain 403 with no InvalidObjectState signature — e.g. a weekend/holiday URL that was
    // never going to have a file. Must still be skipped exactly as before this fix.
    addResponseHandler("/holiday.zip", 403, "<Error><Code>AccessDenied</Code></Error>");

    HttpSourceConfig config = csvStreamConfig(baseUrl + "/holiday.zip", 403, 404);
    HttpSource src = sourceWithRawCache(config, "holiday");

    int rows = drain(src.fetch(Collections.<String, String>emptyMap()));
    assertEquals(0, rows, "a genuine skip-eligible 403 must still yield no rows, no exception");
  }

  @Test void notFoundStillSkipsCleanly() throws IOException {
    addResponseHandler("/missing.zip", 404, "Not Found");

    HttpSourceConfig config = csvStreamConfig(baseUrl + "/missing.zip", 403, 404);
    HttpSource src = sourceWithRawCache(config, "missing");

    int rows = drain(src.fetch(Collections.<String, String>emptyMap()));
    assertEquals(0, rows, "404 must still be skipped cleanly — the fix only narrows 403 handling");
  }

  private static int drain(Iterator<Map<String, Object>> it) {
    int count = 0;
    while (it.hasNext()) {
      it.next();
      count++;
    }
    return count;
  }
}
