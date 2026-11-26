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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * With {@code response.strictRecords}, a CSV body delivered with HTTP 200 that is cut short or
 * ends in the server's error payload fails the fetch and does not stay in the raw cache.
 */
@Tag("unit")
class HttpSourceStrictRecordsTest {

  private static final String HEADER = "id,name,value\n";
  private static final String COMPLETE = HEADER + "1,a,10\n2,\"b,\nb\",20\n3,c,\n\n";

  @TempDir Path cacheDir;

  private HttpServer server;
  private volatile String body;

  @BeforeEach void startServer() throws IOException {
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext("/data.csv", exchange -> {
      byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
      exchange.sendResponseHeaders(200, 0);
      try (OutputStream out = exchange.getResponseBody()) {
        out.write(bytes);
      }
    });
    server.start();
  }

  @AfterEach void stopServer() {
    server.stop(0);
  }

  private HttpSource source(boolean strict) {
    Map<String, Object> response = new HashMap<String, Object>();
    response.put("format", "csv");
    response.put("strictRecords", strict);
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("http://127.0.0.1:" + server.getAddress().getPort() + "/data.csv")
        .response(HttpSourceConfig.ResponseConfig.fromMap(response))
        .rawCache(HttpSourceConfig.RawCacheConfig.enabled())
        .build();
    return new HttpSource(config, (HooksConfig) null, new LocalFileStorageProvider(),
        cacheDir.toString());
  }

  private static List<Map<String, Object>> readAll(HttpSource source) throws IOException {
    List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
    Iterator<Map<String, Object>> it =
        source.fetch(Collections.singletonMap("type", "t"));
    while (it.hasNext()) {
      rows.add(it.next());
    }
    return rows;
  }

  private long cachedFiles() throws IOException {
    try (Stream<Path> files = Files.walk(cacheDir)) {
      return files.filter(p -> p.getFileName().toString().startsWith("response")).count();
    }
  }

  private void assertRejected(String badBody) throws IOException {
    body = badBody;
    HttpSource source = source(true);
    IllegalStateException e =
        assertThrows(IllegalStateException.class, () -> readAll(source));
    assertTrue(e.getMessage().startsWith("Incomplete CSV body"), e.getMessage());
    assertTrue(e.getMessage().contains("the cached body was removed"), e.getMessage());
    assertEquals(0, cachedFiles());
  }

  @Test void completeBodyIsReadAndKept() throws IOException {
    body = COMPLETE;
    assertEquals(3, readAll(source(true)).size());
    assertEquals(1, cachedFiles());
  }

  @Test void errorPayloadAppendedMidRecordIsRejected() throws IOException {
    assertRejected(HEADER + "1,a,10\n2,b{\n  \"error\" : true,\n"
        + "  \"message\" : \"Internal error\",\n  \"status\" : 500\n}\n");
  }

  @Test void errorPayloadAfterACompleteRecordIsRejected() throws IOException {
    assertRejected(HEADER + "1,a,10\n{\n  \"error\" : true\n}\n");
  }

  @Test void shortFinalRecordIsRejected() throws IOException {
    assertRejected(HEADER + "1,a,10\n2,b");
  }

  @Test void shortOnlyRecordIsRejected() throws IOException {
    assertRejected(HEADER + "1,a");
  }

  @Test void unterminatedQuoteInLastFieldIsRejected() throws IOException {
    assertRejected(HEADER + "1,a,10\n2,b,\"20");
  }

  @Test void nextRunDownloadsAgainAfterARejection() throws IOException {
    assertRejected(HEADER + "1,a,10\n2,b");
    body = COMPLETE;
    assertEquals(3, readAll(source(true)).size());
  }

  @Test void shortRecordIsAcceptedWhenNotStrict() throws IOException {
    body = HEADER + "1,a,10\n2,b\n";
    assertEquals(2, readAll(source(false)).size());
  }

  @Test void strictRecordsOnJsonIsRefused() {
    Map<String, Object> response = new HashMap<String, Object>();
    response.put("format", "json");
    response.put("strictRecords", true);
    assertThrows(IllegalArgumentException.class,
        () -> HttpSourceConfig.ResponseConfig.fromMap(response));
  }
}
