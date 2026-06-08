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
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for HttpSource {@code urlResolver} indirection: the configured url returns JSON
 * pointing at the real download URL, which HttpSource then fetches.
 *
 * <p>Uses a local {@link HttpServer} on a random port — no network access required.
 */
class HttpSourceUrlResolverTest {

  private HttpServer server;
  private String baseUrl;

  private final AtomicReference<String> resolverUri = new AtomicReference<>();
  private final AtomicReference<String> downloadUri = new AtomicReference<>();

  @BeforeEach void startServer() throws IOException {
    server = HttpServer.create(new InetSocketAddress(0), 0);
    baseUrl = "http://localhost:" + server.getAddress().getPort();
    // The actual download endpoint: records its URI, returns a top-level JSON array.
    server.createContext("/download", new HttpHandler() {
      @Override public void handle(HttpExchange exchange) throws IOException {
        downloadUri.set(exchange.getRequestURI().toString());
        send(exchange, "[{\"id\":\"1\"},{\"id\":\"2\"}]");
      }
    });
    server.start();
  }

  @AfterEach void stopServer() {
    if (server != null) {
      server.stop(0);
    }
    resolverUri.set(null);
    downloadUri.set(null);
  }

  private static void send(HttpExchange exchange, String body) throws IOException {
    byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
    exchange.sendResponseHeaders(200, bytes.length);
    try (OutputStream out = exchange.getResponseBody()) {
      out.write(bytes);
    }
  }

  /** Registers a resolver endpoint that records its URI and returns the given JSON body. */
  private void addResolver(String path, final String responseBody) {
    server.createContext(path, new HttpHandler() {
      @Override public void handle(HttpExchange exchange) throws IOException {
        resolverUri.set(exchange.getRequestURI().toString());
        send(exchange, responseBody);
      }
    });
  }

  private static HttpSourceConfig.ResponseConfig jsonResponse() {
    Map<String, Object> responseMap = new LinkedHashMap<String, Object>();
    responseMap.put("format", "JSON");
    return HttpSourceConfig.ResponseConfig.fromMap(responseMap);
  }

  private static int drain(Iterator<Map<String, Object>> it) {
    int count = 0;
    while (it.hasNext()) {
      it.next();
      count++;
    }
    return count;
  }

  // ── sole-value extraction (single-field object, no urlField) ────────────────

  @Test @Tag("unit") void soleValueResolvesAndFetchesDownload() throws IOException {
    // Resolver echoes the key as the field name — value is the real download URL.
    addResolver("/signedurl",
        "{\"master_files/reta/reta-2023.zip\":\"" + baseUrl + "/download\"}");

    Map<String, String> params = new LinkedHashMap<String, String>();
    params.put("key", "master_files/reta/reta-2023.zip");
    HttpSourceConfig cfg = HttpSourceConfig.builder()
        .url(baseUrl + "/signedurl")
        .parameters(params)
        .response(jsonResponse())
        .urlResolver(HttpSourceConfig.UrlResolverConfig.fromMap(
            Collections.<String, Object>emptyMap()))
        .build();

    int rows = drain(new HttpSource(cfg).fetch(Collections.<String, String>emptyMap()));

    assertEquals(2, rows, "resolved download must be fetched and parsed");
    assertNotNull(resolverUri.get(), "resolver endpoint must have been called");
    assertTrue(resolverUri.get().contains("key="),
        "query params must go to the resolver — got: " + resolverUri.get());
    assertNotNull(downloadUri.get(), "download endpoint must have been called");
    assertFalse(downloadUri.get().contains("key="),
        "resolver params must NOT be re-applied to the resolved URL — got: " + downloadUri.get());
  }

  // ── named-field extraction (urlField selects from a multi-field object) ──────

  @Test @Tag("unit") void namedFieldResolvesFromMultiFieldObject() throws IOException {
    addResolver("/signedurl",
        "{\"href\":\"" + baseUrl + "/download\",\"expiresIn\":900}");

    Map<String, Object> resolverMap = new LinkedHashMap<String, Object>();
    resolverMap.put("urlField", "href");
    HttpSourceConfig cfg = HttpSourceConfig.builder()
        .url(baseUrl + "/signedurl")
        .response(jsonResponse())
        .urlResolver(HttpSourceConfig.UrlResolverConfig.fromMap(resolverMap))
        .build();

    int rows = drain(new HttpSource(cfg).fetch(Collections.<String, String>emptyMap()));
    assertEquals(2, rows, "named-field resolver must fetch the resolved download");
  }

  // ── ambiguous multi-field object with no urlField is an error ────────────────

  @Test @Tag("unit") void multiFieldWithoutUrlFieldThrows() {
    addResolver("/signedurl",
        "{\"href\":\"" + baseUrl + "/download\",\"other\":\"x\"}");

    HttpSourceConfig cfg = HttpSourceConfig.builder()
        .url(baseUrl + "/signedurl")
        .response(jsonResponse())
        .urlResolver(HttpSourceConfig.UrlResolverConfig.fromMap(
            Collections.<String, Object>emptyMap()))
        .build();

    HttpSource src = new HttpSource(cfg);
    IOException ex = assertThrows(IOException.class,
        () -> drain(src.fetch(Collections.<String, String>emptyMap())));
    assertTrue(ex.getMessage().contains("single-field"),
        "error must explain the single-field expectation — got: " + ex.getMessage());
    assertNotNull(resolverUri.get(), "resolver must have been called");
    assertEquals(null, downloadUri.get(), "download must not be reached on resolver error");
  }
}
