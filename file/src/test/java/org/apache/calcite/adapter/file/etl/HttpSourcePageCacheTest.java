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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
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
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * A raw cache hit on a paginated JSON source must give the transformer the same input the fetch
 * gave it, so that it yields the same rows.
 */
@Tag("unit")
class HttpSourcePageCacheTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  @TempDir File tempDir;

  private HttpServer server;
  private final AtomicInteger requests = new AtomicInteger();

  /** Serves two records, then one, nested under {@code data} beside unrelated fields. */
  @BeforeEach void startServer() throws IOException {
    server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
    server.createContext("/data", new HttpHandler() {
      @Override public void handle(HttpExchange exchange) throws IOException {
        requests.incrementAndGet();
        boolean first = exchange.getRequestURI().getQuery().contains("offset=0");
        String body = "{\"meta\":{\"total\":3},\"data\":"
            + (first ? "[{\"id\":1},{\"id\":2}]" : "[{\"id\":3}]") + "}";
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.sendResponseHeaders(200, bytes.length);
        OutputStream out = exchange.getResponseBody();
        out.write(bytes);
        out.close();
      }
    });
    server.start();
  }

  @AfterEach void stopServer() {
    server.stop(0);
  }

  /** Reads the upstream body only: the records under {@code data}. */
  static class UpstreamBodyTransformer implements ResponseTransformer {
    @Override public String transform(String response, RequestContext context) {
      try {
        ArrayNode out = MAPPER.createArrayNode();
        for (JsonNode record : MAPPER.readTree(response).path("data")) {
          out.add(record);
        }
        return out.toString();
      } catch (IOException e) {
        throw new IllegalStateException(e);
      }
    }
  }

  /** Reads either the upstream body or the merged envelope. */
  static class EnvelopeTransformer implements MergedEnvelopeResponseTransformer {
    @Override public String transform(String response, RequestContext context) {
      try {
        JsonNode root = MAPPER.readTree(response);
        return (root.has("results") ? root.path("results") : root.path("data")).toString();
      } catch (IOException e) {
        throw new IllegalStateException(e);
      }
    }
  }

  private HttpSource source(ResponseTransformer transformer, String dataPath) {
    Map<String, Object> pagination = new LinkedHashMap<String, Object>();
    pagination.put("type", "OFFSET");
    pagination.put("limitParam", "limit");
    pagination.put("offsetParam", "offset");
    pagination.put("pageSize", 2);
    pagination.put("countPath", "meta.total");
    Map<String, Object> response = new LinkedHashMap<String, Object>();
    response.put("format", "JSON");
    response.put("pagination", pagination);
    if (dataPath != null) {
      response.put("dataPath", dataPath);
    }
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("http://localhost:" + server.getAddress().getPort() + "/data")
        .response(HttpSourceConfig.ResponseConfig.fromMap(response))
        .rawCache(HttpSourceConfig.RawCacheConfig.enabled())
        .build();
    HooksConfig hooks = HooksConfig.builder()
        .responseTransformerClass(transformer.getClass().getName())
        .build();
    return new HttpSource(config, hooks, new LocalFileStorageProvider(),
        new File(tempDir, "raw").getAbsolutePath(), tempDir.getAbsolutePath());
  }

  private File cacheFile() {
    return new File(tempDir, "raw/response.json");
  }

  private static List<Object> ids(HttpSource source) throws IOException {
    List<Object> ids = new ArrayList<Object>();
    Iterator<Map<String, Object>> rows =
        source.fetch(Collections.<String, String>emptyMap());
    while (rows.hasNext()) {
      ids.add(rows.next().get("id"));
    }
    return ids;
  }

  private void writeCache(String content) throws IOException {
    Files.createDirectories(cacheFile().getParentFile().toPath());
    Files.write(cacheFile().toPath(), content.getBytes(StandardCharsets.UTF_8));
  }

  private static List<Object> allIds() {
    List<Object> ids = new ArrayList<Object>();
    Collections.<Object>addAll(ids, 1, 2, 3);
    return ids;
  }

  @Test void upstreamBodyTransformerReplaysEveryPage() throws IOException {
    assertEquals(allIds(), ids(source(new UpstreamBodyTransformer(), null)));
    assertEquals(2, requests.get());

    assertEquals(allIds(), ids(source(new UpstreamBodyTransformer(), null)));
    assertEquals(2, requests.get(), "second read served from the raw cache");
    assertEquals(2, MAPPER.readTree(cacheFile()).path("pages").size());
  }

  @Test void mergedRecordsEntryIsRefetchedForUpstreamBodyTransformer() throws IOException {
    writeCache("{\"results\":[]}");

    assertEquals(allIds(), ids(source(new UpstreamBodyTransformer(), null)));
    assertEquals(2, requests.get());
    assertEquals(2, MAPPER.readTree(cacheFile()).path("pages").size());
  }

  @Test void envelopeTransformerKeepsMergedRecords() throws IOException {
    assertEquals(allIds(), ids(source(new EnvelopeTransformer(), "data")));
    assertEquals(3, MAPPER.readTree(cacheFile()).path("results").size());

    assertEquals(allIds(), ids(source(new EnvelopeTransformer(), "data")));
    assertEquals(2, requests.get(), "second read served from the raw cache");
  }

  @Test void pageEntryIsRefetchedForEnvelopeTransformer() throws IOException {
    writeCache("{\"pages\":[\"{}\"]}");

    assertEquals(allIds(), ids(source(new EnvelopeTransformer(), "data")));
    assertEquals(2, requests.get());
  }

  @Test void mergingAPageWithoutARecordArrayFails() {
    final HttpSource source = source(new EnvelopeTransformer(), null);

    assertThrows(IllegalStateException.class, () -> ids(source));
  }
}
