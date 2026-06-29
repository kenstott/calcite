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
package org.apache.calcite.adapter.file;

import org.apache.calcite.adapter.file.converters.CrawlerConfiguration;
import org.apache.calcite.adapter.file.converters.HtmlCrawler;
import org.apache.calcite.adapter.file.storage.HttpStorageProvider;
import org.apache.calcite.adapter.file.storage.StorageProvider;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
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
import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Hermetic requirement-recording tests for the HTML crawler discovery (FILE-018)
 * and HttpStorageProvider POST behaviors (FILE-162).
 *
 * <p>All requests target an in-JVM {@link HttpServer} bound to 127.0.0.1 on an
 * ephemeral port. No external network access is performed.
 *
 * <p>Crawler page graph served by this fixture:
 * <pre>
 *   /a.html -> /b.html, /c.html, http://external.invalid/x.html
 *   /b.html -> /d.html
 *   /c.html -> (none)
 *   /d.html -> (none)
 * </pre>
 *
 * <p>NOTE: {@code linkPattern} is, per {@link CrawlerConfiguration} usage in
 * HtmlLinkCache, applied to DATA-FILE link discovery (isDataFile), not to which
 * HTML pages are crawled. The FILE-018 linkPattern assertion therefore pins the
 * data-file-filtering behavior that is actually reachable, rather than an
 * assumed HTML-page filter.
 */
@Tag("unit")
public class HtmlCrawlerHttpPostRequirementsTest {

  private HttpServer server;
  private int port;
  private String baseUrl;

  @BeforeEach
  public void setup() throws IOException {
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    port = server.getAddress().getPort();
    baseUrl = "http://127.0.0.1:" + port;

    setupCrawlEndpoints();
    setupHttpProviderEndpoints();

    server.start();
  }

  @AfterEach
  public void tearDown() {
    if (server != null) {
      server.stop(0);
    }
  }

  // ------------------------------------------------------------------
  // crawler page-set endpoints
  // ------------------------------------------------------------------

  private void setupCrawlEndpoints() {
    // Page A links to B, C (internal) and one EXTERNAL link.
    String pageA = "<html><body>"
        + "<a href=\"" + baseUrl + "/b.html\">B</a>"
        + "<a href=\"" + baseUrl + "/c.html\">C</a>"
        + "<a href=\"http://external.invalid/x.html\">EXT</a>"
        + "<a href=\"" + baseUrl + "/data.csv\">CSV</a>"
        + "<a href=\"" + baseUrl + "/skip.json\">JSON</a>"
        + "</body></html>";
    // Page B links to D.
    String pageB = "<html><body>"
        + "<a href=\"" + baseUrl + "/d.html\">D</a>"
        + "</body></html>";
    String pageC = "<html><body><p>c leaf</p></body></html>";
    String pageD = "<html><body><p>d leaf</p></body></html>";

    registerHtml("/a.html", pageA);
    registerHtml("/b.html", pageB);
    registerHtml("/c.html", pageC);
    registerHtml("/d.html", pageD);

    // Data files referenced from page A (GET + HEAD for content-length probe).
    registerData("/data.csv", "id,name\n1,Alice\n", "text/csv");
    registerData("/skip.json", "[{\"id\":1}]", "application/json");
  }

  private void registerHtml(String path, final String body) {
    final byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
    server.createContext(path, new HttpHandler() {
      @Override public void handle(HttpExchange exchange) throws IOException {
        String method = exchange.getRequestMethod();
        exchange.getResponseHeaders().set("Content-Type", "text/html");
        if ("HEAD".equals(method)) {
          exchange.getResponseHeaders().set("Content-Length", String.valueOf(bytes.length));
          exchange.sendResponseHeaders(200, -1);
        } else {
          exchange.sendResponseHeaders(200, bytes.length);
          try (OutputStream os = exchange.getResponseBody()) {
            os.write(bytes);
          }
        }
        exchange.close();
      }
    });
  }

  private void registerData(String path, final String body, final String contentType) {
    final byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
    server.createContext(path, new HttpHandler() {
      @Override public void handle(HttpExchange exchange) throws IOException {
        String method = exchange.getRequestMethod();
        exchange.getResponseHeaders().set("Content-Type", contentType);
        if ("HEAD".equals(method)) {
          exchange.getResponseHeaders().set("Content-Length", String.valueOf(bytes.length));
          exchange.sendResponseHeaders(200, -1);
        } else {
          exchange.sendResponseHeaders(200, bytes.length);
          try (OutputStream os = exchange.getResponseBody()) {
            os.write(bytes);
          }
        }
        exchange.close();
      }
    });
  }

  // ------------------------------------------------------------------
  // HttpStorageProvider endpoints
  // ------------------------------------------------------------------

  private void setupHttpProviderEndpoints() {
    // POST search endpoint echoing a deterministic body-dependent response.
    server.createContext("/api/search", new HttpHandler() {
      @Override public void handle(HttpExchange exchange) throws IOException {
        if ("POST".equals(exchange.getRequestMethod())) {
          String requestBody = readRequestBody(exchange);
          String response;
          if (requestBody.contains("\"filter\":\"active\"")) {
            response = "{\"results\":[{\"id\":1,\"status\":\"active\"}]}";
          } else {
            response = "{\"results\":[]}";
          }
          byte[] bytes = response.getBytes(StandardCharsets.UTF_8);
          exchange.getResponseHeaders().set("Content-Type", "application/json");
          exchange.sendResponseHeaders(200, bytes.length);
          try (OutputStream os = exchange.getResponseBody()) {
            os.write(bytes);
          }
        } else {
          exchange.sendResponseHeaders(405, 0);
        }
        exchange.close();
      }
    });

    // Endpoint returning JSON but declaring text/plain (for mimeType override).
    server.createContext("/api/broken", new HttpHandler() {
      @Override public void handle(HttpExchange exchange) throws IOException {
        String method = exchange.getRequestMethod();
        String response = "[{\"id\":1,\"value\":\"test\"}]";
        exchange.getResponseHeaders().set("Content-Type", "text/plain");
        if ("HEAD".equals(method)) {
          exchange.getResponseHeaders().set("Content-Length",
              String.valueOf(response.getBytes(StandardCharsets.UTF_8).length));
          exchange.sendResponseHeaders(200, -1);
        } else {
          byte[] bytes = response.getBytes(StandardCharsets.UTF_8);
          exchange.sendResponseHeaders(200, bytes.length);
          try (OutputStream os = exchange.getResponseBody()) {
            os.write(bytes);
          }
        }
        exchange.close();
      }
    });
  }

  private String readRequestBody(HttpExchange exchange) throws IOException {
    try (InputStream is = exchange.getRequestBody();
         Scanner scanner = new Scanner(is, StandardCharsets.UTF_8.name())) {
      return scanner.useDelimiter("\\A").hasNext() ? scanner.next() : "";
    }
  }

  private CrawlerConfiguration baseCrawlConfig() {
    CrawlerConfiguration config = new CrawlerConfiguration();
    config.setEnabled(true);
    // Keep the test fast: no inter-request sleep.
    config.setRequestDelay(Duration.ZERO);
    return config;
  }

  // ==================================================================
  // FILE-018: HTML crawler discovery / page-set golden
  // ==================================================================

  @Test
  @Tag("FILE-018")
  public void testMaxDepthBoundsVisitedSetGolden() throws IOException {
    String start = baseUrl + "/a.html";

    // maxDepth = 0 -> only the start page is visited.
    CrawlerConfiguration depth0 = baseCrawlConfig();
    depth0.setMaxDepth(0);
    HtmlCrawler crawler0 = new HtmlCrawler(depth0);
    try {
      Set<String> visited0 = crawler0.crawl(start).getVisitedUrls();
      Set<String> expected0 = new HashSet<>();
      expected0.add(start);
      assertEquals(expected0, visited0, "maxDepth=0 must visit only the start page");
    } finally {
      crawler0.cleanup();
    }

    // maxDepth = 1 -> start page plus its direct internal HTML links (B, C).
    CrawlerConfiguration depth1 = baseCrawlConfig();
    depth1.setMaxDepth(1);
    HtmlCrawler crawler1 = new HtmlCrawler(depth1);
    try {
      Set<String> visited1 = crawler1.crawl(start).getVisitedUrls();
      Set<String> expected1 = new HashSet<>();
      expected1.add(start);
      expected1.add(baseUrl + "/b.html");
      expected1.add(baseUrl + "/c.html");
      assertEquals(expected1, visited1, "maxDepth=1 golden page-set (A,B,C)");
    } finally {
      crawler1.cleanup();
    }

    // maxDepth = 2 -> adds D (reached via B).
    CrawlerConfiguration depth2 = baseCrawlConfig();
    depth2.setMaxDepth(2);
    HtmlCrawler crawler2 = new HtmlCrawler(depth2);
    try {
      Set<String> visited2 = crawler2.crawl(start).getVisitedUrls();
      Set<String> expected2 = new HashSet<>();
      expected2.add(start);
      expected2.add(baseUrl + "/b.html");
      expected2.add(baseUrl + "/c.html");
      expected2.add(baseUrl + "/d.html");
      assertEquals(expected2, visited2, "maxDepth=2 golden page-set (A,B,C,D)");
    } finally {
      crawler2.cleanup();
    }
  }

  @Test
  @Tag("FILE-018")
  public void testFollowExternalLinksFalseExcludesExternalGolden() throws IOException {
    String start = baseUrl + "/a.html";

    // followExternalLinks defaults to false; the external page must never appear,
    // and the exact visited set is pinned for a fully-expanded crawl.
    CrawlerConfiguration config = baseCrawlConfig();
    config.setMaxDepth(3);
    config.setFollowExternalLinks(false);

    HtmlCrawler crawler = new HtmlCrawler(config);
    try {
      Set<String> visited = crawler.crawl(start).getVisitedUrls();

      Set<String> expected = new HashSet<>();
      expected.add(start);
      expected.add(baseUrl + "/b.html");
      expected.add(baseUrl + "/c.html");
      expected.add(baseUrl + "/d.html");
      assertEquals(expected, visited,
          "followExternalLinks=false golden page-set excludes the external host");

      // Explicit: no visited URL points at the external host.
      for (String url : visited) {
        assertFalse(url.contains("external.invalid"),
            "external page must be excluded: " + url);
      }
    } finally {
      crawler.cleanup();
    }
  }

  @Test
  @Tag("FILE-018")
  public void testLinkPatternFiltersDataFileDiscovery() throws IOException {
    // NOTE: per HtmlLinkCache.isDataFile, linkPattern gates DATA-FILE links
    // (not which HTML pages are crawled). Page A references data.csv and
    // skip.json; a linkPattern matching only *.csv must discover the CSV
    // data file and exclude the JSON one. Discovered data files are recorded
    // as CrawlResult.getDataFiles() keyed by source URL.
    String start = baseUrl + "/a.html";

    CrawlerConfiguration config = baseCrawlConfig();
    config.setMaxDepth(0); // single page A is enough; both data links live on A
    config.setLinkPattern(java.util.regex.Pattern.compile(".*\\.csv$"));

    HtmlCrawler crawler = new HtmlCrawler(config);
    try {
      HtmlCrawler.CrawlResult result = crawler.crawl(start);
      Map<String, java.io.File> dataFiles = result.getDataFiles();

      assertTrue(dataFiles.containsKey(baseUrl + "/data.csv"),
          "linkPattern .*\\.csv$ must admit the CSV data file");
      assertFalse(dataFiles.containsKey(baseUrl + "/skip.json"),
          "linkPattern .*\\.csv$ must exclude the JSON data file");
    } finally {
      crawler.cleanup();
    }
  }

  // ==================================================================
  // FILE-162: HttpStorageProvider POST / mimeType / missing-url
  // ==================================================================

  @Test
  @Tag("FILE-162")
  public void testPostWithBodyReturnsServerResponseBytes() throws IOException {
    Map<String, String> headers = new HashMap<>();
    headers.put("Content-Type", "application/json");

    HttpStorageProvider provider =
        new HttpStorageProvider("POST", "{\"filter\":\"active\"}", headers, null);

    String url = baseUrl + "/api/search";
    assertTrue(provider.exists(url), "POST endpoint must report exists()==true");

    try (InputStream is = provider.openInputStream(url)) {
      String content = new String(is.readAllBytes(), StandardCharsets.UTF_8);
      assertEquals("{\"results\":[{\"id\":1,\"status\":\"active\"}]}", content,
          "POST body must drive the server response bytes");
    }
  }

  @Test
  @Tag("FILE-162")
  public void testMimeTypeOverridesServerContentType() throws IOException {
    // Server declares text/plain; provider override must win in getMetadata.
    HttpStorageProvider provider =
        new HttpStorageProvider("GET", null, new HashMap<>(), "application/json");

    String url = baseUrl + "/api/broken";
    StorageProvider.FileMetadata metadata = provider.getMetadata(url);
    assertNotNull(metadata);
    assertEquals("application/json", metadata.getContentType(),
        "configured mimeType must override the server Content-Type");
  }

  @Test
  @Tag("FILE-162")
  public void testMissingUrlExistsFalseAndOpenThrows() throws IOException {
    HttpStorageProvider provider = new HttpStorageProvider();
    String url = baseUrl + "/does-not-exist";

    assertFalse(provider.exists(url), "missing URL must report exists()==false");
    assertThrows(IOException.class, () -> provider.openInputStream(url),
        "missing URL openInputStream must throw IOException");
  }
}
