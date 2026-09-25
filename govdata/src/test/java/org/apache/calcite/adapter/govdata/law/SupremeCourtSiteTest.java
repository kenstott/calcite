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
package org.apache.calcite.adapter.govdata.law;

import org.apache.calcite.adapter.govdata.law.SupremeCourtSite.NotFoundException;

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
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link SupremeCourtSite} against a local HTTP server that answers with a
 * scripted sequence of statuses.
 */
@Tag("unit")
class SupremeCourtSiteTest {

  private HttpServer server;
  private final AtomicInteger requests = new AtomicInteger();
  private volatile List<Integer> script = Collections.emptyList();
  private volatile String contentType = "text/html";
  private volatile byte[] body = "<html>ok</html>".getBytes(StandardCharsets.UTF_8);
  private final List<Long> arrivalTimes = Collections.synchronizedList(new ArrayList<Long>());

  @BeforeEach void start() throws IOException {
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext("/", new HttpHandler() {
      @Override public void handle(HttpExchange ex) throws IOException {
        arrivalTimes.add(System.currentTimeMillis());
        int n = requests.getAndIncrement();
        int status = n < script.size() ? script.get(n) : 200;
        if (status == 200) {
          ex.getResponseHeaders().add("Content-Type", contentType);
          ex.sendResponseHeaders(200, body.length);
          OutputStream out = ex.getResponseBody();
          out.write(body);
          out.close();
        } else {
          ex.sendResponseHeaders(status, -1);
        }
        ex.close();
      }
    });
    server.start();
  }

  @AfterEach void stop() {
    server.stop(0);
  }

  private String url() {
    return "http://127.0.0.1:" + server.getAddress().getPort() + "/page";
  }

  private static SupremeCourtSite site(long intervalMs, int retries) {
    return new SupremeCourtSite(intervalMs, new long[retries]);
  }

  @Test void aSuccessfulPageIsReturned() throws IOException {
    assertEquals("<html>ok</html>", site(0, 2).getHtml(url()));
    assertEquals(1, requests.get());
  }

  @Test void a404IsNotFoundAndIsNotRetried() {
    script = java.util.Arrays.asList(404);
    NotFoundException e = assertThrows(NotFoundException.class, () -> site(0, 3).getHtml(url()));
    assertEquals(404, e.status);
    assertEquals(1, requests.get());
  }

  @Test void a410IsNotFound() {
    script = java.util.Arrays.asList(410);
    assertEquals(410, assertThrows(NotFoundException.class, () -> site(0, 3).getHtml(url())).status);
  }

  @Test void a403IsRetriedAndSucceedsWhenTheBlockLifts() throws IOException {
    script = java.util.Arrays.asList(403, 403);
    assertEquals("<html>ok</html>", site(0, 3).getHtml(url()));
    assertEquals(3, requests.get());
  }

  @Test void aPersistent403IsABlockNeverAMissingFile() {
    script = java.util.Arrays.asList(403, 403, 403, 403, 403, 403);
    IOException e = assertThrows(IOException.class, () -> site(0, 3).getHtml(url()));
    assertFalse(e instanceof NotFoundException, "a 403 must not be reported as not found");
    assertEquals(4, requests.get(), "one attempt plus three retries");
    assertTrue(e.getMessage().contains("HTTP 403"), e.getMessage());
    assertTrue(e.getMessage().contains("blocking or rate limiting"), e.getMessage());
  }

  @Test void a429AndA5xxAreRetried() throws IOException {
    script = java.util.Arrays.asList(429, 503);
    assertEquals("<html>ok</html>", site(0, 3).getHtml(url()));
    assertEquals(3, requests.get());
  }

  @Test void anUnexpectedStatusFailsAtOnceWithoutRetry() {
    script = java.util.Arrays.asList(401);
    IOException e = assertThrows(IOException.class, () -> site(0, 3).getHtml(url()));
    assertFalse(e instanceof NotFoundException);
    assertTrue(e.getMessage().contains("HTTP 401"), e.getMessage());
    assertEquals(1, requests.get());
  }

  @Test void aDownloadWritesTheFile(@TempDir File dir) throws IOException {
    contentType = "application/pdf";
    body = new byte[] {'%', 'P', 'D', 'F', 1, 2, 3};
    File out = new File(dir, "x.pdf");
    site(0, 1).download(url(), out);
    assertEquals(7, Files.readAllBytes(out.toPath()).length);
  }

  @Test void aDownloadThatComesBackAsHtmlIsRetriedThenFailsAsARefusal(@TempDir File dir) {
    contentType = "text/html";
    IOException e = assertThrows(IOException.class,
        () -> site(0, 2).download(url(), new File(dir, "x.pdf")));
    assertFalse(e instanceof NotFoundException);
    assertEquals(3, requests.get());
    assertTrue(e.getMessage().contains("HTML where a file was expected"), e.getMessage());
  }

  @Test void requestsAreSpacedByTheMinimumInterval() throws IOException {
    SupremeCourtSite spaced = site(80, 0);
    for (int i = 0; i < 4; i++) {
      spaced.getHtml(url());
    }
    for (int i = 1; i < arrivalTimes.size(); i++) {
      long gap = arrivalTimes.get(i) - arrivalTimes.get(i - 1);
      assertTrue(gap >= 70, "gap " + gap + " ms between request " + (i - 1) + " and " + i);
    }
  }
}
