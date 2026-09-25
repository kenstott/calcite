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
package org.apache.calcite.adapter.govdata;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * How {@link ZipDownloadUtils#downloadToFile} reads HTTP statuses: 404 and 410 mean the file is
 * absent and are not retried; 403 is a refusal, is retried, and never reported as a missing file.
 */
@Tag("unit")
class ZipDownloadUtilsStatusTest {

  private HttpServer server;
  private final AtomicInteger requests = new AtomicInteger();
  private volatile List<Integer> script = Collections.emptyList();

  @BeforeEach void start() throws IOException {
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext("/", new HttpHandler() {
      @Override public void handle(HttpExchange ex) throws IOException {
        int n = requests.getAndIncrement();
        int status = n < script.size() ? script.get(n) : 200;
        if (status == 200) {
          byte[] body = new byte[] {1, 2, 3, 4};
          ex.getResponseHeaders().add("Content-Type", "application/octet-stream");
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
    return "http://127.0.0.1:" + server.getAddress().getPort() + "/file.bin";
  }

  @Test void a404IsAbsentAndIsNotRetried(@TempDir File dir) {
    script = Arrays.asList(404);
    FileNotFoundException e = assertThrows(FileNotFoundException.class,
        () -> ZipDownloadUtils.downloadToFile(url(), null, new File(dir, "f")));
    assertTrue(e.getMessage().contains("HTTP 404"), e.getMessage());
    assertEquals(1, requests.get());
  }

  @Test void a410IsAbsentAndIsNotRetried(@TempDir File dir) {
    script = Arrays.asList(410);
    assertThrows(FileNotFoundException.class,
        () -> ZipDownloadUtils.downloadToFile(url(), null, new File(dir, "f")));
    assertEquals(1, requests.get());
  }

  @Test void a403IsRetriedNotTreatedAsAbsent(@TempDir File dir) throws IOException {
    script = Arrays.asList(403);
    File out = new File(dir, "f");
    ZipDownloadUtils.downloadToFile(url(), null, out);
    assertEquals(2, requests.get(), "the 403 must be retried, not skipped");
    assertEquals(4, Files.readAllBytes(out.toPath()).length);
  }

  /** Takes about 12 s: the helper waits 2, 4 and 6 s between its four attempts. */
  @Test void aPersistent403FailsAsForbiddenNeverAsAMissingFile(@TempDir File dir) {
    script = Arrays.asList(403, 403, 403, 403, 403);
    IOException e = assertThrows(IOException.class,
        () -> ZipDownloadUtils.downloadToFile(url(), null, new File(dir, "f")));
    assertFalse(e instanceof FileNotFoundException, "403 must not read as a missing file");
    assertEquals(4, requests.get());
    assertTrue(e.getCause().getMessage().contains("HTTP 403"), e.getCause().getMessage());
    assertTrue(e.getCause().getMessage().contains("not treated as a missing file"),
        e.getCause().getMessage());
  }
}
