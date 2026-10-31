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

import org.apache.calcite.adapter.file.etl.NetworkOutageWait;
import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.govdata.disasters.StormEventsStreamingTransformer;

import com.sun.net.httpserver.HttpServer;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.api.parallel.ResourceLock;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.zip.GZIPOutputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Transformers and downloaders that open their own {@code HttpURLConnection} wait out a short
 * network outage: the server below comes up a few seconds after the first attempt.
 */
@Tag("unit")
@Execution(ExecutionMode.SAME_THREAD)
@ResourceLock("network-outage-wait")
class NetworkOutageDirectConnectionTest {

  private HttpServer server;

  @AfterEach void cleanUp() {
    NetworkOutageWait.configureMinutes(25);
    if (server != null) {
      server.stop(0);
    }
  }

  private static int freePort() throws IOException {
    try (ServerSocket s = new ServerSocket(0)) {
      return s.getLocalPort();
    }
  }

  /** Starts a server on {@code port} after {@code delayMs}; the caller joins the thread. */
  private Thread startLater(int port, long delayMs, HttpServerSetup setup) {
    Thread t = new Thread(() -> {
      try {
        Thread.sleep(delayMs);
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", port), 0);
        setup.configure(server);
        server.start();
      } catch (Exception e) {
        throw new IllegalStateException(e);
      }
    });
    t.start();
    return t;
  }

  private interface HttpServerSetup {
    void configure(HttpServer s);
  }

  private static void respond(com.sun.net.httpserver.HttpExchange ex, byte[] body)
      throws IOException {
    ex.sendResponseHeaders(200, body.length);
    try (OutputStream os = ex.getResponseBody()) {
      os.write(body);
    }
  }

  @Test void zipDownloadUtilsWaitsForNetwork(@TempDir File dir) throws Exception {
    NetworkOutageWait.configureMinutes(1);
    int port = freePort();
    Thread t = startLater(port, 1_500L,
        s -> s.createContext("/", ex -> respond(ex, new byte[] {1, 2, 3})));
    File dest = new File(dir, "out.bin");
    ZipDownloadUtils.downloadToFile("http://127.0.0.1:" + port + "/f.bin",
        Collections.<String, String>emptyMap(), dest);
    t.join();
    assertEquals(3, Files.size(dest.toPath()));
  }

  @Test void stormEventsWaitsForNetwork() throws Exception {
    NetworkOutageWait.configureMinutes(1);
    final String name = "StormEvents_details-ftp_v1.0_d2024_c20250101.csv.gz";
    java.io.ByteArrayOutputStream gz = new java.io.ByteArrayOutputStream();
    try (GZIPOutputStream out = new GZIPOutputStream(gz)) {
      out.write("EVENT_ID,STATE\n1,TX\n".getBytes(StandardCharsets.UTF_8));
    }
    final byte[] gzBytes = gz.toByteArray();
    int port = freePort();
    Thread t = startLater(port, 1_500L, s -> {
      s.createContext("/dir/" + name, ex -> respond(ex, gzBytes));
      s.createContext("/dir/", ex -> respond(ex,
          ("<a href=\"" + name + "\">" + name + "</a>").getBytes(StandardCharsets.UTF_8)));
    });
    RequestContext ctx = RequestContext.builder()
        .url("http://127.0.0.1:" + port + "/dir/")
        .dimensionValues(Collections.singletonMap("year", "2024")).build();
    Iterator<Map<String, Object>> rows =
        new StormEventsStreamingTransformer().fetchAndTransform(ctx);
    t.join();
    assertTrue(rows.hasNext());
  }
}
