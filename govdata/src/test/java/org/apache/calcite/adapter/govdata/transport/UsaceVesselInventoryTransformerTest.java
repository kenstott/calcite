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
package org.apache.calcite.adapter.govdata.transport;

import org.apache.calcite.adapter.file.etl.NetworkOutageWait;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Tests the HTTP requests of {@link UsaceVesselInventoryTransformer}. */
@Tag("unit")
class UsaceVesselInventoryTransformerTest {

  private HttpServer server;

  @AfterEach void tearDown() {
    NetworkOutageWait.configureMinutes(0);
    if (server != null) {
      server.stop(0);
    }
  }

  @Test void postWaitsOutAnUnreachableHost() throws Exception {
    final InetAddress loopback = InetAddress.getLoopbackAddress();
    final int port;
    try (ServerSocket probe = new ServerSocket(0, 1, loopback)) {
      port = probe.getLocalPort();
    }
    final AtomicReference<String> received = new AtomicReference<String>();
    final AtomicReference<Throwable> startFailure = new AtomicReference<Throwable>();
    Thread starter = new Thread(() -> {
      try {
        // Nothing listens until after the first connect has been refused.
        Thread.sleep(1000L);
        server = HttpServer.create(new InetSocketAddress(loopback, port), 0);
        server.createContext("/search", new HttpHandler() {
          @Override public void handle(HttpExchange exchange) throws IOException {
            received.set(exchange.getRequestMethod() + " " + read(exchange.getRequestBody()));
            byte[] reply = "[]".getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, reply.length);
            try (OutputStream os = exchange.getResponseBody()) {
              os.write(reply);
            }
          }
        });
        server.start();
      } catch (Exception e) {
        startFailure.set(e);
      }
    });
    NetworkOutageWait.configureMinutes(1);
    starter.start();

    byte[] response =
        UsaceVesselInventoryTransformer.download(
            "http://" + loopback.getHostAddress() + ":" + port + "/search",
            "{\"q\":1}".getBytes(StandardCharsets.UTF_8));
    starter.join();

    assertEquals(null, startFailure.get());
    assertEquals("[]", new String(response, StandardCharsets.UTF_8));
    assertEquals("POST {\"q\":1}", received.get());
  }

  private static String read(InputStream in) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    byte[] buf = new byte[1024];
    int len;
    while ((len = in.read(buf)) > 0) {
      out.write(buf, 0, len);
    }
    return new String(out.toByteArray(), StandardCharsets.UTF_8);
  }
}
