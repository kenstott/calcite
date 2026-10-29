/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.adapter.file.etl;

import com.sun.net.httpserver.HttpServer;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.api.parallel.ResourceLock;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests {@link NetworkOutageWait} and its use by {@link SharedHttpClient}. */
@Tag("unit")
@Execution(ExecutionMode.SAME_THREAD)
@ResourceLock("network-outage-wait")
class NetworkOutageWaitTest {

  private HttpServer server;

  @AfterEach void cleanUp() {
    NetworkOutageWait.reset();
    if (server != null) {
      server.stop(0);
    }
  }

  private static int freePort() throws IOException {
    try (ServerSocket s = new ServerSocket(0)) {
      return s.getLocalPort();
    }
  }

  @Test void fetchSucceedsAfterServerComesUp() throws Exception {
    NetworkOutageWait.configureMillis(10_000L, 200L, 400L);
    final int port = freePort();
    Thread starter = new Thread(() -> {
      try {
        Thread.sleep(1_500L);
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", port), 0);
        server.createContext("/", ex -> {
          byte[] body = "ok".getBytes(StandardCharsets.UTF_8);
          ex.sendResponseHeaders(200, body.length);
          try (OutputStream os = ex.getResponseBody()) {
            os.write(body);
          }
        });
        server.start();
      } catch (Exception e) {
        throw new IllegalStateException(e);
      }
    });
    starter.start();
    SharedHttpClient.Response r = SharedHttpClient.execute(
        new SharedHttpClient.Request("GET", "http://127.0.0.1:" + port + "/"));
    assertEquals(200, r.getResponseCode());
    r.disconnect();
    starter.join();
  }

  @Test void unreachablePastLimitFailsWithElapsedTime() throws Exception {
    NetworkOutageWait.configureMillis(1_000L, 200L, 400L);
    int port = freePort();
    IOException e = assertThrows(IOException.class, () -> SharedHttpClient.execute(
        new SharedHttpClient.Request("GET", "http://127.0.0.1:" + port + "/")));
    assertTrue(e.getMessage().contains("Network unavailable for"), e.toString());
    assertTrue(e.getMessage().contains("limit 1 s"), e.getMessage());
  }

  @Test void zeroLimitFailsImmediately() throws Exception {
    NetworkOutageWait.configureMillis(0L, 200L, 400L);
    int port = freePort();
    long start = System.nanoTime();
    IOException e = assertThrows(IOException.class, () -> SharedHttpClient.execute(
        new SharedHttpClient.Request("GET", "http://127.0.0.1:" + port + "/")));
    assertTrue(NetworkOutageWait.isOutage(e));
    assertFalse(String.valueOf(e.getMessage()).contains("Network unavailable for"));
    assertTrue((System.nanoTime() - start) / 1_000_000L < 1_000L);
  }

  @Test void httpStatusIsNeverWaitedOn() throws Exception {
    NetworkOutageWait.configureMillis(10_000L, 200L, 400L);
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    final AtomicInteger hits = new AtomicInteger();
    server.createContext("/", ex -> {
      hits.incrementAndGet();
      ex.sendResponseHeaders(404, -1);
      ex.close();
    });
    server.start();
    SharedHttpClient.Response r = SharedHttpClient.execute(new SharedHttpClient.Request(
        "GET", "http://127.0.0.1:" + server.getAddress().getPort() + "/"));
    assertEquals(404, r.getResponseCode());
    r.disconnect();
    assertEquals(1, hits.get());
  }

  @Test void classifiesNameResolutionFailureAsOutage() {
    assertTrue(NetworkOutageWait.isOutage(new IOException("x", new UnknownHostException("h"))));
    assertFalse(NetworkOutageWait.isOutage(new IOException("HTTP 404")));
  }
}
