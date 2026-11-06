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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.net.URI;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** A fixed-length body the server cuts short is an error, not a shorter file. */
@Tag("unit")
class CompleteBodyTest {
  private HttpServer server;

  @BeforeEach void start() throws IOException {
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext("/full", ex -> {
      byte[] b = "a,b\n1,2\n".getBytes(StandardCharsets.UTF_8);
      ex.sendResponseHeaders(200, b.length);
      ex.getResponseBody().write(b);
      ex.close();
    });
    server.createContext("/cut", ex -> {
      byte[] b = "a,b\n1,".getBytes(StandardCharsets.UTF_8);
      ex.sendResponseHeaders(200, b.length + 100);
      ex.getResponseBody().write(b);
      ex.close();
    });
    server.createContext("/chunked", ex -> {
      ex.sendResponseHeaders(200, 0);
      ex.getResponseBody().write("a,b\n".getBytes(StandardCharsets.UTF_8));
      ex.close();
    });
    server.start();
  }

  @AfterEach void stop() {
    server.stop(0);
  }

  private HttpURLConnection open(String path) throws IOException {
    return (HttpURLConnection) URI.create(
        "http://127.0.0.1:" + server.getAddress().getPort() + path).toURL().openConnection();
  }

  private static String drain(InputStream in) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    byte[] buf = new byte[3];
    int n;
    while ((n = in.read(buf)) != -1) {
      out.write(buf, 0, n);
    }
    return new String(out.toByteArray(), StandardCharsets.UTF_8);
  }

  @Test void completeBodyReadsNormally() throws Exception {
    try (InputStream in = CompleteBody.of(open("/full"))) {
      assertEquals("a,b\n1,2\n", drain(in));
    }
  }

  @Test void truncatedBodyThrowsInsteadOfEndingQuietly() throws Exception {
    try (InputStream in = CompleteBody.of(open("/cut"))) {
      SocketException e = assertThrows(SocketException.class, () -> drain(in));
      assertTrue(e.getMessage().contains("truncated body: 6 of 106"), e.getMessage());
    }
  }

  @Test void bodyWithNoDeclaredLengthIsPassedThrough() throws Exception {
    try (InputStream in = CompleteBody.of(open("/chunked"))) {
      assertEquals("a,b\n", drain(in));
    }
  }
}
