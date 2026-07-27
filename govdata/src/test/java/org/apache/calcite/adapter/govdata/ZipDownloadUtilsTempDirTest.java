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
package org.apache.calcite.adapter.govdata;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Verifies that a failed ZIP download leaves no temp directory behind.
 *
 * <p>TIGER partitions that legitimately do not exist (voting_districts is published
 * only for census vintages) 404 on every other year and state. When the extract
 * directory was created before the download, each of those 404s stranded an empty
 * directory in the JVM temp dir that no caller could ever delete.
 */
@Tag("unit")
public class ZipDownloadUtilsTempDirTest {

  private static final String PREFIX = "zdu-404-test";

  @Test void failedDownloadLeavesNoTempDirectory() throws Exception {
    HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext("/missing.zip", new HttpHandler() {
      @Override public void handle(HttpExchange exchange) throws IOException {
        exchange.sendResponseHeaders(404, -1);
        exchange.close();
      }
    });
    server.start();
    try {
      String url = "http://127.0.0.1:" + server.getAddress().getPort() + "/missing.zip";
      int before = countTempEntries();

      assertThrows(IOException.class,
          () -> ZipDownloadUtils.downloadZipToTempDir(url, null, PREFIX));

      assertEquals(before, countTempEntries(),
          "404 download stranded a temp entry in " + System.getProperty("java.io.tmpdir"));
    } finally {
      server.stop(0);
    }
  }

  /** Counts temp-dir entries this test's prefix would have created. */
  private static int countTempEntries() {
    File tmpRoot = new File(System.getProperty("java.io.tmpdir"));
    File[] matches = tmpRoot.listFiles((dir, name) -> name.startsWith(PREFIX));
    return matches == null ? 0 : matches.length;
  }
}
