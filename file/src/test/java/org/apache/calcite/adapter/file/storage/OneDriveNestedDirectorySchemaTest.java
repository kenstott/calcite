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
package org.apache.calcite.adapter.file.storage;

import org.apache.calcite.adapter.file.FileSchemaFactory;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A OneDrive schema whose {@code directory} is a nested folder, with a glob and recursive
 * listing, discovers and reads the CSV in that folder (a fake Microsoft Graph serves the drive).
 */
@Tag("unit")
public class OneDriveNestedDirectorySchemaTest {

  @TempDir Path operatingDir;

  private HttpServer server;
  private String base;
  private final List<String> requests = new CopyOnWriteArrayList<>();

  @BeforeEach void start() throws IOException {
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext("/v1.0", this::handle);
    server.start();
    base = "http://127.0.0.1:" + server.getAddress().getPort() + "/v1.0";
  }

  @AfterEach void stop() {
    server.stop(0);
  }

  private void handle(HttpExchange ex) throws IOException {
    String path = ex.getRequestURI().getPath();
    requests.add(path);
    String drive = "/v1.0/drives/drv1/";
    String body = null;
    if (path.equals("/v1.0/users/pat@contoso.com/drive")) {
      body = "{\"id\":\"drv1\"}";
    } else if (path.equals(drive + "root:/run1/inner:/children")) {
      body = "{\"value\":[{\"name\":\"orders.csv\",\"size\":22,\"file\":{},"
          + "\"lastModifiedDateTime\":\"2026-11-20T10:00:00Z\"}]}";
    } else if (path.equals(drive + "root:/run1/inner:")) {
      body = "{\"name\":\"inner\",\"folder\":{},"
          + "\"lastModifiedDateTime\":\"2026-11-20T10:00:00Z\"}";
    } else if (path.equals(drive + "root:/run1/inner/orders.csv:")) {
      body = "{\"name\":\"orders.csv\",\"size\":22,\"file\":{},"
          + "\"lastModifiedDateTime\":\"2026-11-20T10:00:00Z\"}";
    } else if (path.equals(drive + "root:/run1/inner/orders.csv:/content")) {
      send(ex, 200, "id,amount\n1,10\n2,20\n".getBytes(StandardCharsets.UTF_8));
      return;
    }
    if (body == null) {
      send(ex, 404, "{\"error\":{\"message\":\"not found\"}}".getBytes(StandardCharsets.UTF_8));
    } else {
      send(ex, 200, body.getBytes(StandardCharsets.UTF_8));
    }
  }

  private static void send(HttpExchange ex, int status, byte[] body) throws IOException {
    ex.getResponseHeaders().add("Content-Type", "application/json");
    ex.sendResponseHeaders(status, body.length);
    try (OutputStream out = ex.getResponseBody()) {
      out.write(body);
    }
  }

  @Test void nestedDirectoryGlobRecursiveFindsTheCsv() throws Exception {
    MicrosoftGraphStorageProvider provider = new MicrosoftGraphStorageProvider(
        new MicrosoftGraphTokenManager("graph-token", "https://graph.microsoft.com"),
        "pat@contoso.com", base);
    Map<String, Object> operand = new HashMap<>();
    operand.put("storageType", "onedrive");
    operand.put("_storageProvider", provider);
    operand.put("directory", "run1/inner");
    operand.put("baseDirectory", operatingDir.toString());
    operand.put("glob", "*.csv");
    operand.put("recursive", true);
    operand.put("ephemeralCache", true);
    operand.put("executionEngine", "duckdb");

    Properties info = new Properties();
    info.put("lex", "JAVA");
    try (Connection conn = DriverManager.getConnection("jdbc:calcite:", info)) {
      SchemaPlus root = conn.unwrap(CalciteConnection.class).getRootSchema();
      root.add("od_live", FileSchemaFactory.INSTANCE.create(root, "od_live", operand));
      try (Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM od_live.orders")) {
        assertTrue(rs.next());
        assertEquals(2, rs.getLong(1), "Graph requests: " + requests);
      }
    } catch (Exception e) {
      throw new AssertionError("Graph requests: " + requests, e);
    }
  }
}
