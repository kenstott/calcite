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
package org.apache.calcite.adapter.salesforce;

import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.InformationSchema;

import com.sun.net.httpserver.HttpServer;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
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
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.nullValue;

/**
 * information_schema over a Salesforce schema (#518). The factory registers none, so
 * pgwire-calcite adds Calcite's to the connection root; this does the same and reads the
 * schema's sObjects and fields through it. The instance is a local server answering the two
 * REST calls the schema makes (sObject list, describe) with the documented shapes.
 */
class InformationSchemaTest {

  @TempDir Path describeCache;

  private HttpServer server;

  @BeforeEach void start() throws IOException {
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext("/services/data/v58.0/sobjects/Account/describe", exchange ->
        answer(exchange, "{\"name\":\"Account\",\"label\":\"Account\",\"queryable\":true,"
            + "\"fields\":["
            + "{\"name\":\"Id\",\"label\":\"Account ID\",\"type\":\"id\",\"length\":18},"
            + "{\"name\":\"Name\",\"label\":\"Account Name\",\"type\":\"string\","
            + "\"length\":255,\"nillable\":false}]}"));
    server.createContext("/services/data/v58.0/sobjects", exchange ->
        answer(exchange, "{\"sobjects\":["
            + "{\"name\":\"Account\",\"label\":\"Account\",\"queryable\":true}]}"));
    server.start();
  }

  @AfterEach void stop() {
    server.stop(0);
  }

  private static void answer(com.sun.net.httpserver.HttpExchange exchange, String json)
      throws IOException {
    final byte[] body = json.getBytes(StandardCharsets.UTF_8);
    exchange.getResponseHeaders().add("Content-Type", "application/json");
    exchange.sendResponseHeaders(200, body.length);
    try (OutputStream out = exchange.getResponseBody()) {
      out.write(body);
    }
  }

  private static List<String> strings(Connection conn, String sql) throws SQLException {
    final List<String> values = new ArrayList<>();
    try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(sql)) {
      while (rs.next()) {
        values.add(rs.getString(1));
      }
    }
    return values;
  }

  @Test void informationSchemaListsTheSchemasObjectsAndFields() throws Exception {
    try (Connection conn = DriverManager.getConnection("jdbc:calcite:lex=JAVA")) {
      final SchemaPlus root = conn.unwrap(CalciteConnection.class).getRootSchema();
      final Map<String, Object> operand = new HashMap<>();
      operand.put("accessToken", "token");
      operand.put("instanceUrl", "http://127.0.0.1:" + server.getAddress().getPort());
      operand.put("describeCacheDirectory", describeCache.toString());
      root.add("sf", new SalesforceSchemaFactory().create(root, "sf", operand));
      // the factory registers none; pgwire-calcite's backend adds this one
      assertThat(root.subSchemas().get("information_schema"), nullValue());
      root.add("information_schema", new InformationSchema(root, "CALCITE"));

      assertThat(strings(conn, "SELECT TABLE_NAME FROM information_schema.TABLES "
          + "WHERE TABLE_SCHEMA = 'sf'"), hasItems("Account"));
      assertThat(strings(conn, "SELECT COLUMN_NAME FROM information_schema.COLUMNS "
          + "WHERE TABLE_SCHEMA = 'sf' AND TABLE_NAME = 'Account'"), hasItems("Id", "Name"));
    }
  }
}
