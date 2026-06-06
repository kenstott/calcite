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
package org.apache.calcite.adapter.file.clickhouse;

import org.apache.calcite.adapter.file.execution.clickhouse.ClickHouseConfig;
import org.apache.calcite.adapter.file.execution.clickhouse.ClickHouseExecutionEngine;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.net.HttpURLConnection;
import java.net.ServerSocket;
import java.net.URI;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration tests for ClickHouse local (embedded) mode.
 *
 * <p>These tests require {@code clickhouse-local} binary on PATH or configured
 * via {@code CLICKHOUSE_LOCAL_PATH} environment variable.
 *
 * <p>Run with: {@code ./gradlew :file:test -PincludeTags=integration --tests "*ClickHouseLocalIntegrationTest*"}
 */
@Tag("integration")
class ClickHouseLocalIntegrationTest {

  @TempDir
  File tempDir;

  static boolean isClickHouseLocalAvailable() {
    return ClickHouseExecutionEngine.isLocalAvailable();
  }

  static boolean isClickHouseDriverAvailable() {
    return ClickHouseExecutionEngine.isAvailable();
  }

  @Test @EnabledIf("isClickHouseLocalAvailable")
  void testStartAndStopClickHouseLocal() throws Exception {
    String binaryPath = ClickHouseExecutionEngine.findLocalBinaryPath(null);
    assertNotNull(binaryPath, "clickhouse-local binary should be found");

    // Find a free port
    int port;
    try (ServerSocket socket = new ServerSocket(0)) {
      socket.setReuseAddress(true);
      port = socket.getLocalPort();
    }

    // Create data directory
    File dataDir = new File(tempDir, "ch-data");
    dataDir.mkdirs();

    // Start clickhouse-local
    ProcessBuilder pb =
        new ProcessBuilder(binaryPath,
        "--http_port", String.valueOf(port),
        "--path", dataDir.getAbsolutePath(),
        "--log-level", "warning");
    pb.redirectErrorStream(true);
    Process process = pb.start();

    try {
      // Wait for HTTP interface to become available
      long deadline = System.currentTimeMillis() + 30_000;
      boolean ready = false;
      while (System.currentTimeMillis() < deadline) {
        try {
          HttpURLConnection conn = (HttpURLConnection)
              new URI("http://localhost:" + port + "/ping").toURL().openConnection();
          conn.setConnectTimeout(1000);
          conn.setReadTimeout(1000);
          if (conn.getResponseCode() == 200) {
            ready = true;
            break;
          }
        } catch (Exception e) {
          // Not ready yet
        }
        Thread.sleep(200);
      }

      assertTrue(ready, "clickhouse-local should become ready within 30 seconds");

      // Verify it responds to a simple query
      if (isClickHouseDriverAvailable()) {
        String jdbcUrl = "jdbc:clickhouse://localhost:" + port + "/default";
        try (Connection conn = DriverManager.getConnection(jdbcUrl);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT 1 AS result")) {
          assertTrue(rs.next());
          assertEquals(1, rs.getInt("result"));
        }
      }

    } finally {
      // Shutdown
      process.destroy();
      boolean exited = process.waitFor(5, java.util.concurrent.TimeUnit.SECONDS);
      if (!exited) {
        process.destroyForcibly();
      }
      assertTrue(!process.isAlive(), "Process should be stopped");
    }
  }

  @Test @EnabledIf("isClickHouseLocalAvailable")
  void testClickHouseLocalConfig() {
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("mode", "local");
    configMap.put("dataDir", tempDir.getAbsolutePath());
    configMap.put("maxMemory", "1GB");
    configMap.put("maxThreads", 2);

    ClickHouseConfig config = new ClickHouseConfig(configMap);
    assertTrue(config.isLocalMode());
    assertEquals(tempDir.getAbsolutePath(), config.getDataDir());
    assertEquals("1GB", config.getMaxMemory());
    assertEquals(2, config.getMaxThreads());
  }

  @Test @EnabledIf("isClickHouseLocalAvailable")
  void testClickHouseLocalCreateDatabase() throws Exception {
    String binaryPath = ClickHouseExecutionEngine.findLocalBinaryPath(null);
    assertNotNull(binaryPath);

    int port;
    try (ServerSocket socket = new ServerSocket(0)) {
      socket.setReuseAddress(true);
      port = socket.getLocalPort();
    }

    File dataDir = new File(tempDir, "ch-db-test");
    dataDir.mkdirs();

    ProcessBuilder pb =
        new ProcessBuilder(binaryPath,
        "--http_port", String.valueOf(port),
        "--path", dataDir.getAbsolutePath(),
        "--log-level", "warning");
    pb.redirectErrorStream(true);
    Process process = pb.start();

    try {
      // Wait for ready
      long deadline = System.currentTimeMillis() + 30_000;
      boolean ready = false;
      while (System.currentTimeMillis() < deadline) {
        try {
          HttpURLConnection conn = (HttpURLConnection)
              new URI("http://localhost:" + port + "/ping").toURL().openConnection();
          conn.setConnectTimeout(1000);
          conn.setReadTimeout(1000);
          if (conn.getResponseCode() == 200) {
            ready = true;
            break;
          }
        } catch (Exception e) {
          // Not ready yet
        }
        Thread.sleep(200);
      }
      assertTrue(ready, "clickhouse-local should be ready");

      if (isClickHouseDriverAvailable()) {
        String jdbcUrl = "jdbc:clickhouse://localhost:" + port + "/default";
        try (Connection conn = DriverManager.getConnection(jdbcUrl);
             Statement stmt = conn.createStatement()) {

          // Create a test database
          stmt.execute("CREATE DATABASE IF NOT EXISTS \"test_schema\"");

          // Verify the database was created
          try (ResultSet rs = stmt.executeQuery("SHOW DATABASES")) {
            boolean found = false;
            while (rs.next()) {
              if ("test_schema".equals(rs.getString(1))) {
                found = true;
                break;
              }
            }
            assertTrue(found, "test_schema database should exist");
          }
        }
      }

    } finally {
      process.destroy();
      process.waitFor(5, java.util.concurrent.TimeUnit.SECONDS);
      if (process.isAlive()) {
        process.destroyForcibly();
      }
    }
  }
}
