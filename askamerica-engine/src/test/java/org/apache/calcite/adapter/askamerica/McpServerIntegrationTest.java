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
package org.apache.calcite.adapter.askamerica;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Integration tests for the MCP server shadow JAR.
 *
 * <p>Exercises the full end-to-end path that unit tests on the Gradle classpath
 * cannot cover: shadow JAR service registrations, S3A filesystem availability,
 * Iceberg table initialization from R2, and JDBC metadata exposure.
 *
 * <p>Key regressions caught here but NOT by JdbcMetadataIntegrationTest:
 * <ul>
 *   <li>Missing {@code fs.s3a.impl} in shadow JAR (service files dropped during merge)</li>
 *   <li>Shadow JAR class exclusions that break the read path at runtime</li>
 *   <li>{@code list_tables} returning {@code []} when Iceberg init silently fails</li>
 *   <li>{@code describe_table} returning {@code []} (the original MCP regression)</li>
 * </ul>
 *
 * <p>Requires shadow JAR to be built first (task dependency in build.gradle.kts handles this).
 * Run: {@code ./gradlew :askamerica-engine:test -PincludeTags=integration}
 */
@Tag("integration")
public class McpServerIntegrationTest {

  private static final Logger LOGGER = Logger.getLogger(McpServerIntegrationTest.class.getName());

  /** Timeout covering R2 connection + Iceberg metadata reads on first schema access. */
  private static final long SCHEMA_INIT_TIMEOUT_MS = 120_000;
  /** Timeout for responses once schema is warmed up. */
  private static final long TOOL_TIMEOUT_MS = 30_000;

  private static final AtomicInteger ID_SEQ = new AtomicInteger(1);

  /** Schema used for schema-init tests. ref is static reference data that is always fully loaded. */
  private static final String TEST_SCHEMA = "ref";
  /** A table known to exist in TEST_SCHEMA. */
  private static final String TEST_TABLE = "sec_company_tickers";
  /** A column known to exist in TEST_TABLE. */
  private static final String TEST_COLUMN = "ticker";

  private static File shadowJar;

  private Process mcpProcess;
  private BufferedWriter mcpStdin;
  private BufferedReader mcpStdout;

  @BeforeAll
  static void locateShadowJar() {
    File libsDir = new File("build/libs");
    assumeTrue(libsDir.exists(),
        "build/libs not found — run :askamerica-engine:shadowJar first");
    File[] jars = libsDir.listFiles(f ->
        f.getName().endsWith(".jar")
            && f.getName().contains("askamerica-engine")
            && !f.getName().contains("launcher"));
    assumeTrue(jars != null && jars.length > 0,
        "askamerica-engine shadow JAR not found — run :askamerica-engine:shadowJar first");
    shadowJar = jars[0];
    LOGGER.info("Shadow JAR: " + shadowJar.getAbsolutePath());
  }

  @BeforeEach
  void startServer() throws Exception {
    // Shadow JAR has no Main-Class — invoke McpServer directly via -cp with --mcp flag
    ProcessBuilder pb =
        new ProcessBuilder(System.getProperty("java.home") + "/bin/java",
        "-cp", shadowJar.getAbsolutePath(),
        "org.apache.calcite.adapter.askamerica.McpServer",
        "--mcp");
    pb.redirectErrorStream(false);
    // Propagate R2 credentials so the subprocess can reach govdata-parquet-v1
    String apiKey = System.getenv("ASKAMERICA_API_KEY");
    if (apiKey != null && !apiKey.isEmpty()) {
      pb.environment().put("ASKAMERICA_API_KEY", apiKey);
    }
    // Each test gets its own isolated data directory to prevent DuckDB file conflicts
    // when tests run concurrently (DuckDB allows only one writer per catalog file).
    File testDataDir = File.createTempFile("mcp_test_", "_data");
    testDataDir.delete();
    testDataDir.mkdirs();
    pb.environment().put("MCP_DATA_DIR", testDataDir.getAbsolutePath());
    mcpProcess = pb.start();
    mcpStdin  = new BufferedWriter(new OutputStreamWriter(mcpProcess.getOutputStream(), StandardCharsets.UTF_8));
    mcpStdout = new BufferedReader(new InputStreamReader(mcpProcess.getInputStream(), StandardCharsets.UTF_8));

    // MCP handshake
    send("{\"jsonrpc\":\"2.0\",\"method\":\"initialize\",\"params\":"
        + "{\"protocolVersion\":\"2024-11-05\","
        + "\"clientInfo\":{\"name\":\"test\",\"version\":\"0.0.1\"}},"
        + "\"id\":0}");
    String initResp = readUntilId(0, TOOL_TIMEOUT_MS);
    assertTrue(initResp.contains("\"result\""), "initialize failed: " + initResp);

    send("{\"jsonrpc\":\"2.0\",\"method\":\"notifications/initialized\"}");
  }

  @AfterEach
  void stopServer() throws InterruptedException {
    if (mcpProcess != null && mcpProcess.isAlive()) {
      mcpProcess.destroyForcibly();
      mcpProcess.waitFor(5, TimeUnit.SECONDS);
    }
  }

  // ── list_schemas ──────────────────────────────────────────────────────────

  @Test void listSchemas_includesRefAndFec() throws Exception {
    String resp = callTool("list_schemas", "{}", TOOL_TIMEOUT_MS);
    String text = extractText(resp);
    assertTrue(text.contains("ref"),
        "list_schemas must include 'ref'; got: " + resp);
    assertTrue(text.contains("fec"),
        "list_schemas must include 'fec'; got: " + resp);
  }

  // ── list_tables ───────────────────────────────────────────────────────────

  @Test void listTables_notEmpty() throws Exception {
    // PRIMARY SHADOW-JAR REGRESSION: if fs.s3a.impl is missing, IcebergTable init
    // fails silently and list_tables returns []. This test catches it.
    String resp =
        callTool("list_tables", "{\"schema\":\"" + TEST_SCHEMA + "\"}", SCHEMA_INIT_TIMEOUT_MS);
    String text = extractText(resp);
    assertFalse(text.equals("[]"),
        "list_tables(" + TEST_SCHEMA + ") returned []. "
            + "Shadow JAR classpath failure — likely missing fs.s3a.impl registration. "
            + "Check MCP server stderr for IcebergTable init errors.");
    assertTrue(text.contains(TEST_TABLE),
        TEST_SCHEMA + " table list must include '" + TEST_TABLE + "'; got: " + text);
  }

  @Test void listTables_tableTypeIsTable() throws Exception {
    String resp =
        callTool("list_tables", "{\"schema\":\"" + TEST_SCHEMA + "\"}", SCHEMA_INIT_TIMEOUT_MS);
    String text = extractText(resp);
    int idx = text.indexOf("\"" + TEST_TABLE + "\"");
    assertTrue(idx >= 0, TEST_TABLE + " not found in: " + text);
    String slice = text.substring(Math.max(0, idx - 50), Math.min(text.length(), idx + 100));
    assertTrue(slice.contains("\"TABLE\""),
        TEST_SCHEMA + "." + TEST_TABLE + " must have type=TABLE (IcebergTable regression); context: " + slice);
  }

  @Test void listTables_includesView() throws Exception {
    // ref schema has ticker_instrument_map as a SQL VIEW
    String resp =
        callTool("list_tables", "{\"schema\":\"" + TEST_SCHEMA + "\"}", SCHEMA_INIT_TIMEOUT_MS);
    String text = extractText(resp);
    assertTrue(text.contains("ticker_instrument_map"),
        TEST_SCHEMA + " must include ticker_instrument_map view; got: " + text);
    int idx = text.indexOf("\"ticker_instrument_map\"");
    String slice = text.substring(Math.max(0, idx - 50), Math.min(text.length(), idx + 100));
    assertTrue(slice.contains("\"VIEW\""),
        "ref.ticker_instrument_map must have type=VIEW; context: " + slice);
  }

  // ── describe_table ────────────────────────────────────────────────────────

  @Test void describeTable_returnsColumns() throws Exception {
    // THE CORE MCP REGRESSION: describe_table was returning [] for all Iceberg tables
    String resp =
        callTool("describe_table", "{\"schema\":\"" + TEST_SCHEMA + "\",\"table\":\"" + TEST_TABLE + "\"}",
        SCHEMA_INIT_TIMEOUT_MS);
    String text = extractText(resp);
    assertFalse(text.equals("[]"),
        "describe_table(" + TEST_SCHEMA + "." + TEST_TABLE + ") returned []. "
            + "This is the core regression — Iceberg tables must expose JDBC metadata.");
    assertTrue(text.contains(TEST_COLUMN),
        TEST_TABLE + " must have " + TEST_COLUMN + " column; got: " + text);
  }

  @Test void describeTable_viewReturnsColumns() throws Exception {
    String resp =
        callTool("describe_table", "{\"schema\":\"" + TEST_SCHEMA + "\",\"table\":\"ticker_instrument_map\"}",
        SCHEMA_INIT_TIMEOUT_MS);
    String text = extractText(resp);
    assertFalse(text.equals("[]"),
        "describe_table(ref.ticker_instrument_map) returned []. SQL views must expose columns.");
    assertTrue(text.contains(TEST_COLUMN),
        "ticker_instrument_map view must expose " + TEST_COLUMN + "; got: " + text);
  }

  // ── query ─────────────────────────────────────────────────────────────────

  @Test void query_executesAndReturnsRows() throws Exception {
    String resp =
        callTool("query", "{\"sql\":\"SELECT ticker, title FROM ref.sec_company_tickers FETCH FIRST 3 ROWS ONLY\","
            + "\"limit\":10}",
        SCHEMA_INIT_TIMEOUT_MS);
    assertFalse(resp.contains("\"isError\":true"),
        "query returned isError:true — " + resp);
    String text = extractText(resp);
    assertTrue(text.contains(TEST_COLUMN),
        "Query result must contain " + TEST_COLUMN + "; got: " + text);
  }

  // ── helpers ───────────────────────────────────────────────────────────────

  private String callTool(String name, String argsJson, long timeoutMs) throws Exception {
    int id = ID_SEQ.getAndIncrement();
    send("{\"jsonrpc\":\"2.0\",\"method\":\"tools/call\","
        + "\"params\":{\"name\":\"" + name + "\",\"arguments\":" + argsJson + "},"
        + "\"id\":" + id + "}");
    return readUntilId(id, timeoutMs);
  }

  private void send(String line) throws IOException {
    mcpStdin.write(line);
    mcpStdin.newLine();
    mcpStdin.flush();
  }

  private String readUntilId(int id, long timeoutMs) throws Exception {
    long deadline = System.currentTimeMillis() + timeoutMs;
    String idToken = "\"id\":" + id;
    while (System.currentTimeMillis() < deadline) {
      if (!mcpProcess.isAlive()) {
        fail("MCP server process exited unexpectedly");
      }
      String line = mcpStdout.readLine();
      if (line == null) {
        Thread.sleep(50);
        continue;
      }
      line = line.trim();
      if (line.isEmpty()) {
        continue;
      }
      if (line.contains(idToken)) {
        LOGGER.info("Response id=" + id + ": " + (line.length() > 300 ? line.substring(0, 300) + "..." : line));
        return line;
      }
    }
    fail("Timed out after " + timeoutMs + "ms waiting for MCP response id=" + id);
    return null;
  }

  /** Extracts the text content from a tools/call response. */
  private String extractText(String response) {
    // response is {"jsonrpc":"2.0","id":N,"result":{"content":[{"type":"text","text":"..."}],...}}
    int textIdx = response.indexOf("\"text\":\"");
    if (textIdx < 0) {
      return response;
    }
    int start = textIdx + 8;
    // Find closing quote, handling escaped quotes
    StringBuilder sb = new StringBuilder();
    int i = start;
    while (i < response.length()) {
      char c = response.charAt(i);
      if (c == '\\' && i + 1 < response.length()) {
        char next = response.charAt(i + 1);
        if (next == '"') {
          sb.append('"');
          i += 2;
          continue;
        } else if (next == 'n') {
          sb.append('\n');
          i += 2;
          continue;
        } else if (next == '\\') {
          sb.append('\\');
          i += 2;
          continue;
        }
      }
      if (c == '"') {
        break;
      }
      sb.append(c);
      i++;
    }
    return sb.toString();
  }
}
