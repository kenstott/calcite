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

  /**
   * Timeout covering a server process's one-time schema mount.
   *
   * <p>That mount is a per-process cost, not a per-schema or per-query one, and it is not a
   * metadata rebuild: the jar ships a seeded DuckDB catalog, and a server started against a
   * completely empty data dir still reports every Iceberg view reused and none rebuilt (356/0
   * measured), so no object-store metadata reads happen. What it pays for is mounting all 26
   * schemas onto one connection and creating the deferred SQL views. Measured at 231s
   * standalone; 298–377s under this suite, which competes with itself.
   *
   * <p>This class pays it once per <em>test method</em>, because {@code startServer} spawns a
   * fresh process each time. Once a process is up, queries are sub-second — including against
   * a schema it has not touched before. Nothing here reflects a cost a running server imposes
   * on a user.
   *
   * <p>Sits just above the server's own 600s schema-init bound ({@code McpServer}'s
   * {@code latch.await}) so that when a mount overruns, the failure reported is the server's
   * own "still initializing" message rather than a bare client-side timeout that says nothing
   * about why. The old 120s value was below every mount measured here — it never fired only
   * because the read loop blocked past its own deadline (see {@link #readUntilId}).
   */
  private static final long SCHEMA_INIT_TIMEOUT_MS = 660_000;
  /**
   * Timeout for a response that touches no schema at all — the initialize handshake, the
   * prompt templates, tools/list.
   *
   * <p>Not a "warmed up" timeout: {@code startServer} gives every test method its own fresh
   * process, so no test ever runs against a warm schema. A call that reaches a schema needs
   * {@link #SCHEMA_INIT_TIMEOUT_MS} even when it is the test's first, and that includes
   * {@code list_schemas}, which reads {@code information_schema} despite sounding like a
   * local lookup.
   */
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
  private File mcpStderr;

  @BeforeAll
  static void locateShadowJar() {
    File libsDir = new File("build/libs");
    assumeTrue(libsDir.exists(),
        "build/libs not found — run :askamerica-engine:shadowJar first");
    File[] jars = libsDir.listFiles(f ->
        f.getName().endsWith(".jar")
            && f.getName().contains("askamerica-engine")
            && !f.getName().contains("launcher")
            && !f.getName().contains("sources"));
    assumeTrue(jars != null && jars.length > 0,
        "askamerica-engine shadow JAR not found — run :askamerica-engine:shadowJar first");
    // Both the fat shadow jar and the thin 'calcite-askamerica-engine' jar match; only the shadow
    // jar bundles McpServer AND its dependencies, so running the thin jar dies with
    // ClassNotFoundException at startup. The shadow jar is by far the largest — pick it by size,
    // not by listFiles() order (which is unspecified and made this selection flaky).
    File fat = jars[0];
    for (File j : jars) {
      if (j.length() > fat.length()) {
        fat = j;
      }
    }
    shadowJar = fat;
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
    // Drain the server's stderr to a file. The server routes all logging to stderr, and a cold
    // mount is verbose; left as an unread pipe it fills its ~64 KB OS buffer, blocks the server
    // mid-mount, and the process dies before answering the handshake ("exited unexpectedly").
    // A file redirect is drained by the OS and doubles as a diagnostic when a test does fail.
    mcpStderr = new File(testDataDir, "mcp-server.stderr.log");
    pb.redirectError(mcpStderr);
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
    // list_schemas reads information_schema, so on this test's own fresh server it pays the
    // full cold mount like any other schema-touching call.
    String resp = callTool("list_schemas", "{}", SCHEMA_INIT_TIMEOUT_MS);
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
    // Two failures reach this point looking alike, and attributing one to the other sends the
    // reader to the wrong place: an empty list is the classpath regression this test was
    // written for, while a rebuild that overran the server's 600s init bound returns a "still
    // initializing" error instead. Observed once, under two integration suites running
    // concurrently.
    assertFalse(text.contains("is still initializing"),
        "list_tables(" + TEST_SCHEMA + ") did not finish this server process's one-time "
            + "schema mount within the server's own 600s bound, so this is a mount-time "
            + "failure, NOT the classpath regression below. Every test method here spawns a "
            + "fresh server and so pays that mount again (298-377s measured under this "
            + "suite), leaving under 2x headroom that anything competing for R2 bandwidth, "
            + "CPU, or disk can erase. Check for a second test run or an ETL job. Server "
            + "said: " + text);
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

  // ── question quality: ambient teaching, prompts, diagnostics ──────────────

  /**
   * The no-regression guarantee the diagnostics envelope rests on. It is worth an assertion
   * at the wire rather than in a unit test because the promise is about what an existing host
   * receives: the data block must still be the first content block and still be exactly the
   * JSON array it was, with the envelope arriving as a sibling. Merge the two and every host
   * that indexes content[0] and parses it as an array breaks at once.
   */
  @Test void query_dataBlockIsUnchangedAndDiagnosticsRideAlongsideIt() throws Exception {
    String resp =
        callTool("query", "{\"sql\":\"SELECT ticker, title FROM ref.sec_company_tickers "
            + "FETCH FIRST 3 ROWS ONLY\",\"limit\":10}",
        SCHEMA_INIT_TIMEOUT_MS);
    assertFalse(resp.contains("\"isError\":true"), "query returned isError:true — " + resp);

    com.fasterxml.jackson.databind.JsonNode content =
        new com.fasterxml.jackson.databind.ObjectMapper()
            .readTree(resp).path("result").path("content");
    assertEquals(2, content.size(), "expected data + diagnostics blocks; got: " + resp);

    com.fasterxml.jackson.databind.JsonNode data =
        new com.fasterxml.jackson.databind.ObjectMapper()
            .readTree(content.get(0).get("text").asText());
    assertTrue(data.isArray(), "the first block must still be the bare row array");
    assertTrue(data.size() > 0, "expected rows from ref.sec_company_tickers");
    assertFalse(data.get(0).has("diagnostics"),
        "diagnostics must not be mixed into the rows");

    com.fasterxml.jackson.databind.JsonNode diag =
        new com.fasterxml.jackson.databind.ObjectMapper()
            .readTree(content.get(1).get("text").asText()).path("diagnostics");
    assertTrue(diag.has("warnings"), "diagnostics block missing warnings: " + diag);
    assertTrue(diag.path("basis").asText().contains("not a claim of validity"),
        "silence must never read as a clean bill of health: " + diag);
  }

  @Test void criticalDefectsAreVisibleWithoutRunningTheQuery() throws Exception {
    String resp = callTool("critique_query",
        "{\"sql\":\"SELECT corr(a, b) FROM econ.employment_statistics\"}",
        SCHEMA_INIT_TIMEOUT_MS);
    assertFalse(resp.contains("\"isError\":true"), "critique_query failed — " + resp);
    com.fasterxml.jackson.databind.JsonNode out =
        new com.fasterxml.jackson.databind.ObjectMapper()
            .readTree(new com.fasterxml.jackson.databind.ObjectMapper()
                .readTree(resp).path("result").path("content").get(0).get("text").asText());
    String types = out.path("diagnostics").path("warnings").toString();
    assertTrue(types.contains("small_n"),
        "an association with no COUNT(*) cannot be judged for significance: " + types);
    assertTrue(types.contains("uncontrolled_confound"),
        "corr() conditions on nothing and the critique must say so: " + types);
    assertTrue(out.path("rubric").asText().contains("topic, not a question"),
        "the critique should hand back the rubric it judged against");
  }

  @Test void initializeAdvertisesPromptsAndTeachesTheRubric() throws Exception {
    send("{\"jsonrpc\":\"2.0\",\"method\":\"initialize\",\"params\":"
        + "{\"protocolVersion\":\"2024-11-05\","
        + "\"clientInfo\":{\"name\":\"test\",\"version\":\"0.0.1\"}},"
        + "\"id\":9001}");
    String resp = readUntilId(9001, TOOL_TIMEOUT_MS);
    com.fasterxml.jackson.databind.JsonNode result =
        new com.fasterxml.jackson.databind.ObjectMapper().readTree(resp).path("result");
    assertTrue(result.path("capabilities").has("prompts"),
        "prompts must be advertised or no client will ask for them: " + resp);
    String instructions = result.path("instructions").asText();
    assertTrue(instructions.contains("topic, not a question"),
        "the rubric is the global backstop for hosts that read instructions");
    assertTrue(instructions.contains("diagnostics"),
        "a host that is not told the envelope exists will not read it");
  }

  @Test void promptTemplatesAreListableAndRenderFilledIn() throws Exception {
    send("{\"jsonrpc\":\"2.0\",\"method\":\"prompts/list\",\"params\":{},\"id\":9002}");
    String listResp = readUntilId(9002, TOOL_TIMEOUT_MS);
    assertTrue(listResp.contains("marginal_comparison"),
        "prompts/list must return the templates: " + listResp);

    send("{\"jsonrpc\":\"2.0\",\"method\":\"prompts/get\",\"params\":"
        + "{\"name\":\"trend_check\",\"arguments\":{\"measure\":\"violent crime per 100k\","
        + "\"grain\":\"agency\",\"window\":\"2015-2023\"}},\"id\":9003}");
    String getResp = readUntilId(9003, TOOL_TIMEOUT_MS);
    assertTrue(getResp.contains("violent crime per 100k"),
        "prompts/get must substitute its arguments: " + getResp);
    assertFalse(getResp.contains("{measure}"), "placeholder left unsubstituted: " + getResp);
  }

  @Test void toolDescriptionsCarryTheContrastiveExemplars() throws Exception {
    send("{\"jsonrpc\":\"2.0\",\"method\":\"tools/list\",\"params\":{},\"id\":9004}");
    String resp = readUntilId(9004, TOOL_TIMEOUT_MS);
    com.fasterxml.jackson.databind.JsonNode tools =
        new com.fasterxml.jackson.databind.ObjectMapper()
            .readTree(resp).path("result").path("tools");
    String queryDescription = null;
    boolean sawCritique = false;
    for (com.fasterxml.jackson.databind.JsonNode t : tools) {
      if ("query".equals(t.path("name").asText())) {
        queryDescription = t.path("description").asText();
      }
      if ("critique_query".equals(t.path("name").asText())) {
        sawCritique = true;
      }
    }
    assertNotNull(queryDescription, "query tool missing from tools/list");
    assertTrue(sawCritique, "critique_query missing from tools/list");
    int exemplars = 0;
    for (QuestionGuidance.Exemplar e : QuestionGuidance.EXEMPLARS) {
      if (queryDescription.contains(e.vague) && queryDescription.contains(e.sharpened)) {
        exemplars++;
      }
    }
    assertTrue(exemplars >= 6,
        "the query description must carry the full contrastive set; found " + exemplars);
    assertTrue(queryDescription.contains("[honest-refusal]"),
        "the refusal exemplars are what stop the set teaching that everything is answerable");
  }

  // ── helpers ───────────────────────────────────────────────────────────────

  private String callTool(String name, String argsJson, long timeoutMs) throws Exception {
    int id = ID_SEQ.getAndIncrement();
    send("{\"jsonrpc\":\"2.0\",\"method\":\"tools/call\","
        + "\"params\":{\"name\":\"" + name + "\",\"arguments\":" + argsJson + "},"
        + "\"id\":" + id + "}");
    return readUntilId(id, timeoutMs);
  }

  /** Last ~4 KB of the server's redirected stderr, for diagnosing an unexpected exit. */
  private String tailStderr() {
    if (mcpStderr == null || !mcpStderr.isFile()) {
      return "(no stderr captured)";
    }
    try {
      byte[] all = java.nio.file.Files.readAllBytes(mcpStderr.toPath());
      int from = Math.max(0, all.length - 4096);
      return new String(all, from, all.length - from, StandardCharsets.UTF_8);
    } catch (IOException e) {
      return "(could not read stderr: " + e.getMessage() + ")";
    }
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
        fail("MCP server process exited unexpectedly (exit=" + mcpProcess.exitValue()
            + ")\n--- server stderr (tail) ---\n" + tailStderr());
      }
      // readLine() blocks until a line arrives, and stdout carries nothing but JSON-RPC
      // responses (stderr is redirected to a file), so during a several-minute schema mount
      // it blocks straight through the deadline the loop condition is checking. That made
      // every timeout here decorative: mounts measured at 310s passed a nominal 120s bound,
      // and a genuinely hung server would have hung the suite rather than failing it.
      // ready() keeps the deadline real by never entering a blocking read.
      if (!mcpStdout.ready()) {
        Thread.sleep(50);
        continue;
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
