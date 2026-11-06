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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** An engine restart between the queries and the report check does not erase the history. */
@Tag("unit")
@Execution(ExecutionMode.SAME_THREAD)
class CallLogRestartTest {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @BeforeEach void quietLog() {
    McpServer.log = new PrintStream(new ByteArrayOutputStream());
  }

  @AfterEach void reset() throws Exception {
    callLog().clear();
    Field f = McpServer.class.getDeclaredField("callLogStore");
    f.setAccessible(true);
    f.set(null, null);
  }

  @SuppressWarnings("unchecked")
  private static List<ObjectNode> callLog() throws Exception {
    Field f = McpServer.class.getDeclaredField("CALL_LOG");
    f.setAccessible(true);
    return (List<ObjectNode>) f.get(null);
  }

  private static String gate() throws Exception {
    Method m = McpServer.class.getDeclaredMethod("enforceResearchDepthOnGap");
    m.setAccessible(true);
    return (String) m.invoke(null);
  }

  @SuppressWarnings("unchecked")
  private static List<ObjectNode> snapshot() throws Exception {
    Method m = McpServer.class.getDeclaredMethod("recentCallLogSnapshot");
    m.setAccessible(true);
    return (List<ObjectNode>) m.invoke(null);
  }

  private static ObjectNode call(String tool, int seq, long agoMillis) {
    ObjectNode e = MAPPER.createObjectNode();
    e.put("tool", tool);
    e.put("seq", seq);
    e.put("rows", 5);
    e.put("ts", Instant.ofEpochMilli(System.currentTimeMillis() - agoMillis).toString());
    return e;
  }

  private static void write(Path file, ObjectNode... entries) {
    CallLogStore s = new CallLogStore(file, MAPPER, 2000, 10 * 60_000L);
    for (ObjectNode e : entries) {
      s.append(e);
    }
  }

  @Test void queriesBeforeARestartStillCountAfterIt(@TempDir Path dir) throws Exception {
    Path file = dir.resolve("log.jsonl");
    write(file, call("search_catalog", 1, 4 * 60_000L), call("query", 2, 3 * 60_000L));
    assertTrue(callLog().isEmpty(), "a fresh process starts with no calls");
    String refusal = gate();
    assertTrue(refusal.contains("no query call"), refusal);
    assertTrue(refusal.contains("restarted"), "the refusal says the server restarted: " + refusal);

    McpServer.restoreCallLog(file, System.currentTimeMillis());

    assertEquals(2, callLog().size());
    assertNull(gate(), "the reloaded query satisfies the check");
  }

  @Test void theCutAtThePreviousSuccessfulPreviewSurvivesARestart(@TempDir Path dir)
      throws Exception {
    Path file = dir.resolve("log.jsonl");
    write(file, call("query", 1, 5 * 60_000L), call("preview_report", 2, 4 * 60_000L),
        call("search_catalog", 3, 3 * 60_000L));

    McpServer.restoreCallLog(file, System.currentTimeMillis());

    List<ObjectNode> snap = snapshot();
    assertEquals(1, snap.size(), "only the call after the earlier report belongs to this one");
    assertEquals("search_catalog", snap.get(0).get("tool").asText());
  }

  @Test void aPartialLastLineIsReportedLoudlyAndTheRestIsUsed(@TempDir Path dir) throws Exception {
    Path file = dir.resolve("log.jsonl");
    write(file, call("query", 1, 60_000L));
    Files.write(file, "{\"tool\":\"quer".getBytes(StandardCharsets.UTF_8),
        StandardOpenOption.APPEND);
    PrintStream err = System.err;
    ByteArrayOutputStream captured = new ByteArrayOutputStream();
    System.setErr(new PrintStream(captured, true, "UTF-8"));
    try {
      McpServer.restoreCallLog(file, System.currentTimeMillis());
    } finally {
      System.setErr(err);
    }
    assertEquals(1, callLog().size());
    String out = captured.toString("UTF-8");
    assertTrue(out.contains("unreadable"), out);
  }
}
