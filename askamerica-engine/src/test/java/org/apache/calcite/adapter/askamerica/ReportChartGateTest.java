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
package org.apache.calcite.adapter.askamerica;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Daily eval q167 and q50, 2026-11-17: reference answers built on multi-row warehouse results
 * were published with no chart, because the only chart gate ran for validations alone.
 */
@Tag("unit")
@Execution(ExecutionMode.SAME_THREAD)
class ReportChartGateTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  @AfterEach void clearCallLog() throws Exception {
    callLog().clear();
  }

  @SuppressWarnings("unchecked")
  private static List<ObjectNode> callLog() throws Exception {
    Field f = McpServer.class.getDeclaredField("CALL_LOG");
    f.setAccessible(true);
    return (List<ObjectNode>) f.get(null);
  }

  private static String gate(String boardSvg) throws Exception {
    Method m = McpServer.class.getDeclaredMethod("enforceReportChart", String.class);
    m.setAccessible(true);
    return (String) m.invoke(null, boardSvg);
  }

  private static ObjectNode call(String tool, int seq) {
    ObjectNode e = MAPPER.createObjectNode();
    e.put("seq", seq);
    e.put("ts", Instant.now().minusSeconds(60 - seq).toString());
    e.put("tool", tool);
    return e;
  }

  private static ObjectNode query(int seq, int rows) {
    ObjectNode e = call("query", seq);
    e.put("sql", "SELECT period, value FROM econ.household_debt_credit");
    e.put("rows", rows);
    return e;
  }

  @Test void aReportOnASeriesWithNoChartIsRefused() throws Exception {
    callLog().add(call("search_catalog", 1));
    callLog().add(query(2, 64));
    String problem = gate(null);
    assertNotNull(problem);
    assertTrue(problem.contains("no chart"), problem);
    assertTrue(problem.contains("64 rows"), problem);
  }

  @Test void aDashboardOnTheCallSatisfiesIt() throws Exception {
    callLog().add(query(1, 64));
    assertNull(gate("<svg/>"));
  }

  @Test void aChartRenderedForThisReportSatisfiesIt() throws Exception {
    callLog().add(query(1, 64));
    callLog().add(call("render_chart", 2));
    assertNull(gate(null));
  }

  @Test void aFailedChartCallDoesNot() throws Exception {
    callLog().add(query(1, 64));
    ObjectNode failed = call("compose_dashboard", 2);
    failed.put("error", "panels missing");
    callLog().add(failed);
    assertNotNull(gate(null));
  }

  @Test void singleFigureLookupsNeedNoChart() throws Exception {
    callLog().add(query(1, 1));
    callLog().add(call("search_catalog", 2));
    assertNull(gate(null));
  }

  @Test void aChartFromAnEarlierReportDoesNotCount() throws Exception {
    callLog().add(call("render_chart", 1));
    callLog().add(call("preview_report", 2));
    callLog().add(query(3, 15));
    assertNotNull(gate(null), "the chart belongs to the report the earlier preview concluded");
  }
}
