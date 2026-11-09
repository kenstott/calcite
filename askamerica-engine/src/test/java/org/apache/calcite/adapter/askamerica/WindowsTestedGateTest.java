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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A claimed change with no anchored window, graded contrary, must show two windows tested or say
 * why the data holds one.
 */
@Tag("unit")
class WindowsTestedGateTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String Q4 =
      "Electricity prices have increased about 40% over the past five years";

  private static String gate(String claimsJson) throws Exception {
    Method m = McpServer.class.getDeclaredMethod("enforceWindowsTested", JsonNode.class);
    m.setAccessible(true);
    return (String) m.invoke(null, MAPPER.readTree(claimsJson));
  }

  private static String claim(String assertion, String verdict, String extra) {
    return "[{\"assertion\":\"" + assertion + "\",\"verdict\":\"" + verdict + "\"" + extra + "}]";
  }

  private static final String TWO = ",\"windows_tested\":[{\"window\":\"2020 to 2025\","
      + "\"value\":\"+32%\"},{\"window\":\"2020 to 2026 YTD\",\"value\":\"+37%\"}]";

  /** The q4 shape: a length, no endpoints, one window. */
  @Test void relativeWindowOnOneWindowIsRefused() throws Exception {
    String problem = gate(claim(Q4, "partially false", ",\"warehouse_value\":\"32.1%\""));
    assertNotNull(problem);
    assertTrue(problem.contains("over the past five years"), problem);
    assertTrue(problem.contains("windows_tested"), problem);
  }

  @Test void twoWindowsPass() throws Exception {
    assertNull(gate(claim(Q4, "partially false", TWO)));
  }

  @Test void theSameWindowTwiceDoesNotCount() throws Exception {
    assertNotNull(gate(claim(Q4, "mostly false", ",\"windows_tested\":["
        + "{\"window\":\"2020 to 2025\",\"value\":\"+32%\"},"
        + "{\"window\":\" 2020  TO 2025 \",\"value\":\"+32%\"}]")));
  }

  @Test void aWindowWithNoValueDoesNotCount() throws Exception {
    assertNotNull(gate(claim(Q4, "false", ",\"windows_tested\":["
        + "{\"window\":\"2020 to 2025\",\"value\":\"+32%\"},{\"window\":\"2021 to 2026\"}]")));
  }

  @Test void aStatedReasonForOneWindowPasses() throws Exception {
    assertNull(gate(claim(Q4, "partially false",
        ",\"why_single_window\":\"the table holds two years only\"")));
  }

  @Test void aBlankReasonDoesNotPass() throws Exception {
    assertNotNull(gate(claim(Q4, "partially false", ",\"why_single_window\":\"  \"")));
  }

  @Test void anAnchoredWindowIsExempt() throws Exception {
    assertNull(gate(claim("Prices rose 40% from 2020 to 2025", "partially false", "")));
    assertNull(gate(claim("Prices rose 4% since March", "false", "")));
    assertNull(gate(claim("Spending fell 3% in fiscal year terms", "false", "")));
  }

  @Test void supportingVerdictsAreExempt() throws Exception {
    assertNull(gate(claim(Q4, "true", "")));
    assertNull(gate(claim(Q4, "mostly true", "")));
    assertNull(gate(claim(Q4, "not checkable here", "")));
  }

  @Test void causalAttackAndNonChangeClaimsAreExempt() throws Exception {
    assertNull(gate(claim(Q4, "false", ",\"kind\":\"causal\"")));
    assertNull(gate(claim("Prices have increased sharply", "false", "")));
    assertNull(gate(claim("The rule covers loads of 75 MW", "partially false", "")));
  }
}
