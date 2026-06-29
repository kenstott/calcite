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
package org.apache.calcite.adapter.file;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * FILE-051 / FILE-147 — AperioDriver operand construction and variable expansion. buildOperand and
 * expandVarString are private; exercised via reflection to pin the documented/decided defaults.
 */
@Tag("unit")
public class AperioDriverOperandTest {

  @SuppressWarnings("unchecked")
  private static Map<String, Object> buildOperand(String path, Properties info) throws Exception {
    Method m = AperioDriver.class.getDeclaredMethod("buildOperand", String.class, Properties.class);
    m.setAccessible(true);
    return (Map<String, Object>) m.invoke(new AperioDriver(), path, info);
  }

  private static String expandVar(String value) throws Exception {
    Method m = AperioDriver.class.getDeclaredMethod("expandVarString", String.class);
    m.setAccessible(true);
    return (String) m.invoke(new AperioDriver(), value);
  }

  @Test @Tag("FILE-147") @Tag("FILE-051") void operandDefaults() throws Exception {
    Map<String, Object> op = buildOperand("/data/csv", new Properties());
    assertEquals("/data/csv", op.get("directory"));
    assertEquals("duckdb", op.get("executionEngine"),
        "AperioDriver default engine is duckdb (C-12 — differs from ExecutionEngineConfig=parquet)");
    assertEquals("ORACLE", op.get("lex"));
    assertEquals("TO_LOWER", op.get("unquotedCasing"));
    assertEquals("UNCHANGED", op.get("quotedCasing"));
    assertEquals("SMART_CASING", op.get("tableNameCasing"));
    assertEquals("SMART_CASING", op.get("columnNameCasing"));
    assertFalse(op.containsKey("recursive"),
        "recursive is emitted only when explicitly present — no default in the operand (C-01 finding)");
  }

  @Test @Tag("FILE-147") void varExpansionResolvesAndLeavesUndefinedLiteral() throws Exception {
    System.setProperty("APERIO_TEST_VAR_XYZ", "hello");
    try {
      assertEquals("a_hello_b", expandVar("a_${APERIO_TEST_VAR_XYZ}_b"));
      assertEquals("${UNDEFINED_VAR_ZZZ}", expandVar("${UNDEFINED_VAR_ZZZ}"),
          "an undefined variable is left literal (no throw)");
      assertEquals("${APERIO_TEST_VAR_XYZ:def}", expandVar("${APERIO_TEST_VAR_XYZ:def}"),
          "no :default syntax support — colon is part of the var name (C-14)");
    } finally {
      System.clearProperty("APERIO_TEST_VAR_XYZ");
    }
  }
}
