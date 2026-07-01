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
import static org.junit.jupiter.api.Assertions.assertNull;

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

  // ===== FILE-184: rw mode → tracker backend + autoDownload =====

  @Test @Tag("FILE-184") void rwOnDefaultSelectsWritableDuckdbTracker() throws Exception {
    Map<String, Object> op = buildOperand("/data", new Properties());
    assertEquals("duckdb", op.get("trackerBackend"),
        "rw defaults to on → writable local DuckDB tracker (required, else the fail-closed default blocks writes)");
    assertFalse(op.containsKey("autoDownload"),
        "rw=on does not force autoDownload — materialize is permitted, not forced");
  }

  @Test @Tag("FILE-184") void rwOffCompilesReadOnlyModel() throws Exception {
    Properties info = new Properties();
    info.setProperty("rw", "off");
    Map<String, Object> op = buildOperand("/data", info);
    assertEquals("readonly", op.get("trackerBackend"),
        "rw=off → read-only tracker: any write throws (consumer/distribution mode)");
    assertEquals(Boolean.FALSE, op.get("autoDownload"),
        "rw=off disables autoDownload so a shared config cannot trigger ETL");
  }

  @Test @Tag("FILE-184") void explicitTrackerBackendWinsOverRw() throws Exception {
    Properties info = new Properties();
    info.setProperty("rw", "off");
    info.setProperty("trackerBackend", "pg");
    Map<String, Object> op = buildOperand("/data", info);
    assertEquals("pg", op.get("trackerBackend"), "an explicit trackerBackend param overrides the rw-derived default");
  }

  // ===== FILE-185: storage root → baseDirectory, default ~/.aperio =====

  @Test @Tag("FILE-185") void storageDefaultsToHomeAperioNamespacedBySchema() throws Exception {
    Map<String, Object> op = buildOperand("/data", new Properties());
    assertEquals(System.getProperty("user.home") + "/.aperio/files", op.get("baseDirectory"),
        "default storage root is ~/.aperio (per-user, local), namespaced by schema (default 'files')");
  }

  @Test @Tag("FILE-185") void storageParamNamespacedBySchemaWithTildeExpansion() throws Exception {
    Properties info = new Properties();
    info.setProperty("storage", "~/lakes");
    info.setProperty("schema", "sales");
    Map<String, Object> op = buildOperand("/data", info);
    assertEquals(System.getProperty("user.home") + "/lakes/sales", op.get("baseDirectory"),
        "storage param sets the root (~ expands to home), namespaced by schema");
  }

  @Test @Tag("FILE-185") void explicitBaseDirectoryWinsOverStorage() throws Exception {
    Properties info = new Properties();
    info.setProperty("storage", "/ignored");
    info.setProperty("baseDirectory", "/explicit/base");
    Map<String, Object> op = buildOperand("/data", info);
    assertEquals("/explicit/base", op.get("baseDirectory"));
  }

  // ===== FILE-188: consumer URI derivation =====

  @Test @Tag("FILE-188") void deriveConsumerUriPointsAtLakeReadOnly() {
    assertEquals("jdbc:aperio:/mnt/lake?rw=off", AperioDriver.deriveConsumerUri("/mnt/lake", "files"),
        "consumer URI points at the lake with rw=off; default schema omitted");
    assertEquals("jdbc:aperio:/mnt/lake?rw=off&schema=sales",
        AperioDriver.deriveConsumerUri("/mnt/lake", "sales"),
        "non-default schema is carried");
    assertNull(AperioDriver.deriveConsumerUri(null, "files"),
        "no shared storage → nothing to share");
  }

  // ===== FILE-187: multiple directories via glob =====

  @Test @Tag("FILE-187") void globParamForwardedForMultiDirectorySpanning() throws Exception {
    Properties info = new Properties();
    info.setProperty("glob", "{sales,orders}/**/*.csv");
    Map<String, Object> op = buildOperand("/data", info);
    assertEquals("{sales,orders}/**/*.csv", op.get("glob"),
        "glob is forwarded to the operand; a brace glob spans multiple directories under the source dir "
            + "(FileSchema matches it via NIO getPathMatcher) — flat multi-directory, no extra plumbing");
  }
}
