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
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * FILE-146 — the second JDBC driver, {@code FileJdbcDriver}, exact-assertion golden (recode of the
 * weak {@code FileJdbcDriverDeepCoverageTest#testSchemaNameFromUrlIsFiles}, which proved only the
 * schema name). Pins the full snake_case operand-param set parsed from a {@code jdbc:calcite:} URL,
 * the injected defaults (engine=parquet, batchSize=2048, java.io.tmpdir/user.dir paths), and the
 * distinction from {@link AperioDriver} (which uses the {@code jdbc:aperio:} prefix and camelCase
 * operand keys). FILE-168 already pins the float-batch_size fallback; this does not duplicate it.
 *
 * <p>{@code parseConfiguration}/{@code buildOperand} are private and pure (string + system-property
 * parsing, no filesystem), so they are exercised via reflection per the established pattern.
 */
@Tag("unit")
public class FileJdbcDriverRequirementsTest {

  private static Object parseConfig(String url, Properties info) throws Exception {
    Method m = FileJdbcDriver.class.getDeclaredMethod("parseConfiguration", String.class, Properties.class);
    m.setAccessible(true);
    return m.invoke(new FileJdbcDriver(), url, info);
  }

  private static List<?> schemasOf(Object config) throws Exception {
    Method m = config.getClass().getDeclaredMethod("getSchemas");
    m.setAccessible(true);
    return (List<?>) m.invoke(config);
  }

  private static Object get(Object obj, String getter) throws Exception {
    Method m = obj.getClass().getDeclaredMethod(getter);
    m.setAccessible(true);
    return m.invoke(obj);
  }

  @SuppressWarnings("unchecked")
  private static Map<String, Object> aperioOperand(String path, Properties info) throws Exception {
    Method m = AperioDriver.class.getDeclaredMethod("buildOperand", String.class, Properties.class);
    m.setAccessible(true);
    return (Map<String, Object>) m.invoke(new AperioDriver(), path, info);
  }

  @Test @Tag("FILE-146") void snakeCaseParamsEachMapToTheirField() throws Exception {
    Object config = parseConfig(
        "jdbc:calcite:;schema=file;storage_path=/sp;engine=arrow;data_path=/dp;batch_size=4096",
        new Properties());
    List<?> schemas = schemasOf(config);
    assertEquals(1, schemas.size(), "schema=file yields exactly one schema");
    Object sc = schemas.get(0);
    assertEquals("files", get(sc, "getName"), "schema=file -> schema named 'files'");
    assertEquals("/dp", get(sc, "getDataPath"), "data_path");
    assertEquals("/sp", get(sc, "getStoragePath"), "storage_path");
    assertEquals("arrow", get(sc, "getEngineType"), "engine");
    assertEquals(4096, get(sc, "getBatchSize"), "batch_size");
  }

  @Test @Tag("FILE-146") void defaultsWhenOnlySchemaFile() throws Exception {
    Object config = parseConfig("jdbc:calcite:;schema=file", new Properties());
    Object sc = schemasOf(config).get(0);
    assertEquals("parquet", get(sc, "getEngineType"), "default engine is parquet");
    assertEquals(2048, get(sc, "getBatchSize"), "default batchSize is 2048");
    assertEquals(System.getProperty("java.io.tmpdir") + "/calcite_file_storage",
        get(sc, "getStoragePath"), "default storagePath under java.io.tmpdir");
    assertEquals(System.getProperty("user.dir") + "/data",
        get(sc, "getDataPath"), "default dataPath under user.dir");
  }

  @Test @Tag("FILE-146") void acceptsCalciteSchemaFileUrlOnly() throws Exception {
    FileJdbcDriver driver = new FileJdbcDriver();
    assertTrue(driver.acceptsURL("jdbc:calcite:schema=file;data_path=/d"),
        "accepts a jdbc:calcite URL naming schema=file");
    assertFalse(driver.acceptsURL("jdbc:aperio:/data"),
        "does not poach aperio URLs");
    assertFalse(driver.acceptsURL("jdbc:calcite:"),
        "a bare jdbc:calcite URL with no schema=file/materialized_view is not accepted");
  }

  @Test @Tag("FILE-146") void distinctFromAperioDriverByPrefixAndCamelCaseOperand() throws Exception {
    AperioDriver aperio = new AperioDriver();
    assertFalse(aperio.acceptsURL("jdbc:calcite:schema=file;data_path=/d"),
        "AperioDriver (jdbc:aperio: prefix) rejects the jdbc:calcite URL");
    assertTrue(aperio.acceptsURL("jdbc:aperio:/data"), "AperioDriver accepts jdbc:aperio URLs");

    // AperioDriver builds a camelCase operand (executionEngine), NOT the snake_case 'engine' key.
    Map<String, Object> op = aperioOperand("/data", new Properties());
    assertEquals("duckdb", op.get("executionEngine"), "AperioDriver default engine is duckdb");
    assertTrue(op.containsKey("executionEngine"), "AperioDriver uses camelCase executionEngine");
    assertFalse(op.containsKey("engine"), "AperioDriver does NOT use the snake_case engine key");
  }
}
