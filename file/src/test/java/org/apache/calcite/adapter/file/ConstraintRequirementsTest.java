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

import org.apache.calcite.adapter.file.metadata.TableConstraints;
import org.apache.calcite.rel.RelReferentialConstraint;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.util.ImmutableBitSet;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.io.File;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * FILE-080 / FILE-149 / FILE-081 — exact recode of the weak constraint-metadata tests for the
 * file adapter, pinning the precise behavior of {@link TableConstraints#fromConfig} key/FK
 * derivation, {@link TableConstraints#validateForeignKeys} FK filtering, and the JDBC
 * {@link DatabaseMetaData} surfacing of configured primary and foreign keys.
 *
 * <p>NOTE: foreign-key targets are declared and asserted strictly by COLUMN NAME (never by
 * positional target ordinal or {@code IntPair} index ordering), so these assertions remain valid
 * both under the current positional FK-target code and under the by-name fix (C-24).
 */
@Tag("unit")
@Execution(ExecutionMode.SAME_THREAD)
public class ConstraintRequirementsTest {

  @TempDir
  Path tempDir;

  // -------------------------------------------------------------------------
  // Helpers for building the (untyped) constraint config maps consumed by
  // TableConstraints.fromConfig.
  // -------------------------------------------------------------------------

  private static Map<String, Object> tableConfigWithConstraints(Map<String, Object> constraints) {
    Map<String, Object> tableConfig = new LinkedHashMap<String, Object>();
    tableConfig.put("constraints", constraints);
    return tableConfig;
  }

  // =========================================================================
  // FILE-080 — fromConfig key derivation
  // =========================================================================

  @Test @Tag("FILE-080")
  void compositePrimaryKeyYieldsOneKeyWithAllMemberBits() {
    Map<String, Object> constraints = new LinkedHashMap<String, Object>();
    constraints.put("primaryKey", Arrays.asList("year", "state"));

    List<String> columnNames = Arrays.asList("year", "state", "value");

    Statistic result =
        TableConstraints.fromConfig(tableConfigWithConstraints(constraints), columnNames, null);

    assertNotNull(result.getKeys(), "composite PK must produce keys");
    assertEquals(1, result.getKeys().size(),
        "a composite primary key must yield exactly ONE key entry");
    ImmutableBitSet key = result.getKeys().get(0);
    // Exactly the two member-column bits (year=0, state=1) are set — nothing else.
    assertEquals(ImmutableBitSet.of(0, 1), key,
        "the single key bitset must have all member-column bits set and only those");
  }

  @Test @Tag("FILE-080")
  void uniqueKeysAreAddedAsAdditionalKeyEntries() {
    Map<String, Object> constraints = new LinkedHashMap<String, Object>();
    constraints.put("primaryKey", Arrays.asList("id"));
    List<List<String>> uniqueKeys = new ArrayList<List<String>>();
    uniqueKeys.add(Arrays.asList("email"));
    uniqueKeys.add(Arrays.asList("ssn"));
    constraints.put("uniqueKeys", uniqueKeys);

    List<String> columnNames = Arrays.asList("id", "email", "ssn", "name");

    Statistic result =
        TableConstraints.fromConfig(tableConfigWithConstraints(constraints), columnNames, null);

    assertNotNull(result.getKeys());
    // PK (id) + two declared unique keys (email, ssn) = three additional key entries.
    assertEquals(3, result.getKeys().size(),
        "declared uniqueKeys must be added as ADDITIONAL key entries beyond the PK");
    // PK is emitted first, then unique keys in declaration order.
    assertEquals(ImmutableBitSet.of(0), result.getKeys().get(0), "key[0] = PK(id)");
    assertEquals(ImmutableBitSet.of(1), result.getKeys().get(1), "key[1] = unique(email)");
    assertEquals(ImmutableBitSet.of(2), result.getKeys().get(2), "key[2] = unique(ssn)");
  }

  @Test @Tag("FILE-080")
  void noConstraintsReturnsStatisticsUnknown() {
    Map<String, Object> tableConfig = new LinkedHashMap<String, Object>();
    List<String> columnNames = Arrays.asList("id", "name", "value");

    Statistic result = TableConstraints.fromConfig(tableConfig, columnNames, null);

    assertSame(Statistics.UNKNOWN, result,
        "with no constraints map, fromConfig must return Statistics.UNKNOWN");
  }

  @Test @Tag("FILE-080")
  void primaryKeyReferencingUnknownColumnDropsTheKey() {
    Map<String, Object> constraints = new LinkedHashMap<String, Object>();
    constraints.put("primaryKey", Arrays.asList("nonexistent_col"));

    List<String> columnNames = Arrays.asList("id", "name");

    Statistic result =
        TableConstraints.fromConfig(tableConfigWithConstraints(constraints), columnNames, null);

    // The bad key contributes no set bits, so it is dropped entirely (no empty key entry).
    assertTrue(result.getKeys() == null || result.getKeys().isEmpty(),
        "a PK referencing a column absent from the row type must yield empty keys");
  }

  // =========================================================================
  // FILE-149 — foreign-key validation / skipping
  // =========================================================================

  @Test @Tag("FILE-149")
  void foreignKeyMissingRequiredFieldsIsSilentlySkipped() {
    Map<String, Object> fk = new LinkedHashMap<String, Object>();
    // Only source columns present; targetTable + targetColumns are missing.
    fk.put("columns", Arrays.asList("dept_id"));

    Map<String, Object> constraints = new LinkedHashMap<String, Object>();
    List<Map<String, Object>> fkList = new ArrayList<Map<String, Object>>();
    fkList.add(fk);
    constraints.put("foreignKeys", fkList);

    List<String> columnNames = Arrays.asList("id", "dept_id");

    Statistic result =
        TableConstraints.fromConfig(tableConfigWithConstraints(constraints), columnNames, null);

    assertTrue(result.getReferentialConstraints() == null
            || result.getReferentialConstraints().isEmpty(),
        "an FK missing required target fields must be silently skipped (continue)");
  }

  @Test @Tag("FILE-149")
  void validateForeignKeysReturnsSameStatisticWhenAllTargetsExist() {
    Map<String, Object> fk = new LinkedHashMap<String, Object>();
    fk.put("columns", Arrays.asList("dept_id"));
    fk.put("targetTable", "departments");
    // Declare the target column by the SAME name as the source — by-name safe (C-24).
    fk.put("targetColumns", Arrays.asList("dept_id"));

    Map<String, Object> constraints = new LinkedHashMap<String, Object>();
    List<Map<String, Object>> fkList = new ArrayList<Map<String, Object>>();
    fkList.add(fk);
    constraints.put("foreignKeys", fkList);

    List<String> columnNames = Arrays.asList("id", "dept_id");

    Statistic stat = TableConstraints.fromConfig(
        tableConfigWithConstraints(constraints), columnNames, null, "myschema", "employees");
    assertNotNull(stat.getReferentialConstraints());
    assertEquals(1, stat.getReferentialConstraints().size(),
        "the well-formed FK should be present before validation");

    // All targets exist → unchanged statistic instance is returned.
    Statistic validated =
        TableConstraints.validateForeignKeys(stat, "employees", (schema, table) -> true);
    assertSame(stat, validated,
        "validateForeignKeys must return the SAME statistic unchanged when all FK targets exist");
  }

  @Test @Tag("FILE-149")
  void validateForeignKeysDropsFkWhoseTargetTableIsAbsent() {
    Map<String, Object> fk = new LinkedHashMap<String, Object>();
    fk.put("columns", Arrays.asList("dept_id"));
    fk.put("targetTable", "departments");
    fk.put("targetColumns", Arrays.asList("dept_id"));

    Map<String, Object> constraints = new LinkedHashMap<String, Object>();
    List<Map<String, Object>> fkList = new ArrayList<Map<String, Object>>();
    fkList.add(fk);
    constraints.put("foreignKeys", fkList);

    List<String> columnNames = Arrays.asList("id", "dept_id");

    Statistic stat = TableConstraints.fromConfig(
        tableConfigWithConstraints(constraints), columnNames, null, "myschema", "employees");
    assertEquals(1, stat.getReferentialConstraints().size());

    // Target table reported absent → the FK is dropped, a new statistic is returned.
    Statistic validated =
        TableConstraints.validateForeignKeys(stat, "employees", (schema, table) -> false);
    assertNotNull(validated.getReferentialConstraints());
    assertTrue(validated.getReferentialConstraints().isEmpty(),
        "an FK whose target table is absent must be dropped by validateForeignKeys");
  }

  // =========================================================================
  // FILE-081 — JDBC DatabaseMetaData surfacing (mirrors ConstraintMetadataJdbcTest harness)
  // =========================================================================

  @Test @Tag("FILE-081")
  void getPrimaryKeysSurfacesConfiguredPkInKeySeqOrder() throws Exception {
    createParquet("customers.parquet",
        "SELECT CAST(i AS INTEGER) AS customer_id, 'name_' || CAST(i AS VARCHAR) AS name "
        + "FROM generate_series(1, 5) AS t(i)");
    createParquet("orders.parquet",
        "SELECT CAST(i AS INTEGER) AS order_id, CAST(i AS INTEGER) AS customer_id, "
        + "CAST(i AS DOUBLE) * 10.0 AS amount "
        + "FROM generate_series(1, 10) AS t(i)");

    try (Connection conn = openConnection()) {
      DatabaseMetaData meta = conn.getMetaData();

      assertEquals(Arrays.asList("customer_id"), readPkColumnsByKeySeq(meta, "test", "customers"),
          "customers PK must surface as [customer_id]");
      assertEquals(Arrays.asList("order_id"), readPkColumnsByKeySeq(meta, "test", "orders"),
          "orders PK must surface as [order_id]");
    }
  }

  @Test @Tag("FILE-081")
  void getImportedKeysSurfacesConfiguredFkByName() throws Exception {
    createParquet("customers.parquet",
        "SELECT CAST(i AS INTEGER) AS customer_id, 'name_' || CAST(i AS VARCHAR) AS name "
        + "FROM generate_series(1, 5) AS t(i)");
    createParquet("orders.parquet",
        "SELECT CAST(i AS INTEGER) AS order_id, CAST(i AS INTEGER) AS customer_id, "
        + "CAST(i AS DOUBLE) * 10.0 AS amount "
        + "FROM generate_series(1, 10) AS t(i)");

    try (Connection conn = openConnection()) {
      DatabaseMetaData meta = conn.getMetaData();
      // getImportedKeys(catalog, schema, fkTable): orders is the child (FK) table.
      try (ResultSet fks = meta.getImportedKeys(null, "test", "orders")) {
        assertTrue(fks.next(), "orders must have an imported key (FK to customers)");
        // Assert strictly by NAME — valid under positional and by-name (C-24) FK matching.
        assertEquals("customer_id", fks.getString("FKCOLUMN_NAME"),
            "FK column must be customer_id");
        assertEquals("customers", fks.getString("PKTABLE_NAME"),
            "referenced table must be customers");
        assertEquals("customer_id", fks.getString("PKCOLUMN_NAME"),
            "referenced column must be customer_id");
        assertFalse(fks.next(), "orders must have exactly one FK column");
      }
    }
  }

  // -------------------------------------------------------------------------
  // Helpers (mirror ConstraintMetadataJdbcTest)
  // -------------------------------------------------------------------------

  /** Reads PK column names ordered by KEY_SEQ, as exposed by getPrimaryKeys. */
  private List<String> readPkColumnsByKeySeq(DatabaseMetaData meta,
      String schema, String table) throws Exception {
    // Collect (KEY_SEQ -> COLUMN_NAME); getPrimaryKeys does not guarantee row order,
    // so order explicitly by KEY_SEQ.
    Map<Short, String> bySeq = new java.util.TreeMap<Short, String>();
    try (ResultSet rs = meta.getPrimaryKeys(null, schema, table)) {
      while (rs.next()) {
        bySeq.put(rs.getShort("KEY_SEQ"), rs.getString("COLUMN_NAME"));
      }
    }
    return new ArrayList<String>(bySeq.values());
  }

  private void createParquet(String fileName, String selectSql) throws Exception {
    File out = new File(tempDir.toFile(), fileName);
    String sql = "COPY (" + selectSql + ") TO '" + out.getAbsolutePath() + "' (FORMAT PARQUET)";
    ProcessBuilder pb = new ProcessBuilder("duckdb", "-c", sql);
    pb.redirectErrorStream(true);
    Process proc = pb.start();
    int exit = proc.waitFor();
    if (exit != 0) {
      byte[] buf = new byte[4096];
      int len = proc.getInputStream().read(buf);
      String err = len > 0 ? new String(buf, 0, len) : "unknown error";
      fail("DuckDB CLI failed creating " + fileName + ": " + err);
    }
    assertTrue(out.exists(), "Parquet file should exist: " + out);
  }

  private Connection openConnection() throws Exception {
    String dir = tempDir.toFile().getAbsolutePath().replace("\\", "\\\\");
    // tableConstraints: orders has PK(order_id) + FK(customer_id -> customers.customer_id);
    //                   customers has PK(customer_id). FK target column shares the source name.
    String model = "{"
        + "\"version\":\"1.0\","
        + "\"defaultSchema\":\"test\","
        + "\"schemas\":[{"
        + "  \"name\":\"test\","
        + "  \"type\":\"custom\","
        + "  \"factory\":\"org.apache.calcite.adapter.file.FileSchemaFactory\","
        + "  \"operand\":{"
        + "    \"directory\":\"" + dir + "\","
        + "    \"ephemeralCache\":true,"
        + "    \"tableConstraints\":{"
        + "      \"orders\":{"
        + "        \"primaryKey\":[\"order_id\"],"
        + "        \"foreignKeys\":[{"
        + "          \"columns\":[\"customer_id\"],"
        + "          \"targetTable\":\"customers\","
        + "          \"targetColumns\":[\"customer_id\"]"
        + "        }]"
        + "      },"
        + "      \"customers\":{"
        + "        \"primaryKey\":[\"customer_id\"]"
        + "      }"
        + "    }"
        + "  }"
        + "}]}";

    Properties props = new Properties();
    props.put("model", "inline:" + model);
    return DriverManager.getConnection("jdbc:calcite:", props);
  }
}
