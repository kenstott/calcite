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
package org.apache.calcite.adapter.file.metadata;

import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;

import com.google.common.collect.ImmutableMap;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * INFORMATION_SCHEMA.TABLES and .COLUMNS must resolve only the tables a query asks about.
 *
 * <p>They used to scan the whole catalog regardless of the WHERE clause, calling
 * {@code getRowType()} on every table in every schema. That made a single-table {@code
 * describe_table} O(catalog) and — because nothing isolates one table's failure from the scan —
 * let one unresolvable table anywhere make metadata lookups fail for every table, including
 * tables that query perfectly well. These tests pin both properties.
 */
@Tag("unit")
public class InformationSchemaFilterPushdownTest {

  /** Counts row-type resolutions so a test can assert which tables a scan actually touched. */
  private static class CountingTable extends AbstractTable {
    private final AtomicInteger resolutions;

    CountingTable(AtomicInteger resolutions) {
      this.resolutions = resolutions;
    }

    @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      resolutions.incrementAndGet();
      return typeFactory.builder()
          .add("ID", SqlTypeName.INTEGER)
          .add("LABEL", SqlTypeName.VARCHAR)
          .build();
    }
  }

  /** Stands in for a table whose view cannot be resolved — a missing or corrupt Iceberg table. */
  private static class UnresolvableTable extends AbstractTable {
    @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      throw new IllegalStateException("iceberg_scan failed: table metadata unreadable");
    }
  }

  /**
   * A vintage-partitioned table: keyed on (ID, YEAR), so ID alone is not unique. Models
   * geo.counties, which stores one copy of every county per TIGER vintage.
   */
  private static class KeyedVintageTable extends AbstractTable {
    @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      return typeFactory.builder()
          .add("ID", SqlTypeName.VARCHAR)
          .add("LABEL", SqlTypeName.VARCHAR)
          .add("YEAR", SqlTypeName.INTEGER)
          .build();
    }

    @Override public org.apache.calcite.schema.Statistic getStatistic() {
      return org.apache.calcite.schema.Statistics.of(1000d,
          java.util.Collections.singletonList(
              org.apache.calcite.util.ImmutableBitSet.of(0, 2)));
    }
  }

  private static class FixedSchema extends AbstractSchema {
    private final Map<String, Table> tables;

    FixedSchema(Map<String, Table> tables) {
      this.tables = tables;
    }

    @Override protected Map<String, Table> getTableMap() {
      return tables;
    }
  }

  /**
   * Builds a connection with two schemas: {@code good} holding countable tables, and {@code bad}
   * holding one table that throws on row-type resolution.
   */
  private Connection connect(AtomicInteger resolutions) throws Exception {
    Properties info = new Properties();
    Connection conn = DriverManager.getConnection("jdbc:calcite:", info);
    SchemaPlus root = conn.unwrap(CalciteConnection.class).getRootSchema();

    root.add("good", new FixedSchema(ImmutableMap.of(
        "alpha", new CountingTable(resolutions),
        "beta", new CountingTable(resolutions),
        "gamma", new CountingTable(resolutions))));
    root.add("bad", new FixedSchema(ImmutableMap.of(
        "broken", new UnresolvableTable())));
    root.add("keyed", new FixedSchema(ImmutableMap.of(
        "vintaged", new KeyedVintageTable())));
    root.add("information_schema", new InformationSchema(root, "CALCITE"));
    return conn;
  }

  /**
   * The defect that surfaced as "describe_table failed on three tables in three different schemas,
   * all of which are queryable". A broken table in an unrelated schema must not be resolved, and
   * so cannot fail, a lookup scoped to another schema's table.
   */
  @Test void columnsLookupIgnoresBrokenTableInAnotherSchema() throws Exception {
    AtomicInteger resolutions = new AtomicInteger();
    try (Connection conn = connect(resolutions);
         Statement st = conn.createStatement();
         ResultSet rs = st.executeQuery(
             "SELECT COLUMN_NAME FROM \"information_schema\".\"COLUMNS\" "
             + "WHERE LOWER(TABLE_SCHEMA) = 'good' AND LOWER(TABLE_NAME) = 'alpha' "
             + "ORDER BY ORDINAL_POSITION")) {
      assertTrue(rs.next(), "expected columns for good.alpha");
      assertEquals("ID", rs.getString(1));
      assertTrue(rs.next());
      assertEquals("LABEL", rs.getString(1));
      assertTrue(!rs.next(), "only good.alpha's columns should be returned");
    }
    assertEquals(1, resolutions.get(),
        "a single-table lookup must resolve exactly one table, not the whole catalog");
  }

  /** A schema-scoped listing must not resolve tables outside that schema. */
  @Test void tablesLookupScopedToOneSchema() throws Exception {
    AtomicInteger resolutions = new AtomicInteger();
    int found = 0;
    try (Connection conn = connect(resolutions);
         Statement st = conn.createStatement();
         ResultSet rs = st.executeQuery(
             "SELECT TABLE_NAME FROM \"information_schema\".\"TABLES\" "
             + "WHERE LOWER(TABLE_SCHEMA) = 'good' ORDER BY TABLE_NAME")) {
      while (rs.next()) {
        found++;
      }
    }
    assertEquals(3, found, "expected alpha, beta, gamma");
  }

  /**
   * The composite-key query {@code describe_table} runs, over the two constraint tables.
   *
   * <p>Pins the shape that makes a vintage-partitioned table's grain visible: geo.counties keys on
   * (county_fips, year) and stores one copy of every county per TIGER vintage, so a caller that
   * joins on county_fips alone multiplies row counts by the number of years and gets no error.
   * Reporting the declared key is what turns that from a silent fan-out into something a caller
   * can see before choosing a join. Also asserts the constraint scans prune: the broken table in
   * the other schema must not be resolved.
   */
  @Test void primaryKeyLookupReturnsCompositeKeyInOrder() throws Exception {
    AtomicInteger resolutions = new AtomicInteger();
    java.util.List<String> key = new java.util.ArrayList<>();
    try (Connection conn = connect(resolutions);
         Statement st = conn.createStatement();
         ResultSet rs = st.executeQuery(
             "SELECT k.COLUMN_NAME FROM \"information_schema\".\"KEY_COLUMN_USAGE\" k "
             + "JOIN \"information_schema\".\"TABLE_CONSTRAINTS\" tc "
             + "  ON k.CONSTRAINT_NAME = tc.CONSTRAINT_NAME "
             + " AND k.TABLE_SCHEMA = tc.TABLE_SCHEMA "
             + " AND k.TABLE_NAME = tc.TABLE_NAME "
             + "WHERE LOWER(k.TABLE_SCHEMA) = 'keyed' AND LOWER(k.TABLE_NAME) = 'vintaged' "
             + "  AND LOWER(tc.TABLE_SCHEMA) = 'keyed' AND LOWER(tc.TABLE_NAME) = 'vintaged' "
             + "  AND tc.CONSTRAINT_TYPE = 'PRIMARY KEY' "
             + "ORDER BY k.ORDINAL_POSITION")) {
      while (rs.next()) {
        key.add(rs.getString(1));
      }
    }
    assertEquals(java.util.Arrays.asList("ID", "YEAR"), key,
        "the composite key must come back complete and in ordinal order — reporting only ID "
        + "would hide exactly the vintage column that makes the grain non-obvious");
  }

  /**
   * Without a restriction the scan still covers everything, so an unfiltered query over a catalog
   * containing a broken table is expected to fail rather than quietly omit it. Pinned so the
   * pruning is never mistaken for error suppression: the fix narrows what gets touched, it does
   * not swallow failures.
   */
  @Test void unfilteredScanStillSurfacesABrokenTable() throws Exception {
    AtomicInteger resolutions = new AtomicInteger();
    boolean threw = false;
    try (Connection conn = connect(resolutions);
         Statement st = conn.createStatement()) {
      try (ResultSet rs = st.executeQuery(
          "SELECT count(*) FROM \"information_schema\".\"COLUMNS\"")) {
        while (rs.next()) {
          // drain
        }
      } catch (Exception e) {
        threw = true;
      }
    }
    assertTrue(threw,
        "an unresolvable table must still fail a scan that genuinely has to read it");
  }
}
