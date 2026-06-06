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
package org.apache.calcite.adapter.file.statistics;

import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractTable;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit coverage tests for {@link CachePrimer}.
 * Tests internal logic via mocking CalciteConnection and schemas.
 */
@SuppressWarnings("deprecation")
@Tag("unit")
class CachePrimerCoverageTest {

  // ---------------------------------------------------------------
  // TableInfo tests
  // ---------------------------------------------------------------

  @Test void testTableInfoWithFile() {
    File file = new File("/tmp/test.parquet");
    Table table = mock(Table.class);
    CachePrimer.TableInfo info = new CachePrimer.TableInfo("schema1", "table1", table, file);

    assertEquals("schema1", info.schemaName);
    assertEquals("table1", info.tableName);
    assertEquals(table, info.table);
    assertEquals(file, info.file);
  }

  @Test void testTableInfoWithNullFile() {
    Table table = mock(Table.class);
    CachePrimer.TableInfo info = new CachePrimer.TableInfo("schema1", "table1", table, null);

    assertNotNull(info);
    assertEquals(0, info.fileSize);
  }

  @Test void testTableInfoToString() {
    Table table = mock(Table.class);
    CachePrimer.TableInfo info = new CachePrimer.TableInfo("schema1", "table1", table, null);

    String str = info.toString();
    assertTrue(str.contains("schema1.table1"));
    assertTrue(str.contains("MB"));
  }

  @Test void testTableInfoToStringFormat() {
    Table table = mock(Table.class);
    File file = mock(File.class);
    when(file.length()).thenReturn(1048576L); // 1 MB
    CachePrimer.TableInfo info = new CachePrimer.TableInfo("db", "sales", table, file);

    String str = info.toString();
    assertTrue(str.contains("db.sales"));
    assertTrue(str.contains("1.00 MB"));
  }

  @Test void testTableInfoFileSizeFromFile() {
    Table table = mock(Table.class);
    File file = mock(File.class);
    when(file.length()).thenReturn(5242880L); // 5 MB
    CachePrimer.TableInfo info = new CachePrimer.TableInfo("s", "t", table, file);

    assertEquals(5242880L, info.fileSize);
  }

  @Test void testTableInfoNullTable() {
    CachePrimer.TableInfo info = new CachePrimer.TableInfo("s", "t", null, null);
    assertEquals("s", info.schemaName);
    assertEquals("t", info.tableName);
  }

  // ---------------------------------------------------------------
  // PrimingResult tests
  // ---------------------------------------------------------------

  @Test void testPrimingResultBasic() {
    Map<String, Long> timings = new HashMap<>();
    timings.put("table1", 100L);
    List<String> failures = new ArrayList<>();

    CachePrimer.PrimingResult result = new CachePrimer.PrimingResult(
        5, 4, 1, 500L, 1048576L, timings, failures);

    assertEquals(5, result.totalTables);
    assertEquals(4, result.successfullyPrimed);
    assertEquals(1, result.failed);
    assertEquals(500L, result.totalTimeMs);
    assertEquals(1048576L, result.totalBytesProcessed);
    assertEquals(1, result.tableTimings.size());
    assertTrue(result.failures.isEmpty());
  }

  @Test void testPrimingResultWithFailures() {
    Map<String, Long> timings = new HashMap<>();
    List<String> failures = new ArrayList<>();
    failures.add("table_x: Connection refused");
    failures.add("table_y: Timeout");

    CachePrimer.PrimingResult result = new CachePrimer.PrimingResult(
        3, 1, 2, 1000L, 0L, timings, failures);

    assertEquals(2, result.failures.size());
    assertEquals("table_x: Connection refused", result.failures.get(0));
  }

  @Test void testPrimingResultPrintSummary() {
    Map<String, Long> timings = new HashMap<>();
    timings.put("schema.table1 (1.00 MB)", 200L);
    timings.put("schema.table2 (2.00 MB)", 300L);
    List<String> failures = new ArrayList<>();

    CachePrimer.PrimingResult result = new CachePrimer.PrimingResult(
        2, 2, 0, 500L, 3145728L, timings, failures);

    // Should not throw
    result.printSummary();
  }

  @Test void testPrimingResultPrintSummaryWithFailures() {
    Map<String, Long> timings = new HashMap<>();
    List<String> failures = new ArrayList<>();
    failures.add("bad_table: error");

    CachePrimer.PrimingResult result = new CachePrimer.PrimingResult(
        1, 0, 1, 100L, 0L, timings, failures);

    // Should not throw
    result.printSummary();
  }

  @Test void testPrimingResultPrintSummaryZeroSuccess() {
    CachePrimer.PrimingResult result = new CachePrimer.PrimingResult(
        0, 0, 0, 0L, 0L, Collections.emptyMap(), Collections.emptyList());

    // Should not throw even with zero tables
    result.printSummary();
  }

  @Test void testPrimingResultPrintSummaryShowsSlowestTables() {
    Map<String, Long> timings = new HashMap<>();
    for (int i = 0; i < 10; i++) {
      timings.put("table_" + i, (long) (i * 100));
    }
    List<String> failures = new ArrayList<>();

    CachePrimer.PrimingResult result = new CachePrimer.PrimingResult(
        10, 10, 0, 5000L, 10485760L, timings, failures);

    // Should log the top 5 slowest tables
    result.printSummary();
  }

  @Test void testPrimingResultZeroBytesProcessed() {
    CachePrimer.PrimingResult result = new CachePrimer.PrimingResult(
        1, 1, 0, 100L, 0L, Collections.emptyMap(), Collections.emptyList());

    assertEquals(0L, result.totalBytesProcessed);
    result.printSummary();
  }

  @Test void testPrimingResultLargeData() {
    CachePrimer.PrimingResult result = new CachePrimer.PrimingResult(
        100, 99, 1, 60000L, 10737418240L, // 10 GB
        Collections.emptyMap(), Collections.singletonList("one_failure"));

    assertEquals(100, result.totalTables);
    assertEquals(10737418240L, result.totalBytesProcessed);
    result.printSummary();
  }

  // ---------------------------------------------------------------
  // collectTables tests (via reflection)
  // ---------------------------------------------------------------

  @Test void testCollectTablesEmptySchema() throws Exception {
    SchemaPlus schema = mock(SchemaPlus.class);
    when(schema.getTableNames()).thenReturn(Collections.emptySet());

    List<CachePrimer.TableInfo> tables = invokeCollectTables("test_schema", schema);
    assertNotNull(tables);
    assertTrue(tables.isEmpty());
  }

  @Test void testCollectTablesWithGenericTable() throws Exception {
    SchemaPlus schema = mock(SchemaPlus.class);
    Set<String> tableNames = new HashSet<>();
    tableNames.add("generic_table");
    when(schema.getTableNames()).thenReturn(tableNames);

    Table genericTable = mock(AbstractTable.class);
    when(schema.getTable("generic_table")).thenReturn(genericTable);

    List<CachePrimer.TableInfo> tables = invokeCollectTables("test_schema", schema);
    assertEquals(1, tables.size());
    assertEquals("generic_table", tables.get(0).tableName);
    assertEquals("test_schema", tables.get(0).schemaName);
  }

  @Test void testCollectTablesWithMultipleTables() throws Exception {
    SchemaPlus schema = mock(SchemaPlus.class);
    Set<String> tableNames = new HashSet<>(Arrays.asList("t1", "t2", "t3"));
    when(schema.getTableNames()).thenReturn(tableNames);

    for (String name : tableNames) {
      Table table = mock(Table.class);
      when(schema.getTable(name)).thenReturn(table);
    }

    List<CachePrimer.TableInfo> tables = invokeCollectTables("schema", schema);
    assertEquals(3, tables.size());
  }

  @Test void testCollectTablesNullFileForNonParquetTable() throws Exception {
    SchemaPlus schema = mock(SchemaPlus.class);
    Set<String> tableNames = new HashSet<>();
    tableNames.add("csv_table");
    when(schema.getTableNames()).thenReturn(tableNames);

    Table csvTable = mock(Table.class);
    when(schema.getTable("csv_table")).thenReturn(csvTable);

    List<CachePrimer.TableInfo> tables = invokeCollectTables("s", schema);
    assertEquals(1, tables.size());
    assertEquals(0, tables.get(0).fileSize);
  }

  // ---------------------------------------------------------------
  // primeTable tests (via reflection)
  // ---------------------------------------------------------------

  @Test void testPrimeTableWithStatisticsProvider() throws Exception {
    StatisticsTable statsTable = mock(StatisticsTable.class);
    TableStatistics stats = mock(TableStatistics.class);
    when(stats.getRowCount()).thenReturn(1000L);
    when(stats.getColumnStatistics()).thenReturn(Collections.emptyMap());
    when(statsTable.getTableStatistics(null)).thenReturn(stats);

    CachePrimer.TableInfo info = new CachePrimer.TableInfo(
        "schema", "stats_table", statsTable, null);

    invokePrimeTable(mock(Connection.class), info);
    verify(statsTable).getTableStatistics(null);
  }

  @Test void testPrimeTableWithStatisticsProviderNullStats() throws Exception {
    StatisticsTable statsTable = mock(StatisticsTable.class);
    when(statsTable.getTableStatistics(null)).thenReturn(null);

    CachePrimer.TableInfo info = new CachePrimer.TableInfo(
        "schema", "null_stats_table", statsTable, null);

    // Should not throw
    invokePrimeTable(mock(Connection.class), info);
  }

  @Test void testPrimeTableWithRegularTable() throws Exception {
    Table regularTable = mock(Table.class);
    Connection conn = mock(Connection.class);
    Statement stmt = mock(Statement.class);
    ResultSet rs = mock(ResultSet.class);
    ResultSetMetaData metadata = mock(ResultSetMetaData.class);

    when(conn.createStatement()).thenReturn(stmt);
    when(stmt.executeQuery(anyString())).thenReturn(rs);
    when(rs.getMetaData()).thenReturn(metadata);

    CachePrimer.TableInfo info = new CachePrimer.TableInfo(
        "schema", "regular_table", regularTable, null);

    invokePrimeTable(conn, info);
    verify(rs).getMetaData();
  }

  @Test void testPrimeTableWithStatisticsProviderHasStats() throws Exception {
    StatisticsTable statsTable = mock(StatisticsTable.class);
    Map<String, ColumnStatistics> colStats = new HashMap<>();
    colStats.put("col1", mock(ColumnStatistics.class));
    TableStatistics stats = new TableStatistics(500, 1024, colStats, "hash123");
    when(statsTable.getTableStatistics(null)).thenReturn(stats);

    CachePrimer.TableInfo info = new CachePrimer.TableInfo(
        "s", "stats_table_2", statsTable, null);

    invokePrimeTable(mock(Connection.class), info);
    verify(statsTable).getTableStatistics(null);
  }

  // ---------------------------------------------------------------
  // primeTablesInOrder tests (via reflection)
  // ---------------------------------------------------------------

  @Test void testPrimeTablesInOrderEmptyList() throws Exception {
    Connection conn = mock(Connection.class);
    List<CachePrimer.TableInfo> tables = new ArrayList<>();

    CachePrimer.PrimingResult result = invokePrimeTablesInOrder(conn, tables);
    assertNotNull(result);
    assertEquals(0, result.totalTables);
    assertEquals(0, result.successfullyPrimed);
    assertEquals(0, result.failed);
  }

  @Test void testPrimeTablesInOrderWithFailure() throws Exception {
    Connection conn = mock(Connection.class);
    when(conn.createStatement()).thenThrow(new SQLException("Connection lost"));

    Table table = mock(Table.class);
    CachePrimer.TableInfo info = new CachePrimer.TableInfo("s", "t", table, null);
    List<CachePrimer.TableInfo> tables = new ArrayList<>();
    tables.add(info);

    CachePrimer.PrimingResult result = invokePrimeTablesInOrder(conn, tables);
    assertEquals(1, result.totalTables);
    assertEquals(0, result.successfullyPrimed);
    assertEquals(1, result.failed);
    assertEquals(1, result.failures.size());
  }

  @Test void testPrimeTablesInOrderWithSuccessAndFailure() throws Exception {
    Connection conn = mock(Connection.class);
    Statement stmt = mock(Statement.class);
    ResultSet rs = mock(ResultSet.class);
    ResultSetMetaData metadata = mock(ResultSetMetaData.class);

    when(conn.createStatement()).thenReturn(stmt);
    when(stmt.executeQuery(anyString()))
        .thenReturn(rs)
        .thenThrow(new SQLException("Query failed"));
    when(rs.getMetaData()).thenReturn(metadata);

    Table t1 = mock(Table.class);
    Table t2 = mock(Table.class);

    List<CachePrimer.TableInfo> tables = new ArrayList<>();
    tables.add(new CachePrimer.TableInfo("s", "good_table", t1, null));
    tables.add(new CachePrimer.TableInfo("s", "bad_table", t2, null));

    CachePrimer.PrimingResult result = invokePrimeTablesInOrder(conn, tables);
    assertEquals(2, result.totalTables);
    assertEquals(1, result.successfullyPrimed);
    assertEquals(1, result.failed);
  }

  @Test void testPrimeTablesInOrderAllSuccess() throws Exception {
    Connection conn = mock(Connection.class);
    Statement stmt = mock(Statement.class);
    ResultSet rs = mock(ResultSet.class);
    ResultSetMetaData metadata = mock(ResultSetMetaData.class);

    when(conn.createStatement()).thenReturn(stmt);
    when(stmt.executeQuery(anyString())).thenReturn(rs);
    when(rs.getMetaData()).thenReturn(metadata);

    List<CachePrimer.TableInfo> tables = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      tables.add(new CachePrimer.TableInfo("s", "table_" + i, mock(Table.class), null));
    }

    CachePrimer.PrimingResult result = invokePrimeTablesInOrder(conn, tables);
    assertEquals(5, result.totalTables);
    assertEquals(5, result.successfullyPrimed);
    assertEquals(0, result.failed);
    assertTrue(result.failures.isEmpty());
  }

  // ---------------------------------------------------------------
  // primeSchema tests
  // ---------------------------------------------------------------

  @Test void testPrimeSchemaThrowsForMissingSchema() throws Exception {
    Connection conn = mock(Connection.class);
    CalciteConnection calciteConn = mock(CalciteConnection.class);
    SchemaPlus rootSchema = mock(SchemaPlus.class);

    when(conn.unwrap(CalciteConnection.class)).thenReturn(calciteConn);
    when(calciteConn.getRootSchema()).thenReturn(rootSchema);
    when(rootSchema.getSubSchema("missing")).thenReturn(null);

    assertThrows(IllegalArgumentException.class,
        () -> CachePrimer.primeSchema(conn, "missing"));
  }

  @Test void testPrimeSchemaEmptySchema() throws Exception {
    Connection conn = mock(Connection.class);
    CalciteConnection calciteConn = mock(CalciteConnection.class);
    SchemaPlus rootSchema = mock(SchemaPlus.class);
    SchemaPlus subSchema = mock(SchemaPlus.class);

    when(conn.unwrap(CalciteConnection.class)).thenReturn(calciteConn);
    when(calciteConn.getRootSchema()).thenReturn(rootSchema);
    when(rootSchema.getSubSchema("empty")).thenReturn(subSchema);
    when(subSchema.getTableNames()).thenReturn(Collections.emptySet());

    CachePrimer.PrimingResult result = CachePrimer.primeSchema(conn, "empty");
    assertNotNull(result);
    assertEquals(0, result.totalTables);
  }

  // ---------------------------------------------------------------
  // primeSchemas tests
  // ---------------------------------------------------------------

  @Test void testPrimeSchemasMultipleSchemas() throws Exception {
    Connection conn = mock(Connection.class);
    CalciteConnection calciteConn = mock(CalciteConnection.class);
    SchemaPlus rootSchema = mock(SchemaPlus.class);

    SchemaPlus schema1 = mock(SchemaPlus.class);
    SchemaPlus schema2 = mock(SchemaPlus.class);

    when(conn.unwrap(CalciteConnection.class)).thenReturn(calciteConn);
    when(calciteConn.getRootSchema()).thenReturn(rootSchema);
    when(rootSchema.getSubSchema("s1")).thenReturn(schema1);
    when(rootSchema.getSubSchema("s2")).thenReturn(schema2);
    when(schema1.getTableNames()).thenReturn(Collections.emptySet());
    when(schema2.getTableNames()).thenReturn(Collections.emptySet());

    CachePrimer.PrimingResult result = CachePrimer.primeSchemas(conn, "s1", "s2");
    assertNotNull(result);
    assertEquals(0, result.totalTables);
  }

  @Test void testPrimeSchemasSkipsNullSchema() throws Exception {
    Connection conn = mock(Connection.class);
    CalciteConnection calciteConn = mock(CalciteConnection.class);
    SchemaPlus rootSchema = mock(SchemaPlus.class);
    SchemaPlus existsSchema = mock(SchemaPlus.class);

    when(conn.unwrap(CalciteConnection.class)).thenReturn(calciteConn);
    when(calciteConn.getRootSchema()).thenReturn(rootSchema);
    when(rootSchema.getSubSchema("exists")).thenReturn(existsSchema);
    when(rootSchema.getSubSchema("missing")).thenReturn(null);
    when(existsSchema.getTableNames()).thenReturn(Collections.emptySet());

    CachePrimer.PrimingResult result = CachePrimer.primeSchemas(conn, "exists", "missing");
    assertNotNull(result);
  }

  @Test void testPrimeSchemasEmptyArray() throws Exception {
    Connection conn = mock(Connection.class);
    CalciteConnection calciteConn = mock(CalciteConnection.class);
    SchemaPlus rootSchema = mock(SchemaPlus.class);

    when(conn.unwrap(CalciteConnection.class)).thenReturn(calciteConn);
    when(calciteConn.getRootSchema()).thenReturn(rootSchema);

    CachePrimer.PrimingResult result = CachePrimer.primeSchemas(conn);
    assertNotNull(result);
    assertEquals(0, result.totalTables);
  }

  // ---------------------------------------------------------------
  // primeTablesParallel tests
  // ---------------------------------------------------------------

  @Test void testPrimeTablesParallelEmptyList() throws Exception {
    Connection conn = mock(Connection.class);
    List<CachePrimer.TableInfo> tables = new ArrayList<>();

    CachePrimer.PrimingResult result = CachePrimer.primeTablesParallel(conn, tables, 2);
    assertNotNull(result);
    assertEquals(0, result.totalTables);
    assertEquals(0, result.successfullyPrimed);
  }

  @Test void testPrimeTablesParallelSingleTable() throws Exception {
    Connection conn = mock(Connection.class);
    Statement stmt = mock(Statement.class);
    ResultSet rs = mock(ResultSet.class);
    ResultSetMetaData metadata = mock(ResultSetMetaData.class);

    when(conn.createStatement()).thenReturn(stmt);
    when(stmt.executeQuery(anyString())).thenReturn(rs);
    when(rs.getMetaData()).thenReturn(metadata);

    Table table = mock(Table.class);
    CachePrimer.TableInfo info = new CachePrimer.TableInfo("s", "t", table, null);
    List<CachePrimer.TableInfo> tables = new ArrayList<>();
    tables.add(info);

    CachePrimer.PrimingResult result = CachePrimer.primeTablesParallel(conn, tables, 1);
    assertEquals(1, result.totalTables);
    assertEquals(1, result.successfullyPrimed);
  }

  // ---------------------------------------------------------------
  // primeForTesting tests
  // ---------------------------------------------------------------

  @Test void testPrimeForTestingWithInvalidUrl() {
    CachePrimer.PrimingResult result = CachePrimer.primeForTesting(
        "jdbc:invalid://localhost/db", "test_schema");

    assertNotNull(result);
    assertEquals(0, result.totalTables);
    assertEquals(0, result.successfullyPrimed);
    assertTrue(result.failures.size() > 0);
  }

  // ---------------------------------------------------------------
  // clearAllCaches tests
  // ---------------------------------------------------------------

  @Test void testClearAllCachesEmptySchemas() throws Exception {
    Connection conn = mock(Connection.class);
    CalciteConnection calciteConn = mock(CalciteConnection.class);
    SchemaPlus rootSchema = mock(SchemaPlus.class);

    when(conn.unwrap(CalciteConnection.class)).thenReturn(calciteConn);
    when(calciteConn.getRootSchema()).thenReturn(rootSchema);
    when(rootSchema.getSubSchemaNames()).thenReturn(Collections.emptySet());

    // Should not throw
    CachePrimer.clearAllCaches(conn);
  }

  @Test void testClearAllCachesWithSchemaContainingNonParquetTables() throws Exception {
    Connection conn = mock(Connection.class);
    CalciteConnection calciteConn = mock(CalciteConnection.class);
    SchemaPlus rootSchema = mock(SchemaPlus.class);
    SchemaPlus subSchema = mock(SchemaPlus.class);

    when(conn.unwrap(CalciteConnection.class)).thenReturn(calciteConn);
    when(calciteConn.getRootSchema()).thenReturn(rootSchema);
    Set<String> schemaNames = new HashSet<>();
    schemaNames.add("testSchema");
    when(rootSchema.getSubSchemaNames()).thenReturn(schemaNames);
    when(rootSchema.getSubSchema("testSchema")).thenReturn(subSchema);

    Set<String> tableNames = new HashSet<>();
    tableNames.add("regularTable");
    when(subSchema.getTableNames()).thenReturn(tableNames);
    when(subSchema.getTable("regularTable")).thenReturn(mock(Table.class));

    // Should not throw even for non-Parquet tables
    CachePrimer.clearAllCaches(conn);
  }

  @Test void testClearAllCachesWithMultipleSchemas() throws Exception {
    Connection conn = mock(Connection.class);
    CalciteConnection calciteConn = mock(CalciteConnection.class);
    SchemaPlus rootSchema = mock(SchemaPlus.class);

    when(conn.unwrap(CalciteConnection.class)).thenReturn(calciteConn);
    when(calciteConn.getRootSchema()).thenReturn(rootSchema);

    Set<String> schemaNames = new HashSet<>(Arrays.asList("schema_a", "schema_b"));
    when(rootSchema.getSubSchemaNames()).thenReturn(schemaNames);

    for (String name : schemaNames) {
      SchemaPlus sub = mock(SchemaPlus.class);
      when(rootSchema.getSubSchema(name)).thenReturn(sub);
      when(sub.getTableNames()).thenReturn(Collections.emptySet());
    }

    CachePrimer.clearAllCaches(conn);
  }

  // ---------------------------------------------------------------
  // Interface for dual-role mock (Table + StatisticsProvider)
  // ---------------------------------------------------------------

  /** Helper interface so Mockito can mock both Table and StatisticsProvider. */
  interface StatisticsTable extends Table, StatisticsProvider {
  }

  // ---------------------------------------------------------------
  // Reflection helper methods
  // ---------------------------------------------------------------

  @SuppressWarnings("unchecked")
  private List<CachePrimer.TableInfo> invokeCollectTables(String schemaName, SchemaPlus schema)
      throws Exception {
    Method method = CachePrimer.class.getDeclaredMethod(
        "collectTables", String.class, SchemaPlus.class);
    method.setAccessible(true);
    return (List<CachePrimer.TableInfo>) method.invoke(null, schemaName, schema);
  }

  private void invokePrimeTable(Connection conn, CachePrimer.TableInfo tableInfo)
      throws Exception {
    Method method = CachePrimer.class.getDeclaredMethod(
        "primeTable", Connection.class, CachePrimer.TableInfo.class);
    method.setAccessible(true);
    method.invoke(null, conn, tableInfo);
  }

  private CachePrimer.PrimingResult invokePrimeTablesInOrder(Connection conn,
      List<CachePrimer.TableInfo> tables) throws Exception {
    Method method = CachePrimer.class.getDeclaredMethod(
        "primeTablesInOrder", Connection.class, List.class);
    method.setAccessible(true);
    return (CachePrimer.PrimingResult) method.invoke(null, conn, tables);
  }
}
