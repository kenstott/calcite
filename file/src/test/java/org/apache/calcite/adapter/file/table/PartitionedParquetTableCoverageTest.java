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
package org.apache.calcite.adapter.file.table;

import org.apache.calcite.adapter.file.execution.ExecutionEngineConfig;
import org.apache.calcite.adapter.file.partition.PartitionDetector;
import org.apache.calcite.adapter.file.partition.PartitionedTableConfig;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.sql.type.SqlTypeName;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Coverage tests for {@link PartitionedParquetTable} focusing on constructors,
 * getRowType, getFilePaths, comments, partition handling, statistics provider,
 * Parquet type mapping, and partition pruning.
 */
@Tag("unit")
public class PartitionedParquetTableCoverageTest {

  @TempDir
  Path tempDir;

  private ExecutionEngineConfig defaultEngineConfig;

  @BeforeEach
  void setUp() {
    defaultEngineConfig = new ExecutionEngineConfig("PARQUET", 2048);
  }

  // ---------------------------------------------------------------------------
  // Helper to create temp parquet files using DuckDB
  // ---------------------------------------------------------------------------

  private String createParquetFile(String fileName, String selectSql) {
    File file = tempDir.resolve(fileName).toFile();
    try {
      ProcessBuilder pb = new ProcessBuilder("duckdb", "-c",
          "COPY (" + selectSql + ") TO '" + file.getAbsolutePath() + "' (FORMAT PARQUET)");
      pb.redirectErrorStream(true);
      Process p = pb.start();
      int exitCode = p.waitFor();
      if (exitCode != 0) {
        return null;
      }
    } catch (Exception e) {
      return null;
    }
    return file.getAbsolutePath();
  }

  private String createSimpleParquetFile(String fileName) {
    return createParquetFile(fileName,
        "SELECT 1 AS id, 'Alice' AS name, 100.0 AS amount "
            + "UNION ALL SELECT 2, 'Bob', 200.0 "
            + "UNION ALL SELECT 3, 'Charlie', 300.0");
  }

  private PartitionedTableConfig mockConfig(String tableComment,
      Map<String, String> columnComments) {
    Map<String, Object> configMap = new HashMap<String, Object>();
    if (tableComment != null) {
      configMap.put("comment", tableComment);
    }
    if (columnComments != null) {
      List<Map<String, String>> colCommentsList = new ArrayList<Map<String, String>>();
      for (Map.Entry<String, String> entry : columnComments.entrySet()) {
        Map<String, String> item = new HashMap<String, String>();
        item.put("name", entry.getKey());
        item.put("comment", entry.getValue());
        colCommentsList.add(item);
      }
      configMap.put("column_comments", colCommentsList);
    }
    return PartitionedTableConfig.fromMap(configMap);
  }

  // ====================================================================
  // Constructor tests (various overloads)
  // ====================================================================

  @Test void testThreeArgConstructor() {
    List<String> files = Collections.emptyList();
    PartitionedParquetTable table = new PartitionedParquetTable(
        files, null, defaultEngineConfig);
    assertNotNull(table);
    assertTrue(table.getFilePaths().isEmpty());
  }

  @Test void testFourArgConstructor() {
    Map<String, String> types = new HashMap<String, String>();
    types.put("year", "INTEGER");
    PartitionedParquetTable table = new PartitionedParquetTable(
        Collections.emptyList(), null, defaultEngineConfig, types);
    assertNotNull(table);
  }

  @Test void testSixArgConstructor() {
    PartitionedParquetTable table = new PartitionedParquetTable(
        Collections.emptyList(), null, defaultEngineConfig, null, null, null);
    assertNotNull(table);
  }

  @Test void testSevenArgConstructor() {
    Map<String, Object> constraints = new HashMap<String, Object>();
    constraints.put("primaryKey", Arrays.asList("id"));
    PartitionedParquetTable table = new PartitionedParquetTable(
        Collections.emptyList(), null, defaultEngineConfig,
        null, null, null, constraints);
    assertNotNull(table);
  }

  @Test void testNineArgConstructor() {
    PartitionedParquetTable table = new PartitionedParquetTable(
        Collections.emptyList(), null, defaultEngineConfig,
        null, null, null, null, "schema", "table");
    assertNotNull(table);
  }

  @Test void testTenArgConstructor() {
    PartitionedParquetTable table = new PartitionedParquetTable(
        Collections.emptyList(), null, defaultEngineConfig,
        null, null, null, null, "schema", "table", null);
    assertNotNull(table);
  }

  @Test void testFullConstructorWithPartitionInfo() {
    List<String> partCols = Arrays.asList("year", "month");
    PartitionDetector.PartitionInfo info = new PartitionDetector.PartitionInfo(
        new LinkedHashMap<String, String>(), partCols, true);
    Map<String, String> colTypes = new HashMap<String, String>();
    colTypes.put("year", "INTEGER");
    colTypes.put("month", "VARCHAR");

    PartitionedParquetTable table = new PartitionedParquetTable(
        Collections.emptyList(), info, defaultEngineConfig, colTypes,
        null, null, null, null, "test_table", null, null);
    assertNotNull(table);
  }

  @Test void testConstructorWithCustomRegex() {
    String regex = "data_(\\d{4})_(\\d{2}).parquet";
    List<PartitionedTableConfig.ColumnMapping> mappings = Arrays.asList(
        new PartitionedTableConfig.ColumnMapping("year", 1, "INTEGER"),
        new PartitionedTableConfig.ColumnMapping("month", 2, "INTEGER"));
    PartitionedParquetTable table = new PartitionedParquetTable(
        Collections.singletonList("/a.parquet"), null, defaultEngineConfig,
        null, regex, mappings);
    assertNotNull(table);
  }

  @Test void testConstructorWithConfig() {
    PartitionedTableConfig config = new PartitionedTableConfig(
        "test_table", "*.parquet", "partitioned", null);
    PartitionedParquetTable table = new PartitionedParquetTable(
        Collections.emptyList(), null, defaultEngineConfig,
        null, null, null, null, "s", "t", null, config);
    assertNotNull(table);
  }

  @Test void testConstructorWithNullPartitionColumns() {
    PartitionDetector.PartitionInfo info = new PartitionDetector.PartitionInfo(
        new LinkedHashMap<String, String>(), null, false);
    PartitionedParquetTable table = new PartitionedParquetTable(
        Collections.emptyList(), info, defaultEngineConfig);
    assertNotNull(table);
  }

  @Test void testConstructorWithAllNullOptionals() {
    PartitionedParquetTable table = new PartitionedParquetTable(
        Collections.singletonList("/a.parquet"), null, defaultEngineConfig,
        null, null, null, null, null, null, null, null);
    assertNotNull(table);
    assertNull(table.getTableComment());
  }

  // ====================================================================
  // getFilePaths tests
  // ====================================================================

  @Test void testGetFilePathsReturnsList() {
    List<String> files = Arrays.asList("/p1.parquet", "/p2.parquet");
    PartitionedParquetTable table = new PartitionedParquetTable(
        files, null, defaultEngineConfig);
    assertEquals(2, table.getFilePaths().size());
    assertEquals("/p1.parquet", table.getFilePaths().get(0));
  }

  @Test void testGetFilePathsEmptyList() {
    PartitionedParquetTable table = new PartitionedParquetTable(
        Collections.emptyList(), null, defaultEngineConfig);
    assertTrue(table.getFilePaths().isEmpty());
  }

  @Test void testGetFilePathsSingleFile() {
    List<String> files = Collections.singletonList("/single.parquet");
    PartitionedParquetTable table = new PartitionedParquetTable(
        files, null, defaultEngineConfig);
    assertEquals(1, table.getFilePaths().size());
  }

  // ====================================================================
  // Row type with real Parquet files
  // ====================================================================

  @Test void testGetRowTypeWithRealParquetFile() {
    String path = createSimpleParquetFile("simple.parquet");
    if (path == null) {
      return; // DuckDB not available
    }
    PartitionedParquetTable table = new PartitionedParquetTable(
        Collections.singletonList(path), null, defaultEngineConfig);
    RelDataTypeFactory tf = new org.apache.calcite.jdbc.JavaTypeFactoryImpl();
    RelDataType rowType = table.getRowType(tf);
    assertNotNull(rowType);
    assertTrue(rowType.getFieldCount() >= 3);
  }

  @Test void testGetRowTypePartitionColumnAdded() {
    String path = createSimpleParquetFile("part_added.parquet");
    if (path == null) {
      return;
    }
    PartitionDetector.PartitionInfo info = new PartitionDetector.PartitionInfo(
        new LinkedHashMap<String, String>(), Arrays.asList("region"), true);
    PartitionedParquetTable table = new PartitionedParquetTable(
        Collections.singletonList(path), info, defaultEngineConfig);

    RelDataTypeFactory tf = new org.apache.calcite.jdbc.JavaTypeFactoryImpl();
    RelDataType rowType = table.getRowType(tf);
    boolean found = false;
    for (RelDataTypeField f : rowType.getFieldList()) {
      if (f.getName().equals("region")) {
        found = true;
        assertEquals(SqlTypeName.VARCHAR, f.getType().getSqlTypeName());
      }
    }
    assertTrue(found, "region partition column not found");
  }

  @Test void testGetRowTypePartitionColumnInteger() {
    String path = createSimpleParquetFile("int_part.parquet");
    if (path == null) {
      return;
    }
    PartitionDetector.PartitionInfo info = new PartitionDetector.PartitionInfo(
        new LinkedHashMap<String, String>(), Arrays.asList("year"), true);
    Map<String, String> types = new HashMap<String, String>();
    types.put("year", "INTEGER");
    PartitionedParquetTable table = new PartitionedParquetTable(
        Collections.singletonList(path), info, defaultEngineConfig, types);
    RelDataTypeFactory tf = new org.apache.calcite.jdbc.JavaTypeFactoryImpl();
    RelDataType rowType = table.getRowType(tf);
    boolean found = false;
    for (RelDataTypeField f : rowType.getFieldList()) {
      if (f.getName().equals("year")) {
        assertEquals(SqlTypeName.INTEGER, f.getType().getSqlTypeName());
        found = true;
      }
    }
    assertTrue(found);
  }

  @Test void testGetRowTypePartitionColumnBigint() {
    String path = createSimpleParquetFile("bigint_part.parquet");
    if (path == null) {
      return;
    }
    PartitionDetector.PartitionInfo info = new PartitionDetector.PartitionInfo(
        new LinkedHashMap<String, String>(), Arrays.asList("ts"), true);
    Map<String, String> types = new HashMap<String, String>();
    types.put("ts", "BIGINT");
    PartitionedParquetTable table = new PartitionedParquetTable(
        Collections.singletonList(path), info, defaultEngineConfig, types);
    RelDataTypeFactory tf = new org.apache.calcite.jdbc.JavaTypeFactoryImpl();
    RelDataType rowType = table.getRowType(tf);
    boolean found = false;
    for (RelDataTypeField f : rowType.getFieldList()) {
      if (f.getName().equals("ts")) {
        assertEquals(SqlTypeName.BIGINT, f.getType().getSqlTypeName());
        found = true;
      }
    }
    assertTrue(found);
  }

  @Test void testGetRowTypePartitionColumnDouble() {
    String path = createSimpleParquetFile("dbl_part.parquet");
    if (path == null) {
      return;
    }
    PartitionDetector.PartitionInfo info = new PartitionDetector.PartitionInfo(
        new LinkedHashMap<String, String>(), Arrays.asList("score"), true);
    Map<String, String> types = new HashMap<String, String>();
    types.put("score", "DOUBLE");
    PartitionedParquetTable table = new PartitionedParquetTable(
        Collections.singletonList(path), info, defaultEngineConfig, types);
    RelDataTypeFactory tf = new org.apache.calcite.jdbc.JavaTypeFactoryImpl();
    RelDataType rowType = table.getRowType(tf);
    boolean found = false;
    for (RelDataTypeField f : rowType.getFieldList()) {
      if (f.getName().equals("score")) {
        assertEquals(SqlTypeName.DOUBLE, f.getType().getSqlTypeName());
        found = true;
      }
    }
    assertTrue(found);
  }

  @Test void testGetRowTypePartitionColumnBoolean() {
    String path = createSimpleParquetFile("bool_part.parquet");
    if (path == null) {
      return;
    }
    PartitionDetector.PartitionInfo info = new PartitionDetector.PartitionInfo(
        new LinkedHashMap<String, String>(), Arrays.asList("active"), true);
    Map<String, String> types = new HashMap<String, String>();
    types.put("active", "BOOLEAN");
    PartitionedParquetTable table = new PartitionedParquetTable(
        Collections.singletonList(path), info, defaultEngineConfig, types);
    RelDataTypeFactory tf = new org.apache.calcite.jdbc.JavaTypeFactoryImpl();
    RelDataType rowType = table.getRowType(tf);
    boolean found = false;
    for (RelDataTypeField f : rowType.getFieldList()) {
      if (f.getName().equals("active")) {
        assertEquals(SqlTypeName.BOOLEAN, f.getType().getSqlTypeName());
        found = true;
      }
    }
    assertTrue(found);
  }

  @Test void testGetRowTypePartitionColumnSmallint() {
    String path = createSimpleParquetFile("si_part.parquet");
    if (path == null) {
      return;
    }
    PartitionDetector.PartitionInfo info = new PartitionDetector.PartitionInfo(
        new LinkedHashMap<String, String>(), Arrays.asList("code"), true);
    Map<String, String> types = new HashMap<String, String>();
    types.put("code", "SMALLINT");
    PartitionedParquetTable table = new PartitionedParquetTable(
        Collections.singletonList(path), info, defaultEngineConfig, types);
    RelDataTypeFactory tf = new org.apache.calcite.jdbc.JavaTypeFactoryImpl();
    RelDataType rowType = table.getRowType(tf);
    boolean found = false;
    for (RelDataTypeField f : rowType.getFieldList()) {
      if (f.getName().equals("code")) {
        assertEquals(SqlTypeName.SMALLINT, f.getType().getSqlTypeName());
        found = true;
      }
    }
    assertTrue(found);
  }

  @Test void testGetRowTypeInvalidPartitionType() {
    String path = createSimpleParquetFile("inv_type.parquet");
    if (path == null) {
      return;
    }
    PartitionDetector.PartitionInfo info = new PartitionDetector.PartitionInfo(
        new LinkedHashMap<String, String>(), Arrays.asList("region"), true);
    Map<String, String> types = new HashMap<String, String>();
    types.put("region", "NONEXISTENT_TYPE");
    PartitionedParquetTable table = new PartitionedParquetTable(
        Collections.singletonList(path), info, defaultEngineConfig, types);
    RelDataTypeFactory tf = new org.apache.calcite.jdbc.JavaTypeFactoryImpl();
    RelDataType rowType = table.getRowType(tf);
    boolean found = false;
    for (RelDataTypeField f : rowType.getFieldList()) {
      if (f.getName().equals("region")) {
        assertEquals(SqlTypeName.VARCHAR, f.getType().getSqlTypeName());
        found = true;
      }
    }
    assertTrue(found, "Should fallback to VARCHAR for invalid type");
  }

  @Test void testGetRowTypeEmptyFileList() {
    PartitionedParquetTable table = new PartitionedParquetTable(
        Collections.emptyList(), null, defaultEngineConfig);
    RelDataTypeFactory tf = new org.apache.calcite.jdbc.JavaTypeFactoryImpl();
    RelDataType rowType = table.getRowType(tf);
    assertNotNull(rowType);
    assertEquals(0, rowType.getFieldCount());
  }

  @Test void testGetRowTypeMultiplePartitionColumns() {
    String path = createSimpleParquetFile("multi_part.parquet");
    if (path == null) {
      return;
    }
    PartitionDetector.PartitionInfo info = new PartitionDetector.PartitionInfo(
        new LinkedHashMap<String, String>(),
        Arrays.asList("year", "month", "day"), true);
    Map<String, String> types = new HashMap<String, String>();
    types.put("year", "INTEGER");
    types.put("month", "INTEGER");
    types.put("day", "INTEGER");
    PartitionedParquetTable table = new PartitionedParquetTable(
        Collections.singletonList(path), info, defaultEngineConfig, types);
    RelDataTypeFactory tf = new org.apache.calcite.jdbc.JavaTypeFactoryImpl();
    RelDataType rowType = table.getRowType(tf);
    // 3 file cols + 3 partition cols
    assertTrue(rowType.getFieldCount() >= 6);
  }

  @Test void testPartitionColumnsAreNullable() {
    String path = createSimpleParquetFile("nullable_part.parquet");
    if (path == null) {
      return;
    }
    PartitionDetector.PartitionInfo info = new PartitionDetector.PartitionInfo(
        new LinkedHashMap<String, String>(), Arrays.asList("region"), true);
    PartitionedParquetTable table = new PartitionedParquetTable(
        Collections.singletonList(path), info, defaultEngineConfig);
    RelDataTypeFactory tf = new org.apache.calcite.jdbc.JavaTypeFactoryImpl();
    RelDataType rowType = table.getRowType(tf);
    for (RelDataTypeField f : rowType.getFieldList()) {
      if (f.getName().equals("region")) {
        assertTrue(f.getType().isNullable(), "Partition column must be nullable");
      }
    }
  }

  // ====================================================================
  // Statistic tests
  // ====================================================================

  @Test void testGetStatisticWithNoConstraints() {
    List<String> files = Arrays.asList("/a.parquet", "/b.parquet");
    PartitionedParquetTable table = new PartitionedParquetTable(
        files, null, defaultEngineConfig);
    Statistic stat = table.getStatistic();
    assertNotNull(stat);
    assertTrue(stat.getRowCount() > 0);
  }

  @Test void testGetStatisticEmptyFiles() {
    PartitionedParquetTable table = new PartitionedParquetTable(
        Collections.emptyList(), null, defaultEngineConfig);
    Statistic stat = table.getStatistic();
    assertNotNull(stat);
  }

  @Test void testGetStatisticSingleFile() {
    PartitionedParquetTable table = new PartitionedParquetTable(
        Collections.singletonList("/a.parquet"), null, defaultEngineConfig);
    Statistic stat = table.getStatistic();
    assertNotNull(stat);
    // 1 file * (5000 + 100) = 5100
    assertEquals(5100.0, stat.getRowCount(), 0.01);
  }

  @Test void testGetStatisticNullConstraintConfig() {
    PartitionedParquetTable table = new PartitionedParquetTable(
        Collections.singletonList("/a.parquet"), null, defaultEngineConfig,
        null, null, null, null);
    Statistic stat = table.getStatistic();
    assertNotNull(stat);
  }

  @Test void testGetStatisticWithConstraints() {
    String path = createSimpleParquetFile("constraint_stat.parquet");
    if (path == null) {
      return;
    }
    Map<String, Object> constraints = new LinkedHashMap<String, Object>();
    Map<String, Object> pk = new LinkedHashMap<String, Object>();
    pk.put("columns", Arrays.asList("id"));
    constraints.put("primaryKey", pk);

    PartitionedParquetTable table = new PartitionedParquetTable(
        Collections.singletonList(path), null, defaultEngineConfig,
        null, null, null, constraints, "schema", "table");
    Statistic stat = table.getStatistic();
    assertNotNull(stat);
  }

  // ====================================================================
  // estimateRowCount via reflection
  // ====================================================================

  @Test void testEstimateRowCountThreeFiles() throws Exception {
    PartitionedParquetTable table = new PartitionedParquetTable(
        Arrays.asList("/a.parquet", "/b.parquet", "/c.parquet"),
        null, defaultEngineConfig);
    Method m = PartitionedParquetTable.class.getDeclaredMethod("estimateRowCount");
    m.setAccessible(true);
    double estimate = (Double) m.invoke(table);
    assertEquals(15300.0, estimate, 0.01);
  }

  @Test void testEstimateRowCountEmpty() throws Exception {
    PartitionedParquetTable table = new PartitionedParquetTable(
        Collections.emptyList(), null, defaultEngineConfig);
    Method m = PartitionedParquetTable.class.getDeclaredMethod("estimateRowCount");
    m.setAccessible(true);
    double estimate = (Double) m.invoke(table);
    assertEquals(0.0, estimate, 0.01);
  }

  @Test void testEstimateRowCountSingleFile() throws Exception {
    PartitionedParquetTable table = new PartitionedParquetTable(
        Collections.singletonList("/a.parquet"), null, defaultEngineConfig);
    Method m = PartitionedParquetTable.class.getDeclaredMethod("estimateRowCount");
    m.setAccessible(true);
    double estimate = (Double) m.invoke(table);
    assertEquals(5100.0, estimate, 0.01);
  }

  @Test void testEstimateRowCountTenFiles() throws Exception {
    List<String> files = new ArrayList<String>();
    for (int i = 0; i < 10; i++) {
      files.add("/f" + i + ".parquet");
    }
    PartitionedParquetTable table = new PartitionedParquetTable(
        files, null, defaultEngineConfig);
    Method m = PartitionedParquetTable.class.getDeclaredMethod("estimateRowCount");
    m.setAccessible(true);
    double estimate = (Double) m.invoke(table);
    assertEquals(51000.0, estimate, 0.01);
  }

  // ====================================================================
  // containsField via reflection
  // ====================================================================

  @Test void testContainsFieldTrue() throws Exception {
    String path = createSimpleParquetFile("cf_true.parquet");
    if (path == null) {
      return;
    }
    PartitionedParquetTable table = new PartitionedParquetTable(
        Collections.singletonList(path), null, defaultEngineConfig);
    RelDataTypeFactory tf = new org.apache.calcite.jdbc.JavaTypeFactoryImpl();
    RelDataType rowType = table.getRowType(tf);

    Method m = PartitionedParquetTable.class.getDeclaredMethod(
        "containsField", RelDataType.class, String.class);
    m.setAccessible(true);
    assertTrue((Boolean) m.invoke(table, rowType, "id"));
    assertTrue((Boolean) m.invoke(table, rowType, "name"));
  }

  @Test void testContainsFieldFalse() throws Exception {
    String path = createSimpleParquetFile("cf_false.parquet");
    if (path == null) {
      return;
    }
    PartitionedParquetTable table = new PartitionedParquetTable(
        Collections.singletonList(path), null, defaultEngineConfig);
    RelDataTypeFactory tf = new org.apache.calcite.jdbc.JavaTypeFactoryImpl();
    RelDataType rowType = table.getRowType(tf);

    Method m = PartitionedParquetTable.class.getDeclaredMethod(
        "containsField", RelDataType.class, String.class);
    m.setAccessible(true);
    assertFalse((Boolean) m.invoke(table, rowType, "nonexistent"));
  }

  @Test void testContainsFieldCaseInsensitive() throws Exception {
    String path = createSimpleParquetFile("cf_case.parquet");
    if (path == null) {
      return;
    }
    PartitionedParquetTable table = new PartitionedParquetTable(
        Collections.singletonList(path), null, defaultEngineConfig);
    RelDataTypeFactory tf = new org.apache.calcite.jdbc.JavaTypeFactoryImpl();
    RelDataType rowType = table.getRowType(tf);

    Method m = PartitionedParquetTable.class.getDeclaredMethod(
        "containsField", RelDataType.class, String.class);
    m.setAccessible(true);
    assertTrue((Boolean) m.invoke(table, rowType, "ID"));
    assertTrue((Boolean) m.invoke(table, rowType, "Name"));
  }

  // ====================================================================
  // mapParquetTypeToSqlType via reflection
  // ====================================================================

  @Test void testMapParquetInt32() throws Exception {
    PartitionedParquetTable table = new PartitionedParquetTable(
        Collections.emptyList(), null, defaultEngineConfig);
    Method m = PartitionedParquetTable.class.getDeclaredMethod(
        "mapParquetTypeToSqlType", org.apache.parquet.schema.Type.class);
    m.setAccessible(true);
    org.apache.parquet.schema.Type t = org.apache.parquet.schema.Types.required(
        org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32).named("c");
    assertEquals(SqlTypeName.INTEGER, m.invoke(table, t));
  }

  @Test void testMapParquetInt64() throws Exception {
    PartitionedParquetTable table = new PartitionedParquetTable(
        Collections.emptyList(), null, defaultEngineConfig);
    Method m = PartitionedParquetTable.class.getDeclaredMethod(
        "mapParquetTypeToSqlType", org.apache.parquet.schema.Type.class);
    m.setAccessible(true);
    org.apache.parquet.schema.Type t = org.apache.parquet.schema.Types.required(
        org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64).named("c");
    assertEquals(SqlTypeName.BIGINT, m.invoke(table, t));
  }

  @Test void testMapParquetDouble() throws Exception {
    PartitionedParquetTable table = new PartitionedParquetTable(
        Collections.emptyList(), null, defaultEngineConfig);
    Method m = PartitionedParquetTable.class.getDeclaredMethod(
        "mapParquetTypeToSqlType", org.apache.parquet.schema.Type.class);
    m.setAccessible(true);
    org.apache.parquet.schema.Type t = org.apache.parquet.schema.Types.required(
        org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.DOUBLE).named("c");
    assertEquals(SqlTypeName.DOUBLE, m.invoke(table, t));
  }

  @Test void testMapParquetFloat() throws Exception {
    PartitionedParquetTable table = new PartitionedParquetTable(
        Collections.emptyList(), null, defaultEngineConfig);
    Method m = PartitionedParquetTable.class.getDeclaredMethod(
        "mapParquetTypeToSqlType", org.apache.parquet.schema.Type.class);
    m.setAccessible(true);
    org.apache.parquet.schema.Type t = org.apache.parquet.schema.Types.required(
        org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FLOAT).named("c");
    assertEquals(SqlTypeName.REAL, m.invoke(table, t));
  }

  @Test void testMapParquetBoolean() throws Exception {
    PartitionedParquetTable table = new PartitionedParquetTable(
        Collections.emptyList(), null, defaultEngineConfig);
    Method m = PartitionedParquetTable.class.getDeclaredMethod(
        "mapParquetTypeToSqlType", org.apache.parquet.schema.Type.class);
    m.setAccessible(true);
    org.apache.parquet.schema.Type t = org.apache.parquet.schema.Types.required(
        org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BOOLEAN).named("c");
    assertEquals(SqlTypeName.BOOLEAN, m.invoke(table, t));
  }

  @Test void testMapParquetInt96() throws Exception {
    PartitionedParquetTable table = new PartitionedParquetTable(
        Collections.emptyList(), null, defaultEngineConfig);
    Method m = PartitionedParquetTable.class.getDeclaredMethod(
        "mapParquetTypeToSqlType", org.apache.parquet.schema.Type.class);
    m.setAccessible(true);
    org.apache.parquet.schema.Type t = org.apache.parquet.schema.Types.required(
        org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT96).named("c");
    assertEquals(SqlTypeName.TIMESTAMP, m.invoke(table, t));
  }

  @Test void testMapParquetBinaryString() throws Exception {
    PartitionedParquetTable table = new PartitionedParquetTable(
        Collections.emptyList(), null, defaultEngineConfig);
    Method m = PartitionedParquetTable.class.getDeclaredMethod(
        "mapParquetTypeToSqlType", org.apache.parquet.schema.Type.class);
    m.setAccessible(true);
    org.apache.parquet.schema.Type t = org.apache.parquet.schema.Types.required(
        org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY)
        .as(org.apache.parquet.schema.LogicalTypeAnnotation.stringType())
        .named("c");
    assertEquals(SqlTypeName.VARCHAR, m.invoke(table, t));
  }

  // ====================================================================
  // CommentableTable tests
  // ====================================================================

  @Test void testGetTableCommentFromConfig() {
    PartitionedTableConfig config = mockConfig("Test table comment",
        Collections.singletonMap("col1", "Column 1 desc"));
    PartitionedParquetTable table = new PartitionedParquetTable(
        Collections.emptyList(), null, defaultEngineConfig,
        null, null, null, null, null, "t", null, config);
    assertEquals("Test table comment", table.getTableComment());
    assertEquals("Column 1 desc", table.getColumnComment("col1"));
    assertNull(table.getColumnComment("nonexistent"));
  }

  @Test void testGetTableCommentNoConfig() {
    PartitionedParquetTable table = new PartitionedParquetTable(
        Collections.emptyList(), null, defaultEngineConfig,
        null, null, null, null, null, "t", null, null);
    assertNull(table.getTableComment());
    assertNull(table.getColumnComment("col1"));
  }

  @Test void testGetMultipleColumnComments() {
    Map<String, String> comments = new LinkedHashMap<String, String>();
    comments.put("id", "Primary key");
    comments.put("name", "User name");
    comments.put("amount", "Transaction amount");
    PartitionedTableConfig config = mockConfig("Table", comments);
    PartitionedParquetTable table = new PartitionedParquetTable(
        Collections.emptyList(), null, defaultEngineConfig,
        null, null, null, null, null, "t", null, config);
    assertEquals("Primary key", table.getColumnComment("id"));
    assertEquals("User name", table.getColumnComment("name"));
    assertEquals("Transaction amount", table.getColumnComment("amount"));
  }

  // ====================================================================
  // matchesPartitionPredicates via reflection
  // ====================================================================

  @Test void testMatchesHivePredicateSuccess() throws Exception {
    PartitionDetector.PartitionInfo info = new PartitionDetector.PartitionInfo(
        new LinkedHashMap<String, String>(), Arrays.asList("year", "month"), true);
    PartitionedParquetTable table = new PartitionedParquetTable(
        Collections.singletonList("/data/year=2023/month=01/f.parquet"),
        info, defaultEngineConfig);
    Method m = PartitionedParquetTable.class.getDeclaredMethod(
        "matchesPartitionPredicates", String.class, Map.class);
    m.setAccessible(true);
    Map<String, Object> preds = new LinkedHashMap<String, Object>();
    preds.put("year", "2023");
    assertTrue((Boolean) m.invoke(table,
        "/data/year=2023/month=01/f.parquet", preds));
  }

  @Test void testMatchesHivePredicateFailure() throws Exception {
    PartitionDetector.PartitionInfo info = new PartitionDetector.PartitionInfo(
        new LinkedHashMap<String, String>(), Arrays.asList("year"), true);
    PartitionedParquetTable table = new PartitionedParquetTable(
        Collections.singletonList("/data/year=2023/f.parquet"),
        info, defaultEngineConfig);
    Method m = PartitionedParquetTable.class.getDeclaredMethod(
        "matchesPartitionPredicates", String.class, Map.class);
    m.setAccessible(true);
    Map<String, Object> preds = new LinkedHashMap<String, Object>();
    preds.put("year", "2024");
    assertFalse((Boolean) m.invoke(table,
        "/data/year=2023/f.parquet", preds));
  }

  @Test void testMatchesMissingColumnPredicate() throws Exception {
    PartitionDetector.PartitionInfo info = new PartitionDetector.PartitionInfo(
        new LinkedHashMap<String, String>(), Arrays.asList("year"), true);
    PartitionedParquetTable table = new PartitionedParquetTable(
        Collections.singletonList("/data/year=2023/f.parquet"),
        info, defaultEngineConfig);
    Method m = PartitionedParquetTable.class.getDeclaredMethod(
        "matchesPartitionPredicates", String.class, Map.class);
    m.setAccessible(true);
    Map<String, Object> preds = new LinkedHashMap<String, Object>();
    preds.put("country", "US");
    assertFalse((Boolean) m.invoke(table,
        "/data/year=2023/f.parquet", preds));
  }

  // ====================================================================
  // StatisticsProvider interface tests
  // ====================================================================

  @Test void testGetSelectivityDefault() {
    PartitionedParquetTable table = new PartitionedParquetTable(
        Collections.emptyList(), null, defaultEngineConfig);
    assertEquals(0.25, table.getSelectivity(null, null), 0.001);
  }

  @Test void testGetDistinctCountNoStats() {
    PartitionedParquetTable table = new PartitionedParquetTable(
        Collections.emptyList(), null, defaultEngineConfig);
    assertEquals(-1, table.getDistinctCount(null, "col"));
  }

  @Test void testHasStatisticsNoFiles() {
    PartitionedParquetTable table = new PartitionedParquetTable(
        Collections.emptyList(), null, defaultEngineConfig);
    assertFalse(table.hasStatistics(null));
  }

  @Test void testGetTableStatisticsNoFiles() {
    PartitionedParquetTable table = new PartitionedParquetTable(
        Collections.emptyList(), null, defaultEngineConfig);
    assertNull(table.getTableStatistics(null));
  }

  @Test void testGetColumnStatisticsNoFiles() {
    PartitionedParquetTable table = new PartitionedParquetTable(
        Collections.emptyList(), null, defaultEngineConfig);
    assertNull(table.getColumnStatistics(null, "col"));
  }

  @Test void testScheduleStatisticsGeneration() {
    PartitionedParquetTable table = new PartitionedParquetTable(
        Collections.emptyList(), null, defaultEngineConfig);
    // Should not throw
    table.scheduleStatisticsGeneration(null);
  }

  // ====================================================================
  // Table interface verification
  // ====================================================================

  @Test void testImplementsScannableTable() {
    PartitionedParquetTable table = new PartitionedParquetTable(
        Collections.emptyList(), null, defaultEngineConfig);
    assertTrue(table instanceof org.apache.calcite.schema.ScannableTable);
  }

  @Test void testImplementsFilterableTable() {
    PartitionedParquetTable table = new PartitionedParquetTable(
        Collections.emptyList(), null, defaultEngineConfig);
    assertTrue(table instanceof org.apache.calcite.schema.FilterableTable);
  }

  @Test void testImplementsCommentableTable() {
    PartitionedParquetTable table = new PartitionedParquetTable(
        Collections.emptyList(), null, defaultEngineConfig);
    assertTrue(table instanceof org.apache.calcite.schema.CommentableTable);
  }

  @Test void testImplementsStatisticsProvider() {
    PartitionedParquetTable table = new PartitionedParquetTable(
        Collections.emptyList(), null, defaultEngineConfig);
    assertTrue(
        table instanceof org.apache.calcite.adapter.file.statistics.StatisticsProvider);
  }

  // ====================================================================
  // Private field access tests
  // ====================================================================

  @Test void testPartitionColumnsField() throws Exception {
    PartitionDetector.PartitionInfo info = new PartitionDetector.PartitionInfo(
        new LinkedHashMap<String, String>(), Arrays.asList("y", "m"), true);
    PartitionedParquetTable table = new PartitionedParquetTable(
        Collections.emptyList(), info, defaultEngineConfig);
    Field f = PartitionedParquetTable.class.getDeclaredField("partitionColumns");
    f.setAccessible(true);
    @SuppressWarnings("unchecked")
    List<String> cols = (List<String>) f.get(table);
    assertEquals(Arrays.asList("y", "m"), cols);
  }

  @Test void testSchemaNameField() throws Exception {
    PartitionedParquetTable table = new PartitionedParquetTable(
        Collections.emptyList(), null, defaultEngineConfig,
        null, null, null, null, "mySchema", "myTable");
    Field f = PartitionedParquetTable.class.getDeclaredField("schemaName");
    f.setAccessible(true);
    assertEquals("mySchema", f.get(table));
  }

  @Test void testTableNameField() throws Exception {
    PartitionedParquetTable table = new PartitionedParquetTable(
        Collections.emptyList(), null, defaultEngineConfig,
        null, null, null, null, "mySchema", "myTable");
    Field f = PartitionedParquetTable.class.getDeclaredField("tableName");
    f.setAccessible(true);
    assertEquals("myTable", f.get(table));
  }

  @Test void testCustomRegexField() throws Exception {
    String regex = "data_(\\d+).parquet";
    PartitionedParquetTable table = new PartitionedParquetTable(
        Collections.emptyList(), null, defaultEngineConfig,
        null, regex, null);
    Field f = PartitionedParquetTable.class.getDeclaredField("customRegex");
    f.setAccessible(true);
    assertEquals(regex, f.get(table));
  }

  @Test void testEngineConfigField() throws Exception {
    PartitionedParquetTable table = new PartitionedParquetTable(
        Collections.emptyList(), null, defaultEngineConfig);
    Field f = PartitionedParquetTable.class.getDeclaredField("engineConfig");
    f.setAccessible(true);
    assertSame(defaultEngineConfig, f.get(table));
  }

  @Test void testPartitionColumnTypesField() throws Exception {
    Map<String, String> types = new HashMap<String, String>();
    types.put("year", "INTEGER");
    types.put("month", "VARCHAR");
    PartitionDetector.PartitionInfo info = new PartitionDetector.PartitionInfo(
        new LinkedHashMap<String, String>(), Arrays.asList("year"), true);
    PartitionedParquetTable table = new PartitionedParquetTable(
        Collections.emptyList(), info, defaultEngineConfig, types);
    Field f = PartitionedParquetTable.class.getDeclaredField("partitionColumnTypes");
    f.setAccessible(true);
    @SuppressWarnings("unchecked")
    Map<String, String> stored = (Map<String, String>) f.get(table);
    assertEquals("INTEGER", stored.get("year"));
    assertEquals("VARCHAR", stored.get("month"));
  }
}
