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
package org.apache.calcite.adapter.file.iceberg;

import org.apache.calcite.adapter.file.etl.IcebergMaterializationWriter;
import org.apache.calcite.adapter.file.etl.MaterializeConfig;
import org.apache.calcite.adapter.file.etl.MaterializeOptionsConfig;
import org.apache.calcite.adapter.file.etl.MaterializeOutputConfig;
import org.apache.calcite.adapter.file.etl.MaterializePartitionConfig;
import org.apache.calcite.adapter.file.partition.IncrementalTracker;
import org.apache.calcite.adapter.file.storage.StorageProvider;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Coverage tests for {@link IcebergMaterializationWriter} targeting lines
 * in expression evaluation, type casting, DuckDB SQL building, partition
 * management, staging paths, and error handling.
 *
 * <p>Tests private methods via reflection to maximize line coverage without
 * requiring real Iceberg/S3 infrastructure.
 */
@Tag("unit")
public class IcebergMaterializationWriterCoverageTest {

  @TempDir
  Path tempDir;

  private IcebergMaterializationWriter writer;
  private StorageProvider mockStorageProvider;

  @BeforeEach
  void setUp() {
    mockStorageProvider = new StorageProvider() {
      @Override public java.io.InputStream openInputStream(String path) {
        return new java.io.ByteArrayInputStream(new byte[0]);
      }
      @Override public void writeFile(String path, java.io.InputStream data) { }
      @Override public void writeFile(String path, byte[] data) { }
      @Override public boolean delete(String path) { return true; }
      @Override public int deleteBatch(List<String> paths) { return 0; }
      @Override public String getStorageType() { return "test"; }
      @Override public List<FileEntry> listFiles(String path, boolean recursive) {
        return Collections.emptyList();
      }
      @Override public void createDirectories(String path) { }
      @Override public String resolvePath(String base, String relative) {
        return base + "/" + relative;
      }
      @Override public FileMetadata getMetadata(String path) { return null; }
      @Override public Map<String, String> getS3Config() { return Collections.emptyMap(); }
      @Override public void ensureLifecycleRule(String prefix, int days) { }
      @Override public boolean isDirectory(String path) { return false; }
      @Override public boolean exists(String path) { return false; }
      @Override public java.io.Reader openReader(String path) {
        return new java.io.StringReader("");
      }
    };
    writer = new IcebergMaterializationWriter(
        mockStorageProvider, tempDir.toString(), IncrementalTracker.NOOP);
  }

  // ========== Constructor Tests ==========

  @Test void testConstructorSetsDefaults() throws Exception {
    Field totalRowsField = IcebergMaterializationWriter.class.getDeclaredField("totalRowsWritten");
    totalRowsField.setAccessible(true);
    assertEquals(0L, totalRowsField.getLong(writer));

    Field totalFilesField = IcebergMaterializationWriter.class.getDeclaredField("totalFilesWritten");
    totalFilesField.setAccessible(true);
    assertEquals(0, totalFilesField.getInt(writer));

    Field initializedField = IcebergMaterializationWriter.class.getDeclaredField("initialized");
    initializedField.setAccessible(true);
    assertFalse(initializedField.getBoolean(writer));
  }

  @Test void testConstructorWithNullIncrementalTracker() throws Exception {
    IcebergMaterializationWriter w = new IcebergMaterializationWriter(
        mockStorageProvider, tempDir.toString(), null);
    Field field = IcebergMaterializationWriter.class.getDeclaredField("incrementalTracker");
    field.setAccessible(true);
    assertNotNull(field.get(w));
  }

  @Test void testConstructorStoresWarehousePath() throws Exception {
    Field field = IcebergMaterializationWriter.class.getDeclaredField("warehousePath");
    field.setAccessible(true);
    assertEquals(tempDir.toString(), field.get(writer));
  }

  // ========== initialize validation Tests ==========

  @Test void testInitializeNullConfigThrows() {
    assertThrows(IllegalArgumentException.class, () -> writer.initialize(null));
  }

  @Test void testInitializeDisabledConfigThrows() {
    MaterializeConfig config = MaterializeConfig.builder()
        .enabled(false)
        .format(MaterializeConfig.Format.ICEBERG)
        .build();
    assertThrows(IOException.class, () -> writer.initialize(config));
  }

  @Test void testInitializeNonIcebergFormatThrows() {
    MaterializeConfig config = MaterializeConfig.builder()
        .enabled(true)
        .format(MaterializeConfig.Format.PARQUET)
        .output(MaterializeOutputConfig.builder()
            .location("/tmp/output")
            .build())
        .build();
    assertThrows(IllegalArgumentException.class, () -> writer.initialize(config));
  }

  // ========== escapeString Tests ==========

  @Test void testEscapeStringNoQuotes() throws Exception {
    Method m = IcebergMaterializationWriter.class.getDeclaredMethod("escapeString", String.class);
    m.setAccessible(true);
    assertEquals("hello world", m.invoke(null, "hello world"));
  }

  @Test void testEscapeStringSingleQuote() throws Exception {
    Method m = IcebergMaterializationWriter.class.getDeclaredMethod("escapeString", String.class);
    m.setAccessible(true);
    assertEquals("it''s", m.invoke(null, "it's"));
  }

  @Test void testEscapeStringMultipleQuotes() throws Exception {
    Method m = IcebergMaterializationWriter.class.getDeclaredMethod("escapeString", String.class);
    m.setAccessible(true);
    assertEquals("a''b''c", m.invoke(null, "a'b'c"));
  }

  @Test void testEscapeStringEmpty() throws Exception {
    Method m = IcebergMaterializationWriter.class.getDeclaredMethod("escapeString", String.class);
    m.setAccessible(true);
    assertEquals("", m.invoke(null, ""));
  }

  // ========== mapToDuckDBType Tests ==========

  @Test void testMapToDuckDBTypeNull() throws Exception {
    Method m = IcebergMaterializationWriter.class.getDeclaredMethod(
        "mapToDuckDBType", String.class);
    m.setAccessible(true);
    assertEquals("VARCHAR", m.invoke(writer, (Object) null));
  }

  @Test void testMapToDuckDBTypeVarchar() throws Exception {
    Method m = IcebergMaterializationWriter.class.getDeclaredMethod(
        "mapToDuckDBType", String.class);
    m.setAccessible(true);
    assertEquals("VARCHAR", m.invoke(writer, "VARCHAR"));
  }

  @Test void testMapToDuckDBTypeString() throws Exception {
    Method m = IcebergMaterializationWriter.class.getDeclaredMethod(
        "mapToDuckDBType", String.class);
    m.setAccessible(true);
    assertEquals("VARCHAR", m.invoke(writer, "STRING"));
  }

  @Test void testMapToDuckDBTypeChar() throws Exception {
    Method m = IcebergMaterializationWriter.class.getDeclaredMethod(
        "mapToDuckDBType", String.class);
    m.setAccessible(true);
    assertEquals("VARCHAR", m.invoke(writer, "CHAR(10)"));
  }

  @Test void testMapToDuckDBTypeInteger() throws Exception {
    Method m = IcebergMaterializationWriter.class.getDeclaredMethod(
        "mapToDuckDBType", String.class);
    m.setAccessible(true);
    assertEquals("INTEGER", m.invoke(writer, "INTEGER"));
  }

  @Test void testMapToDuckDBTypeInt() throws Exception {
    Method m = IcebergMaterializationWriter.class.getDeclaredMethod(
        "mapToDuckDBType", String.class);
    m.setAccessible(true);
    assertEquals("INTEGER", m.invoke(writer, "INT"));
  }

  @Test void testMapToDuckDBTypeBigint() throws Exception {
    Method m = IcebergMaterializationWriter.class.getDeclaredMethod(
        "mapToDuckDBType", String.class);
    m.setAccessible(true);
    assertEquals("BIGINT", m.invoke(writer, "BIGINT"));
  }

  @Test void testMapToDuckDBTypeLong() throws Exception {
    Method m = IcebergMaterializationWriter.class.getDeclaredMethod(
        "mapToDuckDBType", String.class);
    m.setAccessible(true);
    assertEquals("BIGINT", m.invoke(writer, "LONG"));
  }

  @Test void testMapToDuckDBTypeDouble() throws Exception {
    Method m = IcebergMaterializationWriter.class.getDeclaredMethod(
        "mapToDuckDBType", String.class);
    m.setAccessible(true);
    assertEquals("DOUBLE", m.invoke(writer, "DOUBLE"));
  }

  @Test void testMapToDuckDBTypeFloat() throws Exception {
    Method m = IcebergMaterializationWriter.class.getDeclaredMethod(
        "mapToDuckDBType", String.class);
    m.setAccessible(true);
    assertEquals("DOUBLE", m.invoke(writer, "FLOAT"));
  }

  @Test void testMapToDuckDBTypeBoolean() throws Exception {
    Method m = IcebergMaterializationWriter.class.getDeclaredMethod(
        "mapToDuckDBType", String.class);
    m.setAccessible(true);
    assertEquals("BOOLEAN", m.invoke(writer, "BOOLEAN"));
  }

  @Test void testMapToDuckDBTypeDate() throws Exception {
    Method m = IcebergMaterializationWriter.class.getDeclaredMethod(
        "mapToDuckDBType", String.class);
    m.setAccessible(true);
    assertEquals("DATE", m.invoke(writer, "DATE"));
  }

  @Test void testMapToDuckDBTypeTimestamp() throws Exception {
    Method m = IcebergMaterializationWriter.class.getDeclaredMethod(
        "mapToDuckDBType", String.class);
    m.setAccessible(true);
    assertEquals("TIMESTAMP", m.invoke(writer, "TIMESTAMP"));
  }

  @Test void testMapToDuckDBTypeUnknown() throws Exception {
    Method m = IcebergMaterializationWriter.class.getDeclaredMethod(
        "mapToDuckDBType", String.class);
    m.setAccessible(true);
    assertEquals("VARCHAR", m.invoke(writer, "UNKNOWN_TYPE"));
  }

  // ========== getValueCaseInsensitive Tests ==========

  @Test void testGetValueCaseInsensitiveExactMatch() throws Exception {
    Method m = IcebergMaterializationWriter.class.getDeclaredMethod(
        "getValueCaseInsensitive", Map.class, String.class);
    m.setAccessible(true);

    Map<String, Object> map = new HashMap<String, Object>();
    map.put("name", "John");
    assertEquals("John", m.invoke(writer, map, "name"));
  }

  @Test void testGetValueCaseInsensitiveCaseMatch() throws Exception {
    Method m = IcebergMaterializationWriter.class.getDeclaredMethod(
        "getValueCaseInsensitive", Map.class, String.class);
    m.setAccessible(true);

    Map<String, Object> map = new HashMap<String, Object>();
    map.put("Name", "John");
    assertEquals("John", m.invoke(writer, map, "name"));
  }

  @Test void testGetValueCaseInsensitiveNullMap() throws Exception {
    Method m = IcebergMaterializationWriter.class.getDeclaredMethod(
        "getValueCaseInsensitive", Map.class, String.class);
    m.setAccessible(true);
    assertNull(m.invoke(writer, null, "key"));
  }

  @Test void testGetValueCaseInsensitiveNullKey() throws Exception {
    Method m = IcebergMaterializationWriter.class.getDeclaredMethod(
        "getValueCaseInsensitive", Map.class, String.class);
    m.setAccessible(true);
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("name", "val");
    assertNull(m.invoke(writer, map, null));
  }

  @Test void testGetValueCaseInsensitiveNotFound() throws Exception {
    Method m = IcebergMaterializationWriter.class.getDeclaredMethod(
        "getValueCaseInsensitive", Map.class, String.class);
    m.setAccessible(true);

    Map<String, Object> map = new HashMap<String, Object>();
    map.put("name", "John");
    assertNull(m.invoke(writer, map, "age"));
  }

  // ========== castValue Tests ==========

  @Test void testCastValueNull() throws Exception {
    Method m = IcebergMaterializationWriter.class.getDeclaredMethod(
        "castValue", Object.class, String.class);
    m.setAccessible(true);
    assertNull(m.invoke(writer, null, "BIGINT"));
  }

  @Test void testCastValueEmptyString() throws Exception {
    Method m = IcebergMaterializationWriter.class.getDeclaredMethod(
        "castValue", Object.class, String.class);
    m.setAccessible(true);
    assertNull(m.invoke(writer, "  ", "BIGINT"));
  }

  @Test void testCastValueBigint() throws Exception {
    Method m = IcebergMaterializationWriter.class.getDeclaredMethod(
        "castValue", Object.class, String.class);
    m.setAccessible(true);
    assertEquals(42L, m.invoke(writer, "42", "BIGINT"));
  }

  @Test void testCastValueInt64() throws Exception {
    Method m = IcebergMaterializationWriter.class.getDeclaredMethod(
        "castValue", Object.class, String.class);
    m.setAccessible(true);
    assertEquals(99L, m.invoke(writer, "99", "INT64"));
  }

  @Test void testCastValueLong() throws Exception {
    Method m = IcebergMaterializationWriter.class.getDeclaredMethod(
        "castValue", Object.class, String.class);
    m.setAccessible(true);
    assertEquals(1000L, m.invoke(writer, "1000", "LONG"));
  }

  @Test void testCastValueInteger() throws Exception {
    Method m = IcebergMaterializationWriter.class.getDeclaredMethod(
        "castValue", Object.class, String.class);
    m.setAccessible(true);
    assertEquals(42, m.invoke(writer, "42", "INTEGER"));
  }

  @Test void testCastValueInt() throws Exception {
    Method m = IcebergMaterializationWriter.class.getDeclaredMethod(
        "castValue", Object.class, String.class);
    m.setAccessible(true);
    assertEquals(42, m.invoke(writer, "42", "INT"));
  }

  @Test void testCastValueInt32() throws Exception {
    Method m = IcebergMaterializationWriter.class.getDeclaredMethod(
        "castValue", Object.class, String.class);
    m.setAccessible(true);
    assertEquals(42, m.invoke(writer, "42", "INT32"));
  }

  @Test void testCastValueDouble() throws Exception {
    Method m = IcebergMaterializationWriter.class.getDeclaredMethod(
        "castValue", Object.class, String.class);
    m.setAccessible(true);
    assertEquals(3.14, m.invoke(writer, "3.14", "DOUBLE"));
  }

  @Test void testCastValueFloat8() throws Exception {
    Method m = IcebergMaterializationWriter.class.getDeclaredMethod(
        "castValue", Object.class, String.class);
    m.setAccessible(true);
    assertEquals(2.71, m.invoke(writer, "2.71", "FLOAT8"));
  }

  @Test void testCastValueFloat() throws Exception {
    Method m = IcebergMaterializationWriter.class.getDeclaredMethod(
        "castValue", Object.class, String.class);
    m.setAccessible(true);
    Object result = m.invoke(writer, "1.5", "FLOAT");
    assertTrue(result instanceof Float);
    assertEquals(1.5f, (Float) result, 0.001);
  }

  @Test void testCastValueFloat4() throws Exception {
    Method m = IcebergMaterializationWriter.class.getDeclaredMethod(
        "castValue", Object.class, String.class);
    m.setAccessible(true);
    Object result = m.invoke(writer, "2.0", "FLOAT4");
    assertTrue(result instanceof Float);
  }

  @Test void testCastValueReal() throws Exception {
    Method m = IcebergMaterializationWriter.class.getDeclaredMethod(
        "castValue", Object.class, String.class);
    m.setAccessible(true);
    Object result = m.invoke(writer, "3.0", "REAL");
    assertTrue(result instanceof Float);
  }

  @Test void testCastValueVarchar() throws Exception {
    Method m = IcebergMaterializationWriter.class.getDeclaredMethod(
        "castValue", Object.class, String.class);
    m.setAccessible(true);
    assertEquals("hello", m.invoke(writer, "hello", "VARCHAR"));
  }

  @Test void testCastValueString() throws Exception {
    Method m = IcebergMaterializationWriter.class.getDeclaredMethod(
        "castValue", Object.class, String.class);
    m.setAccessible(true);
    assertEquals("test", m.invoke(writer, "test", "STRING"));
  }

  @Test void testCastValueText() throws Exception {
    Method m = IcebergMaterializationWriter.class.getDeclaredMethod(
        "castValue", Object.class, String.class);
    m.setAccessible(true);
    assertEquals("data", m.invoke(writer, "data", "TEXT"));
  }

  @Test void testCastValueBoolean() throws Exception {
    Method m = IcebergMaterializationWriter.class.getDeclaredMethod(
        "castValue", Object.class, String.class);
    m.setAccessible(true);
    assertEquals(true, m.invoke(writer, "true", "BOOLEAN"));
  }

  @Test void testCastValueBool() throws Exception {
    Method m = IcebergMaterializationWriter.class.getDeclaredMethod(
        "castValue", Object.class, String.class);
    m.setAccessible(true);
    assertEquals(false, m.invoke(writer, "false", "BOOL"));
  }

  @Test void testCastValueUnknownType() throws Exception {
    Method m = IcebergMaterializationWriter.class.getDeclaredMethod(
        "castValue", Object.class, String.class);
    m.setAccessible(true);
    assertEquals("value", m.invoke(writer, "value", "CUSTOM_TYPE"));
  }

  @Test void testCastValueNumberFormatException() throws Exception {
    Method m = IcebergMaterializationWriter.class.getDeclaredMethod(
        "castValue", Object.class, String.class);
    m.setAccessible(true);
    assertNull(m.invoke(writer, "not_a_number", "BIGINT"));
  }

  @Test void testCastValueIntegerNumberFormatException() throws Exception {
    Method m = IcebergMaterializationWriter.class.getDeclaredMethod(
        "castValue", Object.class, String.class);
    m.setAccessible(true);
    assertNull(m.invoke(writer, "abc", "INTEGER"));
  }

  @Test void testCastValueDoubleNumberFormatException() throws Exception {
    Method m = IcebergMaterializationWriter.class.getDeclaredMethod(
        "castValue", Object.class, String.class);
    m.setAccessible(true);
    assertNull(m.invoke(writer, "xyz", "DOUBLE"));
  }

  // ========== evaluateExpression Tests ==========

  @Test void testEvaluateExpressionNull() throws Exception {
    Method m = IcebergMaterializationWriter.class.getDeclaredMethod(
        "evaluateExpression", String.class, Map.class);
    m.setAccessible(true);
    assertNull(m.invoke(writer, null, new HashMap<String, Object>()));
  }

  @Test void testEvaluateExpressionEmpty() throws Exception {
    Method m = IcebergMaterializationWriter.class.getDeclaredMethod(
        "evaluateExpression", String.class, Map.class);
    m.setAccessible(true);
    assertNull(m.invoke(writer, "", new HashMap<String, Object>()));
  }

  @Test void testEvaluateExpressionSrcField() throws Exception {
    Method m = IcebergMaterializationWriter.class.getDeclaredMethod(
        "evaluateExpression", String.class, Map.class);
    m.setAccessible(true);

    Map<String, Object> row = new HashMap<String, Object>();
    row.put("FIELD1", "value1");
    assertEquals("value1", m.invoke(writer, "src.\"FIELD1\"", row));
  }

  @Test void testEvaluateExpressionSrcFieldNoQuotes() throws Exception {
    Method m = IcebergMaterializationWriter.class.getDeclaredMethod(
        "evaluateExpression", String.class, Map.class);
    m.setAccessible(true);

    Map<String, Object> row = new HashMap<String, Object>();
    row.put("field1", "value1");
    assertEquals("value1", m.invoke(writer, "src.field1", row));
  }

  @Test void testEvaluateExpressionBareField() throws Exception {
    Method m = IcebergMaterializationWriter.class.getDeclaredMethod(
        "evaluateExpression", String.class, Map.class);
    m.setAccessible(true);

    Map<String, Object> row = new HashMap<String, Object>();
    row.put("table_name", "test_table");
    assertEquals("test_table", m.invoke(writer, "table_name", row));
  }

  @Test void testEvaluateExpressionCastBigint() throws Exception {
    Method m = IcebergMaterializationWriter.class.getDeclaredMethod(
        "evaluateExpression", String.class, Map.class);
    m.setAccessible(true);

    Map<String, Object> row = new HashMap<String, Object>();
    row.put("VALUE", "12345");
    assertEquals(12345L, m.invoke(writer, "TRY_CAST(src.\"VALUE\" AS BIGINT)", row));
  }

  @Test void testEvaluateExpressionCastDouble() throws Exception {
    Method m = IcebergMaterializationWriter.class.getDeclaredMethod(
        "evaluateExpression", String.class, Map.class);
    m.setAccessible(true);

    Map<String, Object> row = new HashMap<String, Object>();
    row.put("PRICE", "99.99");
    assertEquals(99.99, m.invoke(writer, "CAST(src.\"PRICE\" AS DOUBLE)", row));
  }

  @Test void testEvaluateExpressionBareCast() throws Exception {
    Method m = IcebergMaterializationWriter.class.getDeclaredMethod(
        "evaluateExpression", String.class, Map.class);
    m.setAccessible(true);

    Map<String, Object> row = new HashMap<String, Object>();
    row.put("count", "42");
    assertEquals(42, m.invoke(writer, "TRY_CAST(count AS INT)", row));
  }

  @Test void testEvaluateExpressionReplace() throws Exception {
    Method m = IcebergMaterializationWriter.class.getDeclaredMethod(
        "evaluateExpression", String.class, Map.class);
    m.setAccessible(true);

    Map<String, Object> row = new HashMap<String, Object>();
    row.put("VALUE", "1,234");
    assertEquals("1234", m.invoke(writer, "REPLACE(src.\"VALUE\", ',', '')", row));
  }

  @Test void testEvaluateExpressionCastReplace() throws Exception {
    Method m = IcebergMaterializationWriter.class.getDeclaredMethod(
        "evaluateExpression", String.class, Map.class);
    m.setAccessible(true);

    Map<String, Object> row = new HashMap<String, Object>();
    row.put("VALUE", "1,234");
    assertEquals(1234L, m.invoke(writer,
        "TRY_CAST(REPLACE(src.\"VALUE\", ',', '') AS BIGINT)", row));
  }

  @Test void testEvaluateExpressionSubstring() throws Exception {
    Method m = IcebergMaterializationWriter.class.getDeclaredMethod(
        "evaluateExpression", String.class, Map.class);
    m.setAccessible(true);

    Map<String, Object> row = new HashMap<String, Object>();
    row.put("PERIOD", "Q1-2024");
    assertEquals("Q1", m.invoke(writer, "SUBSTRING(src.\"PERIOD\", 1, 2)", row));
  }

  @Test void testEvaluateExpressionSubstringOutOfBounds() throws Exception {
    Method m = IcebergMaterializationWriter.class.getDeclaredMethod(
        "evaluateExpression", String.class, Map.class);
    m.setAccessible(true);

    Map<String, Object> row = new HashMap<String, Object>();
    row.put("PERIOD", "AB");
    assertNull(m.invoke(writer, "SUBSTRING(src.\"PERIOD\", 5, 2)", row));
  }

  @Test void testEvaluateExpressionRight() throws Exception {
    Method m = IcebergMaterializationWriter.class.getDeclaredMethod(
        "evaluateExpression", String.class, Map.class);
    m.setAccessible(true);

    Map<String, Object> row = new HashMap<String, Object>();
    row.put("CODE", "ABC123");
    assertEquals("123", m.invoke(writer, "RIGHT(src.\"CODE\", 3)", row));
  }

  @Test void testEvaluateExpressionRightLongerThanString() throws Exception {
    Method m = IcebergMaterializationWriter.class.getDeclaredMethod(
        "evaluateExpression", String.class, Map.class);
    m.setAccessible(true);

    Map<String, Object> row = new HashMap<String, Object>();
    row.put("CODE", "AB");
    assertEquals("AB", m.invoke(writer, "RIGHT(src.\"CODE\", 10)", row));
  }

  @Test void testEvaluateExpressionCoalesce() throws Exception {
    Method m = IcebergMaterializationWriter.class.getDeclaredMethod(
        "evaluateExpression", String.class, Map.class);
    m.setAccessible(true);

    Map<String, Object> row = new HashMap<String, Object>();
    row.put("FIELD2", "value2");
    assertEquals("value2", m.invoke(writer,
        "COALESCE(src.\"FIELD1\", src.\"FIELD2\")", row));
  }

  @Test void testEvaluateExpressionCoalesceAllNull() throws Exception {
    Method m = IcebergMaterializationWriter.class.getDeclaredMethod(
        "evaluateExpression", String.class, Map.class);
    m.setAccessible(true);

    Map<String, Object> row = new HashMap<String, Object>();
    assertNull(m.invoke(writer,
        "COALESCE(src.\"FIELD1\", src.\"FIELD2\")", row));
  }

  @Test void testEvaluateExpressionUnrecognized() throws Exception {
    Method m = IcebergMaterializationWriter.class.getDeclaredMethod(
        "evaluateExpression", String.class, Map.class);
    m.setAccessible(true);

    Map<String, Object> row = new HashMap<String, Object>();
    assertNull(m.invoke(writer, "SOME_UNKNOWN_FUNC(x)", row));
  }

  @Test void testEvaluateExpressionReplaceNullSource() throws Exception {
    Method m = IcebergMaterializationWriter.class.getDeclaredMethod(
        "evaluateExpression", String.class, Map.class);
    m.setAccessible(true);

    Map<String, Object> row = new HashMap<String, Object>();
    assertNull(m.invoke(writer, "REPLACE(src.\"MISSING\", ',', '')", row));
  }

  @Test void testEvaluateExpressionCastReplaceNullSource() throws Exception {
    Method m = IcebergMaterializationWriter.class.getDeclaredMethod(
        "evaluateExpression", String.class, Map.class);
    m.setAccessible(true);

    Map<String, Object> row = new HashMap<String, Object>();
    assertNull(m.invoke(writer,
        "TRY_CAST(REPLACE(src.\"MISSING\", ',', '') AS BIGINT)", row));
  }

  @Test void testEvaluateExpressionSubstringNullSource() throws Exception {
    Method m = IcebergMaterializationWriter.class.getDeclaredMethod(
        "evaluateExpression", String.class, Map.class);
    m.setAccessible(true);

    Map<String, Object> row = new HashMap<String, Object>();
    assertNull(m.invoke(writer, "SUBSTRING(src.\"MISSING\", 1, 2)", row));
  }

  @Test void testEvaluateExpressionRightNullSource() throws Exception {
    Method m = IcebergMaterializationWriter.class.getDeclaredMethod(
        "evaluateExpression", String.class, Map.class);
    m.setAccessible(true);

    Map<String, Object> row = new HashMap<String, Object>();
    assertNull(m.invoke(writer, "RIGHT(src.\"MISSING\", 3)", row));
  }

  // ========== buildPartitionFilter Tests ==========

  @Test void testBuildPartitionFilterEmpty() throws Exception {
    Method m = IcebergMaterializationWriter.class.getDeclaredMethod(
        "buildPartitionFilter", Map.class);
    m.setAccessible(true);

    @SuppressWarnings("unchecked")
    Map<String, Object> result = (Map<String, Object>) m.invoke(writer, (Object) null);
    assertTrue(result.isEmpty());
  }

  @Test void testBuildPartitionFilterEmptyVars() throws Exception {
    Method m = IcebergMaterializationWriter.class.getDeclaredMethod(
        "buildPartitionFilter", Map.class);
    m.setAccessible(true);

    @SuppressWarnings("unchecked")
    Map<String, Object> result =
        (Map<String, Object>) m.invoke(writer, Collections.emptyMap());
    assertTrue(result.isEmpty());
  }

  // ========== getRemoteParentPath Tests ==========

  @Test void testGetRemoteParentPathNormal() throws Exception {
    Method m = IcebergMaterializationWriter.class.getDeclaredMethod(
        "getRemoteParentPath", String.class);
    m.setAccessible(true);
    assertEquals("/path/to", m.invoke(writer, "/path/to/file.txt"));
  }

  @Test void testGetRemoteParentPathNoSlash() throws Exception {
    Method m = IcebergMaterializationWriter.class.getDeclaredMethod(
        "getRemoteParentPath", String.class);
    m.setAccessible(true);
    assertNull(m.invoke(writer, "file.txt"));
  }

  @Test void testGetRemoteParentPathRootSlash() throws Exception {
    Method m = IcebergMaterializationWriter.class.getDeclaredMethod(
        "getRemoteParentPath", String.class);
    m.setAccessible(true);
    assertNull(m.invoke(writer, "/file.txt"));
  }

  @Test void testGetRemoteParentPathS3ShortPath() throws Exception {
    Method m = IcebergMaterializationWriter.class.getDeclaredMethod(
        "getRemoteParentPath", String.class);
    m.setAccessible(true);
    assertNull(m.invoke(writer, "s3://bucket"));
  }

  @Test void testGetRemoteParentPathS3aShortPath() throws Exception {
    Method m = IcebergMaterializationWriter.class.getDeclaredMethod(
        "getRemoteParentPath", String.class);
    m.setAccessible(true);
    assertNull(m.invoke(writer, "s3a://bucket"));
  }

  @Test void testGetRemoteParentPathS3DeepPath() throws Exception {
    Method m = IcebergMaterializationWriter.class.getDeclaredMethod(
        "getRemoteParentPath", String.class);
    m.setAccessible(true);
    assertEquals("s3://bucket/path", m.invoke(writer, "s3://bucket/path/file.parquet"));
  }

  // ========== cleanupStagingDirectory Tests ==========

  @Test void testCleanupStagingDirectoryS3Path() throws Exception {
    Method m = IcebergMaterializationWriter.class.getDeclaredMethod(
        "cleanupStagingDirectory", String.class);
    m.setAccessible(true);
    // set staging mode to REMOTE
    Field stagingModeField = IcebergMaterializationWriter.class.getDeclaredField("stagingMode");
    stagingModeField.setAccessible(true);
    stagingModeField.set(writer, MaterializeOptionsConfig.StagingMode.REMOTE);
    // S3 paths skip cleanup - no exception expected
    m.invoke(writer, "s3://bucket/.staging/test");
  }

  @Test void testCleanupStagingDirectoryS3aPath() throws Exception {
    Method m = IcebergMaterializationWriter.class.getDeclaredMethod(
        "cleanupStagingDirectory", String.class);
    m.setAccessible(true);
    Field stagingModeField = IcebergMaterializationWriter.class.getDeclaredField("stagingMode");
    stagingModeField.setAccessible(true);
    stagingModeField.set(writer, MaterializeOptionsConfig.StagingMode.REMOTE);
    m.invoke(writer, "s3a://bucket/.staging/test");
  }

  @Test void testCleanupStagingDirectoryLocalNonExistent() throws Exception {
    Method m = IcebergMaterializationWriter.class.getDeclaredMethod(
        "cleanupStagingDirectory", String.class);
    m.setAccessible(true);
    Field stagingModeField = IcebergMaterializationWriter.class.getDeclaredField("stagingMode");
    stagingModeField.setAccessible(true);
    stagingModeField.set(writer, MaterializeOptionsConfig.StagingMode.LOCAL);
    // Non-existent path should not throw
    m.invoke(writer, tempDir.resolve("nonexistent").toString());
  }

  @Test void testCleanupStagingDirectoryLocalExists() throws Exception {
    Method m = IcebergMaterializationWriter.class.getDeclaredMethod(
        "cleanupStagingDirectory", String.class);
    m.setAccessible(true);
    Field stagingModeField = IcebergMaterializationWriter.class.getDeclaredField("stagingMode");
    stagingModeField.setAccessible(true);
    stagingModeField.set(writer, MaterializeOptionsConfig.StagingMode.LOCAL);

    Path stagingDir = tempDir.resolve("staging_test");
    Files.createDirectories(stagingDir);
    Files.createFile(stagingDir.resolve("test.parquet"));

    m.invoke(writer, stagingDir.toString());
    assertFalse(Files.exists(stagingDir));
  }

  // ========== commitNotInitialized Tests ==========

  @Test void testCommitNotInitializedThrows() {
    assertThrows(IllegalStateException.class, () -> writer.commit());
  }

  // ========== getEnvInt Tests ==========

  @Test void testGetEnvIntDefault() throws Exception {
    Method m = IcebergMaterializationWriter.class.getDeclaredMethod(
        "getEnvInt", String.class, int.class);
    m.setAccessible(true);
    int result = (int) m.invoke(null, "NONEXISTENT_ENV_VAR_FOR_TEST_12345", 42);
    assertEquals(42, result);
  }

  // ========== DUCKDB_MEMORY_LIMIT static field ==========

  @Test void testDuckDBMemoryLimitFieldExists() throws Exception {
    Field f = IcebergMaterializationWriter.class.getDeclaredField("DUCKDB_MEMORY_LIMIT");
    f.setAccessible(true);
    String value = (String) f.get(null);
    assertNotNull(value);
  }

  // ========== configureS3 Tests (coverage for S3 config paths) ==========

  @Test void testConfigureS3WithAllSettings() throws Exception {
    // Create a DuckDB connection for testing
    java.sql.Connection conn = java.sql.DriverManager.getConnection("jdbc:duckdb:");
    try {
      Method m = IcebergMaterializationWriter.class.getDeclaredMethod(
          "configureS3", java.sql.Statement.class, Map.class);
      m.setAccessible(true);

      Map<String, String> s3Config = new HashMap<String, String>();
      s3Config.put("accessKeyId", "AKID");
      s3Config.put("secretAccessKey", "SECRET");
      s3Config.put("endpoint", "http://localhost:9000");
      s3Config.put("region", "us-east-1");

      try (java.sql.Statement stmt = conn.createStatement()) {
        m.invoke(writer, stmt, s3Config);
      }
    } finally {
      conn.close();
    }
  }

  @Test void testConfigureS3WithoutRegion() throws Exception {
    java.sql.Connection conn = java.sql.DriverManager.getConnection("jdbc:duckdb:");
    try {
      Method m = IcebergMaterializationWriter.class.getDeclaredMethod(
          "configureS3", java.sql.Statement.class, Map.class);
      m.setAccessible(true);

      Map<String, String> s3Config = new HashMap<String, String>();
      s3Config.put("accessKeyId", "AKID");
      s3Config.put("secretAccessKey", "SECRET");

      try (java.sql.Statement stmt = conn.createStatement()) {
        m.invoke(writer, stmt, s3Config);
      }
    } finally {
      conn.close();
    }
  }

  @Test void testConfigureS3WithoutCredentials() throws Exception {
    java.sql.Connection conn = java.sql.DriverManager.getConnection("jdbc:duckdb:");
    try {
      Method m = IcebergMaterializationWriter.class.getDeclaredMethod(
          "configureS3", java.sql.Statement.class, Map.class);
      m.setAccessible(true);

      Map<String, String> s3Config = new HashMap<String, String>();
      s3Config.put("region", "us-west-2");

      try (java.sql.Statement stmt = conn.createStatement()) {
        m.invoke(writer, stmt, s3Config);
      }
    } finally {
      conn.close();
    }
  }
}
