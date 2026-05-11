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
package org.apache.calcite.adapter.file.etl;

import org.apache.calcite.adapter.file.iceberg.IcebergCatalogManager;
import org.apache.calcite.adapter.file.storage.LocalFileStorageProvider;
import org.apache.calcite.adapter.file.storage.StorageProvider;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Deep coverage tests for IcebergMaterializationWriter.
 * Tests private methods via reflection and edge cases.
 */
@Tag("unit")
public class IcebergMaterializationWriterDeepCoverageTest2 {

  @TempDir
  Path tempDir;

  private StorageProvider storageProvider;
  private IcebergMaterializationWriter writer;

  @BeforeEach
  void setUp() {
    storageProvider = new LocalFileStorageProvider();
    String warehousePath = tempDir.resolve("warehouse").toString();
    writer = new IcebergMaterializationWriter(storageProvider, warehousePath, null);
  }

  @AfterEach
  void tearDown() {
    try {
      writer.close();
    } catch (IOException e) {
      // ignore
    }
    IcebergCatalogManager.clearCache();
  }

  // ===== Constructor tests =====

  @Test void testConstructorWithNullTracker() {
    IcebergMaterializationWriter w =
        new IcebergMaterializationWriter(storageProvider, "/warehouse", null);
    assertNotNull(w);
    assertEquals(0, w.getTotalRowsWritten());
    assertEquals(0, w.getTotalFilesWritten());
  }

  @Test void testGetFormat() {
    assertEquals(MaterializeConfig.Format.ICEBERG, writer.getFormat());
  }

  @Test void testGetTableLocationBeforeInit() {
    assertNull(writer.getTableLocation());
  }

  @Test void testGetEtlPropertyBeforeInit() {
    assertNull(writer.getEtlProperty("test"));
  }

  // ===== initialize tests =====

  @Test void testInitializeNullConfig() {
    assertThrows(IllegalArgumentException.class, () -> writer.initialize(null));
  }

  @Test void testInitializeDisabledConfig() {
    MaterializeConfig config = MaterializeConfig.builder()
        .enabled(false)
        .format(MaterializeConfig.Format.ICEBERG)
        .output(MaterializeOutputConfig.builder().build())
        .build();

    assertThrows(IOException.class, () -> writer.initialize(config));
  }

  @Test void testInitializeWrongFormat() {
    MaterializeConfig config = MaterializeConfig.builder()
        .enabled(true)
        .format(MaterializeConfig.Format.PARQUET)
        .output(MaterializeOutputConfig.builder().build())
        .build();

    assertThrows(IllegalArgumentException.class, () -> writer.initialize(config));
  }

  @Test void testInitializeNoTargetTableId() {
    MaterializeConfig config = MaterializeConfig.builder()
        .enabled(true)
        .format(MaterializeConfig.Format.ICEBERG)
        .output(MaterializeOutputConfig.builder().build())
        .build();

    // Should fail because no target table ID and no name
    assertThrows(IllegalArgumentException.class, () -> writer.initialize(config));
  }

  // ===== writeBatch before initialize =====

  @Test void testWriteBatchNotInitialized() {
    List<Map<String, Object>> data = new ArrayList<Map<String, Object>>();
    data.add(Collections.<String, Object>singletonMap("id", 1));

    assertThrows(IllegalStateException.class,
        () -> writer.writeBatch(data.iterator(), null));
  }

  // ===== commit before initialize =====

  @Test void testCommitNotInitialized() {
    assertThrows(IllegalStateException.class, () -> writer.commit());
  }

  // ===== mapToIcebergType tests (via reflection) =====

  @Test void testMapToIcebergTypeNull() throws Exception {
    Method method = IcebergMaterializationWriter.class.getDeclaredMethod(
        "mapToIcebergType", String.class);
    method.setAccessible(true);
    assertEquals("STRING", method.invoke(writer, (String) null));
  }

  @Test void testMapToIcebergTypeVarchar() throws Exception {
    Method method = IcebergMaterializationWriter.class.getDeclaredMethod(
        "mapToIcebergType", String.class);
    method.setAccessible(true);
    assertEquals("STRING", method.invoke(writer, "VARCHAR"));
    assertEquals("STRING", method.invoke(writer, "VARCHAR(255)"));
  }

  @Test void testMapToIcebergTypeChar() throws Exception {
    Method method = IcebergMaterializationWriter.class.getDeclaredMethod(
        "mapToIcebergType", String.class);
    method.setAccessible(true);
    assertEquals("STRING", method.invoke(writer, "CHAR"));
    assertEquals("STRING", method.invoke(writer, "CHAR(10)"));
  }

  @Test void testMapToIcebergTypeInteger() throws Exception {
    Method method = IcebergMaterializationWriter.class.getDeclaredMethod(
        "mapToIcebergType", String.class);
    method.setAccessible(true);
    assertEquals("INT", method.invoke(writer, "INTEGER"));
    assertEquals("INT", method.invoke(writer, "INT"));
  }

  @Test void testMapToIcebergTypeBigint() throws Exception {
    Method method = IcebergMaterializationWriter.class.getDeclaredMethod(
        "mapToIcebergType", String.class);
    method.setAccessible(true);
    assertEquals("LONG", method.invoke(writer, "BIGINT"));
    assertEquals("LONG", method.invoke(writer, "LONG"));
  }

  @Test void testMapToIcebergTypeDouble() throws Exception {
    Method method = IcebergMaterializationWriter.class.getDeclaredMethod(
        "mapToIcebergType", String.class);
    method.setAccessible(true);
    assertEquals("DOUBLE", method.invoke(writer, "DOUBLE"));
    assertEquals("DOUBLE", method.invoke(writer, "FLOAT"));
  }

  @Test void testMapToIcebergTypeBoolean() throws Exception {
    Method method = IcebergMaterializationWriter.class.getDeclaredMethod(
        "mapToIcebergType", String.class);
    method.setAccessible(true);
    assertEquals("BOOLEAN", method.invoke(writer, "BOOLEAN"));
  }

  @Test void testMapToIcebergTypeDate() throws Exception {
    Method method = IcebergMaterializationWriter.class.getDeclaredMethod(
        "mapToIcebergType", String.class);
    method.setAccessible(true);
    assertEquals("DATE", method.invoke(writer, "DATE"));
  }

  @Test void testMapToIcebergTypeTimestamp() throws Exception {
    Method method = IcebergMaterializationWriter.class.getDeclaredMethod(
        "mapToIcebergType", String.class);
    method.setAccessible(true);
    assertEquals("TIMESTAMP", method.invoke(writer, "TIMESTAMP"));
  }

  @Test void testMapToIcebergTypeUnknown() throws Exception {
    Method method = IcebergMaterializationWriter.class.getDeclaredMethod(
        "mapToIcebergType", String.class);
    method.setAccessible(true);
    assertEquals("STRING", method.invoke(writer, "DECIMAL(15,2)"));
  }

  // ===== mapToDuckDBType tests (via reflection) =====

  @Test void testMapToDuckDBTypeNull() throws Exception {
    Method method = IcebergMaterializationWriter.class.getDeclaredMethod(
        "mapToDuckDBType", String.class);
    method.setAccessible(true);
    assertEquals("VARCHAR", method.invoke(writer, (String) null));
  }

  @Test void testMapToDuckDBTypeVarchar() throws Exception {
    Method method = IcebergMaterializationWriter.class.getDeclaredMethod(
        "mapToDuckDBType", String.class);
    method.setAccessible(true);
    assertEquals("VARCHAR", method.invoke(writer, "VARCHAR"));
    assertEquals("VARCHAR", method.invoke(writer, "VARCHAR(100)"));
    assertEquals("VARCHAR", method.invoke(writer, "STRING"));
  }

  @Test void testMapToDuckDBTypeChar() throws Exception {
    Method method = IcebergMaterializationWriter.class.getDeclaredMethod(
        "mapToDuckDBType", String.class);
    method.setAccessible(true);
    assertEquals("VARCHAR", method.invoke(writer, "CHAR"));
    assertEquals("VARCHAR", method.invoke(writer, "CHAR(5)"));
  }

  @Test void testMapToDuckDBTypeInteger() throws Exception {
    Method method = IcebergMaterializationWriter.class.getDeclaredMethod(
        "mapToDuckDBType", String.class);
    method.setAccessible(true);
    assertEquals("INTEGER", method.invoke(writer, "INTEGER"));
    assertEquals("INTEGER", method.invoke(writer, "INT"));
  }

  @Test void testMapToDuckDBTypeBigint() throws Exception {
    Method method = IcebergMaterializationWriter.class.getDeclaredMethod(
        "mapToDuckDBType", String.class);
    method.setAccessible(true);
    assertEquals("BIGINT", method.invoke(writer, "BIGINT"));
    assertEquals("BIGINT", method.invoke(writer, "LONG"));
  }

  @Test void testMapToDuckDBTypeDouble() throws Exception {
    Method method = IcebergMaterializationWriter.class.getDeclaredMethod(
        "mapToDuckDBType", String.class);
    method.setAccessible(true);
    assertEquals("DOUBLE", method.invoke(writer, "DOUBLE"));
    assertEquals("DOUBLE", method.invoke(writer, "FLOAT"));
  }

  @Test void testMapToDuckDBTypeBoolean() throws Exception {
    Method method = IcebergMaterializationWriter.class.getDeclaredMethod(
        "mapToDuckDBType", String.class);
    method.setAccessible(true);
    assertEquals("BOOLEAN", method.invoke(writer, "BOOLEAN"));
  }

  @Test void testMapToDuckDBTypeDate() throws Exception {
    Method method = IcebergMaterializationWriter.class.getDeclaredMethod(
        "mapToDuckDBType", String.class);
    method.setAccessible(true);
    assertEquals("DATE", method.invoke(writer, "DATE"));
  }

  @Test void testMapToDuckDBTypeTimestamp() throws Exception {
    Method method = IcebergMaterializationWriter.class.getDeclaredMethod(
        "mapToDuckDBType", String.class);
    method.setAccessible(true);
    assertEquals("TIMESTAMP", method.invoke(writer, "TIMESTAMP"));
  }

  @Test void testMapToDuckDBTypeUnknown() throws Exception {
    Method method = IcebergMaterializationWriter.class.getDeclaredMethod(
        "mapToDuckDBType", String.class);
    method.setAccessible(true);
    assertEquals("VARCHAR", method.invoke(writer, "BLOB"));
  }

  // ===== castValue tests (via reflection) =====

  @Test void testCastValueNull() throws Exception {
    Method method = IcebergMaterializationWriter.class.getDeclaredMethod(
        "castValue", Object.class, String.class);
    method.setAccessible(true);
    assertNull(method.invoke(writer, null, "BIGINT"));
  }

  @Test void testCastValueEmptyString() throws Exception {
    Method method = IcebergMaterializationWriter.class.getDeclaredMethod(
        "castValue", Object.class, String.class);
    method.setAccessible(true);
    assertNull(method.invoke(writer, "", "BIGINT"));
  }

  @Test void testCastValueBigint() throws Exception {
    Method method = IcebergMaterializationWriter.class.getDeclaredMethod(
        "castValue", Object.class, String.class);
    method.setAccessible(true);
    assertEquals(42L, method.invoke(writer, "42", "BIGINT"));
    assertEquals(42L, method.invoke(writer, "42", "INT64"));
    assertEquals(42L, method.invoke(writer, "42", "LONG"));
  }

  @Test void testCastValueInteger() throws Exception {
    Method method = IcebergMaterializationWriter.class.getDeclaredMethod(
        "castValue", Object.class, String.class);
    method.setAccessible(true);
    assertEquals(42, method.invoke(writer, "42", "INTEGER"));
    assertEquals(42, method.invoke(writer, "42", "INT"));
    assertEquals(42, method.invoke(writer, "42", "INT32"));
  }

  @Test void testCastValueDouble() throws Exception {
    Method method = IcebergMaterializationWriter.class.getDeclaredMethod(
        "castValue", Object.class, String.class);
    method.setAccessible(true);
    assertEquals(3.14, method.invoke(writer, "3.14", "DOUBLE"));
    assertEquals(3.14, method.invoke(writer, "3.14", "FLOAT8"));
  }

  @Test void testCastValueFloat() throws Exception {
    Method method = IcebergMaterializationWriter.class.getDeclaredMethod(
        "castValue", Object.class, String.class);
    method.setAccessible(true);
    Object result = method.invoke(writer, "3.14", "FLOAT");
    assertTrue(result instanceof Float);
    assertEquals(3.14f, result);

    result = method.invoke(writer, "1.0", "FLOAT4");
    assertTrue(result instanceof Float);

    result = method.invoke(writer, "2.0", "REAL");
    assertTrue(result instanceof Float);
  }

  @Test void testCastValueVarchar() throws Exception {
    Method method = IcebergMaterializationWriter.class.getDeclaredMethod(
        "castValue", Object.class, String.class);
    method.setAccessible(true);
    assertEquals("hello", method.invoke(writer, "hello", "VARCHAR"));
    assertEquals("world", method.invoke(writer, "world", "STRING"));
    assertEquals("test", method.invoke(writer, "test", "TEXT"));
  }

  @Test void testCastValueBoolean() throws Exception {
    Method method = IcebergMaterializationWriter.class.getDeclaredMethod(
        "castValue", Object.class, String.class);
    method.setAccessible(true);
    assertEquals(true, method.invoke(writer, "true", "BOOLEAN"));
    assertEquals(true, method.invoke(writer, "true", "BOOL"));
    assertEquals(false, method.invoke(writer, "false", "BOOLEAN"));
  }

  @Test void testCastValueUnknownType() throws Exception {
    Method method = IcebergMaterializationWriter.class.getDeclaredMethod(
        "castValue", Object.class, String.class);
    method.setAccessible(true);
    assertEquals("42", method.invoke(writer, "42", "UNKNOWN_TYPE"));
  }

  @Test void testCastValueNumberFormatException() throws Exception {
    Method method = IcebergMaterializationWriter.class.getDeclaredMethod(
        "castValue", Object.class, String.class);
    method.setAccessible(true);
    assertNull(method.invoke(writer, "not_a_number", "BIGINT"));
    assertNull(method.invoke(writer, "abc", "INTEGER"));
    assertNull(method.invoke(writer, "xyz", "DOUBLE"));
    assertNull(method.invoke(writer, "nan!", "FLOAT"));
  }

  // ===== evaluateExpression tests (via reflection) =====

  @Test void testEvaluateExpressionNull() throws Exception {
    Method method = IcebergMaterializationWriter.class.getDeclaredMethod(
        "evaluateExpression", String.class, Map.class);
    method.setAccessible(true);
    assertNull(method.invoke(writer, null, new HashMap<String, Object>()));
    assertNull(method.invoke(writer, "", new HashMap<String, Object>()));
  }

  @Test void testEvaluateExpressionSrcField() throws Exception {
    Method method = IcebergMaterializationWriter.class.getDeclaredMethod(
        "evaluateExpression", String.class, Map.class);
    method.setAccessible(true);

    Map<String, Object> row = new HashMap<String, Object>();
    row.put("NAME", "John");

    Object result = method.invoke(writer, "src.\"NAME\"", row);
    assertEquals("John", result);
  }

  @Test void testEvaluateExpressionSrcFieldUnquoted() throws Exception {
    Method method = IcebergMaterializationWriter.class.getDeclaredMethod(
        "evaluateExpression", String.class, Map.class);
    method.setAccessible(true);

    Map<String, Object> row = new HashMap<String, Object>();
    row.put("age", 30);

    Object result = method.invoke(writer, "src.age", row);
    assertEquals(30, result);
  }

  @Test void testEvaluateExpressionBareField() throws Exception {
    Method method = IcebergMaterializationWriter.class.getDeclaredMethod(
        "evaluateExpression", String.class, Map.class);
    method.setAccessible(true);

    Map<String, Object> row = new HashMap<String, Object>();
    row.put("table_name", "users");

    Object result = method.invoke(writer, "table_name", row);
    assertEquals("users", result);
  }

  @Test void testEvaluateExpressionTryCast() throws Exception {
    Method method = IcebergMaterializationWriter.class.getDeclaredMethod(
        "evaluateExpression", String.class, Map.class);
    method.setAccessible(true);

    Map<String, Object> row = new HashMap<String, Object>();
    row.put("value", "42");

    Object result = method.invoke(writer, "TRY_CAST(src.value AS BIGINT)", row);
    assertEquals(42L, result);
  }

  @Test void testEvaluateExpressionCastWithoutSrc() throws Exception {
    Method method = IcebergMaterializationWriter.class.getDeclaredMethod(
        "evaluateExpression", String.class, Map.class);
    method.setAccessible(true);

    Map<String, Object> row = new HashMap<String, Object>();
    row.put("amount", "99");

    Object result = method.invoke(writer, "CAST(amount AS INTEGER)", row);
    assertEquals(99, result);
  }

  @Test void testEvaluateExpressionReplace() throws Exception {
    Method method = IcebergMaterializationWriter.class.getDeclaredMethod(
        "evaluateExpression", String.class, Map.class);
    method.setAccessible(true);

    Map<String, Object> row = new HashMap<String, Object>();
    row.put("number", "1,000");

    Object result = method.invoke(writer, "REPLACE(src.number, ',', '')", row);
    assertEquals("1000", result);
  }

  @Test void testEvaluateExpressionReplaceNullValue() throws Exception {
    Method method = IcebergMaterializationWriter.class.getDeclaredMethod(
        "evaluateExpression", String.class, Map.class);
    method.setAccessible(true);

    Map<String, Object> row = new HashMap<String, Object>();

    Object result = method.invoke(writer, "REPLACE(src.missing_field, ',', '')", row);
    assertNull(result);
  }

  @Test void testEvaluateExpressionCastReplace() throws Exception {
    Method method = IcebergMaterializationWriter.class.getDeclaredMethod(
        "evaluateExpression", String.class, Map.class);
    method.setAccessible(true);

    Map<String, Object> row = new HashMap<String, Object>();
    row.put("value", "1,234");

    Object result = method.invoke(writer, "TRY_CAST(REPLACE(src.value, ',', '') AS BIGINT)", row);
    assertEquals(1234L, result);
  }

  @Test void testEvaluateExpressionCastReplaceNullSource() throws Exception {
    Method method = IcebergMaterializationWriter.class.getDeclaredMethod(
        "evaluateExpression", String.class, Map.class);
    method.setAccessible(true);

    Map<String, Object> row = new HashMap<String, Object>();

    Object result = method.invoke(writer, "TRY_CAST(REPLACE(src.missing, ',', '') AS BIGINT)", row);
    assertNull(result);
  }

  @Test void testEvaluateExpressionSubstring() throws Exception {
    Method method = IcebergMaterializationWriter.class.getDeclaredMethod(
        "evaluateExpression", String.class, Map.class);
    method.setAccessible(true);

    Map<String, Object> row = new HashMap<String, Object>();
    row.put("period", "Q1-2023");

    Object result = method.invoke(writer, "SUBSTRING(src.period, 1, 2)", row);
    assertEquals("Q1", result);
  }

  @Test void testEvaluateExpressionSubstringNullSource() throws Exception {
    Method method = IcebergMaterializationWriter.class.getDeclaredMethod(
        "evaluateExpression", String.class, Map.class);
    method.setAccessible(true);

    Map<String, Object> row = new HashMap<String, Object>();

    Object result = method.invoke(writer, "SUBSTRING(src.missing, 1, 2)", row);
    assertNull(result);
  }

  @Test void testEvaluateExpressionSubstringBeyondLength() throws Exception {
    Method method = IcebergMaterializationWriter.class.getDeclaredMethod(
        "evaluateExpression", String.class, Map.class);
    method.setAccessible(true);

    Map<String, Object> row = new HashMap<String, Object>();
    row.put("short", "AB");

    Object result = method.invoke(writer, "SUBSTRING(src.short, 1, 10)", row);
    assertEquals("AB", result);
  }

  @Test void testEvaluateExpressionRight() throws Exception {
    Method method = IcebergMaterializationWriter.class.getDeclaredMethod(
        "evaluateExpression", String.class, Map.class);
    method.setAccessible(true);

    Map<String, Object> row = new HashMap<String, Object>();
    row.put("code", "US-12345");

    Object result = method.invoke(writer, "RIGHT(src.code, 5)", row);
    assertEquals("12345", result);
  }

  @Test void testEvaluateExpressionRightFullLength() throws Exception {
    Method method = IcebergMaterializationWriter.class.getDeclaredMethod(
        "evaluateExpression", String.class, Map.class);
    method.setAccessible(true);

    Map<String, Object> row = new HashMap<String, Object>();
    row.put("short", "AB");

    Object result = method.invoke(writer, "RIGHT(src.short, 10)", row);
    assertEquals("AB", result);
  }

  @Test void testEvaluateExpressionRightNullSource() throws Exception {
    Method method = IcebergMaterializationWriter.class.getDeclaredMethod(
        "evaluateExpression", String.class, Map.class);
    method.setAccessible(true);

    Map<String, Object> row = new HashMap<String, Object>();

    Object result = method.invoke(writer, "RIGHT(src.missing, 3)", row);
    assertNull(result);
  }

  @Test void testEvaluateExpressionCoalesce() throws Exception {
    Method method = IcebergMaterializationWriter.class.getDeclaredMethod(
        "evaluateExpression", String.class, Map.class);
    method.setAccessible(true);

    Map<String, Object> row = new HashMap<String, Object>();
    row.put("FIELD2", "found");

    Object result = method.invoke(writer, "COALESCE(src.FIELD1, src.FIELD2)", row);
    assertEquals("found", result);
  }

  @Test void testEvaluateExpressionCoalesceAllNull() throws Exception {
    Method method = IcebergMaterializationWriter.class.getDeclaredMethod(
        "evaluateExpression", String.class, Map.class);
    method.setAccessible(true);

    Map<String, Object> row = new HashMap<String, Object>();

    Object result = method.invoke(writer, "COALESCE(src.A, src.B, src.C)", row);
    assertNull(result);
  }

  @Test void testEvaluateExpressionUnrecognized() throws Exception {
    Method method = IcebergMaterializationWriter.class.getDeclaredMethod(
        "evaluateExpression", String.class, Map.class);
    method.setAccessible(true);

    Map<String, Object> row = new HashMap<String, Object>();
    row.put("x", 1);

    Object result = method.invoke(writer, "COMPLEX_FUNCTION(src.x, src.y, 42)", row);
    assertNull(result);
  }

  // ===== getValueCaseInsensitive tests (via reflection) =====

  @Test void testGetValueCaseInsensitiveExactMatch() throws Exception {
    Method method = IcebergMaterializationWriter.class.getDeclaredMethod(
        "getValueCaseInsensitive", Map.class, String.class);
    method.setAccessible(true);

    Map<String, Object> map = new HashMap<String, Object>();
    map.put("name", "value");

    assertEquals("value", method.invoke(writer, map, "name"));
  }

  @Test void testGetValueCaseInsensitiveCaseMatch() throws Exception {
    Method method = IcebergMaterializationWriter.class.getDeclaredMethod(
        "getValueCaseInsensitive", Map.class, String.class);
    method.setAccessible(true);

    Map<String, Object> map = new HashMap<String, Object>();
    map.put("NAME", "value");

    assertEquals("value", method.invoke(writer, map, "name"));
  }

  @Test void testGetValueCaseInsensitiveNullMap() throws Exception {
    Method method = IcebergMaterializationWriter.class.getDeclaredMethod(
        "getValueCaseInsensitive", Map.class, String.class);
    method.setAccessible(true);
    assertNull(method.invoke(writer, null, "key"));
  }

  @Test void testGetValueCaseInsensitiveNullKey() throws Exception {
    Method method = IcebergMaterializationWriter.class.getDeclaredMethod(
        "getValueCaseInsensitive", Map.class, String.class);
    method.setAccessible(true);
    assertNull(method.invoke(writer, new HashMap<String, Object>(), null));
  }

  @Test void testGetValueCaseInsensitiveNotFound() throws Exception {
    Method method = IcebergMaterializationWriter.class.getDeclaredMethod(
        "getValueCaseInsensitive", Map.class, String.class);
    method.setAccessible(true);
    assertNull(method.invoke(writer, new HashMap<String, Object>(), "missing"));
  }

  // ===== buildPartitionKey tests (via reflection) =====

  @Test void testBuildPartitionKeyNull() throws Exception {
    Method method = IcebergMaterializationWriter.class.getDeclaredMethod(
        "buildPartitionKey", Map.class);
    method.setAccessible(true);
    assertEquals("", method.invoke(writer, (Map<String, String>) null));
  }

  @Test void testBuildPartitionKeyEmpty() throws Exception {
    Method method = IcebergMaterializationWriter.class.getDeclaredMethod(
        "buildPartitionKey", Map.class);
    method.setAccessible(true);
    assertEquals("", method.invoke(writer, new HashMap<String, String>()));
  }

  @Test void testBuildPartitionKeySorted() throws Exception {
    Method method = IcebergMaterializationWriter.class.getDeclaredMethod(
        "buildPartitionKey", Map.class);
    method.setAccessible(true);

    Map<String, String> vars = new LinkedHashMap<String, String>();
    vars.put("year", "2020");
    vars.put("country", "US");

    String result = (String) method.invoke(writer, vars);
    // Should be sorted alphabetically: country=US|year=2020
    assertEquals("country=US|year=2020", result);
  }

  @Test void testBuildPartitionKeySingleEntry() throws Exception {
    Method method = IcebergMaterializationWriter.class.getDeclaredMethod(
        "buildPartitionKey", Map.class);
    method.setAccessible(true);

    Map<String, String> vars = new HashMap<String, String>();
    vars.put("region", "west");

    assertEquals("region=west", method.invoke(writer, vars));
  }

  // ===== buildPartitionFilter tests (via reflection) =====

  @Test void testBuildPartitionFilterNull() throws Exception {
    // Need to initialize the writer minimally to set config field
    // Use reflection to set config
    java.lang.reflect.Field configField =
        IcebergMaterializationWriter.class.getDeclaredField("config");
    configField.setAccessible(true);

    List<String> partitionCols = new ArrayList<String>();
    partitionCols.add("year");

    MaterializeConfig config = MaterializeConfig.builder()
        .enabled(true)
        .format(MaterializeConfig.Format.ICEBERG)
        .output(MaterializeOutputConfig.builder().build())
        .partition(MaterializePartitionConfig.builder()
            .columns(partitionCols).build())
        .name("test")
        .build();
    configField.set(writer, config);

    Method method = IcebergMaterializationWriter.class.getDeclaredMethod(
        "buildPartitionFilter", Map.class);
    method.setAccessible(true);

    Map<String, Object> result =
        (Map<String, Object>) method.invoke(writer, (Map<String, String>) null);
    assertTrue(result.isEmpty());
  }

  @Test void testBuildPartitionFilterEmpty() throws Exception {
    java.lang.reflect.Field configField =
        IcebergMaterializationWriter.class.getDeclaredField("config");
    configField.setAccessible(true);

    List<String> partitionCols = new ArrayList<String>();
    partitionCols.add("year");

    MaterializeConfig config = MaterializeConfig.builder()
        .enabled(true)
        .format(MaterializeConfig.Format.ICEBERG)
        .output(MaterializeOutputConfig.builder().build())
        .partition(MaterializePartitionConfig.builder()
            .columns(partitionCols).build())
        .name("test")
        .build();
    configField.set(writer, config);

    Method method = IcebergMaterializationWriter.class.getDeclaredMethod(
        "buildPartitionFilter", Map.class);
    method.setAccessible(true);

    Map<String, Object> result =
        (Map<String, Object>) method.invoke(writer, new HashMap<String, String>());
    assertTrue(result.isEmpty());
  }

  @Test void testBuildPartitionFilterMatching() throws Exception {
    java.lang.reflect.Field configField =
        IcebergMaterializationWriter.class.getDeclaredField("config");
    configField.setAccessible(true);

    List<String> partitionCols = new ArrayList<String>();
    partitionCols.add("year");

    MaterializeConfig config = MaterializeConfig.builder()
        .enabled(true)
        .format(MaterializeConfig.Format.ICEBERG)
        .output(MaterializeOutputConfig.builder().build())
        .partition(MaterializePartitionConfig.builder()
            .columns(partitionCols).build())
        .name("test")
        .build();
    configField.set(writer, config);

    Method method = IcebergMaterializationWriter.class.getDeclaredMethod(
        "buildPartitionFilter", Map.class);
    method.setAccessible(true);

    Map<String, String> vars = new HashMap<String, String>();
    vars.put("year", "2020");
    vars.put("extra", "ignored");

    Map<String, Object> result = (Map<String, Object>) method.invoke(writer, vars);
    assertEquals(1, result.size());
    assertEquals("2020", result.get("year"));
  }

  // ===== convertToS3aScheme tests (via reflection) =====

  @Test void testConvertToS3aSchemeS3() throws Exception {
    Method method = IcebergMaterializationWriter.class.getDeclaredMethod(
        "convertToS3aScheme", String.class);
    method.setAccessible(true);
    assertEquals("s3a://bucket/path", method.invoke(writer, "s3://bucket/path"));
  }

  @Test void testConvertToS3aSchemeAlreadyS3a() throws Exception {
    Method method = IcebergMaterializationWriter.class.getDeclaredMethod(
        "convertToS3aScheme", String.class);
    method.setAccessible(true);
    assertEquals("s3a://bucket/path", method.invoke(writer, "s3a://bucket/path"));
  }

  @Test void testConvertToS3aSchemeLocalPath() throws Exception {
    Method method = IcebergMaterializationWriter.class.getDeclaredMethod(
        "convertToS3aScheme", String.class);
    method.setAccessible(true);
    assertEquals("/local/path", method.invoke(writer, "/local/path"));
  }

  @Test void testConvertToS3aSchemeNull() throws Exception {
    Method method = IcebergMaterializationWriter.class.getDeclaredMethod(
        "convertToS3aScheme", String.class);
    method.setAccessible(true);
    assertNull(method.invoke(writer, (String) null));
  }

  // ===== buildHadoopS3Config tests (via reflection) =====

  @Test void testBuildHadoopS3Config() throws Exception {
    Method method = IcebergMaterializationWriter.class.getDeclaredMethod(
        "buildHadoopS3Config", Map.class);
    method.setAccessible(true);

    Map<String, String> s3Config = new HashMap<String, String>();
    s3Config.put("accessKeyId", "AK123");
    s3Config.put("secretAccessKey", "SK456");
    s3Config.put("endpoint", "http://localhost:9000");
    s3Config.put("region", "us-east-1");

    Map<String, String> result = (Map<String, String>) method.invoke(writer, s3Config);
    assertEquals("AK123", result.get("fs.s3a.access.key"));
    assertEquals("SK456", result.get("fs.s3a.secret.key"));
    assertEquals("http://localhost:9000", result.get("fs.s3a.endpoint"));
    assertEquals("true", result.get("fs.s3a.path.style.access"));
    assertEquals("us-east-1", result.get("fs.s3a.endpoint.region"));
    assertEquals("org.apache.hadoop.fs.s3a.S3AFileSystem", result.get("fs.s3a.impl"));
  }

  @Test void testBuildHadoopS3ConfigPartial() throws Exception {
    Method method = IcebergMaterializationWriter.class.getDeclaredMethod(
        "buildHadoopS3Config", Map.class);
    method.setAccessible(true);

    Map<String, String> s3Config = new HashMap<String, String>();
    s3Config.put("accessKeyId", "AK");

    Map<String, String> result = (Map<String, String>) method.invoke(writer, s3Config);
    assertEquals("AK", result.get("fs.s3a.access.key"));
    assertNull(result.get("fs.s3a.secret.key"));
    assertNull(result.get("fs.s3a.endpoint"));
    assertNull(result.get("fs.s3a.endpoint.region"));
  }

  // ===== escapeString tests (via reflection) =====

  @Test void testEscapeString() throws Exception {
    Method method = IcebergMaterializationWriter.class.getDeclaredMethod(
        "escapeString", String.class);
    method.setAccessible(true);
    assertEquals("hello''world", method.invoke(null, "hello'world"));
    assertEquals("no quotes", method.invoke(null, "no quotes"));
    assertEquals("''''", method.invoke(null, "''"));
  }

  // ===== getRemoteParentPath tests (via reflection) =====

  @Test void testGetRemoteParentPath() throws Exception {
    Method method = IcebergMaterializationWriter.class.getDeclaredMethod(
        "getRemoteParentPath", String.class);
    method.setAccessible(true);
    assertEquals("/data/warehouse", method.invoke(writer, "/data/warehouse/file.parquet"));
  }

  @Test void testGetRemoteParentPathNoSlash() throws Exception {
    Method method = IcebergMaterializationWriter.class.getDeclaredMethod(
        "getRemoteParentPath", String.class);
    method.setAccessible(true);
    assertNull(method.invoke(writer, "file.parquet"));
  }

  @Test void testGetRemoteParentPathS3Prefix() throws Exception {
    Method method = IcebergMaterializationWriter.class.getDeclaredMethod(
        "getRemoteParentPath", String.class);
    method.setAccessible(true);
    assertNull(method.invoke(writer, "s3://bucket"));
  }

  @Test void testGetRemoteParentPathS3aPrefix() throws Exception {
    Method method = IcebergMaterializationWriter.class.getDeclaredMethod(
        "getRemoteParentPath", String.class);
    method.setAccessible(true);
    assertNull(method.invoke(writer, "s3a://bucket"));
  }

  @Test void testGetRemoteParentPathS3Deep() throws Exception {
    Method method = IcebergMaterializationWriter.class.getDeclaredMethod(
        "getRemoteParentPath", String.class);
    method.setAccessible(true);
    assertEquals("s3://bucket/prefix",
        method.invoke(writer, "s3://bucket/prefix/file.parquet"));
  }

  // ===== buildCatalogConfig tests (via reflection) =====

  @Test void testBuildCatalogConfigNullIcebergConfig() throws Exception {
    Method method = IcebergMaterializationWriter.class.getDeclaredMethod(
        "buildCatalogConfig", MaterializeConfig.IcebergConfig.class);
    method.setAccessible(true);

    Map<String, Object> result =
        (Map<String, Object>) method.invoke(writer, (MaterializeConfig.IcebergConfig) null);
    assertEquals("hadoop", result.get("catalog"));
  }

  @Test void testBuildCatalogConfigHadoop() throws Exception {
    Method method = IcebergMaterializationWriter.class.getDeclaredMethod(
        "buildCatalogConfig", MaterializeConfig.IcebergConfig.class);
    method.setAccessible(true);

    MaterializeConfig.IcebergConfig icebergConfig =
        MaterializeConfig.IcebergConfig.builder()
            .catalogType(MaterializeConfig.IcebergConfig.CatalogType.HADOOP)
            .warehousePath("/warehouse")
            .build();

    Map<String, Object> result =
        (Map<String, Object>) method.invoke(writer, icebergConfig);
    assertEquals("hadoop", result.get("catalog"));
    assertEquals("/warehouse", result.get("warehousePath"));
  }

  @Test void testBuildCatalogConfigRest() throws Exception {
    Method method = IcebergMaterializationWriter.class.getDeclaredMethod(
        "buildCatalogConfig", MaterializeConfig.IcebergConfig.class);
    method.setAccessible(true);

    MaterializeConfig.IcebergConfig icebergConfig =
        MaterializeConfig.IcebergConfig.builder()
            .catalogType(MaterializeConfig.IcebergConfig.CatalogType.REST)
            .restUri("http://localhost:8181")
            .build();

    Map<String, Object> result =
        (Map<String, Object>) method.invoke(writer, icebergConfig);
    assertEquals("rest", result.get("catalog"));
    assertEquals("http://localhost:8181", result.get("uri"));
  }

  @Test void testBuildCatalogConfigHive() throws Exception {
    Method method = IcebergMaterializationWriter.class.getDeclaredMethod(
        "buildCatalogConfig", MaterializeConfig.IcebergConfig.class);
    method.setAccessible(true);

    MaterializeConfig.IcebergConfig icebergConfig =
        MaterializeConfig.IcebergConfig.builder()
            .catalogType(MaterializeConfig.IcebergConfig.CatalogType.HIVE)
            .build();

    Map<String, Object> result =
        (Map<String, Object>) method.invoke(writer, icebergConfig);
    assertEquals("hive", result.get("catalog"));
  }

  @Test void testBuildCatalogConfigS3Conversion() throws Exception {
    Method method = IcebergMaterializationWriter.class.getDeclaredMethod(
        "buildCatalogConfig", MaterializeConfig.IcebergConfig.class);
    method.setAccessible(true);

    MaterializeConfig.IcebergConfig icebergConfig =
        MaterializeConfig.IcebergConfig.builder()
            .warehousePath("s3://bucket/warehouse")
            .build();

    Map<String, Object> result =
        (Map<String, Object>) method.invoke(writer, icebergConfig);
    assertEquals("s3a://bucket/warehouse", result.get("warehousePath"));
  }

  // ===== transformRows tests (via reflection) =====

  @Test void testTransformRowsNoColumns() throws Exception {
    java.lang.reflect.Field configField =
        IcebergMaterializationWriter.class.getDeclaredField("config");
    configField.setAccessible(true);

    MaterializeConfig config = MaterializeConfig.builder()
        .enabled(true)
        .format(MaterializeConfig.Format.ICEBERG)
        .output(MaterializeOutputConfig.builder().build())
        .name("test")
        .build();
    configField.set(writer, config);

    Method method = IcebergMaterializationWriter.class.getDeclaredMethod(
        "transformRows", List.class, Map.class);
    method.setAccessible(true);

    List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
    Map<String, Object> row = new HashMap<String, Object>();
    row.put("id", 1);
    rows.add(row);

    List<Map<String, Object>> result =
        (List<Map<String, Object>>) method.invoke(writer, rows, null);
    // No columns means no transformation
    assertEquals(1, result.size());
    assertEquals(1, result.get(0).get("id"));
  }

  @Test void testTransformRowsWithDirectColumn() throws Exception {
    java.lang.reflect.Field configField =
        IcebergMaterializationWriter.class.getDeclaredField("config");
    configField.setAccessible(true);

    List<ColumnConfig> columns = new ArrayList<ColumnConfig>();
    columns.add(new ColumnConfig.Builder().name("output_id").source("id").build());

    MaterializeConfig config = MaterializeConfig.builder()
        .enabled(true)
        .format(MaterializeConfig.Format.ICEBERG)
        .output(MaterializeOutputConfig.builder().build())
        .columns(columns)
        .name("test")
        .build();
    configField.set(writer, config);

    Method method = IcebergMaterializationWriter.class.getDeclaredMethod(
        "transformRows", List.class, Map.class);
    method.setAccessible(true);

    List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
    Map<String, Object> row = new HashMap<String, Object>();
    row.put("id", 42);
    rows.add(row);

    List<Map<String, Object>> result =
        (List<Map<String, Object>>) method.invoke(writer, rows, null);
    assertEquals(1, result.size());
    assertEquals(42, result.get(0).get("output_id"));
  }

  @Test void testTransformRowsWithPartitionVariables() throws Exception {
    java.lang.reflect.Field configField =
        IcebergMaterializationWriter.class.getDeclaredField("config");
    configField.setAccessible(true);

    List<ColumnConfig> columns = new ArrayList<ColumnConfig>();
    columns.add(new ColumnConfig.Builder().name("name").build());

    MaterializeConfig config = MaterializeConfig.builder()
        .enabled(true)
        .format(MaterializeConfig.Format.ICEBERG)
        .output(MaterializeOutputConfig.builder().build())
        .columns(columns)
        .name("test")
        .build();
    configField.set(writer, config);

    Method method = IcebergMaterializationWriter.class.getDeclaredMethod(
        "transformRows", List.class, Map.class);
    method.setAccessible(true);

    List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
    Map<String, Object> row = new HashMap<String, Object>();
    row.put("name", "Alice");
    rows.add(row);

    Map<String, String> partVars = new HashMap<String, String>();
    partVars.put("year", "2020");

    List<Map<String, Object>> result =
        (List<Map<String, Object>>) method.invoke(writer, rows, partVars);
    assertEquals(1, result.size());
    assertEquals("Alice", result.get(0).get("name"));
    assertEquals("2020", result.get(0).get("year"));
  }

  @Test void testTransformRowsComputedColumnWithPartitionVar() throws Exception {
    java.lang.reflect.Field configField =
        IcebergMaterializationWriter.class.getDeclaredField("config");
    configField.setAccessible(true);

    List<ColumnConfig> columns = new ArrayList<ColumnConfig>();
    columns.add(new ColumnConfig.Builder().name("year").expression("{year}").build());

    MaterializeConfig config = MaterializeConfig.builder()
        .enabled(true)
        .format(MaterializeConfig.Format.ICEBERG)
        .output(MaterializeOutputConfig.builder().build())
        .columns(columns)
        .name("test")
        .build();
    configField.set(writer, config);

    Method method = IcebergMaterializationWriter.class.getDeclaredMethod(
        "transformRows", List.class, Map.class);
    method.setAccessible(true);

    List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
    rows.add(new HashMap<String, Object>());

    Map<String, String> partVars = new HashMap<String, String>();
    partVars.put("year", "2020");

    List<Map<String, Object>> result =
        (List<Map<String, Object>>) method.invoke(writer, rows, partVars);
    assertEquals(1, result.size());
    assertEquals("2020", result.get(0).get("year"));
  }

  // ===== close tests =====

  @Test void testCloseWithoutInit() throws Exception {
    // Should not throw
    writer.close();
  }

  @Test void testGetEnvInt() throws Exception {
    Method method = IcebergMaterializationWriter.class.getDeclaredMethod(
        "getEnvInt", String.class, int.class);
    method.setAccessible(true);
    // Nonexistent env var should return default
    int result = (Integer) method.invoke(null, "NONEXISTENT_TEST_VAR_XYZ", 42);
    assertEquals(42, result);
  }

  // ===== cleanupStagingDirectory for S3 path =====

  @Test void testCleanupStagingDirectoryS3Path() throws Exception {
    // Set staging mode to REMOTE
    java.lang.reflect.Field modeField =
        IcebergMaterializationWriter.class.getDeclaredField("stagingMode");
    modeField.setAccessible(true);
    modeField.set(writer, MaterializeOptionsConfig.StagingMode.REMOTE);

    Method method = IcebergMaterializationWriter.class.getDeclaredMethod(
        "cleanupStagingDirectory", String.class);
    method.setAccessible(true);
    // S3 paths should be skipped (lifecycle rule handles it)
    method.invoke(writer, "s3://bucket/staging/test");
  }

  @Test void testCleanupStagingDirectoryS3aPath() throws Exception {
    java.lang.reflect.Field modeField =
        IcebergMaterializationWriter.class.getDeclaredField("stagingMode");
    modeField.setAccessible(true);
    modeField.set(writer, MaterializeOptionsConfig.StagingMode.REMOTE);

    Method method = IcebergMaterializationWriter.class.getDeclaredMethod(
        "cleanupStagingDirectory", String.class);
    method.setAccessible(true);
    method.invoke(writer, "s3a://bucket/staging/test");
  }

  @Test void testCleanupStagingDirectoryLocalPath() throws Exception {
    java.lang.reflect.Field modeField =
        IcebergMaterializationWriter.class.getDeclaredField("stagingMode");
    modeField.setAccessible(true);
    modeField.set(writer, MaterializeOptionsConfig.StagingMode.LOCAL);

    Method method = IcebergMaterializationWriter.class.getDeclaredMethod(
        "cleanupStagingDirectory", String.class);
    method.setAccessible(true);

    // Create a local staging dir to clean up
    Path stagingDir = tempDir.resolve("staging_cleanup");
    java.nio.file.Files.createDirectories(stagingDir);
    java.nio.file.Files.write(stagingDir.resolve("test.json"),
        "test".getBytes(java.nio.charset.StandardCharsets.UTF_8));

    method.invoke(writer, stagingDir.toString());
    assertFalse(java.nio.file.Files.exists(stagingDir));
  }
}
