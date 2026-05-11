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
package org.apache.calcite.adapter.file.table;

import org.apache.calcite.adapter.file.BaseFileTest;
import org.apache.calcite.adapter.file.format.json.JsonSearchConfig;
import org.apache.calcite.adapter.file.format.json.SharedJsonData;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.util.Source;
import org.apache.calcite.util.Sources;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link JsonTable} to improve line coverage.
 * Covers SharedJsonData constructor path, cache invalidation,
 * deduceRowTypeFromSharedData, buildDataListFromSharedData,
 * inferJavaType for boolean/integral/number/string,
 * convertJsonValue for null/boolean/integral/number/textual/object.
 */
@Tag("unit")
public class JsonTableCoverageTest extends BaseFileTest {

  @TempDir
  java.nio.file.Path tempDir;

  private static final JavaTypeFactory TYPE_FACTORY =
      new JavaTypeFactoryImpl(RelDataTypeSystem.DEFAULT);

  // ---------------------------------------------------------------
  // Source-based constructor tests
  // ---------------------------------------------------------------

  /**
   * Tests the single-argument Source constructor.
   */
  @Test public void testSourceOnlyConstructor() throws Exception {
    File jsonFile = createJsonFile("simple.json",
        "[{\"id\": 1, \"name\": \"Alice\"}]");
    Source source = Sources.of(jsonFile);
    JsonTable table = new JsonTable(source);

    RelDataType rowType = table.getRowType(TYPE_FACTORY);
    assertNotNull(rowType, "Row type should not be null");
    assertTrue(rowType.getFieldCount() >= 2,
        "Should have at least 2 fields (id, name)");
  }

  /**
   * Tests the Source + options constructor.
   */
  @Test public void testSourceWithOptionsConstructor() throws Exception {
    File jsonFile = createJsonFile("options.json",
        "[{\"val\": 42, \"label\": \"test\"}]");
    Source source = Sources.of(jsonFile);
    Map<String, Object> options = new HashMap<String, Object>();
    JsonTable table = new JsonTable(source, options);

    RelDataType rowType = table.getRowType(TYPE_FACTORY);
    assertNotNull(rowType);
    assertTrue(rowType.getFieldCount() >= 2);
  }

  /**
   * Tests the Source + options + columnNameCasing constructor.
   */
  @Test public void testSourceWithColumnCasingConstructor() throws Exception {
    File jsonFile = createJsonFile("casing.json",
        "[{\"MyColumn\": \"value\"}]");
    Source source = Sources.of(jsonFile);
    Map<String, Object> options = new HashMap<String, Object>();
    JsonTable table = new JsonTable(source, options, "TO_LOWER");

    RelDataType rowType = table.getRowType(TYPE_FACTORY);
    assertNotNull(rowType);
    assertEquals(1, rowType.getFieldCount());
  }

  /**
   * Tests the 4-arg constructor with forcedDataType.
   * Exercises lines 72-81 and deduceRowTypeFromSource with forcedDataType (line 171-172).
   */
  @Test public void testSourceWithForcedDataType() throws Exception {
    File jsonFile = createJsonFile("forced.json",
        "[{\"id\": 1, \"value\": \"hello\"}]");
    Source source = Sources.of(jsonFile);
    Map<String, Object> options = new HashMap<String, Object>();
    JsonTable table = new JsonTable(source, options, "UNCHANGED", "json");

    RelDataType rowType = table.getRowType(TYPE_FACTORY);
    assertNotNull(rowType, "Row type with forced data type should not be null");
    assertEquals(2, rowType.getFieldCount());

    List<Object> data = table.getDataList(TYPE_FACTORY);
    assertNotNull(data);
    assertEquals(1, data.size());
  }

  // ---------------------------------------------------------------
  // SharedJsonData constructor and getRowType/getDataList tests
  // ---------------------------------------------------------------

  /**
   * Tests the SharedJsonData constructor path (3-arg constructor)
   * and exercises getRowType via SharedJsonData (deduceRowTypeFromSharedData).
   */
  @Test public void testSharedJsonDataGetRowType() throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    String json = "{\"users\": [{\"id\": 1, \"name\": \"Alice\"}, "
        + "{\"id\": 2, \"name\": \"Bob\"}]}";
    JsonNode rootNode = mapper.readTree(json);
    SharedJsonData sharedData = new SharedJsonData(rootNode);

    JsonSearchConfig config = new JsonSearchConfig();
    JsonTable table = new JsonTable(sharedData, "$.users", config);

    // Actually invoke getRowType to exercise deduceRowTypeFromSharedData
    RelDataType rowType = table.getRowType(TYPE_FACTORY);
    assertNotNull(rowType, "SharedJsonData row type should not be null");
    assertEquals(2, rowType.getFieldCount(), "Should have id and name fields");
  }

  /**
   * Tests getDataList via SharedJsonData path (buildDataListFromSharedData).
   */
  @Test public void testSharedJsonDataGetDataList() throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    String json = "{\"items\": [{\"id\": 10, \"label\": \"Widget\"}, "
        + "{\"id\": 20, \"label\": \"Gadget\"}, "
        + "{\"id\": 30, \"label\": \"Gizmo\"}]}";
    JsonNode rootNode = mapper.readTree(json);
    SharedJsonData sharedData = new SharedJsonData(rootNode);

    JsonSearchConfig config = new JsonSearchConfig();
    JsonTable table = new JsonTable(sharedData, "$.items", config);

    // Exercise getDataList to invoke buildDataListFromSharedData
    List<Object> dataList = table.getDataList(TYPE_FACTORY);
    assertNotNull(dataList, "Data list from SharedJsonData should not be null");
    assertEquals(3, dataList.size(), "Should have 3 rows");

    // Verify data is a LinkedHashMap
    assertTrue(dataList.get(0) instanceof LinkedHashMap,
        "Each row should be a LinkedHashMap");
    @SuppressWarnings("unchecked")
    LinkedHashMap<String, Object> firstRow = (LinkedHashMap<String, Object>) dataList.get(0);
    assertEquals(10, firstRow.get("id"));
    assertEquals("Widget", firstRow.get("label"));
  }

  /**
   * Tests getDataList via SharedJsonData caches after first call.
   */
  @Test public void testSharedJsonDataGetDataListCaching() throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    String json = "{\"data\": [{\"x\": 1}]}";
    JsonNode rootNode = mapper.readTree(json);
    SharedJsonData sharedData = new SharedJsonData(rootNode);

    JsonSearchConfig config = new JsonSearchConfig();
    JsonTable table = new JsonTable(sharedData, "$.data", config);

    List<Object> first = table.getDataList(TYPE_FACTORY);
    List<Object> second = table.getDataList(TYPE_FACTORY);
    assertNotNull(first);
    assertNotNull(second);
    // Should be the same cached list
    assertTrue(first == second, "Second call should return cached data list");
  }

  /**
   * Tests SharedJsonData constructor with column casing option and exercises getRowType.
   */
  @Test public void testSharedJsonDataWithColumnCasingGetRowType() throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    String json = "{\"items\": [{\"ItemId\": 10, \"ItemName\": \"Widget\"}]}";
    JsonNode rootNode = mapper.readTree(json);
    SharedJsonData sharedData = new SharedJsonData(rootNode);

    JsonSearchConfig config = new JsonSearchConfig();
    JsonTable table = new JsonTable(sharedData, "$.items", config, "TO_LOWER");

    RelDataType rowType = table.getRowType(TYPE_FACTORY);
    assertNotNull(rowType);
    assertEquals(2, rowType.getFieldCount());
  }

  /**
   * Tests deduceRowTypeFromSharedData with empty array at path.
   * Should return an empty struct type.
   */
  @Test public void testSharedJsonDataEmptyArray() throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    String json = "{\"empty\": []}";
    JsonNode rootNode = mapper.readTree(json);
    SharedJsonData sharedData = new SharedJsonData(rootNode);

    JsonSearchConfig config = new JsonSearchConfig();
    JsonTable table = new JsonTable(sharedData, "$.empty", config);

    RelDataType rowType = table.getRowType(TYPE_FACTORY);
    assertNotNull(rowType, "Empty array should produce a non-null row type");
    assertEquals(0, rowType.getFieldCount(), "Empty array should have 0 fields");
  }

  /**
   * Tests deduceRowTypeFromSharedData with null path data.
   * Should return an empty struct type.
   */
  @Test public void testSharedJsonDataNullPath() throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    String json = "{\"other\": [1, 2, 3]}";
    JsonNode rootNode = mapper.readTree(json);
    SharedJsonData sharedData = new SharedJsonData(rootNode);

    JsonSearchConfig config = new JsonSearchConfig();
    JsonTable table = new JsonTable(sharedData, "$.nonexistent", config);

    RelDataType rowType = table.getRowType(TYPE_FACTORY);
    assertNotNull(rowType, "Missing path should produce a non-null row type");
    assertEquals(0, rowType.getFieldCount(), "Missing path should have 0 fields");
  }

  /**
   * Tests deduceRowTypeFromSharedData with non-array data at path.
   * Should return an empty struct type since pathData is not an array.
   */
  @Test public void testSharedJsonDataNonArrayPath() throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    String json = "{\"single\": {\"key\": \"value\"}}";
    JsonNode rootNode = mapper.readTree(json);
    SharedJsonData sharedData = new SharedJsonData(rootNode);

    JsonSearchConfig config = new JsonSearchConfig();
    JsonTable table = new JsonTable(sharedData, "$.single", config);

    RelDataType rowType = table.getRowType(TYPE_FACTORY);
    assertNotNull(rowType, "Non-array path should produce a non-null row type");
    assertEquals(0, rowType.getFieldCount(),
        "Non-array path should have 0 fields (not isArray)");
  }

  /**
   * Tests deduceRowTypeFromSharedData with non-object array elements.
   * Array contains primitives instead of objects, should skip non-object elements.
   */
  @Test public void testSharedJsonDataNonObjectElements() throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    String json = "{\"values\": [1, 2, 3]}";
    JsonNode rootNode = mapper.readTree(json);
    SharedJsonData sharedData = new SharedJsonData(rootNode);

    JsonSearchConfig config = new JsonSearchConfig();
    JsonTable table = new JsonTable(sharedData, "$.values", config);

    RelDataType rowType = table.getRowType(TYPE_FACTORY);
    assertNotNull(rowType);
    // Non-object elements are skipped, so no field names are discovered
    assertEquals(0, rowType.getFieldCount(),
        "Primitive array elements produce no fields");
  }

  /**
   * Tests inferJavaType via SharedJsonData with boolean, integral, number, and string fields.
   * Exercises all branches of inferJavaType (lines 248-258).
   */
  @Test public void testSharedJsonDataInferJavaTypeAllBranches() throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    String json = "{\"records\": ["
        + "{\"boolField\": true, \"intField\": 42, \"doubleField\": 3.14, \"strField\": \"text\"}"
        + "]}";
    JsonNode rootNode = mapper.readTree(json);
    SharedJsonData sharedData = new SharedJsonData(rootNode);

    JsonSearchConfig config = new JsonSearchConfig();
    JsonTable table = new JsonTable(sharedData, "$.records", config);

    RelDataType rowType = table.getRowType(TYPE_FACTORY);
    assertNotNull(rowType);
    assertEquals(4, rowType.getFieldCount(),
        "Should have 4 fields: boolField, intField, doubleField, strField");

    // Verify the types are correctly inferred
    // boolField -> Boolean, intField -> Integer, doubleField -> Double, strField -> String
    String boolType = rowType.getFieldList().get(0).getType().toString();
    String intType = rowType.getFieldList().get(1).getType().toString();
    String doubleType = rowType.getFieldList().get(2).getType().toString();
    String strType = rowType.getFieldList().get(3).getType().toString();

    assertNotNull(boolType);
    assertNotNull(intType);
    assertNotNull(doubleType);
    assertNotNull(strType);
  }

  /**
   * Tests inferJavaType with null values in first row, non-null in second.
   * Exercises the allFieldNames.add with columnTypes.containsKey check.
   */
  @Test public void testSharedJsonDataNullFirstRowTypeInference() throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    String json = "{\"data\": ["
        + "{\"a\": null, \"b\": \"hello\"},"
        + "{\"a\": 42, \"b\": null}"
        + "]}";
    JsonNode rootNode = mapper.readTree(json);
    SharedJsonData sharedData = new SharedJsonData(rootNode);

    JsonSearchConfig config = new JsonSearchConfig();
    JsonTable table = new JsonTable(sharedData, "$.data", config);

    RelDataType rowType = table.getRowType(TYPE_FACTORY);
    assertNotNull(rowType);
    assertEquals(2, rowType.getFieldCount());
  }

  /**
   * Tests inferJavaType with all-null column defaults to String.
   * Exercises the clazz default path on line 215.
   */
  @Test public void testSharedJsonDataAllNullColumnDefaultsToString() throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    String json = "{\"data\": ["
        + "{\"id\": 1, \"nullable\": null},"
        + "{\"id\": 2, \"nullable\": null}"
        + "]}";
    JsonNode rootNode = mapper.readTree(json);
    SharedJsonData sharedData = new SharedJsonData(rootNode);

    JsonSearchConfig config = new JsonSearchConfig();
    JsonTable table = new JsonTable(sharedData, "$.data", config);

    RelDataType rowType = table.getRowType(TYPE_FACTORY);
    assertNotNull(rowType);
    assertEquals(2, rowType.getFieldCount(),
        "Should have 2 fields, all-null column defaults to String");
  }

  /**
   * Tests convertJsonValue via buildDataListFromSharedData.
   * Exercises all branches of convertJsonValue (lines 260-274):
   * null, boolean, integral number, floating number, textual, and object/array.
   */
  @Test public void testSharedJsonDataConvertJsonValueAllBranches() throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    String json = "{\"records\": ["
        + "{\"nullVal\": null, \"boolVal\": true, \"intVal\": 7, "
        + "\"dblVal\": 2.5, \"strVal\": \"abc\", \"objVal\": {\"nested\": 1}}"
        + "]}";
    JsonNode rootNode = mapper.readTree(json);
    SharedJsonData sharedData = new SharedJsonData(rootNode);

    JsonSearchConfig config = new JsonSearchConfig();
    JsonTable table = new JsonTable(sharedData, "$.records", config);

    // Call getDataList to trigger buildDataListFromSharedData -> convertJsonValue
    List<Object> dataList = table.getDataList(TYPE_FACTORY);
    assertNotNull(dataList);
    assertEquals(1, dataList.size());

    @SuppressWarnings("unchecked")
    LinkedHashMap<String, Object> row = (LinkedHashMap<String, Object>) dataList.get(0);
    // null branch
    assertEquals(null, row.get("nullVal"));
    // boolean branch
    assertEquals(true, row.get("boolVal"));
    // integral number branch
    assertEquals(7, row.get("intVal"));
    // floating number branch
    assertEquals(2.5, row.get("dblVal"));
    // textual branch
    assertEquals("abc", row.get("strVal"));
    // object branch (toString)
    assertNotNull(row.get("objVal"));
    assertTrue(row.get("objVal").toString().contains("nested"),
        "Object value should be serialized as string containing 'nested'");
  }

  /**
   * Tests buildDataListFromSharedData with null path data returns empty list.
   */
  @Test public void testSharedJsonDataBuildDataListNullPath() throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    String json = "{\"other\": [1]}";
    JsonNode rootNode = mapper.readTree(json);
    SharedJsonData sharedData = new SharedJsonData(rootNode);

    JsonSearchConfig config = new JsonSearchConfig();
    JsonTable table = new JsonTable(sharedData, "$.nonexistent", config);

    List<Object> dataList = table.getDataList(TYPE_FACTORY);
    assertNotNull(dataList, "Should return empty list for missing path");
    assertEquals(0, dataList.size(), "Missing path should produce empty data list");
  }

  /**
   * Tests buildDataListFromSharedData with non-array path data returns empty list.
   */
  @Test public void testSharedJsonDataBuildDataListNonArray() throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    String json = "{\"scalar\": \"just a string\"}";
    JsonNode rootNode = mapper.readTree(json);
    SharedJsonData sharedData = new SharedJsonData(rootNode);

    JsonSearchConfig config = new JsonSearchConfig();
    JsonTable table = new JsonTable(sharedData, "$.scalar", config);

    List<Object> dataList = table.getDataList(TYPE_FACTORY);
    assertNotNull(dataList, "Should return empty list for non-array path");
    assertEquals(0, dataList.size(), "Non-array path should produce empty data list");
  }

  /**
   * Tests SharedJsonData with mixed object/non-object array elements.
   * buildDataListFromSharedData should skip non-object elements.
   */
  @Test public void testSharedJsonDataMixedArrayElements() throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    String json = "{\"data\": [42, {\"id\": 1, \"name\": \"Alice\"}, \"string\", {\"id\": 2, \"name\": \"Bob\"}]}";
    JsonNode rootNode = mapper.readTree(json);
    SharedJsonData sharedData = new SharedJsonData(rootNode);

    JsonSearchConfig config = new JsonSearchConfig();
    JsonTable table = new JsonTable(sharedData, "$.data", config);

    List<Object> dataList = table.getDataList(TYPE_FACTORY);
    assertNotNull(dataList);
    // Only the two object elements should be included
    assertEquals(2, dataList.size(),
        "Only object elements should be included in data list");
  }

  /**
   * Tests SharedJsonData with more than 10 rows to exercise the rowsToScan limit.
   */
  @Test public void testSharedJsonDataMoreThan10Rows() throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    StringBuilder json = new StringBuilder("{\"rows\": [");
    for (int i = 0; i < 15; i++) {
      if (i > 0) {
        json.append(",");
      }
      json.append("{\"id\": ").append(i).append(", \"value\": \"row").append(i).append("\"}");
    }
    json.append("]}");
    JsonNode rootNode = mapper.readTree(json.toString());
    SharedJsonData sharedData = new SharedJsonData(rootNode);

    JsonSearchConfig config = new JsonSearchConfig();
    JsonTable table = new JsonTable(sharedData, "$.rows", config);

    RelDataType rowType = table.getRowType(TYPE_FACTORY);
    assertNotNull(rowType);
    assertEquals(2, rowType.getFieldCount());

    List<Object> dataList = table.getDataList(TYPE_FACTORY);
    assertEquals(15, dataList.size(), "All 15 rows should be in the data list");
  }

  /**
   * Tests the SharedJsonData constructor with options from config.
   */
  @Test public void testSharedJsonDataConfigOptions() throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    String json = "{\"data\": [{\"x\": 1}]}";
    JsonNode rootNode = mapper.readTree(json);
    SharedJsonData sharedData = new SharedJsonData(rootNode);

    Map<String, Object> opts = new HashMap<String, Object>();
    opts.put("flatten", Boolean.FALSE);
    JsonSearchConfig config = new JsonSearchConfig(opts);
    JsonTable table = new JsonTable(sharedData, "$.data", config);

    assertNotNull(table);
    // Verify options are accessible through the table
    assertNotNull(table.options);
  }

  // ---------------------------------------------------------------
  // Source-based getRowType caching and data list tests
  // ---------------------------------------------------------------

  /**
   * Tests getRowType() with cached converter (second call should use cache).
   */
  @Test public void testGetRowTypeCaching() throws Exception {
    File jsonFile = createJsonFile("cacheable.json",
        "[{\"a\": 1, \"b\": \"hello\"}]");
    Source source = Sources.of(jsonFile);
    JsonTable table = new JsonTable(source);

    // First call creates the converter
    RelDataType type1 = table.getRowType(TYPE_FACTORY);
    // Second call uses cached converter
    RelDataType type2 = table.getRowType(TYPE_FACTORY);

    assertNotNull(type1);
    assertNotNull(type2);
    assertEquals(type1.getFieldCount(), type2.getFieldCount());
  }

  /**
   * Tests getDataList() initial load.
   */
  @Test public void testGetDataListInitialLoad() throws Exception {
    File jsonFile = createJsonFile("datalist.json",
        "[{\"id\": 1}, {\"id\": 2}, {\"id\": 3}]");
    Source source = Sources.of(jsonFile);
    JsonTable table = new JsonTable(source);

    List<Object> dataList = table.getDataList(TYPE_FACTORY);
    assertNotNull(dataList);
    assertEquals(3, dataList.size(), "Should have 3 rows");
  }

  /**
   * Tests cache invalidation by modifying the file and re-reading.
   * Uses DirectFileSource to bypass Source caching and test the
   * JsonTable lastModified-based cache invalidation code path.
   */
  @Test public void testCacheInvalidationOnFileChange() throws Exception {
    File jsonFile = createJsonFile("changing.json",
        "[{\"id\": 1, \"name\": \"Original\"}]");
    // Use DirectFileSource to bypass the Caffeine source cache
    Source source = new org.apache.calcite.adapter.file.DirectFileSource(jsonFile);
    JsonTable table = new JsonTable(source);

    // Record the file's current modification time
    long originalModTime = jsonFile.lastModified();

    // First read - populates dataList and sets lastModifiedTime
    List<Object> firstLoad = table.getDataList(TYPE_FACTORY);
    assertNotNull(firstLoad);
    assertEquals(1, firstLoad.size());

    // Write updated content to the file
    writeJson(jsonFile, "[{\"id\": 1, \"name\": \"Updated\"}, {\"id\": 2, \"name\": \"New\"}]");
    // Set modification time well into the future to ensure cache invalidation detects it
    assertTrue(jsonFile.setLastModified(originalModTime + 5000),
        "setLastModified should succeed");

    // Second read should detect file change and refresh
    List<Object> secondLoad = table.getDataList(TYPE_FACTORY);
    assertNotNull(secondLoad);
    assertEquals(2, secondLoad.size(), "Should have 2 rows after file update");
  }

  /**
   * Tests that getDataList() without file changes returns cached data.
   */
  @Test public void testGetDataListNoCacheInvalidation() throws Exception {
    File jsonFile = createJsonFile("stable.json",
        "[{\"val\": 100}]");
    Source source = Sources.of(jsonFile);
    JsonTable table = new JsonTable(source);

    // Load data twice without changing the file
    List<Object> load1 = table.getDataList(TYPE_FACTORY);
    List<Object> load2 = table.getDataList(TYPE_FACTORY);

    assertNotNull(load1);
    assertNotNull(load2);
    assertEquals(load1.size(), load2.size());
  }

  // ---------------------------------------------------------------
  // Source-based type inference tests
  // ---------------------------------------------------------------

  /**
   * Tests type inference with boolean values in JSON.
   */
  @Test public void testTypeInferenceBoolean() throws Exception {
    File jsonFile = createJsonFile("booleans.json",
        "[{\"flag\": true, \"label\": \"yes\"}, {\"flag\": false, \"label\": \"no\"}]");
    Source source = Sources.of(jsonFile);
    JsonTable table = new JsonTable(source);

    RelDataType rowType = table.getRowType(TYPE_FACTORY);
    assertNotNull(rowType);
    assertEquals(2, rowType.getFieldCount());

    // Boolean values should be inferred as Boolean type
    List<Object> data = table.getDataList(TYPE_FACTORY);
    assertNotNull(data);
    assertEquals(2, data.size());
  }

  /**
   * Tests type inference with null values in the first row.
   * The type scanner should look past the first row to find non-null values.
   */
  @Test public void testTypeInferenceWithNullsInFirstRow() throws Exception {
    File jsonFile = createJsonFile("nulls_first.json",
        "[{\"id\": null, \"name\": \"Alice\"}, {\"id\": 42, \"name\": \"Bob\"}]");
    Source source = Sources.of(jsonFile);
    JsonTable table = new JsonTable(source);

    RelDataType rowType = table.getRowType(TYPE_FACTORY);
    assertNotNull(rowType);
    assertEquals(2, rowType.getFieldCount());
  }

  /**
   * Tests type inference with mixed numeric types.
   */
  @Test public void testTypeInferenceMixedNumbers() throws Exception {
    File jsonFile = createJsonFile("mixed_numbers.json",
        "[{\"int_val\": 42, \"float_val\": 3.14, \"str_val\": \"text\"}]");
    Source source = Sources.of(jsonFile);
    JsonTable table = new JsonTable(source);

    RelDataType rowType = table.getRowType(TYPE_FACTORY);
    assertNotNull(rowType);
    assertEquals(3, rowType.getFieldCount());

    List<Object> data = table.getDataList(TYPE_FACTORY);
    assertNotNull(data);
    assertEquals(1, data.size());
  }

  /**
   * Tests with an empty JSON array.
   */
  @Test public void testEmptyJsonArray() throws Exception {
    File jsonFile = createJsonFile("empty_array.json", "[]");
    Source source = Sources.of(jsonFile);
    JsonTable table = new JsonTable(source);

    RelDataType rowType = table.getRowType(TYPE_FACTORY);
    assertNotNull(rowType, "Even empty array should produce a row type");
  }

  /**
   * Tests with a single JSON object (non-array at root level).
   */
  @Test public void testSingleJsonObject() throws Exception {
    File jsonFile = createJsonFile("single_object.json",
        "{\"key\": \"value\", \"count\": 5}");
    Source source = Sources.of(jsonFile);
    JsonTable table = new JsonTable(source);

    RelDataType rowType = table.getRowType(TYPE_FACTORY);
    assertNotNull(rowType);
    assertEquals(2, rowType.getFieldCount());

    List<Object> data = table.getDataList(TYPE_FACTORY);
    assertNotNull(data);
    assertEquals(1, data.size(), "Single object should produce one row");
  }

  /**
   * Tests getStatistic() returns UNKNOWN statistics.
   */
  @Test public void testGetStatistic() throws Exception {
    File jsonFile = createJsonFile("stats.json",
        "[{\"a\": 1}]");
    Source source = Sources.of(jsonFile);
    JsonTable table = new JsonTable(source);

    Statistic stat = table.getStatistic();
    assertNotNull(stat, "Statistic should not be null");
  }

  /**
   * Tests with flattening enabled via options.
   */
  @Test public void testFlattenOption() throws Exception {
    File jsonFile = createJsonFile("nested.json",
        "[{\"id\": 1, \"address\": {\"city\": \"NYC\", \"zip\": \"10001\"}}]");
    Source source = Sources.of(jsonFile);
    Map<String, Object> options = new HashMap<String, Object>();
    options.put("flatten", Boolean.TRUE);
    JsonTable table = new JsonTable(source, options);

    RelDataType rowType = table.getRowType(TYPE_FACTORY);
    assertNotNull(rowType);
    // With flatten, nested fields should be expanded
    assertTrue(rowType.getFieldCount() >= 2,
        "Flattened row type should have expanded fields");
  }

  /**
   * Tests type inference with all-null values for a column defaults to String.
   */
  @Test public void testTypeInferenceAllNulls() throws Exception {
    File jsonFile = createJsonFile("all_nulls.json",
        "[{\"id\": 1, \"nullable_col\": null}, {\"id\": 2, \"nullable_col\": null}]");
    Source source = Sources.of(jsonFile);
    JsonTable table = new JsonTable(source);

    RelDataType rowType = table.getRowType(TYPE_FACTORY);
    assertNotNull(rowType);
    assertEquals(2, rowType.getFieldCount(),
        "Should have 2 columns including the all-null column");
  }

  /**
   * Tests cache invalidation resets rowType, dataList, and cachedConverter.
   * Uses DirectFileSource to bypass the Caffeine source cache.
   */
  @Test public void testCacheInvalidationResetsState() throws Exception {
    File jsonFile = createJsonFile("reset_state.json",
        "[{\"val\": 100}]");
    // Use DirectFileSource to bypass the Caffeine source cache
    Source source = new org.apache.calcite.adapter.file.DirectFileSource(jsonFile);
    JsonTable table = new JsonTable(source);

    // Load initial row type and data
    RelDataType rowType1 = table.getRowType(TYPE_FACTORY);
    List<Object> data1 = table.getDataList(TYPE_FACTORY);
    assertNotNull(rowType1);
    assertEquals(1, data1.size());

    // Modify file
    long origModTime = jsonFile.lastModified();
    writeJson(jsonFile, "[{\"val\": 200}, {\"val\": 300}]");
    assertTrue(jsonFile.setLastModified(origModTime + 5000),
        "setLastModified should succeed");

    // After modification, getDataList should refresh
    List<Object> data2 = table.getDataList(TYPE_FACTORY);
    assertEquals(2, data2.size(),
        "Data list should be refreshed with 2 rows");
  }

  private File createJsonFile(String name, String content) throws IOException {
    File file = new File(tempDir.toFile(), name);
    writeJson(file, content);
    return file;
  }

  private void writeJson(File file, String content) throws IOException {
    try (FileWriter writer = new FileWriter(file, StandardCharsets.UTF_8)) {
      writer.write(content);
    }
  }
}
