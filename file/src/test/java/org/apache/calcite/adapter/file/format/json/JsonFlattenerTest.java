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
package org.apache.calcite.adapter.file.format.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link JsonFlattener} and {@link SharedJsonData}.
 */
@Tag("unit")
public class JsonFlattenerTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(JsonFlattenerTest.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  // --- JsonFlattener tests ---

  @Test
  @DisplayName("Flatten with max depth truncation - objects beyond depth become strings")
  void testFlattenMaxDepthTruncation() {
    // maxDepth=2 means depth 3 objects get stringified
    JsonFlattener flattener = new JsonFlattener(",", 2, "");

    Map<String, Object> level3 = new LinkedHashMap<>();
    level3.put("deep", "value");

    Map<String, Object> level2 = new LinkedHashMap<>();
    level2.put("nested", level3);

    Map<String, Object> level1 = new LinkedHashMap<>();
    level1.put("mid", level2);

    Map<String, Object> root = new LinkedHashMap<>();
    root.put("top", level1);

    Map<String, Object> result = flattener.flatten(root);
    LOGGER.debug("Max depth truncation result: {}", result);

    // At depth 3 (top -> mid -> nested), the value should be stringified
    assertTrue(result.containsKey("top__mid__nested"),
        "Should have key at truncation depth: " + result.keySet());
    Object truncatedValue = result.get("top__mid__nested");
    assertTrue(truncatedValue instanceof String,
        "Beyond max depth, value should be stringified");
  }

  @Test
  @DisplayName("Flatten empty objects are skipped")
  void testFlattenEmptyObjectsSkipped() {
    JsonFlattener flattener = new JsonFlattener();

    Map<String, Object> emptyChild = new LinkedHashMap<>();
    Map<String, Object> root = new LinkedHashMap<>();
    root.put("a", "value");
    root.put("empty", emptyChild);

    Map<String, Object> result = flattener.flatten(root);

    assertTrue(result.containsKey("a"));
    assertFalse(result.containsKey("empty"),
        "Empty objects should be skipped");
  }

  @Test
  @DisplayName("Flatten array of objects returns null (omitted from output)")
  void testFlattenArrayOfObjectsReturnsNull() {
    JsonFlattener flattener = new JsonFlattener();

    List<Object> arrayOfMaps = new ArrayList<>();
    Map<String, Object> obj1 = new HashMap<>();
    obj1.put("name", "Alice");
    arrayOfMaps.add(obj1);

    Map<String, Object> root = new LinkedHashMap<>();
    root.put("users", arrayOfMaps);
    root.put("count", 1);

    Map<String, Object> result = flattener.flatten(root);

    assertFalse(result.containsKey("users"),
        "Array of Maps should be omitted");
    assertEquals(1, result.get("count"));
  }

  @Test
  @DisplayName("Flatten empty array returns empty string")
  void testFlattenEmptyArrayReturnsEmptyString() {
    JsonFlattener flattener = new JsonFlattener();

    Map<String, Object> root = new LinkedHashMap<>();
    root.put("tags", new ArrayList<>());

    Map<String, Object> result = flattener.flatten(root);

    assertEquals("", result.get("tags"));
  }

  @Test
  @DisplayName("Flatten null values are preserved")
  void testFlattenNullValuesPreserved() {
    JsonFlattener flattener = new JsonFlattener();

    Map<String, Object> root = new LinkedHashMap<>();
    root.put("present", "yes");
    root.put("absent", null);

    Map<String, Object> result = flattener.flatten(root);

    assertEquals("yes", result.get("present"));
    assertTrue(result.containsKey("absent"), "Null key should be present");
    assertNull(result.get("absent"), "Null value should remain null");
  }

  @Test
  @DisplayName("Flatten value containing delimiter gets quoted")
  void testFlattenValueContainingDelimiterGetsQuoted() {
    JsonFlattener flattener = new JsonFlattener(",", 3, "");

    Map<String, Object> root = new LinkedHashMap<>();
    List<Object> items = new ArrayList<>(Arrays.asList("hello,world", "simple"));
    root.put("tags", items);

    Map<String, Object> result = flattener.flatten(root);
    String flatTags = (String) result.get("tags");
    LOGGER.debug("Flattened tags with delimiter: {}", flatTags);

    // "hello,world" should be escaped
    assertTrue(flatTags.contains("\"hello,world\""),
        "Value with delimiter should be quoted: " + flatTags);
  }

  @Test
  @DisplayName("Flatten with custom null value")
  void testFlattenCustomNullValue() {
    JsonFlattener flattener = new JsonFlattener(",", 3, "N/A");

    Map<String, Object> root = new LinkedHashMap<>();
    List<Object> items = new ArrayList<>(Arrays.asList("a", null, "c"));
    root.put("vals", items);

    Map<String, Object> result = flattener.flatten(root);
    String flatVals = (String) result.get("vals");

    assertTrue(flatVals.contains("N/A"),
        "Null in array should use custom null value: " + flatVals);
  }

  // --- SharedJsonData tests ---

  @Test
  @DisplayName("getDataAtPath with $ returns root node")
  void testGetDataAtPathRoot() throws Exception {
    JsonNode root = MAPPER.readTree("{\"name\":\"test\",\"value\":42}");
    SharedJsonData data = new SharedJsonData(root);

    JsonNode result = data.getDataAtPath("$");
    assertEquals(root, result, "$ should return root node");
  }

  @Test
  @DisplayName("getDataAtPath navigates nested path correctly")
  void testGetDataAtPathNavigatesNestedPath() throws Exception {
    String json = "{\"data\":{\"users\":[{\"name\":\"Alice\"},{\"name\":\"Bob\"}]}}";
    JsonNode root = MAPPER.readTree(json);
    SharedJsonData data = new SharedJsonData(root);

    JsonNode result = data.getDataAtPath("$.data.users");

    assertTrue(result.isArray());
    assertEquals(2, result.size());
    assertEquals("Alice", result.get(0).get("name").asText());
  }

  @Test
  @DisplayName("getDataAtPath with missing path returns null")
  void testGetDataAtPathMissingReturnsNull() throws Exception {
    JsonNode root = MAPPER.readTree("{\"a\":1}");
    SharedJsonData data = new SharedJsonData(root);

    JsonNode result = data.getDataAtPath("$.nonexistent.path");
    assertNull(result, "Missing path should return null");
  }

  @Test
  @DisplayName("getDataAtPath with array index path")
  void testGetDataAtPathArrayIndex() throws Exception {
    String json = "{\"items\":[\"first\",\"second\",\"third\"]}";
    JsonNode root = MAPPER.readTree(json);
    SharedJsonData data = new SharedJsonData(root);

    JsonNode result = data.getDataAtPath("$.items[0]");

    assertEquals("first", result.asText());
  }

  @Test
  @DisplayName("pathExists returns true for existing path")
  void testPathExistsTrue() throws Exception {
    String json = "{\"a\":{\"b\":1}}";
    JsonNode root = MAPPER.readTree(json);
    SharedJsonData data = new SharedJsonData(root);

    assertTrue(data.pathExists("$.a.b"));
  }

  @Test
  @DisplayName("pathExists returns false for missing path")
  void testPathExistsFalse() throws Exception {
    String json = "{\"a\":{\"b\":1}}";
    JsonNode root = MAPPER.readTree(json);
    SharedJsonData data = new SharedJsonData(root);

    assertFalse(data.pathExists("$.a.c"));
  }

  @Test
  @DisplayName("getPathSize for array returns length")
  void testGetPathSizeForArray() throws Exception {
    String json = "{\"items\":[1,2,3,4,5]}";
    JsonNode root = MAPPER.readTree(json);
    SharedJsonData data = new SharedJsonData(root);

    assertEquals(5, data.getPathSize("$.items"));
  }

  @Test
  @DisplayName("getPathSize for non-collection returns 0")
  void testGetPathSizeForNonCollection() throws Exception {
    String json = "{\"name\":\"hello\"}";
    JsonNode root = MAPPER.readTree(json);
    SharedJsonData data = new SharedJsonData(root);

    assertEquals(0, data.getPathSize("$.name"));
  }
}
