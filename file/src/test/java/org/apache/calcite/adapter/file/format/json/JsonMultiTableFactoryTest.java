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

import org.apache.calcite.schema.Table;
import org.apache.calcite.util.Sources;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link JsonMultiTableFactory} and {@link JsonSearchConfig}.
 */
@Tag("unit")
public class JsonMultiTableFactoryTest {

  @TempDir
  public File tempDir;

  @Test void testCreateTablesDefaultSingleTable() throws IOException {
    File jsonFile =
        writeJsonFile("data.json", "[{\"id\": 1, \"name\": \"Alice\"}, {\"id\": 2, \"name\": \"Bob\"}]");

    JsonSearchConfig config = new JsonSearchConfig();
    Map<String, Table> tables =
        JsonMultiTableFactory.createTables(Sources.of(jsonFile), config);

    assertEquals(1, tables.size());
    assertTrue(tables.containsKey("data"), "Default table should be named after file");
  }

  @Test void testCreateTablesWithExplicitPaths() throws IOException {
    File jsonFile =
        writeJsonFile("multi.json", "{\"users\": [{\"id\": 1}], \"orders\": [{\"oid\": 100}]}");

    List<String> paths = new ArrayList<String>();
    paths.add("$.users");
    paths.add("$.orders");

    JsonSearchConfig config = new JsonSearchConfig();
    config.withJsonSearchPaths(paths);

    Map<String, Table> tables =
        JsonMultiTableFactory.createTables(Sources.of(jsonFile), config);

    assertEquals(2, tables.size());
    assertTrue(tables.containsKey("users"));
    assertTrue(tables.containsKey("orders"));
  }

  @Test void testCreateTablesWithAutoDiscovery() throws IOException {
    File jsonFile =
        writeJsonFile("discover.json", "{\"employees\": [{\"name\": \"A\"}], \"departments\": [{\"name\": \"B\"}]}");

    JsonSearchConfig config = new JsonSearchConfig()
        .withAutoDiscoverTables(true);

    Map<String, Table> tables =
        JsonMultiTableFactory.createTables(Sources.of(jsonFile), config);

    assertEquals(2, tables.size());
    assertTrue(tables.containsKey("employees"));
    assertTrue(tables.containsKey("departments"));
  }

  @Test void testAutoDiscoveryRespectsMinArraySize() throws IOException {
    File jsonFile =
        writeJsonFile("minsize.json", "{\"big\": [{\"a\":1},{\"a\":2},{\"a\":3}], \"small\": [{\"b\":1}]}");

    JsonSearchConfig config = new JsonSearchConfig()
        .withAutoDiscoverTables(true)
        .withMinArraySize(2);

    Map<String, Table> tables =
        JsonMultiTableFactory.createTables(Sources.of(jsonFile), config);

    assertEquals(1, tables.size());
    assertTrue(tables.containsKey("big"), "Only arrays >= minArraySize should be discovered");
  }

  @Test void testAutoDiscoveryRespectsMaxDepth() throws IOException {
    // depth 0 = root object, depth 1 = level1, depth 2 = level2.nested
    File jsonFile =
        writeJsonFile("depth.json", "{\"level1\": [{\"x\":1}], \"wrapper\": {\"level2\": {\"nested\": [{\"y\":1}]}}}");

    JsonSearchConfig config = new JsonSearchConfig()
        .withAutoDiscoverTables(true)
        .withMaxDiscoveryDepth(1);

    Map<String, Table> tables =
        JsonMultiTableFactory.createTables(Sources.of(jsonFile), config);

    // Only level1 should be found (depth 1), nested is at depth 3
    assertEquals(1, tables.size());
    assertTrue(tables.containsKey("level1"));
  }

  @Test void testTableNamePatternWithPathSegment() throws IOException {
    File jsonFile =
        writeJsonFile("pattern.json", "{\"data\": {\"sales\": [{\"amount\": 100}]}}");

    List<String> paths = new ArrayList<String>();
    paths.add("$.data.sales");

    JsonSearchConfig config = new JsonSearchConfig()
        .withJsonSearchPaths(paths)
        .withTableNamePattern("{pathSegment}");

    Map<String, Table> tables =
        JsonMultiTableFactory.createTables(Sources.of(jsonFile), config);

    assertEquals(1, tables.size());
    assertTrue(tables.containsKey("sales"));
  }

  @Test void testTableNamePatternWithFileName() throws IOException {
    File jsonFile =
        writeJsonFile("myfile.json", "{\"items\": [{\"id\": 1}]}");

    List<String> paths = new ArrayList<String>();
    paths.add("$.items");

    JsonSearchConfig config = new JsonSearchConfig()
        .withJsonSearchPaths(paths)
        .withTableNamePattern("{fileName}");

    Map<String, Table> tables =
        JsonMultiTableFactory.createTables(Sources.of(jsonFile), config);

    assertEquals(1, tables.size());
    assertTrue(tables.containsKey("myfile"));
  }

  @Test void testTableNamePatternCombined() throws IOException {
    File jsonFile =
        writeJsonFile("report.json", "{\"data\": {\"metrics\": [{\"val\": 42}]}}");

    List<String> paths = new ArrayList<String>();
    paths.add("$.data.metrics");

    JsonSearchConfig config = new JsonSearchConfig()
        .withJsonSearchPaths(paths)
        .withTableNamePattern("{fileName}_{pathSegment}");

    Map<String, Table> tables =
        JsonMultiTableFactory.createTables(Sources.of(jsonFile), config);

    assertEquals(1, tables.size());
    assertTrue(tables.containsKey("report_metrics"));
  }

  @Test void testNameConflictResolution() throws IOException {
    // Two paths that will produce the same table name via {pathSegment}
    File jsonFile =
        writeJsonFile("conflict.json", "{\"a\": {\"items\": [{\"x\":1}]}, \"b\": {\"items\": [{\"y\":2}]}}");

    List<String> paths = new ArrayList<String>();
    paths.add("$.a.items");
    paths.add("$.b.items");

    JsonSearchConfig config = new JsonSearchConfig()
        .withJsonSearchPaths(paths)
        .withTableNamePattern("{pathSegment}");

    Map<String, Table> tables =
        JsonMultiTableFactory.createTables(Sources.of(jsonFile), config);

    assertEquals(2, tables.size());
    assertTrue(tables.containsKey("items"));
    assertTrue(tables.containsKey("items_1"), "Duplicate name should get _1 suffix");
  }

  @Test void testJsonSearchConfigFromMapWithNullMap() {
    JsonSearchConfig config = new JsonSearchConfig(null);
    assertEquals(JsonSearchConfig.DEFAULT_TABLE_NAME_PATTERN, config.getTableNamePattern());
    assertTrue(config.getJsonSearchPaths() == null, "Paths should be null by default");
    assertFalse(config.isAutoDiscoverTables());
    assertEquals(5, config.getMaxDiscoveryDepth());
    assertEquals(1, config.getMinArraySize());
  }

  @Test void testJsonSearchConfigFromMapWithAllOptions() {
    Map<String, Object> options = new HashMap<String, Object>();
    List<String> paths = new ArrayList<String>();
    paths.add("$.data");
    options.put("jsonSearchPaths", paths);
    options.put("autoDiscoverTables", Boolean.TRUE);
    options.put("tableNamePattern", "{fileName}");
    options.put("maxDiscoveryDepth", Integer.valueOf(10));
    options.put("minArraySize", Integer.valueOf(3));

    JsonSearchConfig config = new JsonSearchConfig(options);

    assertNotNull(config.getJsonSearchPaths());
    assertEquals(1, config.getJsonSearchPaths().size());
    assertTrue(config.isAutoDiscoverTables());
    assertEquals("{fileName}", config.getTableNamePattern());
    assertEquals(10, config.getMaxDiscoveryDepth());
    assertEquals(3, config.getMinArraySize());
  }

  @Test void testJsonSearchConfigFluentApi() {
    List<String> paths = new ArrayList<String>();
    paths.add("$.x");

    JsonSearchConfig config = new JsonSearchConfig()
        .withJsonSearchPaths(paths)
        .withAutoDiscoverTables(true)
        .withTableNamePattern("{parentSegment}")
        .withMaxDiscoveryDepth(8)
        .withMinArraySize(5);

    assertEquals(paths, config.getJsonSearchPaths());
    assertTrue(config.isAutoDiscoverTables());
    assertEquals("{parentSegment}", config.getTableNamePattern());
    assertEquals(8, config.getMaxDiscoveryDepth());
    assertEquals(5, config.getMinArraySize());
  }

  @Test void testIsMultiTableMode() {
    JsonSearchConfig defaultConfig = new JsonSearchConfig();
    assertFalse(defaultConfig.isMultiTableMode());

    List<String> paths = new ArrayList<String>();
    paths.add("$.data");
    JsonSearchConfig pathConfig = new JsonSearchConfig()
        .withJsonSearchPaths(paths);
    assertTrue(pathConfig.isMultiTableMode());

    JsonSearchConfig autoConfig = new JsonSearchConfig()
        .withAutoDiscoverTables(true);
    assertTrue(autoConfig.isMultiTableMode());
  }

  @Test void testCreateTablesSkipsMissingPaths() throws IOException {
    File jsonFile =
        writeJsonFile("sparse.json", "{\"exists\": [{\"id\": 1}]}");

    List<String> paths = new ArrayList<String>();
    paths.add("$.exists");
    paths.add("$.does_not_exist");

    JsonSearchConfig config = new JsonSearchConfig()
        .withJsonSearchPaths(paths);

    Map<String, Table> tables =
        JsonMultiTableFactory.createTables(Sources.of(jsonFile), config);

    assertEquals(1, tables.size());
    assertTrue(tables.containsKey("exists"));
  }

  private File writeJsonFile(String name, String content) throws IOException {
    File file = new File(tempDir, name);
    try (FileWriter writer = new FileWriter(file)) {
      writer.write(content);
    }
    return file;
  }
}
