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

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link ConstantsSource}.
 */
@Tag("unit")
class ConstantsSourceTest {

  @Test void testFetchMapData() throws IOException {
    ConstantsSource.ConstantsSourceConfig config =
        new ConstantsSource.ConstantsSourceConfig(
            "/etl-test/test-constants.yaml",
            "naicsSupersectors",
            "supersector_code",
            "supersector_name");

    ConstantsSource source = new ConstantsSource(config);
    Iterator<Map<String, Object>> iter =
        source.fetch(Collections.<String, String>emptyMap());

    assertNotNull(iter);
    List<Map<String, Object>> rows = toList(iter);

    assertEquals(3, rows.size());

    // Find the Total Nonfarm row
    Map<String, Object> totalNonfarm = findRow(rows, "supersector_code", "00000000");
    assertNotNull(totalNonfarm);
    assertEquals("Total Nonfarm", totalNonfarm.get("supersector_name"));
  }

  @Test void testFetchListOfMapsData() throws IOException {
    ConstantsSource.ConstantsSourceConfig config =
        new ConstantsSource.ConstantsSourceConfig(
            "/etl-test/test-constants.yaml",
            "regionList",
            "key",
            "value");

    ConstantsSource source = new ConstantsSource(config);
    Iterator<Map<String, Object>> iter =
        source.fetch(Collections.<String, String>emptyMap());

    List<Map<String, Object>> rows = toList(iter);
    assertEquals(3, rows.size());

    // List of maps should preserve the map fields
    Map<String, Object> firstRow = rows.get(0);
    assertTrue(firstRow.containsKey("code") || firstRow.containsKey("name"));
  }

  @Test void testFetchScalarListData() throws IOException {
    ConstantsSource.ConstantsSourceConfig config =
        new ConstantsSource.ConstantsSourceConfig(
            "/etl-test/test-constants.yaml",
            "scalarList",
            "key",
            "value");

    ConstantsSource source = new ConstantsSource(config);
    Iterator<Map<String, Object>> iter =
        source.fetch(Collections.<String, String>emptyMap());

    List<Map<String, Object>> rows = toList(iter);
    assertEquals(3, rows.size());

    // Scalar list should put values in the value column
    assertEquals("alpha", rows.get(0).get("value"));
    assertEquals("beta", rows.get(1).get("value"));
    assertEquals("gamma", rows.get(2).get("value"));
  }

  @Test void testFetchNestedPath() throws IOException {
    ConstantsSource.ConstantsSourceConfig config =
        new ConstantsSource.ConstantsSourceConfig(
            "/etl-test/test-constants.yaml",
            "nested.level1.level2.items",
            "key",
            "value");

    ConstantsSource source = new ConstantsSource(config);
    Iterator<Map<String, Object>> iter =
        source.fetch(Collections.<String, String>emptyMap());

    List<Map<String, Object>> rows = toList(iter);
    assertEquals(2, rows.size());
  }

  @Test void testFetchMissingPath() {
    ConstantsSource.ConstantsSourceConfig config =
        new ConstantsSource.ConstantsSourceConfig(
            "/etl-test/test-constants.yaml",
            "nonexistent.path",
            "key",
            "value");

    ConstantsSource source = new ConstantsSource(config);

    assertThrows(IOException.class,
        () -> source.fetch(Collections.<String, String>emptyMap()));
  }

  @Test void testFetchMissingResource() {
    ConstantsSource.ConstantsSourceConfig config =
        new ConstantsSource.ConstantsSourceConfig(
            "/nonexistent-resource.yaml",
            "path",
            "key",
            "value");

    ConstantsSource source = new ConstantsSource(config);

    assertThrows(IOException.class,
        () -> source.fetch(Collections.<String, String>emptyMap()));
  }

  @Test void testGetType() {
    ConstantsSource.ConstantsSourceConfig config =
        new ConstantsSource.ConstantsSourceConfig(
            "/test.yaml", "path", "key", "value");
    ConstantsSource source = new ConstantsSource(config);
    assertEquals("constants", source.getType());
  }

  @Test void testEstimateRowCount() {
    ConstantsSource.ConstantsSourceConfig config =
        new ConstantsSource.ConstantsSourceConfig(
            "/test.yaml", "path", "key", "value");
    ConstantsSource source = new ConstantsSource(config);
    assertEquals(-1, source.estimateRowCount());
  }

  @Test void testFromMap() {
    Map<String, Object> configMap = new HashMap<String, Object>();
    configMap.put("file", "/etl-test/test-constants.yaml");
    configMap.put("path", "naicsSupersectors");
    configMap.put("keyColumn", "code");
    configMap.put("valueColumn", "name");

    ConstantsSource source = ConstantsSource.fromMap(configMap);
    assertNotNull(source);
    assertEquals("constants", source.getType());
  }

  @Test void testConstantsSourceConfigDefaults() {
    ConstantsSource.ConstantsSourceConfig config =
        new ConstantsSource.ConstantsSourceConfig(
            "/test.yaml", "path", null, null);

    assertEquals("key", config.getKeyColumn());
    assertEquals("value", config.getValueColumn());
    assertEquals("/test.yaml", config.getFile());
    assertEquals("path", config.getPath());
  }

  @Test void testConstantsSourceConfigFromMap() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("file", "/data.yaml");
    map.put("path", "items");
    map.put("keyColumn", "id");
    map.put("valueColumn", "desc");

    ConstantsSource.ConstantsSourceConfig config =
        ConstantsSource.ConstantsSourceConfig.fromMap(map);

    assertEquals("/data.yaml", config.getFile());
    assertEquals("items", config.getPath());
    assertEquals("id", config.getKeyColumn());
    assertEquals("desc", config.getValueColumn());
  }

  @Test void testFetchEmptyPath() throws IOException {
    // Empty path should return the root object
    ConstantsSource.ConstantsSourceConfig config =
        new ConstantsSource.ConstantsSourceConfig(
            "/etl-test/test-constants.yaml",
            "",
            "key",
            "value");

    ConstantsSource source = new ConstantsSource(config);
    // Root is a map, not a list - the path navigateToPath returns the data
    // This should work since it returns the full map
    Iterator<Map<String, Object>> iter =
        source.fetch(Collections.<String, String>emptyMap());
    assertNotNull(iter);
  }

  private List<Map<String, Object>> toList(Iterator<Map<String, Object>> iter) {
    List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
    while (iter.hasNext()) {
      list.add(iter.next());
    }
    return list;
  }

  private Map<String, Object> findRow(List<Map<String, Object>> rows,
      String key, Object value) {
    for (Map<String, Object> row : rows) {
      if (value.equals(row.get(key))) {
        return row;
      }
    }
    return null;
  }
}
