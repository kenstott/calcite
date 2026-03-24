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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link MaterializePartitionConfig}.
 */
@Tag("unit")
class MaterializePartitionConfigTest {

  @Test void testBuilderWithColumnsAndBatchBy() {
    MaterializePartitionConfig config = MaterializePartitionConfig.builder()
        .columns(Arrays.asList("year", "region"))
        .batchBy(Arrays.asList("year"))
        .build();

    assertEquals(2, config.getColumns().size());
    assertEquals("year", config.getColumns().get(0));
    assertEquals("region", config.getColumns().get(1));
    assertEquals(1, config.getBatchBy().size());
    assertEquals("year", config.getBatchBy().get(0));
    assertTrue(config.hasBatching());
  }

  @Test void testBuilderDefaults() {
    MaterializePartitionConfig config = MaterializePartitionConfig.builder().build();

    assertTrue(config.getColumns().isEmpty());
    assertTrue(config.getBatchBy().isEmpty());
    assertFalse(config.hasBatching());
    assertTrue(config.getColumnDefinitions().isEmpty());
  }

  @Test void testColumnDefinition() {
    MaterializePartitionConfig.ColumnDefinition colDef =
        new MaterializePartitionConfig.ColumnDefinition("year", "INTEGER");

    assertEquals("year", colDef.getName());
    assertEquals("INTEGER", colDef.getType());
  }

  @Test void testColumnDefinitionDefaultType() {
    MaterializePartitionConfig.ColumnDefinition colDef =
        new MaterializePartitionConfig.ColumnDefinition("region", null);

    assertEquals("region", colDef.getName());
    assertEquals("VARCHAR", colDef.getType());
  }

  @Test void testBuilderWithColumnDefinitions() {
    List<MaterializePartitionConfig.ColumnDefinition> defs =
        new ArrayList<MaterializePartitionConfig.ColumnDefinition>();
    defs.add(new MaterializePartitionConfig.ColumnDefinition("year", "INTEGER"));
    defs.add(new MaterializePartitionConfig.ColumnDefinition("region", "VARCHAR"));

    MaterializePartitionConfig config = MaterializePartitionConfig.builder()
        .columnDefinitions(defs)
        .build();

    assertEquals(2, config.getColumnDefinitions().size());
    assertEquals("year", config.getColumnDefinitions().get(0).getName());
    assertEquals("INTEGER", config.getColumnDefinitions().get(0).getType());
  }

  @Test void testFromMap() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("columns", Arrays.asList("year", "region"));
    map.put("batchBy", Arrays.asList("year"));

    MaterializePartitionConfig config = MaterializePartitionConfig.fromMap(map);
    assertNotNull(config);
    assertEquals(2, config.getColumns().size());
    assertEquals(1, config.getBatchBy().size());
    assertTrue(config.hasBatching());
  }

  @Test void testFromMapWithColumnDefinitions() {
    Map<String, Object> map = new HashMap<String, Object>();

    List<Map<String, Object>> colDefs = new ArrayList<Map<String, Object>>();
    Map<String, Object> yearDef = new HashMap<String, Object>();
    yearDef.put("name", "year");
    yearDef.put("type", "INTEGER");
    colDefs.add(yearDef);

    Map<String, Object> regionDef = new HashMap<String, Object>();
    regionDef.put("name", "region");
    regionDef.put("type", "VARCHAR");
    colDefs.add(regionDef);

    map.put("columnDefinitions", colDefs);

    MaterializePartitionConfig config = MaterializePartitionConfig.fromMap(map);
    assertNotNull(config);
    assertEquals(2, config.getColumnDefinitions().size());
  }

  @Test void testFromMapNull() {
    assertNull(MaterializePartitionConfig.fromMap(null));
  }

  @Test void testFromMapEmpty() {
    MaterializePartitionConfig config = MaterializePartitionConfig.fromMap(
        new HashMap<String, Object>());
    assertNotNull(config);
    assertTrue(config.getColumns().isEmpty());
    assertTrue(config.getBatchBy().isEmpty());
  }

  @Test void testColumnsAreImmutable() {
    MaterializePartitionConfig config = MaterializePartitionConfig.builder()
        .columns(Arrays.asList("year"))
        .build();

    try {
      config.getColumns().add("region");
      // If we get here, the list was not immutable
      assertTrue(false, "Expected UnsupportedOperationException");
    } catch (UnsupportedOperationException e) {
      // Expected
    }
  }

  @Test void testBatchByIsImmutable() {
    MaterializePartitionConfig config = MaterializePartitionConfig.builder()
        .batchBy(Arrays.asList("year"))
        .build();

    try {
      config.getBatchBy().add("region");
      assertTrue(false, "Expected UnsupportedOperationException");
    } catch (UnsupportedOperationException e) {
      // Expected
    }
  }

  @Test void testFromMapSkipsNonStringColumns() {
    Map<String, Object> map = new HashMap<String, Object>();
    List<Object> columns = new ArrayList<Object>();
    columns.add("year");
    columns.add(123); // not a string
    columns.add("region");
    map.put("columns", columns);

    MaterializePartitionConfig config = MaterializePartitionConfig.fromMap(map);
    assertNotNull(config);
    assertEquals(2, config.getColumns().size());
  }

  @Test void testFromMapSkipsNullColumnDefNames() {
    Map<String, Object> map = new HashMap<String, Object>();
    List<Map<String, Object>> colDefs = new ArrayList<Map<String, Object>>();
    Map<String, Object> noName = new HashMap<String, Object>();
    noName.put("type", "INTEGER");
    colDefs.add(noName);
    map.put("columnDefinitions", colDefs);

    MaterializePartitionConfig config = MaterializePartitionConfig.fromMap(map);
    assertNotNull(config);
    assertTrue(config.getColumnDefinitions().isEmpty());
  }
}
