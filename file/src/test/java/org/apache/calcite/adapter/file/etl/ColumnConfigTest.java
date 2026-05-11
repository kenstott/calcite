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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link ColumnConfig}.
 */
@Tag("unit")
class ColumnConfigTest {

  @Test void testBuilderBasic() {
    ColumnConfig config = ColumnConfig.builder()
        .name("region_code")
        .type("VARCHAR")
        .source("regionCode")
        .build();

    assertEquals("region_code", config.getName());
    assertEquals("VARCHAR", config.getType());
    assertEquals("regionCode", config.getSource());
    assertNull(config.getExpression());
    assertTrue(config.isRequired());
    assertFalse(config.isComputed());
  }

  @Test void testBuilderComputedColumn() {
    ColumnConfig config = ColumnConfig.builder()
        .name("quarter")
        .type("VARCHAR")
        .expression("SUBSTR(period, 1, 2)")
        .build();

    assertEquals("quarter", config.getName());
    assertTrue(config.isComputed());
    assertEquals("SUBSTR(period, 1, 2)", config.getExpression());
  }

  @Test void testBuilderRequiredFalse() {
    ColumnConfig config = ColumnConfig.builder()
        .name("optional_col")
        .type("VARCHAR")
        .required(false)
        .build();

    assertFalse(config.isRequired());
  }

  @Test void testBuilderRequiredDefaultTrue() {
    ColumnConfig config = ColumnConfig.builder()
        .name("col")
        .build();

    assertTrue(config.isRequired());
  }

  @Test void testBuilderNullNameThrows() {
    assertThrows(IllegalArgumentException.class, () ->
        ColumnConfig.builder().name(null).build());
  }

  @Test void testBuilderEmptyNameThrows() {
    assertThrows(IllegalArgumentException.class, () ->
        ColumnConfig.builder().name("").build());
  }

  @Test void testGetEffectiveSourceWithSource() {
    ColumnConfig config = ColumnConfig.builder()
        .name("region_code")
        .source("regionCode")
        .build();

    assertEquals("regionCode", config.getEffectiveSource());
  }

  @Test void testGetEffectiveSourceWithoutSource() {
    ColumnConfig config = ColumnConfig.builder()
        .name("region_code")
        .build();

    assertEquals("region_code", config.getEffectiveSource());
  }

  @Test void testGetEffectiveSourceEmptySource() {
    ColumnConfig config = ColumnConfig.builder()
        .name("region_code")
        .source("")
        .build();

    assertEquals("region_code", config.getEffectiveSource());
  }

  @Test void testBuildSelectExpressionSimple() {
    ColumnConfig config = ColumnConfig.builder()
        .name("region_code")
        .build();

    assertEquals("region_code", config.buildSelectExpression());
  }

  @Test void testBuildSelectExpressionWithRename() {
    ColumnConfig config = ColumnConfig.builder()
        .name("region_code")
        .source("regionCode")
        .build();

    assertEquals("\"regionCode\" AS region_code", config.buildSelectExpression());
  }

  @Test void testBuildSelectExpressionComputed() {
    ColumnConfig config = ColumnConfig.builder()
        .name("quarter")
        .expression("SUBSTR(period, 1, 2)")
        .build();

    assertEquals("SUBSTR(period, 1, 2) AS quarter", config.buildSelectExpression());
  }

  @Test void testBuildSelectExpressionWithTableAlias() {
    ColumnConfig config = ColumnConfig.builder()
        .name("region_code")
        .source("regionCode")
        .build();

    Set<String> sourceColumns = new HashSet<String>(Arrays.asList("regionCode", "year"));
    String result = config.buildSelectExpression("src", sourceColumns);
    assertEquals("src.\"regionCode\" AS region_code", result);
  }

  @Test void testBuildSelectExpressionComputedWithTableAlias() {
    ColumnConfig config = ColumnConfig.builder()
        .name("quarter")
        .expression("SUBSTR(period, 1, 2)")
        .build();

    Set<String> sourceColumns = new HashSet<String>(Arrays.asList("period"));
    String result = config.buildSelectExpression("src", sourceColumns);
    assertEquals("SUBSTR(src.\"period\", 1, 2) AS quarter", result);
  }

  @Test void testBuildSelectExpressionWithPartitionVariables() {
    ColumnConfig config = ColumnConfig.builder()
        .name("table_id")
        .expression("'{tablename}'")
        .build();

    Set<String> sourceColumns = new HashSet<String>();
    Map<String, String> partitionVars = new HashMap<String, String>();
    partitionVars.put("tablename", "SAINC1");

    String result = config.buildSelectExpression("src", sourceColumns, partitionVars);
    assertEquals("'SAINC1' AS table_id", result);
  }

  @Test void testBuildSelectExpressionPartitionVariableFallback() {
    // When source column doesn't exist but partition variable does
    ColumnConfig config = ColumnConfig.builder()
        .name("year")
        .build();

    Set<String> sourceColumns = new HashSet<String>(Arrays.asList("value", "name"));
    Map<String, String> partitionVars = new HashMap<String, String>();
    partitionVars.put("year", "2024");

    String result = config.buildSelectExpression("src", sourceColumns, partitionVars);
    assertEquals("'2024' AS year", result);
  }

  @Test void testBuildSelectExpressionPartitionVarEscapesSingleQuotes() {
    ColumnConfig config = ColumnConfig.builder()
        .name("label")
        .build();

    Set<String> sourceColumns = new HashSet<String>();
    Map<String, String> partitionVars = new HashMap<String, String>();
    partitionVars.put("label", "it's");

    String result = config.buildSelectExpression("src", sourceColumns, partitionVars);
    assertEquals("'it''s' AS label", result);
  }

  @Test void testFromMap() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("name", "region_code");
    map.put("type", "VARCHAR");
    map.put("source", "regionCode");
    map.put("required", Boolean.FALSE);

    ColumnConfig config = ColumnConfig.fromMap(map);
    assertNotNull(config);
    assertEquals("region_code", config.getName());
    assertEquals("VARCHAR", config.getType());
    assertEquals("regionCode", config.getSource());
    assertFalse(config.isRequired());
  }

  @Test void testFromMapWithExpression() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("name", "quarter");
    map.put("type", "VARCHAR");
    map.put("expression", "SUBSTR(period, 1, 2)");

    ColumnConfig config = ColumnConfig.fromMap(map);
    assertNotNull(config);
    assertTrue(config.isComputed());
  }

  @Test void testFromMapNull() {
    assertNull(ColumnConfig.fromMap(null));
  }

  @Test void testFromListEmpty() {
    List<ColumnConfig> result = ColumnConfig.fromList(null);
    assertTrue(result.isEmpty());

    result = ColumnConfig.fromList(Collections.emptyList());
    assertTrue(result.isEmpty());
  }

  @Test void testFromListWithMaps() {
    List<Object> list = new ArrayList<Object>();
    Map<String, Object> col1 = new HashMap<String, Object>();
    col1.put("name", "col1");
    col1.put("type", "VARCHAR");
    list.add(col1);

    Map<String, Object> col2 = new HashMap<String, Object>();
    col2.put("name", "col2");
    col2.put("type", "INTEGER");
    list.add(col2);

    List<ColumnConfig> result = ColumnConfig.fromList(list);
    assertEquals(2, result.size());
    assertEquals("col1", result.get(0).getName());
    assertEquals("col2", result.get(1).getName());
  }

  @Test void testFromListSkipsNonMaps() {
    List<Object> list = new ArrayList<Object>();
    list.add("not a map");
    Map<String, Object> col = new HashMap<String, Object>();
    col.put("name", "col1");
    list.add(col);

    List<ColumnConfig> result = ColumnConfig.fromList(list);
    assertEquals(1, result.size());
  }

  @Test void testFromListThrowsOnNullName() {
    List<Object> list = new ArrayList<Object>();
    Map<String, Object> col = new HashMap<String, Object>();
    col.put("type", "VARCHAR");
    // name is null - builder requires name
    list.add(col);

    try {
      ColumnConfig.fromList(list);
      assertTrue(false, "Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("name"));
    }
  }

  @Test void testIsComputedEmptyExpression() {
    ColumnConfig config = ColumnConfig.builder()
        .name("col")
        .expression("")
        .build();
    assertFalse(config.isComputed());
  }

  @Test void testBuildSelectExpressionSameSourceAndName() {
    ColumnConfig config = ColumnConfig.builder()
        .name("year")
        .source("year")
        .build();

    assertEquals("year", config.buildSelectExpression());
  }
}
