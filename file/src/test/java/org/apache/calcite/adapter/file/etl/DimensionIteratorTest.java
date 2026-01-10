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

import java.util.Arrays;
import java.util.Calendar;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for DimensionIterator and DimensionConfig.
 */
@Tag("unit")
public class DimensionIteratorTest {

  @Test void testDimensionTypeFromString() {
    assertEquals(DimensionType.RANGE, DimensionType.fromString("range"));
    assertEquals(DimensionType.RANGE, DimensionType.fromString("RANGE"));
    assertEquals(DimensionType.LIST, DimensionType.fromString("list"));
    assertEquals(DimensionType.QUERY, DimensionType.fromString("query"));
    assertEquals(DimensionType.YEAR_RANGE, DimensionType.fromString("yearRange"));
    assertEquals(DimensionType.YEAR_RANGE, DimensionType.fromString("year_range"));
    assertEquals(DimensionType.LIST, DimensionType.fromString("unknown"));
    assertEquals(DimensionType.LIST, DimensionType.fromString(null));
  }

  @Test void testDimensionConfigRangeBuilder() {
    DimensionConfig config = DimensionConfig.builder()
        .name("year")
        .type(DimensionType.RANGE)
        .start(2020)
        .end(2024)
        .step(1)
        .build();

    assertEquals("year", config.getName());
    assertEquals(DimensionType.RANGE, config.getType());
    assertEquals(Integer.valueOf(2020), config.getStart());
    assertEquals(Integer.valueOf(2024), config.getEnd());
    assertEquals(Integer.valueOf(1), config.getStep());
  }

  @Test void testDimensionConfigListBuilder() {
    DimensionConfig config = DimensionConfig.builder()
        .name("region")
        .type(DimensionType.LIST)
        .values(Arrays.asList("NORTH", "SOUTH", "EAST", "WEST"))
        .build();

    assertEquals("region", config.getName());
    assertEquals(DimensionType.LIST, config.getType());
    assertEquals(4, config.getValues().size());
    assertEquals("NORTH", config.getValues().get(0));
  }

  @Test void testDimensionConfigQueryBuilder() {
    DimensionConfig config = DimensionConfig.builder()
        .name("region")
        .type(DimensionType.QUERY)
        .sql("SELECT DISTINCT region FROM regions WHERE active = true")
        .build();

    assertEquals("region", config.getName());
    assertEquals(DimensionType.QUERY, config.getType());
    assertEquals("SELECT DISTINCT region FROM regions WHERE active = true", config.getSql());
  }

  @Test void testDimensionConfigFromMap() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("type", "range");
    map.put("start", 2020);
    map.put("end", 2024);
    map.put("step", 1);

    DimensionConfig config = DimensionConfig.fromMap("year", map);

    assertEquals("year", config.getName());
    assertEquals(DimensionType.RANGE, config.getType());
    assertEquals(Integer.valueOf(2020), config.getStart());
    assertEquals(Integer.valueOf(2024), config.getEnd());
  }

  @Test void testDimensionConfigFromMapWithList() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("type", "list");
    map.put("values", Arrays.asList("A", "B", "C"));

    DimensionConfig config = DimensionConfig.fromMap("frequency", map);

    assertEquals("frequency", config.getName());
    assertEquals(DimensionType.LIST, config.getType());
    assertEquals(3, config.getValues().size());
  }

  @Test void testDimensionConfigFromMapCurrentYear() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("type", "yearRange");
    map.put("start", 2020);
    map.put("end", "current");

    DimensionConfig config = DimensionConfig.fromMap("year", map);

    assertEquals("year", config.getName());
    assertEquals(DimensionType.YEAR_RANGE, config.getType());
    assertEquals(Integer.valueOf(2020), config.getStart());
    assertEquals(null, config.getEnd());  // "current" resolved to null, will be runtime evaluated
  }

  @Test void testDimensionConfigFromDimensionsMap() {
    Map<String, Object> dimensionsMap = new LinkedHashMap<String, Object>();

    Map<String, Object> yearMap = new HashMap<String, Object>();
    yearMap.put("type", "range");
    yearMap.put("start", 2020);
    yearMap.put("end", 2022);
    dimensionsMap.put("year", yearMap);

    Map<String, Object> regionMap = new HashMap<String, Object>();
    regionMap.put("type", "list");
    regionMap.put("values", Arrays.asList("N", "S"));
    dimensionsMap.put("region", regionMap);

    Map<String, DimensionConfig> configs = DimensionConfig.fromDimensionsMap(dimensionsMap);

    assertEquals(2, configs.size());
    assertTrue(configs.containsKey("year"));
    assertTrue(configs.containsKey("region"));
    assertEquals(DimensionType.RANGE, configs.get("year").getType());
    assertEquals(DimensionType.LIST, configs.get("region").getType());
  }

  @Test void testDimensionConfigRequiresName() {
    assertThrows(IllegalArgumentException.class, () -> {
      DimensionConfig.builder()
          .type(DimensionType.LIST)
          .values(Arrays.asList("A", "B"))
          .build();
    });
  }

  @Test void testDimensionIteratorExpandRange() {
    Map<String, DimensionConfig> dimensions = new LinkedHashMap<String, DimensionConfig>();
    dimensions.put("year", DimensionConfig.builder()
        .name("year")
        .type(DimensionType.RANGE)
        .start(2020)
        .end(2022)
        .build());

    DimensionIterator iterator = new DimensionIterator();
    List<Map<String, String>> combinations = iterator.expand(dimensions);

    assertEquals(3, combinations.size());
    assertEquals("2020", combinations.get(0).get("year"));
    assertEquals("2021", combinations.get(1).get("year"));
    assertEquals("2022", combinations.get(2).get("year"));
  }

  @Test void testDimensionIteratorExpandRangeWithStep() {
    Map<String, DimensionConfig> dimensions = new LinkedHashMap<String, DimensionConfig>();
    dimensions.put("year", DimensionConfig.builder()
        .name("year")
        .type(DimensionType.RANGE)
        .start(2020)
        .end(2024)
        .step(2)
        .build());

    DimensionIterator iterator = new DimensionIterator();
    List<Map<String, String>> combinations = iterator.expand(dimensions);

    assertEquals(3, combinations.size());
    assertEquals("2020", combinations.get(0).get("year"));
    assertEquals("2022", combinations.get(1).get("year"));
    assertEquals("2024", combinations.get(2).get("year"));
  }

  @Test void testDimensionIteratorExpandList() {
    Map<String, DimensionConfig> dimensions = new LinkedHashMap<String, DimensionConfig>();
    dimensions.put("region", DimensionConfig.builder()
        .name("region")
        .type(DimensionType.LIST)
        .values(Arrays.asList("NORTH", "SOUTH", "EAST", "WEST"))
        .build());

    DimensionIterator iterator = new DimensionIterator();
    List<Map<String, String>> combinations = iterator.expand(dimensions);

    assertEquals(4, combinations.size());
    assertEquals("NORTH", combinations.get(0).get("region"));
    assertEquals("SOUTH", combinations.get(1).get("region"));
    assertEquals("EAST", combinations.get(2).get("region"));
    assertEquals("WEST", combinations.get(3).get("region"));
  }

  @Test void testDimensionIteratorExpandYearRange() {
    int currentYear = Calendar.getInstance().get(Calendar.YEAR);

    Map<String, DimensionConfig> dimensions = new LinkedHashMap<String, DimensionConfig>();
    dimensions.put("year", DimensionConfig.builder()
        .name("year")
        .type(DimensionType.YEAR_RANGE)
        .start(currentYear - 2)
        .end(null)  // "current"
        .build());

    DimensionIterator iterator = new DimensionIterator();
    List<Map<String, String>> combinations = iterator.expand(dimensions);

    assertEquals(3, combinations.size());
    assertEquals(String.valueOf(currentYear - 2), combinations.get(0).get("year"));
    assertEquals(String.valueOf(currentYear - 1), combinations.get(1).get("year"));
    assertEquals(String.valueOf(currentYear), combinations.get(2).get("year"));
  }

  @Test void testDimensionIteratorCartesianProduct() {
    Map<String, DimensionConfig> dimensions = new LinkedHashMap<String, DimensionConfig>();
    dimensions.put("year", DimensionConfig.builder()
        .name("year")
        .type(DimensionType.RANGE)
        .start(2020)
        .end(2021)
        .build());
    dimensions.put("region", DimensionConfig.builder()
        .name("region")
        .type(DimensionType.LIST)
        .values(Arrays.asList("N", "S", "E"))
        .build());

    DimensionIterator iterator = new DimensionIterator();
    List<Map<String, String>> combinations = iterator.expand(dimensions);

    // 2 years x 3 regions = 6 combinations
    assertEquals(6, combinations.size());

    // Verify first combination (year=2020, region=N)
    assertEquals("2020", combinations.get(0).get("year"));
    assertEquals("N", combinations.get(0).get("region"));

    // Verify last combination (year=2021, region=E)
    assertEquals("2021", combinations.get(5).get("year"));
    assertEquals("E", combinations.get(5).get("region"));
  }

  @Test void testDimensionIteratorThreeDimensions() {
    Map<String, DimensionConfig> dimensions = new LinkedHashMap<String, DimensionConfig>();
    dimensions.put("year", DimensionConfig.builder()
        .name("year")
        .type(DimensionType.RANGE)
        .start(2020)
        .end(2021)
        .build());
    dimensions.put("region", DimensionConfig.builder()
        .name("region")
        .type(DimensionType.LIST)
        .values(Arrays.asList("N", "S"))
        .build());
    dimensions.put("freq", DimensionConfig.builder()
        .name("freq")
        .type(DimensionType.LIST)
        .values(Arrays.asList("A", "M", "Q"))
        .build());

    DimensionIterator iterator = new DimensionIterator();
    List<Map<String, String>> combinations = iterator.expand(dimensions);

    // 2 years x 2 regions x 3 frequencies = 12 combinations
    assertEquals(12, combinations.size());

    // Each combination should have all three dimensions
    for (Map<String, String> combo : combinations) {
      assertEquals(3, combo.size());
      assertTrue(combo.containsKey("year"));
      assertTrue(combo.containsKey("region"));
      assertTrue(combo.containsKey("freq"));
    }
  }

  @Test void testDimensionIteratorEmptyDimensions() {
    DimensionIterator iterator = new DimensionIterator();
    List<Map<String, String>> combinations = iterator.expand(null);

    // Should return single empty map
    assertEquals(1, combinations.size());
    assertTrue(combinations.get(0).isEmpty());
  }

  @Test void testDimensionIteratorEmptyDimensionsMap() {
    DimensionIterator iterator = new DimensionIterator();
    List<Map<String, String>> combinations =
        iterator.expand(new LinkedHashMap<String, DimensionConfig>());

    // Should return single empty map
    assertEquals(1, combinations.size());
    assertTrue(combinations.get(0).isEmpty());
  }

  @Test void testDimensionIteratorQueryWithoutConnection() {
    Map<String, DimensionConfig> dimensions = new LinkedHashMap<String, DimensionConfig>();
    dimensions.put("region", DimensionConfig.builder()
        .name("region")
        .type(DimensionType.QUERY)
        .sql("SELECT DISTINCT region FROM regions")
        .build());

    DimensionIterator iterator = new DimensionIterator();

    assertThrows(IllegalStateException.class, () -> {
      iterator.expand(dimensions);
    });
  }

  @Test void testDimensionIteratorResolveDimension() {
    DimensionIterator iterator = new DimensionIterator();

    // Test RANGE
    DimensionConfig rangeConfig = DimensionConfig.builder()
        .name("year")
        .type(DimensionType.RANGE)
        .start(2020)
        .end(2022)
        .build();
    List<String> rangeValues = iterator.resolveDimension(rangeConfig);
    assertEquals(Arrays.asList("2020", "2021", "2022"), rangeValues);

    // Test LIST
    DimensionConfig listConfig = DimensionConfig.builder()
        .name("region")
        .type(DimensionType.LIST)
        .values(Arrays.asList("A", "B"))
        .build();
    List<String> listValues = iterator.resolveDimension(listConfig);
    assertEquals(Arrays.asList("A", "B"), listValues);
  }

  @Test void testDimensionIteratorRangeWithMissingBounds() {
    DimensionIterator iterator = new DimensionIterator();

    // Missing start
    DimensionConfig noStart = DimensionConfig.builder()
        .name("year")
        .type(DimensionType.RANGE)
        .end(2024)
        .build();
    List<String> noStartValues = iterator.resolveDimension(noStart);
    assertTrue(noStartValues.isEmpty());

    // Missing end
    DimensionConfig noEnd = DimensionConfig.builder()
        .name("year")
        .type(DimensionType.RANGE)
        .start(2020)
        .build();
    List<String> noEndValues = iterator.resolveDimension(noEnd);
    assertTrue(noEndValues.isEmpty());
  }

  @Test void testDimensionConfigToString() {
    DimensionConfig rangeConfig = DimensionConfig.builder()
        .name("year")
        .type(DimensionType.RANGE)
        .start(2020)
        .end(2024)
        .build();

    String toString = rangeConfig.toString();
    assertTrue(toString.contains("year"));
    assertTrue(toString.contains("RANGE"));
    assertTrue(toString.contains("2020"));
    assertTrue(toString.contains("2024"));
  }

  @Test void testDimensionIteratorPreservesOrder() {
    Map<String, DimensionConfig> dimensions = new LinkedHashMap<String, DimensionConfig>();
    dimensions.put("a", DimensionConfig.builder()
        .name("a")
        .type(DimensionType.LIST)
        .values(Arrays.asList("1", "2"))
        .build());
    dimensions.put("b", DimensionConfig.builder()
        .name("b")
        .type(DimensionType.LIST)
        .values(Arrays.asList("x", "y"))
        .build());

    DimensionIterator iterator = new DimensionIterator();
    List<Map<String, String>> combinations = iterator.expand(dimensions);

    // Order should be a-first iteration: (1,x), (1,y), (2,x), (2,y)
    assertEquals(4, combinations.size());
    assertEquals("1", combinations.get(0).get("a"));
    assertEquals("x", combinations.get(0).get("b"));
    assertEquals("1", combinations.get(1).get("a"));
    assertEquals("y", combinations.get(1).get("b"));
    assertEquals("2", combinations.get(2).get("a"));
    assertEquals("x", combinations.get(2).get("b"));
    assertEquals("2", combinations.get(3).get("a"));
    assertEquals("y", combinations.get(3).get("b"));
  }
}
