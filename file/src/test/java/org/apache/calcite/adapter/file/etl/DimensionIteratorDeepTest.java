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

import org.apache.calcite.adapter.file.storage.LocalFileStorageProvider;
import org.apache.calcite.adapter.file.storage.StorageProvider;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Deep tests for {@link DimensionIterator}, {@link DimensionPartitionPlan},
 * {@link DimensionPartition}, and CUSTOM dimension expansion.
 */
@Tag("unit")
class DimensionIteratorDeepTest {

  private final StorageProvider storageProvider = new LocalFileStorageProvider();

  /**
   * A simple DimensionResolver for testing that returns hard-coded values
   * based on context.
   */
  private final DimensionResolver testResolver = new DimensionResolver() {
    @Override public List<String> resolve(String dimensionName, DimensionConfig config,
        Map<String, String> context, StorageProvider sp) {
      if ("custom_dim".equals(dimensionName)) {
        String state = context.get("state");
        if ("CA".equals(state)) {
          return Arrays.asList("CA001", "CA002", "CA003");
        } else if ("NY".equals(state)) {
          return Arrays.asList("NY001", "NY002");
        } else if ("TX".equals(state)) {
          return Collections.emptyList(); // Empty for TX
        }
        return Arrays.asList("DEFAULT1");
      }
      if ("custom_dim2".equals(dimensionName)) {
        return Arrays.asList("X", "Y");
      }
      return Collections.emptyList();
    }
  };

  // --- CUSTOM dimension expansion ---

  @Test void testExpandWithCustomDimension() {
    DimensionIterator iterator = new DimensionIterator(testResolver, storageProvider);

    Map<String, DimensionConfig> dimensions = new LinkedHashMap<String, DimensionConfig>();
    dimensions.put("state", DimensionConfig.builder()
        .name("state")
        .type(DimensionType.LIST)
        .values(Arrays.asList("CA", "NY"))
        .build());
    dimensions.put("custom_dim", DimensionConfig.builder()
        .name("custom_dim")
        .type(DimensionType.CUSTOM)
        .build());

    List<Map<String, String>> combinations = iterator.expand(dimensions);

    // CA has 3 custom values, NY has 2 = 5 combinations total
    assertEquals(5, combinations.size());

    // Verify CA combinations
    assertEquals("CA", combinations.get(0).get("state"));
    assertEquals("CA001", combinations.get(0).get("custom_dim"));
    assertEquals("CA", combinations.get(1).get("state"));
    assertEquals("CA002", combinations.get(1).get("custom_dim"));
    assertEquals("CA", combinations.get(2).get("state"));
    assertEquals("CA003", combinations.get(2).get("custom_dim"));

    // Verify NY combinations
    assertEquals("NY", combinations.get(3).get("state"));
    assertEquals("NY001", combinations.get(3).get("custom_dim"));
    assertEquals("NY", combinations.get(4).get("state"));
    assertEquals("NY002", combinations.get(4).get("custom_dim"));
  }

  @Test void testExpandWithCustomDimensionEmptyForSomeContexts() {
    DimensionIterator iterator = new DimensionIterator(testResolver, storageProvider);

    Map<String, DimensionConfig> dimensions = new LinkedHashMap<String, DimensionConfig>();
    dimensions.put("state", DimensionConfig.builder()
        .name("state")
        .type(DimensionType.LIST)
        .values(Arrays.asList("CA", "TX"))
        .build());
    dimensions.put("custom_dim", DimensionConfig.builder()
        .name("custom_dim")
        .type(DimensionType.CUSTOM)
        .build());

    List<Map<String, String>> combinations = iterator.expand(dimensions);

    // CA has 3 values, TX has 0 (skipped) = 3 total
    assertEquals(3, combinations.size());
    for (Map<String, String> combo : combinations) {
      assertEquals("CA", combo.get("state"));
    }
  }

  @Test void testExpandCustomDimensionWithoutResolver() {
    DimensionIterator iterator = new DimensionIterator(); // No resolver

    Map<String, DimensionConfig> dimensions = new LinkedHashMap<String, DimensionConfig>();
    dimensions.put("custom_dim", DimensionConfig.builder()
        .name("custom_dim")
        .type(DimensionType.CUSTOM)
        .build());

    // Without resolver, CUSTOM dimensions should throw
    assertThrows(IllegalStateException.class, () -> iterator.expand(dimensions));
  }

  @Test void testExpandCustomWithNonCustomDimensions() {
    // When there are CUSTOM dims but also non-CUSTOM, the context-aware path is used
    DimensionIterator iterator = new DimensionIterator(testResolver, storageProvider);

    Map<String, DimensionConfig> dimensions = new LinkedHashMap<String, DimensionConfig>();
    dimensions.put("year", DimensionConfig.builder()
        .name("year")
        .type(DimensionType.RANGE)
        .start(2020)
        .end(2021)
        .build());
    dimensions.put("state", DimensionConfig.builder()
        .name("state")
        .type(DimensionType.LIST)
        .values(Arrays.asList("CA", "NY"))
        .build());
    dimensions.put("custom_dim", DimensionConfig.builder()
        .name("custom_dim")
        .type(DimensionType.CUSTOM)
        .build());

    List<Map<String, String>> combinations = iterator.expand(dimensions);

    // 2 years x (CA=3 + NY=2) = 2 x 5 = 10
    assertEquals(10, combinations.size());

    // Each combination should have year, state, custom_dim
    for (Map<String, String> combo : combinations) {
      assertEquals(3, combo.size());
      assertNotNull(combo.get("year"));
      assertNotNull(combo.get("state"));
      assertNotNull(combo.get("custom_dim"));
    }
  }

  // --- planPartitions and expandPartition ---

  @Test void testPlanPartitionsNull() {
    DimensionIterator iterator = new DimensionIterator(testResolver, storageProvider);
    assertNull(iterator.planPartitions(null));
  }

  @Test void testPlanPartitionsEmpty() {
    DimensionIterator iterator = new DimensionIterator(testResolver, storageProvider);
    assertNull(iterator.planPartitions(new LinkedHashMap<String, DimensionConfig>()));
  }

  @Test void testPlanPartitionsNoCustomDimensions() {
    DimensionIterator iterator = new DimensionIterator(testResolver, storageProvider);

    Map<String, DimensionConfig> dimensions = new LinkedHashMap<String, DimensionConfig>();
    dimensions.put("year", DimensionConfig.builder()
        .name("year")
        .type(DimensionType.RANGE)
        .start(2020)
        .end(2022)
        .build());

    assertNull(iterator.planPartitions(dimensions));
  }

  @Test void testPlanPartitionsNoResolver() {
    DimensionIterator iterator = new DimensionIterator(); // No resolver

    Map<String, DimensionConfig> dimensions = new LinkedHashMap<String, DimensionConfig>();
    dimensions.put("state", DimensionConfig.builder()
        .name("state")
        .type(DimensionType.LIST)
        .values(Arrays.asList("CA", "NY"))
        .build());
    dimensions.put("custom_dim", DimensionConfig.builder()
        .name("custom_dim")
        .type(DimensionType.CUSTOM)
        .build());

    assertNull(iterator.planPartitions(dimensions));
  }

  @Test void testPlanPartitionsWithCustomDimension() {
    DimensionIterator iterator = new DimensionIterator(testResolver, storageProvider);

    Map<String, DimensionConfig> dimensions = new LinkedHashMap<String, DimensionConfig>();
    dimensions.put("state", DimensionConfig.builder()
        .name("state")
        .type(DimensionType.LIST)
        .values(Arrays.asList("CA", "NY", "TX"))
        .build());
    dimensions.put("custom_dim", DimensionConfig.builder()
        .name("custom_dim")
        .type(DimensionType.CUSTOM)
        .build());

    DimensionPartitionPlan plan = iterator.planPartitions(dimensions);

    assertNotNull(plan);
    assertEquals("state", plan.getContextKey());
    assertEquals(3, plan.getPartitionCount());
    assertTrue(plan.getContextValues().contains("CA"));
    assertTrue(plan.getContextValues().contains("NY"));
    assertTrue(plan.getContextValues().contains("TX"));
    assertEquals("custom_dim", plan.getCustomDimName());
    assertNotNull(plan.getCustomDimConfig());
    assertEquals(1, plan.getCustomDimNames().size());
    assertEquals(1, plan.getCustomDimConfigs().size());
    assertEquals(3, plan.getTotalPrefixCount());
  }

  @Test void testExpandPartitionCA() {
    DimensionIterator iterator = new DimensionIterator(testResolver, storageProvider);

    Map<String, DimensionConfig> dimensions = new LinkedHashMap<String, DimensionConfig>();
    dimensions.put("state", DimensionConfig.builder()
        .name("state")
        .type(DimensionType.LIST)
        .values(Arrays.asList("CA", "NY", "TX"))
        .build());
    dimensions.put("custom_dim", DimensionConfig.builder()
        .name("custom_dim")
        .type(DimensionType.CUSTOM)
        .build());

    DimensionPartitionPlan plan = iterator.planPartitions(dimensions);
    assertNotNull(plan);

    // Expand CA partition
    DimensionPartition caPartition = iterator.expandPartition(plan, "CA");
    assertNotNull(caPartition);
    assertEquals(3, caPartition.getCombinations().size());
    assertEquals("CA", caPartition.getPartitionContext().get("state"));
    assertEquals("CA001", caPartition.getCombinations().get(0).get("custom_dim"));
  }

  @Test void testExpandPartitionNY() {
    DimensionIterator iterator = new DimensionIterator(testResolver, storageProvider);

    Map<String, DimensionConfig> dimensions = new LinkedHashMap<String, DimensionConfig>();
    dimensions.put("state", DimensionConfig.builder()
        .name("state")
        .type(DimensionType.LIST)
        .values(Arrays.asList("CA", "NY"))
        .build());
    dimensions.put("custom_dim", DimensionConfig.builder()
        .name("custom_dim")
        .type(DimensionType.CUSTOM)
        .build());

    DimensionPartitionPlan plan = iterator.planPartitions(dimensions);
    DimensionPartition nyPartition = iterator.expandPartition(plan, "NY");

    assertNotNull(nyPartition);
    assertEquals(2, nyPartition.getCombinations().size());
    assertEquals("NY001", nyPartition.getCombinations().get(0).get("custom_dim"));
    assertEquals("NY002", nyPartition.getCombinations().get(1).get("custom_dim"));
  }

  @Test void testExpandPartitionEmpty() {
    DimensionIterator iterator = new DimensionIterator(testResolver, storageProvider);

    Map<String, DimensionConfig> dimensions = new LinkedHashMap<String, DimensionConfig>();
    dimensions.put("state", DimensionConfig.builder()
        .name("state")
        .type(DimensionType.LIST)
        .values(Arrays.asList("CA", "TX"))
        .build());
    dimensions.put("custom_dim", DimensionConfig.builder()
        .name("custom_dim")
        .type(DimensionType.CUSTOM)
        .build());

    DimensionPartitionPlan plan = iterator.planPartitions(dimensions);
    DimensionPartition txPartition = iterator.expandPartition(plan, "TX");

    // TX returns empty custom values, so partition should be null
    assertNull(txPartition);
  }

  @Test void testExpandPartitionNonexistentContext() {
    DimensionIterator iterator = new DimensionIterator(testResolver, storageProvider);

    Map<String, DimensionConfig> dimensions = new LinkedHashMap<String, DimensionConfig>();
    dimensions.put("state", DimensionConfig.builder()
        .name("state")
        .type(DimensionType.LIST)
        .values(Arrays.asList("CA"))
        .build());
    dimensions.put("custom_dim", DimensionConfig.builder()
        .name("custom_dim")
        .type(DimensionType.CUSTOM)
        .build());

    DimensionPartitionPlan plan = iterator.planPartitions(dimensions);

    // ZZ doesn't exist in the prefix combinations
    DimensionPartition zzPartition = iterator.expandPartition(plan, "ZZ");
    assertNull(zzPartition);
  }

  @Test void testPlanPartitionsWithContextKeyProperty() {
    DimensionIterator iterator = new DimensionIterator(testResolver, storageProvider);

    Map<String, String> properties = new HashMap<String, String>();
    properties.put("contextKey", "state");

    Map<String, DimensionConfig> dimensions = new LinkedHashMap<String, DimensionConfig>();
    dimensions.put("year", DimensionConfig.builder()
        .name("year")
        .type(DimensionType.RANGE)
        .start(2020)
        .end(2021)
        .build());
    dimensions.put("state", DimensionConfig.builder()
        .name("state")
        .type(DimensionType.LIST)
        .values(Arrays.asList("CA", "NY"))
        .build());
    dimensions.put("custom_dim", DimensionConfig.builder()
        .name("custom_dim")
        .type(DimensionType.CUSTOM)
        .properties(properties)
        .build());

    DimensionPartitionPlan plan = iterator.planPartitions(dimensions);

    assertNotNull(plan);
    assertEquals("state", plan.getContextKey());
    assertEquals(2, plan.getPartitionCount());
    // 2 years x 2 states = 4 prefix combinations
    assertEquals(4, plan.getTotalPrefixCount());
  }

  @Test void testExpandPartitionWithMultiplePrefixDimensions() {
    DimensionIterator iterator = new DimensionIterator(testResolver, storageProvider);

    Map<String, DimensionConfig> dimensions = new LinkedHashMap<String, DimensionConfig>();
    dimensions.put("year", DimensionConfig.builder()
        .name("year")
        .type(DimensionType.RANGE)
        .start(2020)
        .end(2021)
        .build());
    dimensions.put("state", DimensionConfig.builder()
        .name("state")
        .type(DimensionType.LIST)
        .values(Arrays.asList("CA", "NY"))
        .build());
    dimensions.put("custom_dim", DimensionConfig.builder()
        .name("custom_dim")
        .type(DimensionType.CUSTOM)
        .build());

    DimensionPartitionPlan plan = iterator.planPartitions(dimensions);
    assertNotNull(plan);

    DimensionPartition caPartition = iterator.expandPartition(plan, "CA");
    assertNotNull(caPartition);
    // 2 years x 3 custom values for CA = 6
    assertEquals(6, caPartition.getCombinations().size());

    DimensionPartition nyPartition = iterator.expandPartition(plan, "NY");
    assertNotNull(nyPartition);
    // 2 years x 2 custom values for NY = 4
    assertEquals(4, nyPartition.getCombinations().size());
  }

  // --- DimensionPartitionPlan tests ---

  @Test void testDimensionPartitionPlanToString() {
    DimensionIterator iterator = new DimensionIterator(testResolver, storageProvider);

    Map<String, DimensionConfig> dimensions = new LinkedHashMap<String, DimensionConfig>();
    dimensions.put("state", DimensionConfig.builder()
        .name("state")
        .type(DimensionType.LIST)
        .values(Arrays.asList("CA", "NY"))
        .build());
    dimensions.put("custom_dim", DimensionConfig.builder()
        .name("custom_dim")
        .type(DimensionType.CUSTOM)
        .build());

    DimensionPartitionPlan plan = iterator.planPartitions(dimensions);
    String str = plan.toString();

    assertTrue(str.contains("state"));
    assertTrue(str.contains("partitions=2"));
    assertTrue(str.contains("custom_dim"));
  }

  @Test void testDimensionPartitionPlanPrefixForMissingContext() {
    DimensionIterator iterator = new DimensionIterator(testResolver, storageProvider);

    Map<String, DimensionConfig> dimensions = new LinkedHashMap<String, DimensionConfig>();
    dimensions.put("state", DimensionConfig.builder()
        .name("state")
        .type(DimensionType.LIST)
        .values(Arrays.asList("CA"))
        .build());
    dimensions.put("custom_dim", DimensionConfig.builder()
        .name("custom_dim")
        .type(DimensionType.CUSTOM)
        .build());

    DimensionPartitionPlan plan = iterator.planPartitions(dimensions);
    List<Map<String, String>> missing = plan.getPrefixCombinations("ZZ");
    assertTrue(missing.isEmpty());
  }

  // --- DimensionPartition tests ---

  @Test void testDimensionPartitionToString() {
    Map<String, String> context = Collections.singletonMap("state", "CA");
    List<Map<String, String>> combos =
        Arrays.asList(createCombo("state", "CA", "ori", "CA001"),
        createCombo("state", "CA", "ori", "CA002"));

    DimensionPartition partition = new DimensionPartition(context, combos);

    String str = partition.toString();
    assertTrue(str.contains("CA"));
    assertTrue(str.contains("combinations=2"));
  }

  @Test void testDimensionPartitionContextImmutable() {
    Map<String, String> context = new HashMap<String, String>();
    context.put("state", "CA");
    List<Map<String, String>> combos = Collections.emptyList();

    DimensionPartition partition = new DimensionPartition(context, combos);

    try {
      partition.getPartitionContext().put("sneaky", "val");
      assertTrue(false, "Expected UnsupportedOperationException");
    } catch (UnsupportedOperationException e) {
      // Expected
    }
  }

  // --- YEAR_RANGE with dataLag ---

  @Test void testYearRangeWithDataLag() {
    DimensionIterator iterator = new DimensionIterator();
    int currentYear = Calendar.getInstance().get(Calendar.YEAR);

    DimensionConfig config = DimensionConfig.builder()
        .name("year")
        .type(DimensionType.YEAR_RANGE)
        .start(currentYear - 2)
        .dataLag(1) // data through current - 1
        .build();

    List<String> values = iterator.resolveDimension(config);

    // start to currentYear-1 = 2 values
    assertEquals(2, values.size());
    assertEquals(String.valueOf(currentYear - 2), values.get(0));
    assertEquals(String.valueOf(currentYear - 1), values.get(1));
  }

  @Test void testYearRangeWithExcludeYears() {
    DimensionIterator iterator = new DimensionIterator();

    DimensionConfig config = DimensionConfig.builder()
        .name("year")
        .type(DimensionType.YEAR_RANGE)
        .start(2018)
        .end(2022)
        .excludeYears(Arrays.asList(2019, 2021))
        .build();

    List<String> values = iterator.resolveDimension(config);

    assertEquals(3, values.size());
    assertEquals("2018", values.get(0));
    assertEquals("2020", values.get(1));
    assertEquals("2022", values.get(2));
  }

  @Test void testYearRangeMissingStart() {
    DimensionIterator iterator = new DimensionIterator();

    DimensionConfig config = DimensionConfig.builder()
        .name("year")
        .type(DimensionType.YEAR_RANGE)
        .end(2022)
        .build();

    List<String> values = iterator.resolveDimension(config);
    assertTrue(values.isEmpty());
  }

  // --- JSON_CATALOG dimension ---

  @Test void testJsonCatalogDimensionNoSource() {
    DimensionIterator iterator = new DimensionIterator();

    DimensionConfig config = DimensionConfig.builder()
        .name("items")
        .type(DimensionType.JSON_CATALOG)
        .build();

    List<String> values = iterator.resolveDimension(config);
    assertTrue(values.isEmpty());
  }

  // --- Factory methods ---

  @Test void testCreateStaticFactory() {
    DimensionIterator iterator = DimensionIterator.create();
    assertNotNull(iterator);

    // Should work for basic expansion
    Map<String, DimensionConfig> dims = new LinkedHashMap<String, DimensionConfig>();
    dims.put("x", DimensionConfig.builder()
        .name("x")
        .type(DimensionType.LIST)
        .values(Arrays.asList("A", "B"))
        .build());

    List<Map<String, String>> combos = iterator.expand(dims);
    assertEquals(2, combos.size());
  }

  // --- Constructors ---

  @Test void testDimensionIteratorWithResolverOnly() {
    DimensionIterator iterator = new DimensionIterator(testResolver, storageProvider);

    Map<String, DimensionConfig> dims = new LinkedHashMap<String, DimensionConfig>();
    dims.put("custom_dim", DimensionConfig.builder()
        .name("custom_dim")
        .type(DimensionType.CUSTOM)
        .build());

    List<Map<String, String>> combos = iterator.expand(dims);
    // No context provided initially, so resolver gets empty context -> returns "DEFAULT1"
    assertEquals(1, combos.size());
    assertEquals("DEFAULT1", combos.get(0).get("custom_dim"));
  }

  // --- DimensionConfig additional coverage ---

  @Test void testDimensionConfigFromMapWithProperties() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("type", "custom");

    Map<String, Object> props = new HashMap<String, Object>();
    props.put("contextKey", "state_abbr");
    props.put("catalogPath", "/data/catalog.json");
    map.put("properties", props);

    DimensionConfig config = DimensionConfig.fromMap("my_dim", map);

    assertEquals("my_dim", config.getName());
    assertEquals(DimensionType.CUSTOM, config.getType());
    assertNotNull(config.getProperties());
    assertEquals("state_abbr", config.getProperties().get("contextKey"));
  }

  @Test void testDimensionConfigFromMapWithDataLag() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("type", "yearRange");
    map.put("start", 2015);
    map.put("dataLag", 1);

    DimensionConfig config = DimensionConfig.fromMap("year", map);

    assertEquals(DimensionType.YEAR_RANGE, config.getType());
    assertEquals(Integer.valueOf(2015), config.getStart());
    assertEquals(Integer.valueOf(1), config.getDataLag());
  }

  @Test void testDimensionConfigFromMapWithExcludeYears() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("type", "yearRange");
    map.put("start", 2015);
    map.put("end", 2025);
    map.put("excludeYears", Arrays.asList(2020));

    DimensionConfig config = DimensionConfig.fromMap("year", map);

    assertNotNull(config.getExcludeYears());
    assertEquals(1, config.getExcludeYears().size());
    assertTrue(config.getExcludeYears().contains(2020));
  }

  @Test void testDimensionConfigFromMapJsonCatalog() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("type", "jsonCatalog");
    map.put("source", "/catalog/states.json");
    map.put("path", "states[*].code");

    DimensionConfig config = DimensionConfig.fromMap("state", map);

    assertEquals(DimensionType.JSON_CATALOG, config.getType());
    assertEquals("/catalog/states.json", config.getSource());
    assertEquals("states[*].code", config.getPath());
  }

  @Test void testDimensionTypeFromStringJsonCatalog() {
    assertEquals(DimensionType.JSON_CATALOG, DimensionType.fromString("jsonCatalog"));
    assertEquals(DimensionType.JSON_CATALOG, DimensionType.fromString("json_catalog"));
    assertEquals(DimensionType.CUSTOM, DimensionType.fromString("custom"));
  }

  private Map<String, String> createCombo(String... kvPairs) {
    Map<String, String> map = new LinkedHashMap<String, String>();
    for (int i = 0; i < kvPairs.length; i += 2) {
      map.put(kvPairs[i], kvPairs[i + 1]);
    }
    return map;
  }
}
