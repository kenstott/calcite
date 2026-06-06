/*
 * Copyright (c) 2026 Kenneth Stott
 *
 * This source code is licensed under the Business Source License 1.1
 * found in the LICENSE-BSL.txt file in the root directory of this source tree.
 *
 * NOTICE: Use of this software for training artificial intelligence or
 * machine learning models is strictly prohibited without explicit written
 * permission from the copyright holder.
 */
package org.apache.calcite.adapter.file.etl;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link DimensionPartitionPlan}.
 */
@Tag("unit")
class DimensionPartitionPlanTest {

  @SuppressWarnings("deprecation")
  @Test void testSingleCustomDimension() {
    DimensionConfig customConfig = DimensionConfig.builder()
        .name("ori")
        .type(DimensionType.CUSTOM)
        .build();

    Map<String, List<Map<String, String>>> prefixByContext =
        new LinkedHashMap<String, List<Map<String, String>>>();

    List<Map<String, String>> caCombos = new ArrayList<Map<String, String>>();
    Map<String, String> ca1 = new HashMap<String, String>();
    ca1.put("state_abbr", "CA");
    ca1.put("year", "2020");
    caCombos.add(ca1);
    prefixByContext.put("CA", caCombos);

    List<Map<String, String>> nyCombos = new ArrayList<Map<String, String>>();
    Map<String, String> ny1 = new HashMap<String, String>();
    ny1.put("state_abbr", "NY");
    ny1.put("year", "2020");
    nyCombos.add(ny1);
    prefixByContext.put("NY", nyCombos);

    DimensionPartitionPlan plan =
        new DimensionPartitionPlan("state_abbr",
        Arrays.asList("CA", "NY"),
        Arrays.asList("ori"),
        Arrays.asList(customConfig),
        prefixByContext);

    assertEquals("state_abbr", plan.getContextKey());
    assertEquals(2, plan.getPartitionCount());
    assertEquals(2, plan.getContextValues().size());
    assertEquals("ori", plan.getCustomDimName());
    assertNotNull(plan.getCustomDimConfig());
    assertEquals(1, plan.getCustomDimNames().size());
    assertEquals(1, plan.getCustomDimConfigs().size());
    assertEquals(1, plan.getPrefixCombinations("CA").size());
    assertEquals(1, plan.getPrefixCombinations("NY").size());
    assertEquals(2, plan.getTotalPrefixCount());
  }

  @Test void testMultiCustomDimension() {
    DimensionConfig customConfig1 = DimensionConfig.builder()
        .name("dataset")
        .type(DimensionType.CUSTOM)
        .build();
    DimensionConfig customConfig2 = DimensionConfig.builder()
        .name("variables")
        .type(DimensionType.CUSTOM)
        .build();

    Map<String, List<Map<String, String>>> prefixByContext =
        new LinkedHashMap<String, List<Map<String, String>>>();

    List<Map<String, String>> combos = new ArrayList<Map<String, String>>();
    Map<String, String> combo = new HashMap<String, String>();
    combo.put("year", "2020");
    combos.add(combo);
    prefixByContext.put("2020", combos);

    DimensionPartitionPlan plan =
        new DimensionPartitionPlan("year",
        Arrays.asList("2020"),
        Arrays.asList("dataset", "variables"),
        Arrays.asList(customConfig1, customConfig2),
        prefixByContext);

    assertEquals("year", plan.getContextKey());
    assertEquals(1, plan.getPartitionCount());
    assertEquals(2, plan.getCustomDimNames().size());
    assertEquals(2, plan.getCustomDimConfigs().size());
    assertEquals("dataset", plan.getCustomDimName()); // backward compat, first
    assertNotNull(plan.getCustomDimConfig());
  }

  @Test void testGetPrefixCombinationsMissing() {
    Map<String, List<Map<String, String>>> prefixByContext =
        new LinkedHashMap<String, List<Map<String, String>>>();

    DimensionPartitionPlan plan =
        new DimensionPartitionPlan("key",
        Collections.<String>emptyList(),
        Collections.<String>emptyList(),
        Collections.<DimensionConfig>emptyList(),
        prefixByContext);

    List<Map<String, String>> result = plan.getPrefixCombinations("nonexistent");
    assertTrue(result.isEmpty());
  }

  @Test void testTotalPrefixCountEmpty() {
    Map<String, List<Map<String, String>>> prefixByContext =
        new LinkedHashMap<String, List<Map<String, String>>>();

    DimensionPartitionPlan plan =
        new DimensionPartitionPlan("key",
        Collections.<String>emptyList(),
        Collections.<String>emptyList(),
        Collections.<DimensionConfig>emptyList(),
        prefixByContext);

    assertEquals(0, plan.getTotalPrefixCount());
  }

  @Test void testToString() {
    DimensionConfig config = DimensionConfig.builder()
        .name("ori")
        .type(DimensionType.CUSTOM)
        .build();

    Map<String, List<Map<String, String>>> prefixByContext =
        new LinkedHashMap<String, List<Map<String, String>>>();
    prefixByContext.put("CA", Collections.<Map<String, String>>emptyList());
    prefixByContext.put("NY", Collections.<Map<String, String>>emptyList());

    DimensionPartitionPlan plan =
        new DimensionPartitionPlan("state",
        Arrays.asList("CA", "NY"),
        Arrays.asList("ori"),
        Arrays.asList(config),
        prefixByContext);

    String str = plan.toString();
    assertTrue(str.contains("state"));
    assertTrue(str.contains("2"));
    assertTrue(str.contains("ori"));
  }

  @Test void testContextValuesAreImmutable() {
    DimensionPartitionPlan plan =
        new DimensionPartitionPlan("key",
        Arrays.asList("A", "B"),
        Collections.<String>emptyList(),
        Collections.<DimensionConfig>emptyList(),
        new LinkedHashMap<String, List<Map<String, String>>>());

    try {
      plan.getContextValues().add("C");
      assertTrue(false, "Expected UnsupportedOperationException");
    } catch (UnsupportedOperationException e) {
      // Expected
    }
  }
}
