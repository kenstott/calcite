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
package org.apache.calcite.adapter.file.partition;

import org.apache.calcite.adapter.file.partition.AlternatePartitionRegistry.AlternateInfo;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for AlternatePartitionRegistry.
 */
@Tag("unit")
public class AlternatePartitionRegistryTest {

  private AlternatePartitionRegistry registry;

  @BeforeEach
  void setUp() {
    registry = new AlternatePartitionRegistry();
  }

  @Test
  void testRegisterAndRetrieve() {
    List<String> partitionKeys = Arrays.asList("geo", "year");

    registry.register(
        "source_table",
        "_mv_abc123",
        "type=income/geo=*/*.parquet",
        partitionKeys,
        null,
        null);

    AlternateInfo info = registry.getByName("_mv_abc123");
    assertNotNull(info);
    assertEquals("_mv_abc123", info.getAlternateName());
    assertEquals("source_table", info.getSourceTableName());
    assertEquals("type=income/geo=*/*.parquet", info.getPattern());
    assertEquals(partitionKeys, info.getPartitionKeys());
    assertFalse(info.isMaterialized());
  }

  @Test
  void testGetAlternatesForSource() {
    // Register multiple alternates for same source
    registry.register("source_table", "_mv_alt1", "pattern1", Arrays.asList("col1"), null, null);
    registry.register("source_table", "_mv_alt2", "pattern2", Arrays.asList("col2"), null, null);
    registry.register("other_table", "_mv_alt3", "pattern3", Arrays.asList("col3"), null, null);

    List<AlternateInfo> alternates = registry.getAlternates("source_table");
    assertEquals(2, alternates.size());

    List<AlternateInfo> otherAlternates = registry.getAlternates("other_table");
    assertEquals(1, otherAlternates.size());
  }

  @Test
  void testMarkMaterialized() {
    registry.register("source_table", "_mv_test", "pattern", Arrays.asList("col1"), null, null);

    assertFalse(registry.isMaterialized("_mv_test"));

    registry.markMaterialized("_mv_test");

    assertTrue(registry.isMaterialized("_mv_test"));
  }

  @Test
  void testCoversFilters() {
    List<String> partitionKeys = Arrays.asList("geo", "year");
    registry.register("source_table", "_mv_test", "pattern", partitionKeys, null, null);
    registry.markMaterialized("_mv_test");

    AlternateInfo info = registry.getByName("_mv_test");

    // Fully covered filters
    Set<String> covered = new HashSet<String>(Arrays.asList("geo", "year"));
    assertTrue(info.coversFilters(covered));

    // Partially covered (should return false)
    Set<String> partial = new HashSet<String>(Arrays.asList("geo", "state"));
    assertFalse(info.coversFilters(partial));

    // Single column covered
    Set<String> single = new HashSet<String>(Arrays.asList("geo"));
    assertTrue(info.coversFilters(single));

    // Empty filters
    Set<String> empty = new HashSet<String>();
    assertFalse(info.coversFilters(empty));
  }

  @Test
  void testFindBestAlternate() {
    // Register alternates with different partition key counts
    registry.register("source", "_mv_few", "pattern1", Arrays.asList("geo"), null, null);
    registry.markMaterialized("_mv_few");

    registry.register("source", "_mv_many", "pattern2", Arrays.asList("geo", "year", "state"), null, null);
    registry.markMaterialized("_mv_many");

    // Filter by geo - should prefer _mv_few (fewer keys)
    Set<String> geoFilter = new HashSet<String>(Arrays.asList("geo"));
    AlternateInfo best = registry.findBestAlternate("source", geoFilter);

    assertNotNull(best);
    assertEquals("_mv_few", best.getAlternateName());
    assertEquals(1, best.getPartitionKeys().size());
  }

  @Test
  void testFindBestAlternateNonMaterialized() {
    // Register but don't materialize
    registry.register("source", "_mv_test", "pattern", Arrays.asList("geo"), null, null);

    Set<String> filter = new HashSet<String>(Arrays.asList("geo"));
    AlternateInfo best = registry.findBestAlternate("source", filter);

    // Should return null because alternate is not materialized
    assertNull(best);
  }

  @Test
  void testUnregister() {
    registry.register("source", "_mv_test", "pattern", Arrays.asList("col"), null, null);

    assertNotNull(registry.getByName("_mv_test"));
    assertEquals(1, registry.getAlternates("source").size());

    registry.unregister("_mv_test");

    assertNull(registry.getByName("_mv_test"));
    assertEquals(0, registry.getAlternates("source").size());
  }

  @Test
  void testClear() {
    registry.register("source1", "_mv_1", "p1", Arrays.asList("c1"), null, null);
    registry.register("source2", "_mv_2", "p2", Arrays.asList("c2"), null, null);

    assertEquals(2, registry.size());

    registry.clear();

    assertEquals(0, registry.size());
    assertEquals(0, registry.getAlternateNames().size());
    assertEquals(0, registry.getSourceTables().size());
  }

  @Test
  void testReplaceDuplicateRegistration() {
    registry.register("source", "_mv_test", "pattern1", Arrays.asList("col1"), null, null);
    registry.register("source", "_mv_test", "pattern2", Arrays.asList("col2", "col3"), null, null);

    // Should have replaced, not duplicated
    assertEquals(1, registry.size());
    assertEquals(1, registry.getAlternates("source").size());

    AlternateInfo info = registry.getByName("_mv_test");
    assertEquals("pattern2", info.getPattern());
    assertEquals(2, info.getPartitionKeys().size());
  }

  @Test
  void testCaseInsensitiveFilterMatching() {
    registry.register("source", "_mv_test", "pattern", Arrays.asList("Geo", "YEAR"), null, null);
    registry.markMaterialized("_mv_test");

    AlternateInfo info = registry.getByName("_mv_test");

    // Test case-insensitive matching
    Set<String> lowerCase = new HashSet<String>(Arrays.asList("geo", "year"));
    assertTrue(info.coversFilters(lowerCase));

    Set<String> mixedCase = new HashSet<String>(Arrays.asList("GEO", "Year"));
    assertTrue(info.coversFilters(mixedCase));
  }
}
