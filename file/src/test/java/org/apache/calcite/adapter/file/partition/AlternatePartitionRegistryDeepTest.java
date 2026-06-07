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
package org.apache.calcite.adapter.file.partition;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Deep coverage tests for AlternatePartitionRegistry covering all branches:
 * registration, lookup, findBestAlternate, coversFilters, unregister, clear.
 */
@Tag("unit")
public class AlternatePartitionRegistryDeepTest {

  private AlternatePartitionRegistry registry;

  @BeforeEach
  void setUp() {
    registry = new AlternatePartitionRegistry();
  }

  // ===== Registration =====

  @Test void testRegisterAndRetrieve() {
    registry.register("source", "alt1", "pattern/*",
        Arrays.asList("year", "month"), null, null);

    assertEquals(1, registry.size());
    assertNotNull(registry.getByName("alt1"));
    assertEquals("source", registry.getByName("alt1").getSourceTableName());
    assertEquals("pattern/*", registry.getByName("alt1").getPattern());
    assertEquals(2, registry.getByName("alt1").getPartitionKeys().size());
  }

  @Test void testRegisterReplacesExisting() {
    registry.register("source", "alt1", "pattern1/*",
        Collections.singletonList("year"), null, null);
    registry.register("source", "alt1", "pattern2/*",
        Arrays.asList("year", "month"), null, null);

    assertEquals(1, registry.size());
    assertEquals("pattern2/*", registry.getByName("alt1").getPattern());
    assertEquals(2, registry.getByName("alt1").getPartitionKeys().size());
  }

  @Test void testRegisterMultipleAlternatesForSameSource() {
    registry.register("source", "alt1", "p1/*", Collections.singletonList("year"), null, null);
    registry.register("source", "alt2", "p2/*", Arrays.asList("year", "month"), null, null);

    assertEquals(2, registry.size());
    assertEquals(2, registry.getAlternates("source").size());
  }

  @Test void testRegisterFromConfig() {
    PartitionedTableConfig.PartitionConfig pc =
        new PartitionedTableConfig.PartitionConfig("hive", null,
            Arrays.asList(
                new PartitionedTableConfig.ColumnDefinition("year", "INTEGER"),
                new PartitionedTableConfig.ColumnDefinition("month", "INTEGER")),
            null, null);
    PartitionedTableConfig.AlternatePartitionConfig apc =
        new PartitionedTableConfig.AlternatePartitionConfig("alt_test", "pattern/*", pc, "comment");

    registry.registerFromConfig("source", apc, "iceberg-id");

    AlternatePartitionRegistry.AlternateInfo info = registry.getByName("alt_test");
    assertNotNull(info);
    assertEquals("source", info.getSourceTableName());
    assertEquals(2, info.getPartitionKeys().size());
    assertEquals("iceberg-id", info.getIcebergTableId());
    assertEquals(apc, info.getConfig());
  }

  @Test void testRegisterFromConfigNullPartitions() {
    PartitionedTableConfig.AlternatePartitionConfig apc =
        new PartitionedTableConfig.AlternatePartitionConfig("alt_test", "pattern/*", null, null);
    registry.registerFromConfig("source", apc, null);

    AlternatePartitionRegistry.AlternateInfo info = registry.getByName("alt_test");
    assertNotNull(info);
    assertTrue(info.getPartitionKeys().isEmpty());
    assertNull(info.getIcebergTableId());
  }

  // ===== Lookup =====

  @Test void testGetByNameNotFound() {
    assertNull(registry.getByName("nonexistent"));
  }

  @Test void testGetAlternatesNoEntries() {
    assertTrue(registry.getAlternates("nonexistent").isEmpty());
  }

  @Test void testGetAlternatesReturnsUnmodifiable() {
    registry.register("source", "alt1", "p/*", Collections.singletonList("year"), null, null);
    List<AlternatePartitionRegistry.AlternateInfo> alts = registry.getAlternates("source");
    assertThrows(UnsupportedOperationException.class, () -> alts.add(null));
  }

  // ===== findBestAlternate =====

  @Test void testFindBestAlternateNullFilters() {
    assertNull(registry.findBestAlternate("source", null));
  }

  @Test void testFindBestAlternateEmptyFilters() {
    assertNull(registry.findBestAlternate("source", new HashSet<String>()));
  }

  @Test void testFindBestAlternateNoAlternates() {
    Set<String> filters = new HashSet<>();
    filters.add("year");
    assertNull(registry.findBestAlternate("source", filters));
  }

  @Test void testFindBestAlternateNonMaterialized() {
    registry.register("source", "alt1", "p/*", Collections.singletonList("year"), null, null);
    // Not materialized

    Set<String> filters = new HashSet<>();
    filters.add("year");
    assertNull(registry.findBestAlternate("source", filters));
  }

  @Test void testFindBestAlternateMaterialized() {
    registry.register("source", "alt1", "p/*", Collections.singletonList("year"), null, null);
    registry.markMaterialized("alt1");

    Set<String> filters = new HashSet<>();
    filters.add("year");
    AlternatePartitionRegistry.AlternateInfo best =
        registry.findBestAlternate("source", filters);
    assertNotNull(best);
    assertEquals("alt1", best.getAlternateName());
  }

  @Test void testFindBestAlternatePrefewerKeys() {
    // Two alternates, both cover "year" filter, one has fewer keys
    registry.register("source", "alt1", "p1/*",
        Arrays.asList("year", "month"), null, null);
    registry.markMaterialized("alt1");

    registry.register("source", "alt2", "p2/*",
        Collections.singletonList("year"), null, null);
    registry.markMaterialized("alt2");

    Set<String> filters = new HashSet<>();
    filters.add("year");
    AlternatePartitionRegistry.AlternateInfo best =
        registry.findBestAlternate("source", filters);
    assertNotNull(best);
    assertEquals("alt2", best.getAlternateName()); // Fewer keys preferred
  }

  @Test void testFindBestAlternateFilterNotCovered() {
    registry.register("source", "alt1", "p/*",
        Collections.singletonList("year"), null, null);
    registry.markMaterialized("alt1");

    Set<String> filters = new HashSet<>();
    filters.add("region"); // Not a partition key
    assertNull(registry.findBestAlternate("source", filters));
  }

  // ===== coversFilters =====

  @Test void testCoversFiltersNullFilters() {
    AlternatePartitionRegistry.AlternateInfo info =
        new AlternatePartitionRegistry.AlternateInfo(
            "alt1", "source", "p/*",
            Collections.singletonList("year"), null, null);
    assertFalse(info.coversFilters(null));
  }

  @Test void testCoversFiltersEmptyFilters() {
    AlternatePartitionRegistry.AlternateInfo info =
        new AlternatePartitionRegistry.AlternateInfo(
            "alt1", "source", "p/*",
            Collections.singletonList("year"), null, null);
    assertFalse(info.coversFilters(new HashSet<String>()));
  }

  @Test void testCoversFiltersCaseInsensitive() {
    AlternatePartitionRegistry.AlternateInfo info =
        new AlternatePartitionRegistry.AlternateInfo(
            "alt1", "source", "p/*",
            Arrays.asList("Year", "Month"), null, null);
    Set<String> filters = new HashSet<>();
    filters.add("year");
    filters.add("month");
    assertTrue(info.coversFilters(filters));
  }

  @Test void testCoversFiltersPartialCoverage() {
    AlternatePartitionRegistry.AlternateInfo info =
        new AlternatePartitionRegistry.AlternateInfo(
            "alt1", "source", "p/*",
            Collections.singletonList("year"), null, null);
    Set<String> filters = new HashSet<>();
    filters.add("year");
    filters.add("month");
    assertFalse(info.coversFilters(filters)); // Only covers year, not month
  }

  // ===== Materialization =====

  @Test void testIsMaterializedNotFound() {
    assertFalse(registry.isMaterialized("nonexistent"));
  }

  @Test void testMarkMaterializedNotFound() {
    // Should not throw
    registry.markMaterialized("nonexistent");
  }

  @Test void testMaterializationLifecycle() {
    registry.register("source", "alt1", "p/*", Collections.singletonList("year"), null, null);
    assertFalse(registry.isMaterialized("alt1"));

    registry.markMaterialized("alt1");
    assertTrue(registry.isMaterialized("alt1"));
  }

  // ===== Unregister =====

  @Test void testUnregister() {
    registry.register("source", "alt1", "p/*", Collections.singletonList("year"), null, null);
    assertEquals(1, registry.size());

    registry.unregister("alt1");
    assertEquals(0, registry.size());
    assertNull(registry.getByName("alt1"));
    assertTrue(registry.getAlternates("source").isEmpty());
  }

  @Test void testUnregisterNotFound() {
    // Should not throw
    registry.unregister("nonexistent");
  }

  @Test void testUnregisterLeavesOtherAlternates() {
    registry.register("source", "alt1", "p1/*", Collections.singletonList("year"), null, null);
    registry.register("source", "alt2", "p2/*", Collections.singletonList("month"), null, null);

    registry.unregister("alt1");
    assertEquals(1, registry.size());
    assertNull(registry.getByName("alt1"));
    assertNotNull(registry.getByName("alt2"));
    assertEquals(1, registry.getAlternates("source").size());
  }

  // ===== Clear =====

  @Test void testClear() {
    registry.register("source1", "alt1", "p1/*", Collections.singletonList("year"), null, null);
    registry.register("source2", "alt2", "p2/*", Collections.singletonList("month"), null, null);
    assertEquals(2, registry.size());

    registry.clear();
    assertEquals(0, registry.size());
    assertTrue(registry.getSourceTables().isEmpty());
    assertTrue(registry.getAlternateNames().isEmpty());
  }

  // ===== Source tables and alternate names =====

  @Test void testGetSourceTables() {
    registry.register("source1", "alt1", "p/*", Collections.singletonList("year"), null, null);
    registry.register("source2", "alt2", "p/*", Collections.singletonList("year"), null, null);

    Set<String> sources = registry.getSourceTables();
    assertEquals(2, sources.size());
    assertTrue(sources.contains("source1"));
    assertTrue(sources.contains("source2"));
  }

  @Test void testGetAlternateNames() {
    registry.register("source", "alt1", "p/*", Collections.singletonList("year"), null, null);
    registry.register("source", "alt2", "p/*", Collections.singletonList("month"), null, null);

    Set<String> names = registry.getAlternateNames();
    assertEquals(2, names.size());
    assertTrue(names.contains("alt1"));
    assertTrue(names.contains("alt2"));
  }

  // ===== AlternateInfo =====

  @Test void testAlternateInfoNullPartitionKeys() {
    AlternatePartitionRegistry.AlternateInfo info =
        new AlternatePartitionRegistry.AlternateInfo(
            "alt1", "source", "p/*", null, null, null);
    assertTrue(info.getPartitionKeys().isEmpty());
  }

  @Test void testAlternateInfoToString() {
    AlternatePartitionRegistry.AlternateInfo info =
        new AlternatePartitionRegistry.AlternateInfo(
            "alt1", "source", "p/*",
            Collections.singletonList("year"), null, null);
    String str = info.toString();
    assertTrue(str.contains("alt1"));
    assertTrue(str.contains("source"));
    assertTrue(str.contains("year"));
    assertTrue(str.contains("materialized=false"));
  }

  // ===== toString =====

  @Test void testRegistryToString() {
    registry.register("source", "alt1", "p/*", Collections.singletonList("year"), null, null);
    String str = registry.toString();
    assertTrue(str.contains("sources=1"));
    assertTrue(str.contains("alternates=1"));
  }
}
