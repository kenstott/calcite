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

import org.apache.calcite.adapter.file.storage.StorageProvider;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Coverage tests for {@link AlternatePartitionMaterializer}.
 */
@Tag("unit")
class AlternatePartitionMaterializerCoverageTest {

  @TempDir
  File tempDir;

  private StorageProvider mockStorageProvider;

  @BeforeEach void setUp() {
    mockStorageProvider = mock(StorageProvider.class);
    when(mockStorageProvider.resolvePath(anyString(), anyString()))
        .thenAnswer(inv -> inv.getArgument(0) + "/" + inv.getArgument(1));
    when(mockStorageProvider.getStorageType()).thenReturn("mock");
  }

  // ===== Factory Methods =====

  @Test void testCreateFactory() {
    AlternatePartitionMaterializer materializer =
        AlternatePartitionMaterializer.create(mockStorageProvider, tempDir.getAbsolutePath(),
        tempDir.getAbsolutePath(), 2020, 2024);
    assertNotNull(materializer);
  }

  @Test void testCreateWithIcebergFactory() {
    AlternatePartitionMaterializer materializer =
        AlternatePartitionMaterializer.createWithIceberg(mockStorageProvider, tempDir.getAbsolutePath(),
        tempDir.getAbsolutePath(), 2020, 2024);
    assertNotNull(materializer);
  }

  // ===== materializeAlternates =====

  @Test void testMaterializeAlternatesNoAlternates() {
    AlternatePartitionMaterializer materializer =
        new AlternatePartitionMaterializer(mockStorageProvider, tempDir.getAbsolutePath(),
        tempDir.getAbsolutePath(), 2020, 2024, false);

    PartitionedTableConfig config =
        new PartitionedTableConfig("test_table", "type=test/year=*/data.parquet", "partitioned", null);

    int result = materializer.materializeAlternates(config);
    assertEquals(0, result);
  }

  @Test void testMaterializeAlternatesNullAlternates() {
    AlternatePartitionMaterializer materializer =
        new AlternatePartitionMaterializer(mockStorageProvider, tempDir.getAbsolutePath(),
        tempDir.getAbsolutePath(), 2020, 2024, false);

    PartitionedTableConfig config =
        new PartitionedTableConfig("test_table", "type=test/year=*/data.parquet", "partitioned",
        null, null, null, null, null);

    int result = materializer.materializeAlternates(config);
    assertEquals(0, result);
  }

  @Test void testMaterializeAlternatesDisabledAlternate() {
    AlternatePartitionMaterializer materializer =
        new AlternatePartitionMaterializer(mockStorageProvider, tempDir.getAbsolutePath(),
        tempDir.getAbsolutePath(), 2020, 2024, false);

    List<PartitionedTableConfig.ColumnDefinition> colDefs =
        Arrays.asList(new PartitionedTableConfig.ColumnDefinition("geo", "VARCHAR"));

    PartitionedTableConfig.PartitionConfig partConfig =
        new PartitionedTableConfig.PartitionConfig("hive", null, colDefs, null, null);

    PartitionedTableConfig.AlternatePartitionConfig disabled =
        new PartitionedTableConfig.AlternatePartitionConfig(
            "alt_disabled", "type=alt/*/*.parquet", partConfig, "disabled",
            null, null, 0, null, 0, false);

    PartitionedTableConfig config =
        new PartitionedTableConfig("test_table", "type=test/year=*/data.parquet", "partitioned",
        null, null, null, null, Arrays.asList(disabled));

    int result = materializer.materializeAlternates(config);
    assertEquals(0, result);
  }

  @Test void testMaterializeAlternatesNoPartitionConfig() {
    AlternatePartitionMaterializer materializer =
        new AlternatePartitionMaterializer(null, tempDir.getAbsolutePath(),
        tempDir.getAbsolutePath(), 2020, 2024, false);

    PartitionedTableConfig.AlternatePartitionConfig alternate =
        new PartitionedTableConfig.AlternatePartitionConfig(
            "alt_no_partition", "type=alt/*/*.parquet", null, "no partition");

    PartitionedTableConfig config =
        new PartitionedTableConfig("test_table", "type=test/year=*/data.parquet", "partitioned",
        null, null, null, null, Arrays.asList(alternate));

    int result = materializer.materializeAlternates(config);
    assertEquals(0, result);
  }

  @Test void testMaterializeAlternatesEmptyColumnDefinitions() {
    AlternatePartitionMaterializer materializer =
        new AlternatePartitionMaterializer(null, tempDir.getAbsolutePath(),
        tempDir.getAbsolutePath(), 2020, 2024, false);

    PartitionedTableConfig.PartitionConfig partConfig =
        new PartitionedTableConfig.PartitionConfig("hive", null,
            Collections.<PartitionedTableConfig.ColumnDefinition>emptyList(), null, null);

    PartitionedTableConfig.AlternatePartitionConfig alternate =
        new PartitionedTableConfig.AlternatePartitionConfig(
            "alt_empty", "type=alt/*/*.parquet", partConfig, "empty");

    PartitionedTableConfig config =
        new PartitionedTableConfig("test_table", "type=test/year=*/data.parquet", "partitioned",
        null, null, null, null, Arrays.asList(alternate));

    int result = materializer.materializeAlternates(config);
    assertEquals(0, result);
  }

  @Test void testMaterializeAlternatesNullColumnDefinitions() {
    AlternatePartitionMaterializer materializer =
        new AlternatePartitionMaterializer(null, tempDir.getAbsolutePath(),
        tempDir.getAbsolutePath(), 2020, 2024, false);

    PartitionedTableConfig.PartitionConfig partConfig =
        new PartitionedTableConfig.PartitionConfig("hive", null, null, null, null);

    PartitionedTableConfig.AlternatePartitionConfig alternate =
        new PartitionedTableConfig.AlternatePartitionConfig(
            "alt_null_cols", "type=alt/*/*.parquet", partConfig, "null cols");

    PartitionedTableConfig config =
        new PartitionedTableConfig("test_table", "type=test/year=*/data.parquet", "partitioned",
        null, null, null, null, Arrays.asList(alternate));

    int result = materializer.materializeAlternates(config);
    assertEquals(0, result);
  }

  // ===== materializeAll =====

  @Test void testMaterializeAllEmptyList() {
    AlternatePartitionMaterializer materializer =
        new AlternatePartitionMaterializer(mockStorageProvider, tempDir.getAbsolutePath(),
        tempDir.getAbsolutePath(), 2020, 2024, false);

    int result = materializer.materializeAll(Collections.<PartitionedTableConfig>emptyList());
    assertEquals(0, result);
  }

  @Test void testMaterializeAllMultipleTables() {
    AlternatePartitionMaterializer materializer =
        new AlternatePartitionMaterializer(mockStorageProvider, tempDir.getAbsolutePath(),
        tempDir.getAbsolutePath(), 2020, 2024, false);

    // Config without alternates
    PartitionedTableConfig config1 =
        new PartitionedTableConfig("table1", "type=t1/*.parquet", "partitioned", null);
    PartitionedTableConfig config2 =
        new PartitionedTableConfig("table2", "type=t2/*.parquet", "partitioned", null);

    List<PartitionedTableConfig> tables = new ArrayList<PartitionedTableConfig>();
    tables.add(config1);
    tables.add(config2);

    int result = materializer.materializeAll(tables);
    assertEquals(0, result); // No alternates to materialize
  }

  // ===== setIncrementalTracker =====

  @Test void testSetIncrementalTracker() {
    AlternatePartitionMaterializer materializer =
        new AlternatePartitionMaterializer(mockStorageProvider, tempDir.getAbsolutePath(),
        tempDir.getAbsolutePath(), 2020, 2024, false);

    IncrementalTracker tracker = IncrementalTracker.NOOP;
    materializer.setIncrementalTracker(tracker);
    // No assertion needed - just verify it doesn't throw
  }

  // ===== buildSourceGlob (tested indirectly) =====

  @Test void testMaterializeAlternateWithExceptionHandling() {
    // This tests the try/catch block in materializeAlternates
    AlternatePartitionMaterializer materializer =
        new AlternatePartitionMaterializer(mockStorageProvider, tempDir.getAbsolutePath(),
        tempDir.getAbsolutePath(), 2020, 2024, false);

    List<PartitionedTableConfig.ColumnDefinition> colDefs =
        Arrays.asList(new PartitionedTableConfig.ColumnDefinition("geo", "VARCHAR"));

    PartitionedTableConfig.PartitionConfig partConfig =
        new PartitionedTableConfig.PartitionConfig("hive", null, colDefs, null, null);

    // This alternate will have batch columns but mockStorageProvider won't support
    // the operations needed, so it will fail gracefully
    PartitionedTableConfig.AlternatePartitionConfig alternate =
        new PartitionedTableConfig.AlternatePartitionConfig(
            "alt_failing", "type=alt/geo=*/*.parquet", partConfig, "will fail",
            Arrays.asList("year"), null, 2);

    PartitionedTableConfig config =
        new PartitionedTableConfig("test_table", "type=test/year=*/data.parquet", "partitioned",
        null, null, null, null, Arrays.asList(alternate));

    // The DuckDB connection will fail since there's no real data,
    // but the exception should be caught and logged
    int result = materializer.materializeAlternates(config);
    // May be 0 or 1 depending on error handling behavior
    // The important thing is it doesn't throw
  }

  // ===== extractTargetBase (tested indirectly) =====

  @Test void testExtractTargetBaseFromAlternateWithBase() {
    AlternatePartitionMaterializer materializer =
        new AlternatePartitionMaterializer(null, tempDir.getAbsolutePath(),
        tempDir.getAbsolutePath(), 2020, 2024, false);

    List<PartitionedTableConfig.ColumnDefinition> colDefs =
        Arrays.asList(new PartitionedTableConfig.ColumnDefinition("geo", "VARCHAR"));

    PartitionedTableConfig.PartitionConfig partConfig =
        new PartitionedTableConfig.PartitionConfig("hive", null, colDefs, null, null);

    // Pattern with a concrete base before the first wildcard
    PartitionedTableConfig.AlternatePartitionConfig alternate =
        new PartitionedTableConfig.AlternatePartitionConfig(
            "alt_with_base", "type=foo_by_geo/geo=*/data.parquet",
            partConfig, "has base");

    PartitionedTableConfig config =
        new PartitionedTableConfig("test_table", "type=test/year=*/data.parquet", "partitioned",
        null, null, null, null, Arrays.asList(alternate));

    // Exercise the code path, even though DuckDB may fail
    int result = materializer.materializeAlternates(config);
    // Just verifying no exception
  }

  @Test void testExtractTargetBaseFromAlternateWithWildcardStart() {
    AlternatePartitionMaterializer materializer =
        new AlternatePartitionMaterializer(null, tempDir.getAbsolutePath(),
        tempDir.getAbsolutePath(), 2020, 2024, false);

    List<PartitionedTableConfig.ColumnDefinition> colDefs =
        Arrays.asList(new PartitionedTableConfig.ColumnDefinition("freq", "VARCHAR"));

    PartitionedTableConfig.PartitionConfig partConfig =
        new PartitionedTableConfig.PartitionConfig("hive", null, colDefs, null, null);

    // Pattern where first segment already has a wildcard
    PartitionedTableConfig.AlternatePartitionConfig alternate =
        new PartitionedTableConfig.AlternatePartitionConfig(
            "alt_wildcard_start", "frequency=*/*.parquet",
            partConfig, "wildcard start");

    PartitionedTableConfig config =
        new PartitionedTableConfig("test_table", "type=test/year=*/data.parquet", "partitioned",
        null, null, null, null, Arrays.asList(alternate));

    // Exercise the code path
    int result = materializer.materializeAlternates(config);
    // Just verifying no exception
  }

  // ===== buildSourceGlob with concrete parquet filename =====

  @Test void testBuildSourceGlobConcrete() {
    AlternatePartitionMaterializer materializer =
        new AlternatePartitionMaterializer(null, tempDir.getAbsolutePath(),
        tempDir.getAbsolutePath(), 2020, 2024, false);

    List<PartitionedTableConfig.ColumnDefinition> colDefs =
        Arrays.asList(new PartitionedTableConfig.ColumnDefinition("geo", "VARCHAR"));

    PartitionedTableConfig.PartitionConfig partConfig =
        new PartitionedTableConfig.PartitionConfig("hive", null, colDefs, null, null);

    // Source pattern ends with a concrete filename
    PartitionedTableConfig.AlternatePartitionConfig alternate =
        new PartitionedTableConfig.AlternatePartitionConfig(
            "alt_concrete", "type=foo/geo=*/data.parquet",
            partConfig, "concrete filename");

    PartitionedTableConfig config =
        new PartitionedTableConfig("test_table", "type=test/year=*/specific_data.parquet", "partitioned",
        null, null, null, null, Arrays.asList(alternate));

    int result = materializer.materializeAlternates(config);
    // Just verifying code paths are exercised
  }

  @Test void testBuildSourceGlobNonParquetFile() {
    AlternatePartitionMaterializer materializer =
        new AlternatePartitionMaterializer(null, tempDir.getAbsolutePath(),
        tempDir.getAbsolutePath(), 2020, 2024, false);

    List<PartitionedTableConfig.ColumnDefinition> colDefs =
        Arrays.asList(new PartitionedTableConfig.ColumnDefinition("geo", "VARCHAR"));

    PartitionedTableConfig.PartitionConfig partConfig =
        new PartitionedTableConfig.PartitionConfig("hive", null, colDefs, null, null);

    PartitionedTableConfig.AlternatePartitionConfig alternate =
        new PartitionedTableConfig.AlternatePartitionConfig(
            "alt_glob", "type=foo/geo=*/data.parquet",
            partConfig, "already glob");

    // Source pattern that doesn't end with .parquet
    PartitionedTableConfig config =
        new PartitionedTableConfig("test_table", "type=test/year=*/*.csv", "partitioned",
        null, null, null, null, Arrays.asList(alternate));

    int result = materializer.materializeAlternates(config);
    // Just verifying code paths
  }

  // ===== Constructor with null StorageProvider =====

  @Test void testConstructorNullStorageProvider() {
    AlternatePartitionMaterializer materializer =
        new AlternatePartitionMaterializer(null, tempDir.getAbsolutePath(),
        tempDir.getAbsolutePath(), 2020, 2024, false);
    assertNotNull(materializer);
  }

  @Test void testConstructorIcebergSource() {
    AlternatePartitionMaterializer materializer =
        new AlternatePartitionMaterializer(mockStorageProvider, tempDir.getAbsolutePath(),
        tempDir.getAbsolutePath(), 2020, 2024, true);
    assertNotNull(materializer);
  }
}
