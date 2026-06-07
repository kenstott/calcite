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

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Deep coverage tests for PartitionDetector covering all code paths:
 * null inputs, empty inputs, single/multi-level Hive partitions,
 * directory partitions, custom regex, and inconsistent schemas.
 */
@Tag("unit")
public class PartitionDetectorDeepTest {

  // ===== detectPartitionScheme =====

  @Test void testDetectPartitionSchemeNull() {
    assertNull(PartitionDetector.detectPartitionScheme(null));
  }

  @Test void testDetectPartitionSchemeEmpty() {
    assertNull(PartitionDetector.detectPartitionScheme(Collections.<String>emptyList()));
  }

  @Test void testDetectPartitionSchemeAllHiveStyle() {
    List<String> paths =
        Arrays.asList("/data/year=2020/month=01/file.parquet",
        "/data/year=2020/month=02/file.parquet",
        "/data/year=2021/month=01/file.parquet");
    PartitionDetector.PartitionInfo info = PartitionDetector.detectPartitionScheme(paths);
    assertNotNull(info);
    assertTrue(info.isHiveStyle());
    assertEquals(2, info.getPartitionColumns().size());
    assertTrue(info.getPartitionColumns().contains("year"));
    assertTrue(info.getPartitionColumns().contains("month"));
  }

  @Test void testDetectPartitionSchemeNonHivePaths() {
    List<String> paths =
        Arrays.asList("/data/2020/01/file.parquet",
        "/data/2020/02/file.parquet");
    PartitionDetector.PartitionInfo info = PartitionDetector.detectPartitionScheme(paths);
    assertNull(info); // Not Hive-style
  }

  @Test void testDetectPartitionSchemeMixedStyles() {
    List<String> paths =
        Arrays.asList("/data/year=2020/file.parquet",
        "/data/2021/file.parquet"); // Not Hive-style
    PartitionDetector.PartitionInfo info = PartitionDetector.detectPartitionScheme(paths);
    assertNull(info); // Mixed styles -> null
  }

  @Test void testDetectPartitionSchemeInconsistentColumns() {
    List<String> paths =
        Arrays.asList("/data/year=2020/file.parquet",
        "/data/region=US/file.parquet"); // Different partition columns
    PartitionDetector.PartitionInfo info = PartitionDetector.detectPartitionScheme(paths);
    assertNull(info); // Inconsistent columns -> null
  }

  @Test void testDetectPartitionSchemeSingleFile() {
    List<String> paths =
        Collections.singletonList("/data/year=2020/month=01/file.parquet");
    PartitionDetector.PartitionInfo info = PartitionDetector.detectPartitionScheme(paths);
    assertNotNull(info);
    assertTrue(info.isHiveStyle());
    assertEquals(2, info.getPartitionColumns().size());
  }

  // ===== extractHivePartitions =====

  @Test void testExtractHivePartitionsSingleLevel() {
    PartitionDetector.PartitionInfo info =
        PartitionDetector.extractHivePartitions("/data/year=2020/file.parquet");
    assertNotNull(info);
    assertTrue(info.isHiveStyle());
    assertEquals(1, info.getPartitionColumns().size());
    assertEquals("year", info.getPartitionColumns().get(0));
    assertEquals("2020", info.getPartitionValues().get("year"));
  }

  @Test void testExtractHivePartitionsMultiLevel() {
    PartitionDetector.PartitionInfo info =
        PartitionDetector.extractHivePartitions("/data/year=2021/month=06/day=15/file.parquet");
    assertNotNull(info);
    assertEquals(3, info.getPartitionColumns().size());
    assertEquals("year", info.getPartitionColumns().get(0));
    assertEquals("month", info.getPartitionColumns().get(1));
    assertEquals("day", info.getPartitionColumns().get(2));
    assertEquals("2021", info.getPartitionValues().get("year"));
    assertEquals("06", info.getPartitionValues().get("month"));
    assertEquals("15", info.getPartitionValues().get("day"));
  }

  @Test void testExtractHivePartitionsNoPartitions() {
    PartitionDetector.PartitionInfo info =
        PartitionDetector.extractHivePartitions("/data/files/file.parquet");
    assertNull(info);
  }

  @Test void testExtractHivePartitionsRootFile() {
    PartitionDetector.PartitionInfo info =
        PartitionDetector.extractHivePartitions("file.parquet");
    assertNull(info);
  }

  @Test void testExtractHivePartitionsWithNonPartitionDirs() {
    // Mix of Hive and non-Hive directories
    PartitionDetector.PartitionInfo info =
        PartitionDetector.extractHivePartitions("/base/data/year=2020/file.parquet");
    assertNotNull(info);
    assertEquals(1, info.getPartitionColumns().size());
    assertEquals("year", info.getPartitionColumns().get(0));
  }

  @Test void testExtractHivePartitionsSpecialCharValues() {
    PartitionDetector.PartitionInfo info =
        PartitionDetector.extractHivePartitions("/data/region=US-EAST/file.parquet");
    assertNotNull(info);
    assertEquals("US-EAST", info.getPartitionValues().get("region"));
  }

  // ===== extractDirectoryPartitions =====

  @Test void testExtractDirectoryPartitionsNullColumns() {
    assertNull(PartitionDetector.extractDirectoryPartitions("/data/2020/file.parquet", null));
  }

  @Test void testExtractDirectoryPartitionsEmptyColumns() {
    assertNull(
        PartitionDetector.extractDirectoryPartitions(
        "/data/2020/file.parquet", Collections.<String>emptyList()));
  }

  @Test void testExtractDirectoryPartitionsSingleLevel() {
    PartitionDetector.PartitionInfo info =
        PartitionDetector.extractDirectoryPartitions("/data/2020/file.parquet", Collections.singletonList("year"));
    assertNotNull(info);
    assertFalse(info.isHiveStyle());
    assertEquals("2020", info.getPartitionValues().get("year"));
  }

  @Test void testExtractDirectoryPartitionsMultiLevel() {
    PartitionDetector.PartitionInfo info =
        PartitionDetector.extractDirectoryPartitions("/base/data/US/2020/file.parquet", Arrays.asList("region", "year"));
    assertNotNull(info);
    assertFalse(info.isHiveStyle());
    assertEquals(2, info.getPartitionValues().size());
  }

  @Test void testExtractDirectoryPartitionsMoreColumnsThanDirs() {
    // More column names than directory levels
    PartitionDetector.PartitionInfo info =
        PartitionDetector.extractDirectoryPartitions("/data/file.parquet", Arrays.asList("level1", "level2", "level3"));
    assertNotNull(info);
    // Should match as many as available
    assertTrue(info.getPartitionValues().size() <= 3);
  }

  // ===== extractCustomPartitions =====

  @Test void testExtractCustomPartitionsNullRegex() {
    assertNull(
        PartitionDetector.extractCustomPartitions(
        "/data/file.parquet", null, new ArrayList<PartitionedTableConfig.ColumnMapping>()));
  }

  @Test void testExtractCustomPartitionsNullMappings() {
    assertNull(
        PartitionDetector.extractCustomPartitions(
        "/data/file.parquet", ".*", null));
  }

  @Test void testExtractCustomPartitionsNoMatch() {
    List<PartitionedTableConfig.ColumnMapping> mappings =
        Collections.singletonList(new PartitionedTableConfig.ColumnMapping("year", 1, "INTEGER"));
    assertNull(
        PartitionDetector.extractCustomPartitions(
        "/data/file.parquet", "-(\\d{4})\\.csv", mappings));
  }

  @Test void testExtractCustomPartitionsSimpleMatch() {
    List<PartitionedTableConfig.ColumnMapping> mappings =
        Collections.singletonList(new PartitionedTableConfig.ColumnMapping("year", 1, "INTEGER"));
    PartitionDetector.PartitionInfo info =
        PartitionDetector.extractCustomPartitions("/data/report-2020.csv", "report-(\\d{4})\\.csv", mappings);
    assertNotNull(info);
    assertFalse(info.isHiveStyle());
    assertEquals("2020", info.getPartitionValues().get("year"));
  }

  @Test void testExtractCustomPartitionsMultipleGroups() {
    List<PartitionedTableConfig.ColumnMapping> mappings =
        Arrays.asList(new PartitionedTableConfig.ColumnMapping("year", 1, "INTEGER"),
        new PartitionedTableConfig.ColumnMapping("month", 2, "INTEGER"));
    PartitionDetector.PartitionInfo info =
        PartitionDetector.extractCustomPartitions("/data/report-2020-06.csv", "report-(\\d{4})-(\\d{2})\\.csv", mappings);
    assertNotNull(info);
    assertEquals("2020", info.getPartitionValues().get("year"));
    assertEquals("06", info.getPartitionValues().get("month"));
  }

  @Test void testExtractCustomPartitionsInvalidGroupIndex() {
    // Group index that doesn't exist in the regex match
    List<PartitionedTableConfig.ColumnMapping> mappings =
        Collections.singletonList(new PartitionedTableConfig.ColumnMapping("field", 99, "VARCHAR"));
    PartitionDetector.PartitionInfo info =
        PartitionDetector.extractCustomPartitions("/data/report-2020.csv", "report-(\\d{4})\\.csv", mappings);
    assertNotNull(info);
    // Should have empty partition values (exception caught)
    assertTrue(info.getPartitionValues().isEmpty());
  }

  // ===== PartitionInfo =====

  @Test void testPartitionInfoGetters() {
    java.util.Map<String, String> values = new java.util.LinkedHashMap<>();
    values.put("year", "2020");
    List<String> columns = Collections.singletonList("year");
    PartitionDetector.PartitionInfo info =
        new PartitionDetector.PartitionInfo(values, columns, true);

    assertEquals(values, info.getPartitionValues());
    assertEquals(columns, info.getPartitionColumns());
    assertTrue(info.isHiveStyle());
  }

  @Test void testPartitionInfoNonHive() {
    PartitionDetector.PartitionInfo info =
        new PartitionDetector.PartitionInfo(
            new java.util.LinkedHashMap<>(), new ArrayList<>(), false);
    assertFalse(info.isHiveStyle());
  }
}
