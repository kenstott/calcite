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

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link PartitionDetector}.
 */
@Tag("unit")
public class PartitionDetectorTest {

  // ===== extractHivePartitions =====

  @Test void testExtractHivePartitionsSingleLevel() {
    PartitionDetector.PartitionInfo info =
        PartitionDetector.extractHivePartitions("/data/year=2023/data.parquet");

    assertNotNull(info);
    assertTrue(info.isHiveStyle());
    assertEquals(Collections.singletonList("year"), info.getPartitionColumns());
    assertEquals("2023", info.getPartitionValues().get("year"));
  }

  @Test void testExtractHivePartitionsMultipleLevel() {
    PartitionDetector.PartitionInfo info =
        PartitionDetector.extractHivePartitions(
            "/data/year=2023/month=06/day=15/data.parquet");

    assertNotNull(info);
    assertTrue(info.isHiveStyle());
    assertEquals(Arrays.asList("year", "month", "day"), info.getPartitionColumns());
    assertEquals("2023", info.getPartitionValues().get("year"));
    assertEquals("06", info.getPartitionValues().get("month"));
    assertEquals("15", info.getPartitionValues().get("day"));
  }

  @Test void testExtractHivePartitionsNoPartitions() {
    PartitionDetector.PartitionInfo info =
        PartitionDetector.extractHivePartitions("/data/simple/data.parquet");

    assertNull(info);
  }

  @Test void testExtractHivePartitionsMixedDirectories() {
    // Some directories are Hive-style, some are not
    PartitionDetector.PartitionInfo info =
        PartitionDetector.extractHivePartitions(
            "/data/type=income/somedir/year=2023/data.parquet");

    assertNotNull(info);
    assertTrue(info.isHiveStyle());
    // Should find both hive partitions, skipping non-hive dirs
    Map<String, String> values = info.getPartitionValues();
    assertTrue(values.containsKey("year"));
    assertEquals("2023", values.get("year"));
    assertTrue(values.containsKey("type"));
    assertEquals("income", values.get("type"));
  }

  @Test void testExtractHivePartitionsSpecialCharValues() {
    PartitionDetector.PartitionInfo info =
        PartitionDetector.extractHivePartitions(
            "/data/region=US-East/data.parquet");

    assertNotNull(info);
    assertEquals("US-East", info.getPartitionValues().get("region"));
  }

  // ===== detectPartitionScheme =====

  @Test void testDetectPartitionSchemeNullInput() {
    assertNull(PartitionDetector.detectPartitionScheme(null));
  }

  @Test void testDetectPartitionSchemeEmptyList() {
    assertNull(PartitionDetector.detectPartitionScheme(new ArrayList<String>()));
  }

  @Test void testDetectPartitionSchemeConsistentHive() {
    List<String> paths = Arrays.asList(
        "/data/year=2020/geo=US/data.parquet",
        "/data/year=2021/geo=UK/data.parquet",
        "/data/year=2022/geo=CA/data.parquet"
    );

    PartitionDetector.PartitionInfo info =
        PartitionDetector.detectPartitionScheme(paths);

    assertNotNull(info);
    assertTrue(info.isHiveStyle());
    assertEquals(Arrays.asList("year", "geo"), info.getPartitionColumns());
  }

  @Test void testDetectPartitionSchemeInconsistentColumns() {
    List<String> paths = Arrays.asList(
        "/data/year=2020/geo=US/data.parquet",
        "/data/year=2021/state=CA/data.parquet"  // different partition column
    );

    PartitionDetector.PartitionInfo info =
        PartitionDetector.detectPartitionScheme(paths);

    // Should return null due to inconsistent columns
    assertNull(info);
  }

  @Test void testDetectPartitionSchemeNonHiveFiles() {
    List<String> paths = Arrays.asList(
        "/data/2020/data.parquet",
        "/data/2021/data.parquet"
    );

    PartitionDetector.PartitionInfo info =
        PartitionDetector.detectPartitionScheme(paths);

    assertNull(info);
  }

  @Test void testDetectPartitionSchemeSingleFile() {
    List<String> paths = Collections.singletonList(
        "/data/year=2023/data.parquet"
    );

    PartitionDetector.PartitionInfo info =
        PartitionDetector.detectPartitionScheme(paths);

    assertNotNull(info);
    assertTrue(info.isHiveStyle());
    assertEquals(Collections.singletonList("year"), info.getPartitionColumns());
  }

  // ===== extractDirectoryPartitions =====

  @Test void testExtractDirectoryPartitionsNullColumnNames() {
    assertNull(PartitionDetector.extractDirectoryPartitions("/data/2023/us/data.parquet", null));
  }

  @Test void testExtractDirectoryPartitionsEmptyColumnNames() {
    assertNull(
        PartitionDetector.extractDirectoryPartitions(
            "/data/2023/us/data.parquet", new ArrayList<String>()));
  }

  @Test void testExtractDirectoryPartitionsSingleColumn() {
    PartitionDetector.PartitionInfo info =
        PartitionDetector.extractDirectoryPartitions(
            "/data/2023/data.parquet",
            Collections.singletonList("year"));

    assertNotNull(info);
    assertFalse(info.isHiveStyle());
    assertEquals(Collections.singletonList("year"), info.getPartitionColumns());
    assertEquals("2023", info.getPartitionValues().get("year"));
  }

  @Test void testExtractDirectoryPartitionsMultipleColumns() {
    PartitionDetector.PartitionInfo info =
        PartitionDetector.extractDirectoryPartitions(
            "/data/2023/US/data.parquet",
            Arrays.asList("year", "country"));

    assertNotNull(info);
    assertFalse(info.isHiveStyle());
    assertEquals("2023", info.getPartitionValues().get("year"));
    assertEquals("US", info.getPartitionValues().get("country"));
  }

  @Test void testExtractDirectoryPartitionsMoreColumnsThanDirectories() {
    // Request 3 column names but file is only 2 dirs deep from root
    PartitionDetector.PartitionInfo info =
        PartitionDetector.extractDirectoryPartitions(
            "/data/file.parquet",
            Arrays.asList("a", "b", "c"));

    assertNotNull(info);
    // Only 1 parent directory ("data"), so only partial mapping
    Map<String, String> values = info.getPartitionValues();
    // Depending on the logic, should map last available directory
    assertNotNull(values);
  }

  // ===== extractCustomPartitions =====

  @Test void testExtractCustomPartitionsNullRegex() {
    assertNull(
        PartitionDetector.extractCustomPartitions(
            "/data/2023-US-income.parquet", null,
            Collections.<PartitionedTableConfig.ColumnMapping>emptyList()));
  }

  @Test void testExtractCustomPartitionsNullMappings() {
    assertNull(
        PartitionDetector.extractCustomPartitions(
            "/data/2023-US-income.parquet", "(\\d{4})-(\\w+)", null));
  }

  @Test void testExtractCustomPartitionsMatching() {
    List<PartitionedTableConfig.ColumnMapping> mappings = Arrays.asList(
        new PartitionedTableConfig.ColumnMapping("year", 1, "INTEGER"),
        new PartitionedTableConfig.ColumnMapping("country", 2, "VARCHAR")
    );

    PartitionDetector.PartitionInfo info =
        PartitionDetector.extractCustomPartitions(
            "/data/2023-US-income.parquet",
            "(\\d{4})-(\\w+)",
            mappings);

    assertNotNull(info);
    assertFalse(info.isHiveStyle());
    assertEquals("2023", info.getPartitionValues().get("year"));
    assertEquals("US", info.getPartitionValues().get("country"));
    assertEquals(Arrays.asList("year", "country"), info.getPartitionColumns());
  }

  @Test void testExtractCustomPartitionsNoMatch() {
    List<PartitionedTableConfig.ColumnMapping> mappings = Collections.singletonList(
        new PartitionedTableConfig.ColumnMapping("year", 1, "INTEGER")
    );

    PartitionDetector.PartitionInfo info =
        PartitionDetector.extractCustomPartitions(
            "/data/no-match-here.parquet",
            "(\\d{4})",
            mappings);

    // No match in "no-match-here" for 4 digits
    assertNull(info);
  }

  @Test void testExtractCustomPartitionsInvalidGroup() {
    List<PartitionedTableConfig.ColumnMapping> mappings = Collections.singletonList(
        new PartitionedTableConfig.ColumnMapping("missing", 99, "VARCHAR")
    );

    PartitionDetector.PartitionInfo info =
        PartitionDetector.extractCustomPartitions(
            "/data/2023-income.parquet",
            "(\\d{4})",
            mappings);

    // Regex matches, but group 99 does not exist - should handle gracefully
    assertNotNull(info);
    // The mapping should fail gracefully and the column should not be in values
    assertFalse(info.getPartitionValues().containsKey("missing"));
  }

  // ===== PartitionInfo getters =====

  @Test void testPartitionInfoGetters() {
    PartitionDetector.PartitionInfo info =
        PartitionDetector.extractHivePartitions(
            "/data/year=2023/geo=US/file.parquet");

    assertNotNull(info);
    assertNotNull(info.getPartitionValues());
    assertNotNull(info.getPartitionColumns());
    assertTrue(info.isHiveStyle());
    assertEquals(2, info.getPartitionColumns().size());
    assertEquals(2, info.getPartitionValues().size());
  }
}
