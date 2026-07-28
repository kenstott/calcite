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
package org.apache.calcite.adapter.file.iceberg;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for narrowing a run's staged source files to the batch that must absorb them.
 *
 * <p>One ETL pass writes for every table and year partition it touched, so each batch has to take
 * its own slice. Taking too much feeds another table's parquet to a DuckDB query expecting this
 * table's schema; taking too little silently drops rows, since a returned list suppresses the
 * partition listing that would otherwise have found them.
 */
@Tag("unit")
class IcebergMaterializerStagedFilesTest {

  private static final String BASE = "s3://govdata-parquet-v1/sec";
  private static final String METADATA_PATTERN = BASE + "/year=*/*metadata*.parquet";
  private static final String FACTS_PATTERN = BASE + "/year=*/*facts*.parquet";

  private static List<String> staged() {
    return Arrays.asList(
        BASE + "/year=2026/metadata_batch_0000.parquet",
        BASE + "/year=2026/facts_batch_0001.parquet",
        BASE + "/year=2026/chunks_batch_0002.parquet",
        BASE + "/year=2024/metadata_batch_0003.parquet",
        BASE + "/year=2024/facts_batch_0004.parquet");
  }

  @Test void testSelectsOnlyMatchingYearAndTable() {
    List<String> result =
        IcebergMaterializer.filterStagedFilesForBatch(staged(), METADATA_PATTERN, "2026");

    assertEquals(
        Arrays.asList(BASE + "/year=2026/metadata_batch_0000.parquet"), result,
        "expected only year=2026 metadata files");
  }

  @Test void testSameFilesSplitCleanlyAcrossBatches() {
    // Every staged file must be claimed by exactly one (table, year) batch — a file claimed twice
    // is materialized twice, and one claimed by none is never materialized at all.
    List<String> allClaims = new ArrayList<String>();
    for (String pattern : new String[] {METADATA_PATTERN, FACTS_PATTERN}) {
      for (String year : new String[] {"2024", "2026"}) {
        allClaims.addAll(IcebergMaterializer.filterStagedFilesForBatch(staged(), pattern, year));
      }
    }

    assertEquals(4, allClaims.size(), "each metadata/facts file should be claimed exactly once");
    assertEquals(4, new java.util.HashSet<String>(allClaims).size(), "no file claimed twice");
    assertTrue(!allClaims.contains(BASE + "/year=2026/chunks_batch_0002.parquet"),
        "chunks file belongs to neither the metadata nor the facts batch");
  }

  @Test void testNullStagedListMeansListThePartition() {
    assertNull(IcebergMaterializer.filterStagedFilesForBatch(null, METADATA_PATTERN, "2026"),
        "a null staged list must stay null so the caller falls back to listing");
  }

  @Test void testUnpartitionedBatchMeansListThePartition() {
    assertNull(IcebergMaterializer.filterStagedFilesForBatch(staged(), METADATA_PATTERN, null),
        "without a year the staged list cannot be sliced, so the caller must list instead");
  }

  @Test void testCleanRunWithNoWritesYieldsEmptyNotNull() {
    List<String> result = IcebergMaterializer.filterStagedFilesForBatch(
        new ArrayList<String>(), METADATA_PATTERN, "2026");

    assertEquals(0, result.size());
    assertTrue(result != null,
        "empty means 'nothing new to absorb', which is distinct from 'go and list'");
  }

  @Test void testIgnoresFilesNestedBelowThePartition() {
    List<String> nested = Arrays.asList(
        BASE + "/year=2026/metadata_batch_0000.parquet",
        BASE + "/year=2026/filing_metadata/data/metadata_batch_0001.parquet");

    List<String> result =
        IcebergMaterializer.filterStagedFilesForBatch(nested, METADATA_PATTERN, "2026");

    assertEquals(
        Arrays.asList(BASE + "/year=2026/metadata_batch_0000.parquet"), result,
        "only files directly in the partition are source files; deeper paths are table internals");
  }

  @Test void testYearMatchIsExact() {
    List<String> result =
        IcebergMaterializer.filterStagedFilesForBatch(staged(), METADATA_PATTERN, "202");

    assertEquals(0, result.size(), "year=202 must not match the year=2024 or year=2026 partitions");
  }
}
