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

  /**
   * Materialization must cover the partitions the ETL wrote, not the caller's year of interest.
   *
   * <p>A SEC reprocess is scoped to the accessions' FILING year, while the converter partitions by
   * fiscal period. A run scoped to filing year 2019 was measured writing into eleven partitions,
   * 2011 through 2022. Batching only the configured range strands every file outside it, and the
   * run still reports success — so the rows never appear and the next gap scan re-offers the same
   * accessions.
   */
  @Test void testYearsComeFromTheFilesActuallyWritten() {
    List<String> staged = Arrays.asList(
        BASE + "/year=2019/metadata_batch_0000.parquet",
        BASE + "/year=2018/facts_batch_0001.parquet",
        BASE + "/year=2011/mda_batch_0002.parquet",
        BASE + "/year=2019/facts_batch_0003.parquet");

    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern(METADATA_PATTERN)
            .targetTableId("filing_metadata")
            .yearRange(2019, 2019)          // caller's filing-year scope
            .stagedSourceFiles(staged)
            .build();

    assertEquals(Arrays.asList("2011", "2018", "2019"),
        IcebergMaterializer.yearsFromStagedFiles(config),
        "every written partition must be batched, ascending and deduplicated");
  }

  @Test void testNoStagedListFallsBackToTheConfiguredRange() {
    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern(METADATA_PATTERN)
            .targetTableId("filing_metadata")
            .yearRange(2019, 2019)
            .build();

    assertNull(IcebergMaterializer.yearsFromStagedFiles(config),
        "null, not empty — 'the caller said nothing' must stay distinct from 'wrote nothing'");
  }

  @Test void testCleanPassThatWroteNothingBatchesNothing() {
    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern(METADATA_PATTERN)
            .targetTableId("filing_metadata")
            .yearRange(2019, 2019)
            .stagedSourceFiles(new ArrayList<String>())
            .build();

    assertEquals(0, IcebergMaterializer.yearsFromStagedFiles(config).size(),
        "an empty staged list means there is genuinely nothing to materialize");
  }

  /**
   * The consumer has to honour the empty list, not just receive it.
   *
   * <p>{@code yearsFromStagedFiles} returning empty is only half the contract; buildBatchCombinations
   * previously dropped the year column when the list was empty, leaving no batch columns at all.
   * The caller reads that as "no batching configured" and processes one unpartitioned batch — so
   * "materialize nothing" became a scan of the entire source corpus, carrying year=null, which in
   * turn disables the file-list optimization. Null keeps the two apart.
   */
  @Test void testPassThatStagedNothingBuildsNoBatchesAtAll() throws Exception {
    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern(FACTS_PATTERN)
            .targetTableId("financial_line_items")
            .yearRange(2019, 2026)
            .batchPartitionColumns(Arrays.asList("year"))
            .stagedSourceFiles(new ArrayList<String>())
            .build();

    assertNull(invokeBuildBatchCombinations(config),
        "a pass that staged nothing must build no batches, not fall through to the "
            + "unpartitioned (all) default");
  }

  /** A pass that staged partitions batches exactly those, not the configured range. */
  @Test void testPassThatStagedPartitionsBatchesOnlyThose() throws Exception {
    IcebergMaterializer.MaterializationConfig config =
        IcebergMaterializer.MaterializationConfig.builder()
            .sourcePattern(FACTS_PATTERN)
            .targetTableId("financial_line_items")
            .yearRange(2019, 2026)
            .batchPartitionColumns(Arrays.asList("year"))
            .stagedSourceFiles(staged())
            .build();

    List<?> batches = invokeBuildBatchCombinations(config);
    assertEquals(2, batches.size(), "only the two staged years, not the whole 2019-2026 range");
  }

  private static List<?> invokeBuildBatchCombinations(
      IcebergMaterializer.MaterializationConfig config) throws Exception {
    java.lang.reflect.Method method =
        IcebergMaterializer.class.getDeclaredMethod("buildBatchCombinations",
            IcebergMaterializer.MaterializationConfig.class);
    method.setAccessible(true);
    // The year branch reads only the config, so a materializer over a throwaway warehouse with no
    // storage provider is enough — nothing here touches object storage.
    IcebergMaterializer materializer =
        new IcebergMaterializer("/tmp/staged-files-test-warehouse", null, null);
    return (List<?>) method.invoke(materializer, config);
  }

  @Test void testYearMatchIsExact() {
    List<String> result =
        IcebergMaterializer.filterStagedFilesForBatch(staged(), METADATA_PATTERN, "202");

    assertEquals(0, result.size(), "year=202 must not match the year=2024 or year=2026 partitions");
  }
}
