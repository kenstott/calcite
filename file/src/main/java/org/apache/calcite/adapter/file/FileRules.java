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
package org.apache.calcite.adapter.file;

import org.apache.calcite.adapter.file.format.csv.CsvProjectTableScanRule;
import org.apache.calcite.adapter.file.rules.AlternatePartitionSelectionRule;
import org.apache.calcite.adapter.file.rules.CountStarStatisticsRule;
import org.apache.calcite.adapter.file.rules.HLLCountDistinctRule;
import org.apache.calcite.adapter.file.rules.PartitionDistinctRule;

/** Planner rules relating to the File adapter. */
public abstract class FileRules {
  private FileRules() {}

  /** Rule that matches a {@link org.apache.calcite.rel.core.Project} on
   * a {@link CsvTableScan} and pushes down projects if possible. */
  public static final CsvProjectTableScanRule PROJECT_SCAN =
      CsvProjectTableScanRule.Config.DEFAULT.toRule();

  /** Rule that replaces COUNT(DISTINCT) with HLL sketch lookups when available. */
  public static final HLLCountDistinctRule HLL_COUNT_DISTINCT =
      HLLCountDistinctRule.INSTANCE;

  /** Rule that substitutes source tables with alternate partition tables
   * when query filters match the alternate's partition keys.
   * Implements the "fewest keys that cover the filter" heuristic:
   * selects the most consolidated layout that still supports partition pruning. */
  public static final AlternatePartitionSelectionRule ALTERNATE_PARTITION_SELECTION =
      AlternatePartitionSelectionRule.INSTANCE;

  /** Rule that replaces COUNT(*) with row count from table statistics.
   * Provides instant results for tables with statistics, avoiding expensive
   * S3 file listing for hive-partitioned tables with many files. */
  public static final CountStarStatisticsRule COUNT_STAR_STATISTICS =
      CountStarStatisticsRule.INSTANCE;

  /** Rule that replaces SELECT DISTINCT on partition columns with directory listing.
   * For hive-partitioned tables, partition values are encoded in directory names,
   * so we can retrieve them via fast directory listing instead of scanning parquet files. */
  public static final PartitionDistinctRule PARTITION_DISTINCT =
      PartitionDistinctRule.INSTANCE;
}
