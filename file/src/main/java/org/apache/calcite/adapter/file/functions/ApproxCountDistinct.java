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
package org.apache.calcite.adapter.file.functions;

import org.apache.calcite.adapter.file.statistics.ColumnStatistics;
import org.apache.calcite.adapter.file.statistics.HyperLogLogSketch;
import org.apache.calcite.adapter.file.statistics.StatisticsProvider;
import org.apache.calcite.adapter.file.statistics.TableStatistics;
import org.apache.calcite.adapter.file.table.ParquetTranslatableTable;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.schema.FunctionParameter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * APPROX_COUNT_DISTINCT aggregate function that uses HyperLogLog sketches
 * for fast approximate distinct counting.
 *
 * This function explicitly trades accuracy for speed and should achieve
 * 98%+ accuracy for datasets under 1M rows with precision 14.
 */
public class ApproxCountDistinct { // Temporarily disabled - implements AggregateFunction {
  private static final Logger LOGGER = LoggerFactory.getLogger(ApproxCountDistinct.class);

  // HLL precision - 14 gives ~1.625% standard error
  private static final int HLL_PRECISION = 14;

  private final String columnName;
  private final TableScan tableScan;
  private HyperLogLogSketch precomputedSketch;

  /**
   * Create an APPROX_COUNT_DISTINCT function.
   *
   * @param columnName The column to count distinct values for
   * @param tableScan The table scan to get statistics from
   */
  public ApproxCountDistinct(String columnName, TableScan tableScan) {
    this.columnName = columnName;
    this.tableScan = tableScan;
    loadPrecomputedSketch();
  }

  /**
   * Try to load a precomputed HLL sketch from cache.
   */
  private void loadPrecomputedSketch() {
    if (tableScan == null) {
      return;
    }

    try {
      RelOptTable table = tableScan.getTable();
      ParquetTranslatableTable parquetTable = table.unwrap(ParquetTranslatableTable.class);

      if (parquetTable instanceof StatisticsProvider) {
        StatisticsProvider provider = (StatisticsProvider) parquetTable;
        TableStatistics stats = provider.getTableStatistics(table);

        if (stats != null && stats.getColumnStatistics(columnName) != null) {
          ColumnStatistics colStats = stats.getColumnStatistics(columnName);
          if (colStats.getHllSketch() != null) {
            precomputedSketch = colStats.getHllSketch();
            LOGGER.info("Using precomputed HLL sketch for {}: estimate={}",
                       columnName, precomputedSketch.getEstimate());
          }
        }
      }
    } catch (Exception e) {
      LOGGER.debug("Could not load precomputed HLL sketch: {}", e.getMessage());
    }
  }

  public List<FunctionParameter> getParameters() {
    // This would be defined based on the SQL function signature
    return List.of();
  }

  public Accumulator createAccumulator() {
    // If we have a precomputed sketch, return it immediately
    if (precomputedSketch != null) {
      return new PrecomputedAccumulator(precomputedSketch);
    }

    // Otherwise create a new accumulator that will build the sketch
    return new HLLAccumulator();
  }

  /**
   * Accumulator that builds an HLL sketch from values.
   */
  public static class HLLAccumulator implements Accumulator {
    private final HyperLogLogSketch sketch;

    public HLLAccumulator() {
      this.sketch = new HyperLogLogSketch(HLL_PRECISION);
    }

    public void add(Object value) {
      if (value != null) {
        sketch.add(value.toString());
      }
    }

    public long getResult() {
      long estimate = sketch.getEstimate();
      LOGGER.debug("HLL accumulator returning estimate: {}", estimate);
      return estimate;
    }
  }

  /**
   * Accumulator that uses a precomputed HLL sketch.
   */
  public static class PrecomputedAccumulator implements Accumulator {
    private final HyperLogLogSketch sketch;
    private boolean used = false;

    public PrecomputedAccumulator(HyperLogLogSketch sketch) {
      this.sketch = sketch;
    }

    public void add(Object value) {
      // Ignore values - we're using the precomputed sketch
      if (!used) {
        LOGGER.debug("Using precomputed HLL sketch, ignoring runtime values");
        used = true;
      }
    }

    public long getResult() {
      long estimate = sketch.getEstimate();
      LOGGER.info("Precomputed HLL returning estimate: {} (98%+ accuracy expected)", estimate);
      return estimate;
    }
  }

  /**
   * Interface for HLL accumulators.
   */
  public interface Accumulator {
    void add(Object value);
    long getResult();
  }

  public Class<?> getReturnType() {
    return Long.class;
  }

  public Class<?> getAccumulatorType() {
    return Accumulator.class;
  }
}
