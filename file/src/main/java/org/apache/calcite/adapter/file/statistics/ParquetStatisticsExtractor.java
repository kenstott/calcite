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
package org.apache.calcite.adapter.file.statistics;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Extracts column statistics from Parquet files including min/max values,
 * null counts, and distinct counts where available.
 */
public class ParquetStatisticsExtractor {
  private static final Logger LOGGER = LoggerFactory.getLogger(ParquetStatisticsExtractor.class);

  /**
   * Extract column statistics from a Parquet file.
   *
   * @param parquetFile The Parquet file to read
   * @return Map of column name to statistics builder
   */
  public static Map<String, ColumnStatsBuilder> extractStatistics(File parquetFile) {
    Map<String, ColumnStatsBuilder> columnStats = new HashMap<>();

    try {
      Path path = new Path(parquetFile.getAbsolutePath());
      Configuration conf = new Configuration();

      // Enable vectorized reading for better performance
      conf.set("parquet.enable.vectorized.reader", "true");

      try (@SuppressWarnings("deprecation") ParquetFileReader reader = ParquetFileReader.open(conf, path)) {
        ParquetMetadata metadata = reader.getFooter();
        MessageType schema = metadata.getFileMetaData().getSchema();
        List<BlockMetaData> blocks = metadata.getBlocks();

        // Initialize builders for each column
        for (Type field : schema.getFields()) {
          columnStats.put(field.getName(), new ColumnStatsBuilder(field.getName()));
        }

        // Process each row group's statistics
        for (BlockMetaData block : blocks) {
          long rowCount = block.getRowCount();

          for (ColumnChunkMetaData column : block.getColumns()) {
            String columnName = column.getPath().toDotString();
            ColumnStatsBuilder builder = columnStats.get(columnName);

            if (builder != null) {
              // Update row count
              builder.addRows(rowCount);

              // Get column statistics if available
              Statistics<?> stats = column.getStatistics();
              if (stats != null && !stats.isEmpty()) {
                // Update null count
                builder.addNulls(stats.getNumNulls());

                // Update min/max values
                if (stats.hasNonNullValue()) {
                  Object minValue = convertParquetValue(stats.genericGetMin());
                  Object maxValue = convertParquetValue(stats.genericGetMax());
                  builder.updateMinMax(minValue, maxValue);

                  LOGGER.debug("Column {}: min={}, max={}, nulls={}",
                             columnName, minValue, maxValue, stats.getNumNulls());
                }
              }
            }
          }
        }

        LOGGER.info("Extracted statistics for {} columns from {}",
                   columnStats.size(), parquetFile.getName());
      }
    } catch (java.io.FileNotFoundException e) {
      // File was deleted (likely during test cleanup) - this is expected behavior
      LOGGER.debug("File no longer exists during statistics extraction (likely cleanup): {}", parquetFile);
    } catch (Exception e) {
      LOGGER.error("Failed to extract Parquet statistics from {}: {}",
                  parquetFile, e.getMessage(), e);
    }

    return columnStats;
  }

  /**
   * Convert Parquet statistics value to Java object.
   */
  private static Object convertParquetValue(Object parquetValue) {
    if (parquetValue == null) {
      return null;
    }

    // Handle Binary type (used for strings)
    if (parquetValue instanceof Binary) {
      return ((Binary) parquetValue).toStringUsingUTF8();
    }

    // Other types are typically already in the right format
    return parquetValue;
  }

  /**
   * Builder for column statistics that aggregates across row groups.
   */
  public static class ColumnStatsBuilder {
    private final String columnName;
    private Object minValue;
    private Object maxValue;
    private long nullCount = 0;
    private long totalCount = 0;
    private Long exactDistinctCount; // From Parquet metadata if available
    private HyperLogLogSketch hllSketch;

    public ColumnStatsBuilder(String columnName) {
      this.columnName = columnName;
    }

    public void addRows(long rows) {
      this.totalCount += rows;
    }

    public void addNulls(long nulls) {
      this.nullCount += nulls;
    }

    public void updateMinMax(Object min, Object max) {
      if (min != null) {
        if (minValue == null) {
          minValue = min;
        } else if (min instanceof Comparable) {
          @SuppressWarnings("unchecked")
          Comparable<Object> comparableMin = (Comparable<Object>) min;
          if (comparableMin.compareTo(minValue) < 0) {
            minValue = min;
          }
        }
      }

      if (max != null) {
        if (maxValue == null) {
          maxValue = max;
        } else if (max instanceof Comparable) {
          @SuppressWarnings("unchecked")
          Comparable<Object> comparableMax = (Comparable<Object>) max;
          if (comparableMax.compareTo(maxValue) > 0) {
            maxValue = max;
          }
        }
      }
    }

    public String getColumnName() {
      return columnName;
    }

    public Object getMinValue() {
      return minValue;
    }

    public Object getMaxValue() {
      return maxValue;
    }

    public long getNullCount() {
      return nullCount;
    }

    public long getTotalCount() {
      return totalCount;
    }

    public void setHllSketch(HyperLogLogSketch sketch) {
      this.hllSketch = sketch;
    }

    public HyperLogLogSketch getHllSketch() {
      return hllSketch;
    }

    public ColumnStatistics build() {
      return new ColumnStatistics(columnName, minValue, maxValue,
                                 nullCount, totalCount, hllSketch);
    }
  }
}
