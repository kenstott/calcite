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

import org.apache.calcite.adapter.file.storage.StorageProvider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Materializes alternate partition layouts from schema configuration.
 *
 * <p>When a table has {@code alternate_partitions} defined in its schema config,
 * this utility creates the consolidated data files using ParquetReorganizer.
 *
 * <p>Example: A table with pattern {@code type=foo/year=STAR/geo=STAR/data.parquet}
 * can have an alternate {@code type=foo_by_geo/geo=STAR/data.parquet} that consolidates
 * all years into a single file per geo value.
 *
 * <p>The materializer reads the source data and writes consolidated files
 * partitioned by the alternate's partition keys.
 *
 * <p>For large datasets, set {@code batch_partition_columns} in the schema to process
 * data in smaller batches (e.g., by year and region) to avoid OOM errors.
 */
public class AlternatePartitionMaterializer {
  private static final Logger LOGGER = LoggerFactory.getLogger(AlternatePartitionMaterializer.class);

  private final StorageProvider storageProvider;
  private final String baseDirectory;
  private final int startYear;
  private final int endYear;

  /**
   * Creates a materializer for local file system with default year range.
   *
   * @param baseDirectory Base directory for parquet files
   */
  public AlternatePartitionMaterializer(String baseDirectory) {
    this(null, baseDirectory, 2020, java.time.Year.now().getValue());
  }

  /**
   * Creates a materializer with storage provider and year range.
   *
   * @param storageProvider Storage provider for S3/cloud storage (null for local)
   * @param baseDirectory Base directory for parquet files
   * @param startYear Start year for batching
   * @param endYear End year for batching
   */
  public AlternatePartitionMaterializer(StorageProvider storageProvider, String baseDirectory,
      int startYear, int endYear) {
    this.storageProvider = storageProvider;
    this.baseDirectory = baseDirectory;
    this.startYear = startYear;
    this.endYear = endYear;
  }

  /**
   * Materialize all alternate partitions for a table.
   *
   * @param config The table configuration with alternate_partitions defined
   * @return Number of alternates successfully materialized
   */
  public int materializeAlternates(PartitionedTableConfig config) {
    List<PartitionedTableConfig.AlternatePartitionConfig> alternates = config.getAlternatePartitions();
    if (alternates == null || alternates.isEmpty()) {
      return 0;
    }

    String sourcePattern = config.getPattern();
    String tableName = config.getName();

    LOGGER.info("Materializing {} alternate partition(s) for table '{}'",
        alternates.size(), tableName);

    int materialized = 0;
    for (PartitionedTableConfig.AlternatePartitionConfig alternate : alternates) {
      try {
        if (materializeAlternate(tableName, sourcePattern, alternate)) {
          materialized++;
        }
      } catch (Exception e) {
        LOGGER.error("Failed to materialize alternate '{}' for table '{}': {}",
            alternate.getName(), tableName, e.getMessage(), e);
      }
    }

    LOGGER.info("Materialized {}/{} alternate partition(s) for '{}'",
        materialized, alternates.size(), tableName);

    return materialized;
  }

  /**
   * Materialize a single alternate partition layout.
   */
  private boolean materializeAlternate(String sourceTableName, String sourcePattern,
      PartitionedTableConfig.AlternatePartitionConfig alternate) throws IOException {

    String alternateName = alternate.getName();
    String alternatePattern = alternate.getPattern();
    PartitionedTableConfig.PartitionConfig partitionConfig = alternate.getPartition();

    if (partitionConfig == null || partitionConfig.getColumnDefinitions() == null
        || partitionConfig.getColumnDefinitions().isEmpty()) {
      LOGGER.warn("Alternate '{}' has no partition column definitions - skipping", alternateName);
      return false;
    }

    // Get partition columns
    List<String> partitionColumns = new ArrayList<String>();
    for (PartitionedTableConfig.ColumnDefinition colDef : partitionConfig.getColumnDefinitions()) {
      partitionColumns.add(colDef.getName());
    }

    // Build target base directory from alternate pattern
    String targetBase = extractTargetBase(alternatePattern);

    // Build source glob pattern
    String sourceGlob = buildSourceGlob(sourcePattern);

    // Check if batched processing is needed
    List<String> batchColumns = alternate.getBatchPartitionColumns();
    Map<String, String> columnMappings = alternate.getColumnMappings();
    boolean needsBatching = batchColumns != null && !batchColumns.isEmpty();

    LOGGER.info("Materializing alternate '{}' (partition by: {}, batching: {})",
        alternateName, partitionColumns, needsBatching ? batchColumns : "none");

    // Use ParquetReorganizer for batched processing
    if (needsBatching || storageProvider != null) {
      ParquetReorganizer reorganizer = new ParquetReorganizer(
          storageProvider != null ? storageProvider : createLocalStorageProvider(),
          baseDirectory);

      ParquetReorganizer.ReorgConfig reorgConfig = ParquetReorganizer.ReorgConfig.builder()
          .name(alternateName)
          .sourcePattern(sourceGlob)
          .targetBase(targetBase)
          .partitionColumns(partitionColumns)
          .columnMappings(columnMappings)
          .batchPartitionColumns(batchColumns)
          .yearRange(startYear, endYear)
          .build();

      reorganizer.reorganize(reorgConfig);
      LOGGER.info("Successfully materialized alternate '{}' using ParquetReorganizer", alternateName);
      return true;
    }

    // Fall back to simple DuckDB for local, non-batched cases
    return materializeSimple(sourceGlob, targetBase, partitionColumns, alternateName);
  }

  /**
   * Simple materialization for local files without batching.
   */
  private boolean materializeSimple(String sourceGlob, String targetBase,
      List<String> partitionColumns, String alternateName) {
    try {
      java.nio.file.Path targetPath = java.nio.file.Paths.get(baseDirectory, targetBase);
      java.nio.file.Files.createDirectories(targetPath);

      String fullSourcePattern = java.nio.file.Paths.get(baseDirectory, sourceGlob).toString();
      String targetDir = targetPath.toString();

      try (java.sql.Connection conn = java.sql.DriverManager.getConnection("jdbc:duckdb:")) {
        java.sql.Statement stmt = conn.createStatement();

        StringBuilder partitionByClause = new StringBuilder();
        for (String col : partitionColumns) {
          if (partitionByClause.length() > 0) {
            partitionByClause.append(", ");
          }
          partitionByClause.append(col);
        }

        String sql = String.format(
            "COPY (SELECT * FROM read_parquet('%s', hive_partitioning=true)) "
                + "TO '%s' (FORMAT PARQUET, PARTITION_BY (%s), OVERWRITE_OR_IGNORE)",
            fullSourcePattern, targetDir, partitionByClause.toString());

        LOGGER.debug("Executing: {}", sql);
        stmt.execute(sql);
      }

      LOGGER.info("Successfully materialized alternate '{}' (simple mode)", alternateName);
      return true;

    } catch (Exception e) {
      LOGGER.error("Failed to materialize alternate '{}': {}", alternateName, e.getMessage());
      return false;
    }
  }

  /**
   * Creates a local file storage provider for non-S3 cases.
   */
  private StorageProvider createLocalStorageProvider() {
    return new org.apache.calcite.adapter.file.storage.LocalFileStorageProvider();
  }

  /**
   * Build a glob pattern from a hive-style pattern.
   * Converts "type=foo/year=*\/name=*\/data.parquet" to "type=foo/year=*\/name=*\/*.parquet"
   */
  private String buildSourceGlob(String pattern) {
    // Replace any concrete filename with wildcard
    if (pattern.endsWith(".parquet")) {
      int lastSlash = pattern.lastIndexOf('/');
      if (lastSlash > 0) {
        return pattern.substring(0, lastSlash + 1) + "*.parquet";
      }
    }
    // Already a glob pattern
    return pattern;
  }

  /**
   * Extract the target base directory from alternate pattern.
   * "type=foo_by_geo/geo=*\/data.parquet" -> "type=foo_by_geo"
   */
  private String extractTargetBase(String pattern) {
    // Find first partition wildcard
    int starIndex = pattern.indexOf('*');
    if (starIndex > 0) {
      int slashIndex = pattern.lastIndexOf('/', starIndex);
      if (slashIndex > 0) {
        return pattern.substring(0, slashIndex);
      }
    }
    // Fallback: return everything up to the filename
    int lastSlash = pattern.lastIndexOf('/');
    return lastSlash > 0 ? pattern.substring(0, lastSlash) : pattern;
  }

  /**
   * Materialize alternate partitions for all tables in a list.
   *
   * @param tables List of table configurations
   * @return Total number of alternates materialized
   */
  public int materializeAll(List<PartitionedTableConfig> tables) {
    int total = 0;
    for (PartitionedTableConfig table : tables) {
      total += materializeAlternates(table);
    }
    return total;
  }

  /**
   * Create a materializer for a given base directory (local file system).
   */
  public static AlternatePartitionMaterializer create(String baseDirectory) {
    return new AlternatePartitionMaterializer(baseDirectory);
  }

  /**
   * Create a materializer with storage provider and year range.
   */
  public static AlternatePartitionMaterializer create(StorageProvider storageProvider,
      String baseDirectory, int startYear, int endYear) {
    return new AlternatePartitionMaterializer(storageProvider, baseDirectory, startYear, endYear);
  }
}
