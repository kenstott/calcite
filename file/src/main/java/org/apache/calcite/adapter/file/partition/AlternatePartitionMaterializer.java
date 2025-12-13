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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Materializes alternate partition layouts from schema configuration.
 *
 * <p>When a table has {@code alternate_partitions} defined in its schema config,
 * this utility creates the consolidated data files using DuckDB's COPY with PARTITION_BY.
 *
 * <p>Example: A table with pattern {@code type=foo/year=STAR/geo=STAR/data.parquet}
 * can have an alternate {@code type=foo_by_geo/geo=STAR/data.parquet} that consolidates
 * all years into a single file per geo value.
 *
 * <p>The materializer reads the source data and writes consolidated files
 * partitioned by the alternate's partition keys.
 */
public class AlternatePartitionMaterializer {
  private static final Logger LOGGER = LoggerFactory.getLogger(AlternatePartitionMaterializer.class);

  private final String baseDirectory;

  public AlternatePartitionMaterializer(String baseDirectory) {
    this.baseDirectory = baseDirectory;
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

    // Build source glob pattern
    String sourceGlob = buildSourceGlob(sourcePattern);
    Path sourcePath = Paths.get(baseDirectory, getTypePrefix(sourcePattern));

    if (!Files.exists(sourcePath)) {
      LOGGER.debug("Source path {} does not exist - skipping alternate materialization for {}",
          sourcePath, tableName);
      return 0;
    }

    LOGGER.info("Materializing {} alternate partition(s) for table '{}'",
        alternates.size(), tableName);

    int materialized = 0;
    for (PartitionedTableConfig.AlternatePartitionConfig alternate : alternates) {
      try {
        if (materializeAlternate(tableName, sourceGlob, alternate)) {
          materialized++;
        }
      } catch (Exception e) {
        LOGGER.error("Failed to materialize alternate '{}' for table '{}': {}",
            alternate.getName(), tableName, e.getMessage());
      }
    }

    LOGGER.info("Materialized {}/{} alternate partition(s) for '{}'",
        materialized, alternates.size(), tableName);

    return materialized;
  }

  /**
   * Materialize a single alternate partition layout.
   */
  private boolean materializeAlternate(String sourceTableName, String sourceGlob,
      PartitionedTableConfig.AlternatePartitionConfig alternate) {

    String alternateName = alternate.getName();
    String alternatePattern = alternate.getPattern();
    PartitionedTableConfig.PartitionConfig partitionConfig = alternate.getPartition();

    if (partitionConfig == null || partitionConfig.getColumnDefinitions() == null
        || partitionConfig.getColumnDefinitions().isEmpty()) {
      LOGGER.warn("Alternate '{}' has no partition column definitions - skipping", alternateName);
      return false;
    }

    // Get partition columns
    List<String> partitionColumns = partitionConfig.getColumnDefinitions().stream()
        .map(PartitionedTableConfig.ColumnDefinition::getName)
        .collect(Collectors.toList());

    // Build target directory from alternate pattern
    String targetDir = Paths.get(baseDirectory, getTypePrefix(alternatePattern)).toString();
    Path targetPath = Paths.get(targetDir);

    // Check if already materialized
    if (Files.exists(targetPath)) {
      try (Stream<Path> files = Files.walk(targetPath)) {
        long fileCount = files.filter(p -> p.toString().endsWith(".parquet")).count();
        if (fileCount > 0) {
          LOGGER.debug("Alternate '{}' already materialized with {} files - skipping",
              alternateName, fileCount);
          return true; // Already done
        }
      } catch (Exception e) {
        LOGGER.warn("Error checking existing files for '{}': {}", alternateName, e.getMessage());
      }
    }

    LOGGER.info("Materializing alternate '{}' (partition by: {})",
        alternateName, String.join(", ", partitionColumns));

    try {
      // Create target directory
      Files.createDirectories(targetPath);

      // Build full source path
      String fullSourcePattern = Paths.get(baseDirectory, sourceGlob).toString();

      // Use DuckDB to read source and write partitioned output
      try (Connection conn = DriverManager.getConnection("jdbc:duckdb:")) {
        Statement stmt = conn.createStatement();

        // Build COPY statement with PARTITION_BY
        String partitionByClause = String.join(", ", partitionColumns);
        String sql = String.format(
            "COPY (SELECT * FROM read_parquet('%s', hive_partitioning=true)) "
                + "TO '%s' (FORMAT PARQUET, PARTITION_BY (%s), OVERWRITE_OR_IGNORE)",
            fullSourcePattern, targetDir, partitionByClause);

        LOGGER.debug("Executing: {}", sql);
        stmt.execute(sql);
      }

      // Count created files
      try (Stream<Path> files = Files.walk(targetPath)) {
        long fileCount = files.filter(p -> p.toString().endsWith(".parquet")).count();
        LOGGER.info("Created {} parquet files for alternate '{}'", fileCount, alternateName);
      }

      return true;

    } catch (Exception e) {
      LOGGER.error("Failed to materialize alternate '{}': {}", alternateName, e.getMessage());
      return false;
    }
  }

  /**
   * Build a glob pattern from a hive-style pattern.
   * Converts "type=foo/year=*\/name=*\/data.parquet" to "type=foo/**\/*.parquet"
   */
  private String buildSourceGlob(String pattern) {
    // Extract the type prefix and use recursive glob
    String typePrefix = getTypePrefix(pattern);
    return typePrefix + "/**/*.parquet";
  }

  /**
   * Extract the type prefix from a pattern.
   * "type=regional_income/year=*\/..." -> "type=regional_income"
   */
  private String getTypePrefix(String pattern) {
    // Find first partition wildcard
    int starIndex = pattern.indexOf('*');
    if (starIndex > 0) {
      // Find the last slash before the wildcard
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
   * Create a materializer for a given base directory.
   */
  public static AlternatePartitionMaterializer create(String baseDirectory) {
    return new AlternatePartitionMaterializer(baseDirectory);
  }
}
