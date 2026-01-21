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

  /** DuckDB memory limit - from DUCKDB_MEMORY_LIMIT env var, default 4GB. */
  private static final String DUCKDB_MEMORY_LIMIT =
      System.getenv("DUCKDB_MEMORY_LIMIT") != null
          ? System.getenv("DUCKDB_MEMORY_LIMIT") : "4GB";

  private final StorageProvider storageProvider;
  private final String baseDirectory;
  private final String operatingDirectory;
  private final int startYear;
  private final int endYear;
  private final boolean sourceIsIceberg;

  /** Lazy-initialized incremental tracker for partition status tracking. */
  private IncrementalTracker incrementalTracker;

  /**
   * Creates a materializer with all required parameters.
   *
   * @param storageProvider Storage provider for S3/cloud storage (null for local)
   * @param baseDirectory Directory where data files live (read/write)
   * @param operatingDirectory Directory for status tracking DBs
   * @param startYear Start year for batching
   * @param endYear End year for batching
   * @param sourceIsIceberg Whether source tables are in Iceberg format
   */
  public AlternatePartitionMaterializer(StorageProvider storageProvider, String baseDirectory,
      String operatingDirectory, int startYear, int endYear, boolean sourceIsIceberg) {
    this.storageProvider = storageProvider;
    this.baseDirectory = baseDirectory;
    this.operatingDirectory = operatingDirectory;
    this.startYear = startYear;
    this.endYear = endYear;
    this.sourceIsIceberg = sourceIsIceberg;
  }

  /**
   * Get or create the incremental tracker for partition status tracking.
   * Uses DuckDB-based storage in the operating directory.
   *
   * @return IncrementalTracker instance
   */
  private IncrementalTracker getOrCreateTracker() {
    if (incrementalTracker == null) {
      incrementalTracker = DuckDBPartitionStatusStore.getInstance(operatingDirectory);
      LOGGER.debug("Created incremental tracker in operating directory: {}", operatingDirectory);
    }
    return incrementalTracker;
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
      // Skip disabled alternates
      if (!alternate.isEnabled()) {
        LOGGER.info("Skipping disabled alternate '{}' for table '{}'",
            alternate.getName(), tableName);
        continue;
      }
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
    // If no base in pattern, use alternate name to avoid conflicts
    String targetBase = extractTargetBase(alternatePattern);
    if (targetBase.isEmpty()) {
      targetBase = alternateName;
    }

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
      ParquetReorganizer reorganizer =
          new ParquetReorganizer(storageProvider != null ? storageProvider : createLocalStorageProvider(),
          baseDirectory);

      ParquetReorganizer.ReorgConfig.Builder configBuilder = ParquetReorganizer.ReorgConfig.builder()
          .name(alternateName)
          .sourcePattern(sourceGlob)
          .sourceTable(sourceTableName)
          .targetBase(targetBase)
          .partitionColumns(partitionColumns)
          .columnMappings(columnMappings)
          .batchPartitionColumns(batchColumns)
          .yearRange(startYear, endYear)
          .threads(alternate.getThreads())
          .incrementalKeys(alternate.getIncrementalKeys())
          .incrementalTracker(getOrCreateTracker())
          .currentYearTtlDays(alternate.getCurrentYearTtlDays());

      // Add Iceberg config if source is Iceberg
      if (sourceIsIceberg) {
        configBuilder.sourceIsIceberg(true);
        configBuilder.icebergWarehousePath(baseDirectory);
        LOGGER.info("ParquetReorganizer using Iceberg source: {}/{}", baseDirectory, sourceTableName);
      }

      reorganizer.reorganize(configBuilder.build());
      LOGGER.info("Successfully materialized alternate '{}' using ParquetReorganizer", alternateName);
      return true;
    }

    // Fall back to simple DuckDB for local, non-batched cases
    return materializeSimple(sourceTableName, sourceGlob, targetBase, partitionColumns, alternateName);
  }

  /**
   * Simple materialization for local files without batching.
   *
   * <p>When sourceIsIceberg is true, reads from Iceberg table using iceberg_scan().
   * Otherwise reads from hive-partitioned Parquet files using read_parquet().
   */
  private boolean materializeSimple(String sourceTableName, String sourceGlob, String targetBase,
      List<String> partitionColumns, String alternateName) {
    try {
      java.nio.file.Path targetPath = java.nio.file.Paths.get(baseDirectory, targetBase);
      java.nio.file.Files.createDirectories(targetPath);

      String targetDir = targetPath.toString();

      try (java.sql.Connection conn = java.sql.DriverManager.getConnection("jdbc:duckdb:")) {
        java.sql.Statement stmt = conn.createStatement();

        // Limit memory to avoid OOM on memory-constrained systems
        stmt.execute("SET memory_limit='" + DUCKDB_MEMORY_LIMIT + "'");
        stmt.execute("SET temp_directory='" + baseDirectory + "/.duckdb_tmp'");
        stmt.execute("SET threads=2");

        // Load Iceberg extension if needed
        if (sourceIsIceberg) {
          stmt.execute("INSTALL iceberg");
          stmt.execute("LOAD iceberg");
          // Enable version guessing for tables without version-hint file
          stmt.execute("SET unsafe_enable_version_guessing = true");
        }

        StringBuilder partitionByClause = new StringBuilder();
        for (String col : partitionColumns) {
          if (partitionByClause.length() > 0) {
            partitionByClause.append(", ");
          }
          partitionByClause.append(col);
        }

        // Build source clause based on format
        String sourceClause;
        if (sourceIsIceberg) {
          // Read from Iceberg table using iceberg_scan
          String icebergTablePath = baseDirectory + "/" + sourceTableName;
          sourceClause = String.format("iceberg_scan('%s')", icebergTablePath);
          LOGGER.info("Reading from Iceberg table: {}", icebergTablePath);
        } else {
          // Read from hive-partitioned Parquet files
          String fullSourcePattern = java.nio.file.Paths.get(baseDirectory, sourceGlob).toString();
          sourceClause = String.format("read_parquet('%s', hive_partitioning=true)", fullSourcePattern);
          LOGGER.info("Reading from Parquet files: {}", fullSourcePattern);
        }

        String sql = String.format("COPY (SELECT * FROM %s) "
                + "TO '%s' (FORMAT PARQUET, PARTITION_BY (%s), OVERWRITE_OR_IGNORE)",
            sourceClause, targetDir, partitionByClause.toString());

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
   * "frequency=*\/*.parquet" -> "" (no base before partition)
   */
  private String extractTargetBase(String pattern) {
    // Find first partition wildcard
    int starIndex = pattern.indexOf('*');
    if (starIndex > 0) {
      int slashIndex = pattern.lastIndexOf('/', starIndex);
      if (slashIndex > 0) {
        // Check if this base contains a wildcard (e.g., "frequency=*/...")
        String base = pattern.substring(0, slashIndex);
        if (base.contains("*")) {
          // Pattern starts with partition column, no concrete base
          return "";
        }
        return base;
      }
    }
    // No valid base found
    return "";
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
   * Create a materializer for parquet sources.
   */
  public static AlternatePartitionMaterializer create(StorageProvider storageProvider,
      String baseDirectory, String operatingDirectory, int startYear, int endYear) {
    return new AlternatePartitionMaterializer(storageProvider, baseDirectory, operatingDirectory,
        startYear, endYear, false);
  }

  /**
   * Create a materializer for Iceberg sources.
   */
  public static AlternatePartitionMaterializer createWithIceberg(StorageProvider storageProvider,
      String baseDirectory, String operatingDirectory, int startYear, int endYear) {
    return new AlternatePartitionMaterializer(storageProvider, baseDirectory, operatingDirectory,
        startYear, endYear, true);
  }
}
