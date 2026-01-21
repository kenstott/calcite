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
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Reorganizes parquet data by consolidating files into different partition layouts.
 *
 * <p>Handles large datasets using batched processing to avoid OOM errors:
 * <ul>
 *   <li>Phase 1: Process data in batches (by year, region, etc.) writing to temp location</li>
 *   <li>Phase 2: Consolidate temp files into final single-file partitions</li>
 *   <li>Phase 3: Set lifecycle rules for automatic temp cleanup</li>
 * </ul>
 *
 * <p>Example usage:
 * <pre>
 * ParquetReorganizer reorganizer = new ParquetReorganizer(storageProvider, baseDirectory);
 * reorganizer.reorganize(
 *     "type=income/year=*\/geo=*\/*.parquet",     // source pattern
 *     "type=income_by_geo",                        // target base
 *     Arrays.asList("geo"),                        // partition columns
 *     Collections.singletonMap("geo", "GeoFips"), // column mappings
 *     Arrays.asList("year", "geo_fips_set"),      // batch columns
 *     2020, 2024                                   // year range
 *);
 * </pre>
 */
public class ParquetReorganizer {
  private static final Logger LOGGER = LoggerFactory.getLogger(ParquetReorganizer.class);

  /** DuckDB memory limit - from DUCKDB_MEMORY_LIMIT env var, default 4GB. */
  private static final String DUCKDB_MEMORY_LIMIT =
      System.getenv("DUCKDB_MEMORY_LIMIT") != null
          ? System.getenv("DUCKDB_MEMORY_LIMIT") : "4GB";

  private final StorageProvider storageProvider;
  private final String baseDirectory;

  public ParquetReorganizer(StorageProvider storageProvider, String baseDirectory) {
    this.storageProvider = storageProvider;
    this.baseDirectory = baseDirectory;
  }

  /**
   * Configuration for a reorganization job.
   */
  public static class ReorgConfig {
    private static final int DEFAULT_THREADS = 2;
    private static final int DEFAULT_CURRENT_YEAR_TTL_DAYS = 1;

    private final String sourcePattern;
    private final String targetBase;
    private final List<String> partitionColumns;
    private final Map<String, String> columnMappings;
    private final List<String> batchPartitionColumns;
    private final int startYear;
    private final int endYear;
    private final String name;
    private final int threads;
    private final List<String> incrementalKeys;
    private final IncrementalTracker incrementalTracker;
    private final String sourceTable;
    private final boolean sourceIsIceberg;
    private final String icebergWarehousePath;
    private final int currentYearTtlDays;

    private ReorgConfig(Builder builder) {
      this.sourcePattern = builder.sourcePattern;
      this.targetBase = builder.targetBase;
      this.partitionColumns = builder.partitionColumns != null
          ? builder.partitionColumns : Collections.<String>emptyList();
      this.columnMappings = builder.columnMappings != null
          ? builder.columnMappings : Collections.<String, String>emptyMap();
      this.batchPartitionColumns = builder.batchPartitionColumns != null
          ? builder.batchPartitionColumns : Collections.<String>emptyList();
      this.startYear = builder.startYear;
      this.endYear = builder.endYear;
      this.name = builder.name;
      this.threads = builder.threads > 0 ? builder.threads : DEFAULT_THREADS;
      this.incrementalKeys = builder.incrementalKeys != null
          ? builder.incrementalKeys : Collections.<String>emptyList();
      this.incrementalTracker = builder.incrementalTracker != null
          ? builder.incrementalTracker : IncrementalTracker.NOOP;
      this.sourceTable = builder.sourceTable;
      this.sourceIsIceberg = builder.sourceIsIceberg;
      this.icebergWarehousePath = builder.icebergWarehousePath;
      this.currentYearTtlDays = builder.currentYearTtlDays > 0
          ? builder.currentYearTtlDays : DEFAULT_CURRENT_YEAR_TTL_DAYS;
    }

    public String getSourcePattern() {
      return sourcePattern;
    }

    public String getTargetBase() {
      return targetBase;
    }

    public List<String> getPartitionColumns() {
      return partitionColumns;
    }

    public Map<String, String> getColumnMappings() {
      return columnMappings;
    }

    public List<String> getBatchPartitionColumns() {
      return batchPartitionColumns;
    }

    public int getStartYear() {
      return startYear;
    }

    public int getEndYear() {
      return endYear;
    }

    public String getName() {
      return name;
    }

    public int getThreads() {
      return threads;
    }

    public List<String> getIncrementalKeys() {
      return incrementalKeys;
    }

    public IncrementalTracker getIncrementalTracker() {
      return incrementalTracker;
    }

    public String getSourceTable() {
      return sourceTable;
    }

    public boolean isSourceIsIceberg() {
      return sourceIsIceberg;
    }

    public String getIcebergWarehousePath() {
      return icebergWarehousePath;
    }

    public int getCurrentYearTtlDays() {
      return currentYearTtlDays;
    }

    public long getCurrentYearTtlMillis() {
      return currentYearTtlDays * 24L * 60 * 60 * 1000;
    }

    public boolean supportsIncremental() {
      return !incrementalKeys.isEmpty() && incrementalTracker != IncrementalTracker.NOOP;
    }

    public static Builder builder() {
      return new Builder();
    }

    /**
     * Builder for ReorgConfig.
     */
    public static class Builder {
      private String sourcePattern;
      private String targetBase;
      private List<String> partitionColumns;
      private Map<String, String> columnMappings;
      private List<String> batchPartitionColumns;
      private int startYear;
      private int endYear;
      private String name;
      private int threads;
      private List<String> incrementalKeys;
      private IncrementalTracker incrementalTracker;
      private String sourceTable;
      private boolean sourceIsIceberg;
      private String icebergWarehousePath;
      private int currentYearTtlDays;

      public Builder sourcePattern(String sourcePattern) {
        this.sourcePattern = sourcePattern;
        return this;
      }

      public Builder targetBase(String targetBase) {
        this.targetBase = targetBase;
        return this;
      }

      public Builder partitionColumns(List<String> partitionColumns) {
        this.partitionColumns = partitionColumns;
        return this;
      }

      public Builder columnMappings(Map<String, String> columnMappings) {
        this.columnMappings = columnMappings;
        return this;
      }

      public Builder batchPartitionColumns(List<String> batchPartitionColumns) {
        this.batchPartitionColumns = batchPartitionColumns;
        return this;
      }

      public Builder yearRange(int startYear, int endYear) {
        this.startYear = startYear;
        this.endYear = endYear;
        return this;
      }

      public Builder name(String name) {
        this.name = name;
        return this;
      }

      public Builder threads(int threads) {
        this.threads = threads;
        return this;
      }

      public Builder incrementalKeys(List<String> incrementalKeys) {
        this.incrementalKeys = incrementalKeys;
        return this;
      }

      public Builder incrementalTracker(IncrementalTracker incrementalTracker) {
        this.incrementalTracker = incrementalTracker;
        return this;
      }

      public Builder sourceTable(String sourceTable) {
        this.sourceTable = sourceTable;
        return this;
      }

      public Builder sourceIsIceberg(boolean sourceIsIceberg) {
        this.sourceIsIceberg = sourceIsIceberg;
        return this;
      }

      public Builder icebergWarehousePath(String icebergWarehousePath) {
        this.icebergWarehousePath = icebergWarehousePath;
        return this;
      }

      public Builder currentYearTtlDays(int currentYearTtlDays) {
        this.currentYearTtlDays = currentYearTtlDays;
        return this;
      }

      public ReorgConfig build() {
        if (sourcePattern == null || sourcePattern.isEmpty()) {
          throw new IllegalArgumentException("sourcePattern is required");
        }
        if (targetBase == null || targetBase.isEmpty()) {
          throw new IllegalArgumentException("targetBase is required");
        }
        return new ReorgConfig(this);
      }
    }
  }

  /**
   * Reorganizes data based on the provided configuration.
   *
   * @param config Reorganization configuration
   * @throws IOException if reorganization fails
   */
  public void reorganize(ReorgConfig config) throws IOException {
    String fullTargetBase = storageProvider.resolvePath(baseDirectory, config.getTargetBase());

    // Temp location with timestamp to isolate each run
    String timestamp = new SimpleDateFormat("yyyyMMdd_HHmmss").format(new Date());
    String tempBase = fullTargetBase + "/_temp_reorg/" + timestamp;
    String tempPrefix = config.getTargetBase() + "/_temp_reorg/" + timestamp + "/";

    String displayName = config.getName() != null ? config.getName() : config.getTargetBase();
    LOGGER.info("Reorganizing '{}' with batching:\n  FROM: {}\n  TO: {} (via temp: {})",
        displayName, config.getSourcePattern(), fullTargetBase, tempBase);

    try (Connection conn = getDuckDBConnection()) {
      // Load Iceberg extension if reading from Iceberg source
      if (config.isSourceIsIceberg()) {
        try (Statement icebergStmt = conn.createStatement()) {
          icebergStmt.execute("INSTALL iceberg");
          icebergStmt.execute("LOAD iceberg");
          // Enable version guessing for tables without version-hint file
          icebergStmt.execute("SET unsafe_enable_version_guessing = true");
          LOGGER.debug("Loaded DuckDB Iceberg extension for Iceberg source");
        }
      }

      // Apply memory-optimizing settings to avoid OOM on large datasets
      try (Statement setupStmt = conn.createStatement()) {
        setupStmt.execute("SET threads=" + config.getThreads());
        setupStmt.execute("SET preserve_insertion_order=false");
        // Limit DuckDB native memory to avoid OOM on memory-constrained systems
        setupStmt.execute("SET memory_limit='" + DUCKDB_MEMORY_LIMIT + "'");
        setupStmt.execute("SET temp_directory='" + baseDirectory + "/.duckdb_tmp'");
        LOGGER.debug("Applied DuckDB settings: threads={}, preserve_insertion_order=false, "
            + "memory_limit={}", config.getThreads(), DUCKDB_MEMORY_LIMIT);
      }

      // Phase 1: Process data in batches
      List<Map<String, String>> batchCombinations = buildBatchCombinations(conn, config);

      int processedCount;
      if (batchCombinations.isEmpty()) {
        if (config.isSourceIsIceberg()) {
          // Iceberg source with no batching - write directly to final location
          processAllDataDirect(conn, config, fullTargetBase);
          // No Phase 2/3 needed - data written directly
          LOGGER.info("Successfully reorganized: {}", displayName);
          return;
        } else {
          // Parquet source with no batching - fall back to year-only batching
          processedCount = processYearBatches(conn, config, tempBase);
        }
      } else {
        processedCount = processBatchCombinations(conn, config, batchCombinations, tempBase);
      }

      // Skip Phase 2 if no batches were processed (all skipped via incremental)
      if (processedCount == 0) {
        LOGGER.info("No new data to consolidate - all batches were skipped (incremental)");
        LOGGER.info("Successfully reorganized: {}", displayName);
        return;
      }

      // Phase 2: Consolidate temp files into final location
      LOGGER.info("Phase 2: Consolidating temp files to final location...");
      String consolidateSql =
          buildConsolidationSql(tempBase, fullTargetBase, config.getPartitionColumns());
      LOGGER.debug("Phase 2 SQL:\n{}", consolidateSql);

      try (Statement stmt = conn.createStatement()) {
        stmt.execute(consolidateSql);
        LOGGER.info("  Consolidation complete for {}", displayName);
      }

      // Phase 3: Set lifecycle rule for temp cleanup AFTER consolidation
      LOGGER.info("Phase 3: Setting lifecycle rule for temp cleanup...");
      try {
        storageProvider.ensureLifecycleRule(tempPrefix, 1);
        LOGGER.info("  Lifecycle rule set: {} will auto-expire in 1 day", tempPrefix);
      } catch (IOException e) {
        LOGGER.warn("  Could not set lifecycle rule (temp files will remain): {}", e.getMessage());
      }

      LOGGER.info("Successfully reorganized: {}", displayName);

    } catch (java.sql.SQLException e) {
      String errorMsg =
          String.format("DuckDB batched reorganization failed for '%s': %s", displayName, e.getMessage());
      LOGGER.error(errorMsg, e);
      throw new IOException(errorMsg, e);
    }
  }

  /**
   * Process data year by year when no batch columns are configured.
   * @return number of years successfully processed
   */
  private int processYearBatches(Connection conn, ReorgConfig config, String tempBase)
      throws java.sql.SQLException {
    int startYear = config.getStartYear();
    int endYear = config.getEndYear();
    int processed = 0;

    LOGGER.info("Phase 1: Processing {} years individually (no batch_partition_columns)...",
        (endYear - startYear + 1));

    for (int year = startYear; year <= endYear; year++) {
      String sql;

      if (config.isSourceIsIceberg() && config.getIcebergWarehousePath() != null) {
        // Use iceberg_scan with year filter
        Map<String, String> filters = new LinkedHashMap<String, String>();
        filters.put("year", String.valueOf(year));
        sql = buildReorganizationSql(null, tempBase, config.getPartitionColumns(),
            config.getColumnMappings(), "year_" + year + "_{i}",
            true, config.getIcebergWarehousePath(), config.getSourceTable(), filters);
      } else {
        // Use read_parquet with glob pattern
        String yearSourceGlob = config.getSourcePattern().replace("year=*", "year=" + year);
        String fullYearSourceGlob = storageProvider.resolvePath(baseDirectory, yearSourceGlob);
        sql = buildReorganizationSql(fullYearSourceGlob, tempBase, config.getPartitionColumns(),
            config.getColumnMappings(), "year_" + year + "_{i}");
      }

      LOGGER.debug("Phase 1 SQL (year={}):\n{}", year, sql);
      LOGGER.info("  Starting year {}/{}: {}", year - startYear + 1, endYear - startYear + 1, year);

      try (Statement stmt = conn.createStatement()) {
        long startTime = System.currentTimeMillis();
        stmt.execute(sql);
        long elapsed = System.currentTimeMillis() - startTime;
        LOGGER.info("    Completed year {} in {}ms for {}", year, elapsed, config.getName());
        processed++;
      } catch (java.sql.SQLException e) {
        LOGGER.warn("  Skipped year {} (no data or error): {}", year, e.getMessage());
      }
    }
    return processed;
  }

  /**
   * Process all data from Iceberg source directly to final location (no temp files).
   * Used when no batch_partition_columns are configured for Iceberg sources.
   */
  private void processAllDataDirect(Connection conn, ReorgConfig config, String targetBase)
      throws java.sql.SQLException {
    LOGGER.info("Processing all data from Iceberg source directly to final location...");

    String sql = buildReorganizationSql(null, targetBase, config.getPartitionColumns(),
        config.getColumnMappings(), "data_{i}",
        true, config.getIcebergWarehousePath(), config.getSourceTable(), null);

    LOGGER.debug("Direct write SQL:\n{}", sql);

    try (Statement stmt = conn.createStatement()) {
      long startTime = System.currentTimeMillis();
      stmt.execute(sql);
      long elapsed = System.currentTimeMillis() - startTime;
      LOGGER.info("  Completed direct write in {}ms for {}", elapsed, config.getName());
    }
  }

  /**
   * Process data using configured batch combinations.
   * Supports incremental processing - skips batches whose incremental keys are already processed.
   * @return number of batches successfully processed
   */
  private int processBatchCombinations(Connection conn, ReorgConfig config,
      List<Map<String, String>> batchCombinations, String tempBase) throws java.sql.SQLException {

    // Group batches by incremental key values for tracking
    Map<Map<String, String>, List<Map<String, String>>> batchesByIncrementalKey =
        groupBatchesByIncrementalKey(batchCombinations, config.getIncrementalKeys());

    int totalBatches = batchCombinations.size();
    int skippedGroups = 0;
    int processedBatches = 0;

    if (config.supportsIncremental()) {
      LOGGER.info("Phase 1: Processing {} batch combinations (batch by: {}, incremental by: {})...",
          totalBatches, config.getBatchPartitionColumns(), config.getIncrementalKeys());
    } else {
      LOGGER.info("Phase 1: Processing {} batch combinations (batch by: {})...",
          totalBatches, config.getBatchPartitionColumns());
    }

    // Process batches grouped by incremental key
    for (Map.Entry<Map<String, String>, List<Map<String, String>>> entry : batchesByIncrementalKey.entrySet()) {
      Map<String, String> incrementalKeyValues = entry.getKey();
      List<Map<String, String>> batchesForKey = entry.getValue();

      // Check if this incremental key combination is already processed
      if (config.supportsIncremental() && !incrementalKeyValues.isEmpty()) {
        boolean alreadyProcessed;
        // Use TTL-based check for current year so it gets reprocessed periodically
        if (isCurrentYear(incrementalKeyValues)) {
          long ttlMillis = config.getCurrentYearTtlMillis();
          alreadyProcessed = config.getIncrementalTracker().isProcessedWithTtl(
              config.getName(), config.getSourceTable(), incrementalKeyValues, ttlMillis);
          if (!alreadyProcessed) {
            LOGGER.info("  Current year {} - TTL expired or never processed (TTL: {} days)",
                incrementalKeyValues, config.getCurrentYearTtlDays());
          }
        } else {
          alreadyProcessed = config.getIncrementalTracker().isProcessed(
              config.getName(), config.getSourceTable(), incrementalKeyValues);
        }
        if (alreadyProcessed) {
          LOGGER.info("  Skipping {} batches for incremental key {} (already processed)",
              batchesForKey.size(), incrementalKeyValues);
          skippedGroups++;
          continue;
        }
      }

      // Process all batches for this incremental key
      boolean allSuccessful = true;
      for (Map<String, String> batch : batchesForKey) {
        boolean success =
            processSingleBatch(conn, config, batch, tempBase, processedBatches + 1, totalBatches);
        if (success) {
          processedBatches++;
        } else {
          allSuccessful = false;
        }
      }

      // Mark incremental key as processed if all batches succeeded
      if (config.supportsIncremental() && allSuccessful && !incrementalKeyValues.isEmpty()) {
        config.getIncrementalTracker().markProcessed(
            config.getName(), config.getSourceTable(), incrementalKeyValues, config.getTargetBase());
        LOGGER.info("  Marked incremental key {} as processed", incrementalKeyValues);
      }
    }

    LOGGER.info("  Completed {} batches for {} ({} incremental groups skipped)",
        processedBatches, config.getName(), skippedGroups);
    return processedBatches;
  }

  /**
   * Process a single batch.
   * @return true if batch processed successfully (or had no data), false on error
   */
  private boolean processSingleBatch(Connection conn, ReorgConfig config, Map<String, String> batch,
      String tempBase, int batchNum, int totalBatches) {
    // Build filename pattern from batch values
    StringBuilder filenamePattern = new StringBuilder();
    for (Map.Entry<String, String> entry : batch.entrySet()) {
      String col = entry.getKey();
      String val = entry.getValue();
      if (filenamePattern.length() > 0) {
        filenamePattern.append("_");
      }
      filenamePattern.append(col).append("_").append(val);
    }
    filenamePattern.append("_{i}");

    String sql;

    if (config.isSourceIsIceberg() && config.getIcebergWarehousePath() != null) {
      // Use iceberg_scan with filters from batch
      sql = buildReorganizationSql(null, tempBase, config.getPartitionColumns(),
          config.getColumnMappings(), filenamePattern.toString(),
          true, config.getIcebergWarehousePath(), config.getSourceTable(), batch);
    } else {
      // Use read_parquet with glob pattern (batch values replace wildcards)
      String batchSourceGlob = config.getSourcePattern();
      for (Map.Entry<String, String> entry : batch.entrySet()) {
        String col = entry.getKey();
        String val = entry.getValue();
        batchSourceGlob = batchSourceGlob.replace(col + "=*", col + "=" + val);
      }
      String fullBatchSourceGlob = storageProvider.resolvePath(baseDirectory, batchSourceGlob);
      sql = buildReorganizationSql(fullBatchSourceGlob, tempBase, config.getPartitionColumns(),
          config.getColumnMappings(), filenamePattern.toString());
    }

    LOGGER.debug("Phase 1 SQL (batch={}):\n{}", batch, sql);
    LOGGER.info("  Starting batch {}/{}: {}", batchNum, totalBatches, batch);

    try (Statement stmt = conn.createStatement()) {
      long startTime = System.currentTimeMillis();
      stmt.execute(sql);
      long elapsed = System.currentTimeMillis() - startTime;
      LOGGER.info("    Completed batch {} in {}ms", batch, elapsed);
      return true;
    } catch (java.sql.SQLException e) {
      // Log but continue - some combinations may not have data
      LOGGER.debug("  Skipped batch {} (no data): {}", batch, e.getMessage());
      return true;  // No data is not an error
    }
  }

  /**
   * Groups batches by their incremental key values.
   * For example, with incremental_keys=[year] and batches [{year=2020, geo=STATE}, {year=2020, geo=COUNTY}],
   * returns {{"year":"2020"} -> [{year=2020, geo=STATE}, {year=2020, geo=COUNTY}]}.
   */
  private Map<Map<String, String>, List<Map<String, String>>> groupBatchesByIncrementalKey(
      List<Map<String, String>> batches, List<String> incrementalKeys) {
    Map<Map<String, String>, List<Map<String, String>>> grouped = new LinkedHashMap<>();

    for (Map<String, String> batch : batches) {
      Map<String, String> keyValues = new LinkedHashMap<>();
      if (incrementalKeys != null) {
        for (String key : incrementalKeys) {
          String value = batch.get(key);
          if (value != null) {
            keyValues.put(key, value);
          }
        }
      }

      grouped.computeIfAbsent(keyValues, k -> new ArrayList<>()).add(batch);
    }

    return grouped;
  }

  /**
   * Builds all batch combinations from the configured batch_partition_columns.
   *
   * <p>For example, if batch_partition_columns = [year, geo_fips_set], returns
   * all combinations like [{year=2020, geo_fips_set=STATE}, {year=2020, geo_fips_set=COUNTY}, ...].
   *
   * <p>Special handling for 'year': uses startYear/endYear range instead of querying data.
   * <p>For Iceberg sources, queries distinct values via iceberg_scan() when column not in path.
   */
  private List<Map<String, String>> buildBatchCombinations(Connection conn, ReorgConfig config) {
    List<String> batchPartitionColumns = config.getBatchPartitionColumns();
    if (batchPartitionColumns == null || batchPartitionColumns.isEmpty()) {
      return Collections.emptyList();
    }

    String sourceGlobTemplate = config.getSourcePattern();
    int startYear = config.getStartYear();
    int endYear = config.getEndYear();

    // Build values for each batch column
    List<List<String>> columnValues = new ArrayList<List<String>>();
    List<String> columnNames = new ArrayList<String>();

    for (String col : batchPartitionColumns) {
      List<String> values;
      if ("year".equalsIgnoreCase(col)) {
        // Use configured year range
        values = new ArrayList<String>();
        for (int y = startYear; y <= endYear; y++) {
          values.add(String.valueOf(y));
        }
      } else {
        // Query distinct values from data (supports both Parquet and Iceberg sources)
        values = getDistinctPartitionValues(conn, config, col);
      }

      if (values.isEmpty()) {
        LOGGER.warn("No values found for batch column '{}', skipping batching", col);
        return Collections.emptyList();
      }

      columnNames.add(col);
      columnValues.add(values);
    }

    // Build cartesian product of all column values
    List<Map<String, String>> combinations = new ArrayList<Map<String, String>>();
    buildCombinationsRecursive(columnNames, columnValues, 0,
        new LinkedHashMap<String, String>(), combinations);

    LOGGER.debug("Built {} batch combinations from {} columns",
        combinations.size(), batchPartitionColumns);
    return combinations;
  }

  /**
   * Recursively builds cartesian product of column values.
   */
  private void buildCombinationsRecursive(List<String> columnNames, List<List<String>> columnValues,
      int depth, Map<String, String> current, List<Map<String, String>> result) {
    if (depth == columnNames.size()) {
      result.add(new LinkedHashMap<String, String>(current));
      return;
    }

    String colName = columnNames.get(depth);
    for (String value : columnValues.get(depth)) {
      current.put(colName, value);
      buildCombinationsRecursive(columnNames, columnValues, depth + 1, current, result);
    }
    current.remove(colName);
  }

  /**
   * Gets distinct values for a batch column by listing directories or querying data.
   *
   * <p>For Parquet sources with Hive-style partitioning, extracts values from directory names.
   * For Iceberg sources or columns not in the path pattern, queries distinct values via SQL.
   *
   * @param conn DuckDB connection
   * @param config Reorganization config with source info
   * @param partitionColumn The column to get distinct values for
   * @return List of distinct values, or empty list if not found
   */
  private List<String> getDistinctPartitionValues(Connection conn, ReorgConfig config,
      String partitionColumn) {
    String sourceGlobTemplate = config.getSourcePattern();
    java.util.Set<String> values = new java.util.TreeSet<String>();

    // Build directory pattern to list
    // E.g., "type=income/year=*/geo_fips_set=*/*.parquet" -> need to find geo_fips_set values
    String columnPattern = partitionColumn + "=";

    // Find where this partition column appears in the pattern
    int colIdx = sourceGlobTemplate.indexOf(columnPattern);
    if (colIdx < 0) {
      // Column not in path pattern - for Iceberg sources, query via iceberg_scan
      if (config.isSourceIsIceberg()) {
        LOGGER.debug("Batch column '{}' not in path pattern, querying Iceberg table", partitionColumn);
        return getDistinctValuesFromIceberg(conn, config, partitionColumn);
      }
      LOGGER.warn("Partition column '{}' not found in pattern '{}'", partitionColumn, sourceGlobTemplate);
      return new ArrayList<String>(values);
    }

    // Get the prefix up to but not including this partition level
    String prefix = sourceGlobTemplate.substring(0, colIdx);
    if (prefix.endsWith("/")) {
      prefix = prefix.substring(0, prefix.length() - 1);
    }

    // Check if prefix contains wildcards (e.g., "type=income/year=*")
    // If so, we need to list from a higher level that has no wildcards
    String listPrefix = prefix;
    if (prefix.contains("*")) {
      // Find the last path segment before any wildcard
      int wildcardIdx = prefix.indexOf("*");
      int lastSlashBeforeWildcard = prefix.lastIndexOf("/", wildcardIdx);
      if (lastSlashBeforeWildcard > 0) {
        listPrefix = prefix.substring(0, lastSlashBeforeWildcard);
      } else {
        // Wildcard is in the first segment, list from base
        listPrefix = "";
      }
    }

    String fullPrefix = listPrefix.isEmpty()
        ? baseDirectory
        : storageProvider.resolvePath(baseDirectory, listPrefix);
    LOGGER.debug("Listing directories under '{}' to find {} values (prefix had wildcards: {})",
        fullPrefix, partitionColumn, prefix.contains("*"));

    try {
      // List files/directories under the prefix
      List<StorageProvider.FileEntry> entries =
          storageProvider.listFiles(fullPrefix, true);

      // Extract partition values from paths
      // Look for paths like ".../{partitionColumn}={value}/..."
      java.util.regex.Pattern valuePattern =
          java.util.regex.Pattern.compile(partitionColumn + "=([^/]+)");

      for (StorageProvider.FileEntry entry : entries) {
        java.util.regex.Matcher matcher = valuePattern.matcher(entry.getPath());
        if (matcher.find()) {
          values.add(matcher.group(1));
        }
      }

      LOGGER.debug("Found {} distinct {} values via directory listing: {}",
          values.size(), partitionColumn, values);

    } catch (java.io.IOException e) {
      LOGGER.warn("Failed to list directories for {}: {}", partitionColumn, e.getMessage());
      // Fall back to SQL query if listing fails
      if (config.isSourceIsIceberg()) {
        return getDistinctValuesFromIceberg(conn, config, partitionColumn);
      }
      return getDistinctPartitionValuesViaSql(conn, sourceGlobTemplate, partitionColumn);
    }

    return new ArrayList<String>(values);
  }

  /**
   * Gets distinct values for a column from an Iceberg table via iceberg_scan().
   *
   * <p>Used when batch columns are data columns (not partition columns) in an Iceberg source.
   */
  private List<String> getDistinctValuesFromIceberg(Connection conn, ReorgConfig config,
      String columnName) {
    List<String> values = new ArrayList<String>();

    String icebergPath = config.getIcebergWarehousePath() + "/" + config.getSourceTable();
    String sql = "SELECT DISTINCT \"" + columnName + "\" FROM iceberg_scan("
        + quoteLiteral(icebergPath) + ") WHERE \"" + columnName + "\" IS NOT NULL ORDER BY \""
        + columnName + "\"";

    LOGGER.debug("Getting distinct {} values from Iceberg: {}", columnName, sql);

    try (Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery(sql)) {
      while (rs.next()) {
        String value = rs.getString(1);
        if (value != null) {
          values.add(value);
        }
      }
      LOGGER.info("Found {} distinct {} values from Iceberg table", values.size(), columnName);
    } catch (java.sql.SQLException e) {
      LOGGER.warn("Failed to get distinct {} values from Iceberg: {}", columnName, e.getMessage());
    }

    return values;
  }

  /**
   * Fallback: gets distinct values via SQL query (slower but works if listing fails).
   */
  private List<String> getDistinctPartitionValuesViaSql(Connection conn, String sourceGlobTemplate,
      String partitionColumn) {
    List<String> values = new ArrayList<String>();

    String globPattern = storageProvider.resolvePath(baseDirectory, sourceGlobTemplate);
    String sql = "SELECT DISTINCT " + partitionColumn + " FROM read_parquet("
        + quoteLiteral(globPattern) + ", hive_partitioning=true) ORDER BY " + partitionColumn;

    LOGGER.debug("Getting distinct {} values with SQL (fallback): {}", partitionColumn, sql);

    try (Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery(sql)) {
      while (rs.next()) {
        String value = rs.getString(1);
        if (value != null) {
          values.add(value);
        }
      }
      LOGGER.debug("Found {} distinct {} values via SQL", values.size(), partitionColumn);
    } catch (java.sql.SQLException e) {
      LOGGER.warn("Failed to get distinct {} values via SQL: {}", partitionColumn, e.getMessage());
    }

    return values;
  }

  /**
   * Builds DuckDB SQL for reorganizing data into partitioned files.
   */
  private String buildReorganizationSql(String sourceGlob, String targetBase,
      List<String> partitionColumns, Map<String, String> columnMappings,
      String filenamePattern) {
    return buildReorganizationSql(sourceGlob, targetBase, partitionColumns, columnMappings,
        filenamePattern, false, null, null, null);
  }

  /**
   * Builds DuckDB SQL for reorganizing data into partitioned files.
   * Supports both Parquet (via glob pattern) and Iceberg (via iceberg_scan) sources.
   */
  private String buildReorganizationSql(String sourceGlob, String targetBase,
      List<String> partitionColumns, Map<String, String> columnMappings,
      String filenamePattern, boolean sourceIsIceberg, String icebergTablePath,
      String sourceTable, Map<String, String> filters) {
    StringBuilder sql = new StringBuilder();
    sql.append("COPY (\n");

    // Build SELECT clause with column aliases if mappings exist
    // When mappings exist, wrap in subquery so PARTITION_BY sees aliased columns
    boolean hasColumnMappings = columnMappings != null && !columnMappings.isEmpty();
    StringBuilder selectClause = new StringBuilder("  SELECT *");
    if (hasColumnMappings) {
      for (Map.Entry<String, String> mapping : columnMappings.entrySet()) {
        String targetCol = mapping.getKey();
        String sourceCol = mapping.getValue();
        selectClause.append(", \"").append(sourceCol).append("\" AS ").append(targetCol);
      }
    }

    if (sourceIsIceberg && icebergTablePath != null && sourceTable != null) {
      // Use iceberg_scan for Iceberg source
      String fullIcebergPath = icebergTablePath + "/" + sourceTable;

      // Wrap in subquery when column mappings exist so PARTITION_BY sees aliased columns
      if (hasColumnMappings) {
        sql.append("  SELECT * FROM (\n  ");
      }

      sql.append(selectClause).append(" FROM iceberg_scan(").append(quoteLiteral(fullIcebergPath)).append(")");

      // Add WHERE clause for filters (replaces glob pattern filtering)
      if (filters != null && !filters.isEmpty()) {
        sql.append("\n  WHERE ");
        boolean first = true;
        for (Map.Entry<String, String> filter : filters.entrySet()) {
          if (!first) {
            sql.append(" AND ");
          }
          first = false;
          String col = filter.getKey();
          String val = filter.getValue();
          // Handle numeric values (like year) vs string values
          if (isNumeric(val)) {
            sql.append(col).append(" = ").append(val);
          } else {
            sql.append(col).append(" = ").append(quoteLiteral(val));
          }
        }
      }

      if (hasColumnMappings) {
        sql.append("\n  ) AS _aliased");
      }
      sql.append("\n");
    } else {
      // Use read_parquet for Parquet source
      sql.append(selectClause).append(" FROM read_parquet(");
      sql.append(quoteLiteral(sourceGlob))
         .append(", hive_partitioning=true, union_by_name=true)\n");
    }

    sql.append(") TO ").append(quoteLiteral(targetBase));

    // Add format and partition options
    sql.append(" (FORMAT PARQUET");
    if (partitionColumns != null && !partitionColumns.isEmpty()) {
      sql.append(", PARTITION_BY (");
      StringBuilder colList = new StringBuilder();
      for (String col : partitionColumns) {
        if (colList.length() > 0) {
          colList.append(", ");
        }
        colList.append(col);
      }
      sql.append(colList);
      sql.append(")");
    }
    if (filenamePattern != null && !filenamePattern.isEmpty()) {
      sql.append(", FILENAME_PATTERN ").append(quoteLiteral(filenamePattern));
    }
    sql.append(", OVERWRITE_OR_IGNORE");
    sql.append(");");

    return sql.toString();
  }

  /**
   * Check if a string value is numeric.
   */
  private boolean isNumeric(String str) {
    if (str == null || str.isEmpty()) {
      return false;
    }
    try {
      Long.parseLong(str);
      return true;
    } catch (NumberFormatException e) {
      return false;
    }
  }

  /**
   * Builds SQL for consolidation phase - reads all temp files and writes to final location.
   */
  private String buildConsolidationSql(String tempBase, String finalBase,
      List<String> partitionColumns) {
    StringBuilder sql = new StringBuilder();
    sql.append("COPY (\n");
    sql.append("  SELECT * FROM read_parquet(").append(quoteLiteral(tempBase + "/**/*.parquet"))
       .append(", hive_partitioning=true, union_by_name=true)\n");
    sql.append(") TO ").append(quoteLiteral(finalBase));

    sql.append(" (FORMAT PARQUET");
    if (partitionColumns != null && !partitionColumns.isEmpty()) {
      sql.append(", PARTITION_BY (");
      StringBuilder colList = new StringBuilder();
      for (String col : partitionColumns) {
        if (colList.length() > 0) {
          colList.append(", ");
        }
        colList.append(col);
      }
      sql.append(colList);
      sql.append("), OVERWRITE_OR_IGNORE");
    }
    sql.append(");");

    return sql.toString();
  }

  /**
   * Creates a DuckDB connection with S3 access configured.
   */
  private Connection getDuckDBConnection() throws java.sql.SQLException {
    Connection conn = DriverManager.getConnection("jdbc:duckdb:");
    loadExtensions(conn);
    configureS3Access(conn);
    // Limit memory to avoid OOM on memory-constrained systems
    try (Statement stmt = conn.createStatement()) {
      stmt.execute("SET memory_limit='" + DUCKDB_MEMORY_LIMIT + "'");
      stmt.execute("SET temp_directory='" + baseDirectory + "/.duckdb_tmp'");
    }
    return conn;
  }

  /**
   * Loads required DuckDB extensions.
   */
  private void loadExtensions(Connection conn) {
    try (Statement stmt = conn.createStatement()) {
      stmt.execute("INSTALL parquet");
      stmt.execute("LOAD parquet");
    } catch (java.sql.SQLException e) {
      LOGGER.debug("Parquet extension already loaded or built-in");
    }
  }

  /**
   * Configures S3 access for DuckDB connection if storage provider is S3-based.
   */
  private void configureS3Access(Connection conn) {
    try {
      conn.createStatement().execute("INSTALL httpfs");
      conn.createStatement().execute("LOAD httpfs");

      // Get S3 config from storage provider
      Map<String, String> s3Config = storageProvider.getS3Config();
      if (s3Config != null && !s3Config.isEmpty()) {
        String accessKey = s3Config.get("accessKeyId");
        String secretKey = s3Config.get("secretAccessKey");
        String endpoint = s3Config.get("endpoint");
        String region = s3Config.get("region");

        if (accessKey != null && secretKey != null) {
          conn.createStatement().execute("SET s3_access_key_id='" + accessKey + "'");
          conn.createStatement().execute("SET s3_secret_access_key='" + secretKey + "'");
        }
        if (endpoint != null) {
          conn.createStatement().execute("SET s3_endpoint='" + endpoint + "'");
          conn.createStatement().execute("SET s3_url_style='path'");
        }
        if (region != null) {
          conn.createStatement().execute("SET s3_region='" + region + "'");
        } else {
          conn.createStatement().execute("SET s3_region='auto'");
        }
        LOGGER.debug("Configured DuckDB S3 access from storage provider");
      }
    } catch (java.sql.SQLException e) {
      LOGGER.debug("S3 configuration skipped: {}", e.getMessage());
    }
  }

  /**
   * Checks if the incremental key values contain the current year.
   * Used to apply TTL-based processing for the current year's data.
   */
  private boolean isCurrentYear(Map<String, String> incrementalKeyValues) {
    if (incrementalKeyValues == null || incrementalKeyValues.isEmpty()) {
      return false;
    }
    int currentYear = java.time.Year.now().getValue();
    String currentYearStr = String.valueOf(currentYear);
    // Check if any key named "year" has the current year value
    String yearValue = incrementalKeyValues.get("year");
    return currentYearStr.equals(yearValue);
  }

  /**
   * Quotes a string literal for SQL.
   */
  private static String quoteLiteral(String literal) {
    return "'" + literal.replace("'", "''") + "'";
  }

  /**
   * Creates a ParquetReorganizer for a storage provider.
   */
  public static ParquetReorganizer create(StorageProvider storageProvider, String baseDirectory) {
    return new ParquetReorganizer(storageProvider, baseDirectory);
  }
}
