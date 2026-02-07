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
package org.apache.calcite.adapter.file.iceberg;

import org.apache.calcite.adapter.file.partition.IncrementalTracker;
import org.apache.calcite.adapter.file.partition.PartitionedTableConfig;
import org.apache.calcite.adapter.file.storage.StorageProvider;

import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.io.CloseableIterable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;

/**
 * Materializes data to Iceberg tables with batched processing to prevent OOM.
 *
 * <p>Implements the DuckDB + Iceberg hybrid architecture:
 * <ol>
 *   <li>DuckDB transforms source data (JSON/Parquet) to partitioned Parquet in staging</li>
 *   <li>Files are moved from staging to Iceberg data location</li>
 *   <li>Iceberg commits the files atomically</li>
 * </ol>
 *
 * <p>Key features:
 * <ul>
 *   <li>Batched processing via batch_partition_columns to prevent OOM</li>
 *   <li>Incremental processing via incremental_keys to skip already-processed batches</li>
 *   <li>Error handling with retry and continue-on-failure</li>
 *   <li>Staging directory strategy for efficient file moves</li>
 * </ul>
 */
public class IcebergMaterializer {
  private static final Logger LOGGER = LoggerFactory.getLogger(IcebergMaterializer.class);

  private static final int DEFAULT_MAX_RETRIES = 3;
  private static final long DEFAULT_RETRY_DELAY_MS = 1000;
  private static final int DEFAULT_THREADS = 2;

  /** DuckDB memory limit - from DUCKDB_MEMORY_LIMIT env var, default 4GB. */
  private static final String DUCKDB_MEMORY_LIMIT =
      System.getenv("DUCKDB_MEMORY_LIMIT") != null
          ? System.getenv("DUCKDB_MEMORY_LIMIT") : "4GB";

  private final String warehousePath;
  private final Map<String, Object> catalogConfig;
  private final StorageProvider storageProvider;
  private final IncrementalTracker incrementalTracker;
  private final int maxRetries;
  private final long retryDelayMs;

  /**
   * Creates a materializer with default retry settings.
   *
   * @param warehousePath Path to the Iceberg warehouse
   * @param storageProvider Storage provider for file operations
   * @param incrementalTracker Tracker for incremental processing
   */
  public IcebergMaterializer(String warehousePath, StorageProvider storageProvider,
      IncrementalTracker incrementalTracker) {
    this(warehousePath, storageProvider, incrementalTracker, DEFAULT_MAX_RETRIES, DEFAULT_RETRY_DELAY_MS);
  }

  /**
   * Creates a materializer with custom retry settings.
   *
   * @param warehousePath Path to the Iceberg warehouse
   * @param storageProvider Storage provider for file operations
   * @param incrementalTracker Tracker for incremental processing
   * @param maxRetries Maximum retry attempts per batch
   * @param retryDelayMs Base delay between retries in milliseconds
   */
  public IcebergMaterializer(String warehousePath, StorageProvider storageProvider,
      IncrementalTracker incrementalTracker, int maxRetries, long retryDelayMs) {
    this.warehousePath = warehousePath;
    this.storageProvider = storageProvider;
    this.incrementalTracker = incrementalTracker != null ? incrementalTracker : IncrementalTracker.NOOP;
    this.maxRetries = maxRetries;
    this.retryDelayMs = retryDelayMs;

    // Build catalog config
    this.catalogConfig = new HashMap<String, Object>();
    this.catalogConfig.put("catalog", "hadoop");
    this.catalogConfig.put("warehousePath", warehousePath);

    // Add S3 credentials from storage provider for Hadoop/Iceberg
    if (storageProvider != null) {
      Map<String, String> s3Config = storageProvider.getS3Config();
      if (s3Config != null && !s3Config.isEmpty()) {
        Map<String, String> hadoopConfig = new HashMap<String, String>();
        if (s3Config.containsKey("accessKeyId")) {
          hadoopConfig.put("fs.s3a.access.key", s3Config.get("accessKeyId"));
        }
        if (s3Config.containsKey("secretAccessKey")) {
          hadoopConfig.put("fs.s3a.secret.key", s3Config.get("secretAccessKey"));
        }
        if (s3Config.containsKey("endpoint")) {
          hadoopConfig.put("fs.s3a.endpoint", s3Config.get("endpoint"));
          hadoopConfig.put("fs.s3a.path.style.access", "true");
        }
        if (!hadoopConfig.isEmpty()) {
          this.catalogConfig.put("hadoopConfig", hadoopConfig);
          LOGGER.debug("Configured S3 credentials for Iceberg from StorageProvider");
        }
      }
    }
  }

  /**
   * Configuration for a materialization job.
   */
  public static class MaterializationConfig {
    private final String sourcePattern;
    private final SourceFormat sourceFormat;
    private final String targetTableId;
    private final String sourceTableName;
    private final List<PartitionedTableConfig.ColumnDefinition> partitionColumns;
    private final List<IcebergCatalogManager.ColumnDef> tableColumns;
    private final List<String> batchPartitionColumns;
    private final List<String> incrementalKeys;
    private final int startYear;
    private final int endYear;
    private final int threads;
    private final String description;
    private final Map<String, String> computedColumns;
    private final int rowBatchSize;
    private final String rowFilter;  // Optional WHERE clause filter (e.g., "cik IN ('0001', '0002')")

    private MaterializationConfig(Builder builder) {
      this.sourcePattern = builder.sourcePattern;
      this.sourceFormat = builder.sourceFormat != null ? builder.sourceFormat : SourceFormat.PARQUET;
      this.targetTableId = builder.targetTableId;
      this.sourceTableName = builder.sourceTableName;
      this.partitionColumns = builder.partitionColumns != null
          ? builder.partitionColumns : Collections.<PartitionedTableConfig.ColumnDefinition>emptyList();
      this.tableColumns = builder.tableColumns != null
          ? builder.tableColumns : Collections.<IcebergCatalogManager.ColumnDef>emptyList();
      this.batchPartitionColumns = builder.batchPartitionColumns != null
          ? builder.batchPartitionColumns : Collections.<String>emptyList();
      this.incrementalKeys = builder.incrementalKeys != null
          ? builder.incrementalKeys : Collections.<String>emptyList();
      this.startYear = builder.startYear;
      this.endYear = builder.endYear;
      this.threads = builder.threads > 0 ? builder.threads : DEFAULT_THREADS;
      this.description = builder.description;
      this.computedColumns = builder.computedColumns != null
          ? builder.computedColumns : Collections.<String, String>emptyMap();
      this.rowBatchSize = builder.rowBatchSize;
      this.rowFilter = builder.rowFilter;
    }

    public String getSourcePattern() {
      return sourcePattern;
    }

    public SourceFormat getSourceFormat() {
      return sourceFormat;
    }

    public String getTargetTableId() {
      return targetTableId;
    }

    public String getSourceTableName() {
      return sourceTableName;
    }

    public List<PartitionedTableConfig.ColumnDefinition> getPartitionColumns() {
      return partitionColumns;
    }

    /**
     * Returns the full table column definitions for Iceberg table creation.
     * If empty, the schema will be inferred from partition columns only (legacy behavior).
     */
    public List<IcebergCatalogManager.ColumnDef> getTableColumns() {
      return tableColumns;
    }

    public List<String> getPartitionColumnNames() {
      List<String> names = new ArrayList<String>();
      for (PartitionedTableConfig.ColumnDefinition col : partitionColumns) {
        names.add(col.getName());
      }
      return names;
    }

    public List<String> getBatchPartitionColumns() {
      return batchPartitionColumns;
    }

    public List<String> getIncrementalKeys() {
      return incrementalKeys;
    }

    public int getStartYear() {
      return startYear;
    }

    public int getEndYear() {
      return endYear;
    }

    public int getThreads() {
      return threads;
    }

    public String getDescription() {
      return description != null ? description : targetTableId;
    }

    /**
     * Returns computed columns map (column name -> SQL expression).
     * Computed columns are evaluated by DuckDB during materialization.
     */
    public Map<String, String> getComputedColumns() {
      return computedColumns;
    }

    /**
     * Returns the row batch size for expensive computed columns.
     * When > 0, data is processed in batches of this size to prevent OOM.
     * Use for expensive operations like ML embeddings.
     */
    public int getRowBatchSize() {
      return rowBatchSize;
    }

    /**
     * Returns the optional row filter (WHERE clause) to apply during materialization.
     * This is used to filter source data, e.g., by CIK list.
     */
    public String getRowFilter() {
      return rowFilter;
    }

    public boolean supportsIncremental() {
      return !incrementalKeys.isEmpty();
    }

    public static Builder builder() {
      return new Builder();
    }

    /**
     * Builder for MaterializationConfig.
     */
    public static class Builder {
      private String sourcePattern;
      private SourceFormat sourceFormat;
      private String targetTableId;
      private String sourceTableName;
      private List<PartitionedTableConfig.ColumnDefinition> partitionColumns;
      private List<IcebergCatalogManager.ColumnDef> tableColumns;
      private List<String> batchPartitionColumns;
      private List<String> incrementalKeys;
      private int startYear;
      private int endYear;
      private int threads;
      private String description;
      private Map<String, String> computedColumns;
      private int rowBatchSize;
      private String rowFilter;

      public Builder sourcePattern(String sourcePattern) {
        this.sourcePattern = sourcePattern;
        return this;
      }

      public Builder sourceFormat(SourceFormat sourceFormat) {
        this.sourceFormat = sourceFormat;
        return this;
      }

      public Builder targetTableId(String targetTableId) {
        this.targetTableId = targetTableId;
        return this;
      }

      public Builder sourceTableName(String sourceTableName) {
        this.sourceTableName = sourceTableName;
        return this;
      }

      public Builder partitionColumns(List<PartitionedTableConfig.ColumnDefinition> partitionColumns) {
        this.partitionColumns = partitionColumns;
        return this;
      }

      /**
       * Sets the full table column definitions for Iceberg table creation.
       * This should include ALL columns from the YAML schema, including data columns.
       * If not set, only partition columns will be used (legacy behavior).
       */
      public Builder tableColumns(List<IcebergCatalogManager.ColumnDef> tableColumns) {
        this.tableColumns = tableColumns;
        return this;
      }

      public Builder batchPartitionColumns(List<String> batchPartitionColumns) {
        this.batchPartitionColumns = batchPartitionColumns;
        return this;
      }

      public Builder incrementalKeys(List<String> incrementalKeys) {
        this.incrementalKeys = incrementalKeys;
        return this;
      }

      public Builder yearRange(int startYear, int endYear) {
        this.startYear = startYear;
        this.endYear = endYear;
        return this;
      }

      public Builder threads(int threads) {
        this.threads = threads;
        return this;
      }

      public Builder description(String description) {
        this.description = description;
        return this;
      }

      /**
       * Sets computed columns (column name -> SQL expression).
       * These columns are evaluated by DuckDB during materialization.
       * For example: {"embedding": "embed_jina(text)::FLOAT[768]"}
       */
      public Builder computedColumns(Map<String, String> computedColumns) {
        this.computedColumns = computedColumns;
        return this;
      }

      /**
       * Sets the row batch size for expensive computed columns.
       * When > 0, data is processed in batches of this size to prevent OOM.
       * Use for expensive operations like ML embeddings (e.g., 30 rows at a time).
       */
      public Builder rowBatchSize(int rowBatchSize) {
        this.rowBatchSize = rowBatchSize;
        return this;
      }

      /**
       * Sets an optional row filter (WHERE clause) to apply during materialization.
       * Use to filter source data, e.g., "cik IN ('0001', '0002')".
       */
      public Builder rowFilter(String rowFilter) {
        this.rowFilter = rowFilter;
        return this;
      }

      public MaterializationConfig build() {
        if (sourcePattern == null || sourcePattern.isEmpty()) {
          throw new IllegalArgumentException("sourcePattern is required");
        }
        if (targetTableId == null || targetTableId.isEmpty()) {
          throw new IllegalArgumentException("targetTableId is required");
        }
        return new MaterializationConfig(this);
      }
    }
  }

  /**
   * Source data format.
   */
  public enum SourceFormat {
    JSON,
    PARQUET
  }

  /**
   * Result of a materialization operation.
   */
  public static class MaterializationResult {
    private final String tableId;
    private final int successCount;
    private final int failedCount;
    private final int skippedCount;
    private final long durationMs;
    private final boolean tableRecreated;

    public MaterializationResult(String tableId, int successCount, int failedCount,
        int skippedCount, long durationMs) {
      this(tableId, successCount, failedCount, skippedCount, durationMs, false);
    }

    public MaterializationResult(String tableId, int successCount, int failedCount,
        int skippedCount, long durationMs, boolean tableRecreated) {
      this.tableId = tableId;
      this.successCount = successCount;
      this.failedCount = failedCount;
      this.skippedCount = skippedCount;
      this.durationMs = durationMs;
      this.tableRecreated = tableRecreated;
    }

    public String getTableId() {
      return tableId;
    }

    public int getSuccessCount() {
      return successCount;
    }

    public int getFailedCount() {
      return failedCount;
    }

    public int getSkippedCount() {
      return skippedCount;
    }

    public long getDurationMs() {
      return durationMs;
    }

    public boolean isFullySuccessful() {
      return failedCount == 0;
    }

    /**
     * Returns true if the Iceberg table was recreated due to schema changes.
     * When this is true, DuckDB views need to be recreated to reflect the new schema.
     */
    public boolean isTableRecreated() {
      return tableRecreated;
    }

    @Override public String toString() {
      return String.format("MaterializationResult{table=%s, success=%d, failed=%d, skipped=%d, duration=%dms, recreated=%s}",
          tableId, successCount, failedCount, skippedCount, durationMs, tableRecreated);
    }
  }

  /**
   * Materializes data according to the configuration.
   *
   * @param config The materialization configuration
   * @return Result containing success/failure counts
   * @throws IOException if materialization fails critically
   */
  public MaterializationResult materialize(MaterializationConfig config) throws IOException {
    long startTime = System.currentTimeMillis();
    LOGGER.info("Starting materialization for '{}' -> '{}'",
        config.getDescription(), config.getTargetTableId());

    // Compute current source file watermark (0 if watermarking disabled)
    long currentSourceWatermark = 0;
    boolean enableSourceWatermark = isSourceWatermarkEnabled(config);
    if (enableSourceWatermark) {
      currentSourceWatermark =
          getSourceFileWatermark(config.getSourcePattern(), config.getSourceFormat());
      LOGGER.debug("Current source file watermark: {}", currentSourceWatermark);
    }

    // Fast-path: check if source files have been modified since last run
    if (enableSourceWatermark && currentSourceWatermark > 0) {
      IncrementalTracker.CachedCompletion cached =
          incrementalTracker.getCachedCompletion(config.getTargetTableId());
      if (cached != null && cached.sourceFileWatermark > 0
          && !cached.isSourceFilesModified(currentSourceWatermark)) {
        long durationMs = System.currentTimeMillis() - startTime;
        LOGGER.info("Skipping materialization for '{}' - no source file changes since {} (watermark={})",
            config.getTargetTableId(),
            new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(
                new java.util.Date(cached.completedAt)),
            cached.sourceFileWatermark);
        return new MaterializationResult(config.getTargetTableId(), 0, 0, 1, durationMs);
      }
    }

    // Ensure Iceberg table exists
    TableSetupResult setupResult = ensureTableExists(config);
    Table table = setupResult.table;
    boolean tableWasRecreated = setupResult.wasRecreated;
    if (tableWasRecreated) {
      LOGGER.info("Iceberg table '{}' was recreated due to schema changes - DuckDB views need refresh",
          config.getTargetTableId());
    }
    IcebergTableWriter writer = new IcebergTableWriter(table, storageProvider);

    // Build batch combinations
    List<Map<String, String>> batches = buildBatchCombinations(config);
    if (batches.isEmpty()) {
      // No batching - process all at once
      batches = new ArrayList<Map<String, String>>();
      batches.add(Collections.<String, String>emptyMap());
    }

    // Group batches by incremental key for tracking
    Map<Map<String, String>, List<Map<String, String>>> batchesByIncrementalKey =
        groupBatchesByIncrementalKey(batches, config.getIncrementalKeys());

    int successCount = 0;
    int failedCount = 0;
    int skippedCount = 0;

    // Process batches
    for (Map.Entry<Map<String, String>, List<Map<String, String>>> entry : batchesByIncrementalKey.entrySet()) {
      Map<String, String> incrementalKeyValues = entry.getKey();
      List<Map<String, String>> batchesForKey = entry.getValue();

      // Check if already processed (via tracker or self-healing)
      if (config.supportsIncremental() && !incrementalKeyValues.isEmpty()) {
        if (incrementalTracker.isProcessed(config.getTargetTableId(),
            config.getSourceTableName(), incrementalKeyValues)) {
          LOGGER.info("Skipping {} batches for incremental key {} (already processed)",
              batchesForKey.size(), incrementalKeyValues);
          skippedCount += batchesForKey.size();
          continue;
        }

        // Self-healing: check if partition already has data in Iceberg table
        if (partitionHasData(table, incrementalKeyValues)) {
          // Mark as processed and skip
          incrementalTracker.markProcessed(config.getTargetTableId(),
              config.getSourceTableName(), incrementalKeyValues, config.getTargetTableId());
          LOGGER.info("Self-healing: partition {} already has data, marking processed and skipping {} batches",
              incrementalKeyValues, batchesForKey.size());
          skippedCount += batchesForKey.size();
          continue;
        }
      }

      // Process all batches for this incremental key
      boolean allSuccessful = true;
      for (Map<String, String> batch : batchesForKey) {
        boolean success = processBatchWithRetry(config, table, batch);
        if (success) {
          successCount++;
        } else {
          failedCount++;
          allSuccessful = false;
        }
      }

      // Mark incremental key as processed if all batches succeeded
      if (config.supportsIncremental() && allSuccessful && !incrementalKeyValues.isEmpty()) {
        incrementalTracker.markProcessed(config.getTargetTableId(),
            config.getSourceTableName(), incrementalKeyValues, config.getTargetTableId());
        LOGGER.info("Marked incremental key {} as processed", incrementalKeyValues);
      }
    }

    long durationMs = System.currentTimeMillis() - startTime;
    MaterializationResult result =
        new MaterializationResult(config.getTargetTableId(), successCount, failedCount, skippedCount,
            durationMs, tableWasRecreated);

    LOGGER.info("Materialization complete: {}", result);

    // Run maintenance and record completion if we actually wrote data
    if (result.isFullySuccessful() && successCount > 0) {
      writer.runMaintenance(7, 1);

      // Always mark table complete (with watermark if available, without otherwise)
      incrementalTracker.markTableCompleteWithSourceWatermark(
          config.getTargetTableId(),
          "auto", // config hash - use constant for tracking
          IncrementalTracker.computeDimensionSignature(batches),
          successCount,
          currentSourceWatermark); // 0 for S3, actual watermark for local
      if (currentSourceWatermark > 0) {
        LOGGER.info("Recorded source file watermark {} for '{}'",
            currentSourceWatermark, config.getTargetTableId());
      } else {
        LOGGER.info("Marked table '{}' complete ({} rows)", config.getTargetTableId(), successCount);
      }
    }

    return result;
  }

  /**
   * Processes a single batch with retry logic.
   *
   * @param config The materialization config
   * @param table The Iceberg table
   * @param batch The batch key values
   * @return true if successful
   */
  private boolean processBatchWithRetry(MaterializationConfig config, Table table,
      Map<String, String> batch) {
    int attempts = 0;

    while (attempts < maxRetries) {
      attempts++;
      try {
        processBatch(config, table, batch);
        return true;
      } catch (CommitFailedException e) {
        // Another writer committed - assume idempotent, skip
        LOGGER.warn("Batch {} already committed by another writer, skipping", batch);
        return true;
      } catch (Exception e) {
        String message = e.getMessage();

        // Check for "No files found" - this means source data doesn't exist for this table/batch
        // This is expected when running a 10-K job but the table needs Form 4 or 8-K data
        if (message != null && message.contains("No files found")) {
          LOGGER.debug("Batch {} has no source files, skipping (table may require different filing type)", batch);
          return true;  // Treat as success - no data to process is OK
        }

        LOGGER.warn("Batch {} failed (attempt {}/{}): {}",
            batch, attempts, maxRetries, message);

        if (attempts < maxRetries) {
          try {
            Thread.sleep(retryDelayMs * attempts); // Exponential backoff
          } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            return false;
          }
        }
      }
    }

    LOGGER.error("Batch {} failed after {} attempts, skipping", batch, maxRetries);
    return false;
  }

  /**
   * Processes a single batch: DuckDB SELECT -> Iceberg native parquet writer.
   *
   * <p>Uses Iceberg's native Parquet writer (via IcebergTableWriter.writeRecords)
   * to embed proper field IDs in the parquet schema. This is required for
   * DuckDB's iceberg_scan extension to correctly map columns.
   *
   * <p>When rowBatchSize > 0, uses row-level batching to prevent OOM for large tables.
   */
  private void processBatch(MaterializationConfig config, Table table,
      Map<String, String> batch) throws SQLException, IOException {
    LOGGER.info("Processing batch: {}", batch.isEmpty() ? "(all)" : batch);

    try (Connection conn = getDuckDBConnection(config.getThreads())) {
      // Build source pattern with batch filters applied
      String sourcePattern = config.getSourcePattern();
      for (Map.Entry<String, String> entry : batch.entrySet()) {
        sourcePattern =
            sourcePattern.replace(entry.getKey() + "=*", entry.getKey() + "=" + entry.getValue());
      }

      // Build partition values for Iceberg writer
      Map<String, String> partitionValues = new HashMap<String, String>();
      for (Map.Entry<String, String> entry : batch.entrySet()) {
        if (config.getPartitionColumnNames().contains(entry.getKey())) {
          partitionValues.put(entry.getKey(), entry.getValue());
        }
      }

      // Build typed partition filter for commit
      Map<String, Object> typedPartitionFilter = null;
      if (!partitionValues.isEmpty()) {
        typedPartitionFilter = new HashMap<String, Object>();
        for (Map.Entry<String, String> entry : partitionValues.entrySet()) {
          String colName = entry.getKey();
          String strValue = entry.getValue();
          Object typedValue =
              coerceValue(strValue, findColumnType(config.getPartitionColumns(), colName));
          typedPartitionFilter.put(colName, typedValue);
        }
      }

      // Use row batching for large tables to prevent OOM
      int rowBatchSize = config.getRowBatchSize();
      if (rowBatchSize > 0) {
        processWithRowBatchingToIceberg(config, conn, table, sourcePattern, batch,
            partitionValues, typedPartitionFilter, rowBatchSize);
      } else {
        processAllRowsToIceberg(config, conn, table, sourcePattern, batch,
            partitionValues, typedPartitionFilter);
      }
    }
  }

  /**
   * Processes all rows at once (original behavior for small tables).
   */
  private void processAllRowsToIceberg(MaterializationConfig config, Connection conn, Table table,
      String sourcePattern, Map<String, String> batch, Map<String, String> partitionValues,
      Map<String, Object> typedPartitionFilter) throws SQLException, IOException {

    String sql = buildSelectSql(config, sourcePattern, batch);
    LOGGER.debug("Executing DuckDB SELECT: {}", sql);

    long startTime = System.currentTimeMillis();

    // Fetch all data into memory
    List<Map<String, Object>> rows = fetchRows(conn, sql);

    long fetchElapsed = System.currentTimeMillis() - startTime;
    LOGGER.info("DuckDB SELECT completed: {} rows in {}ms", rows.size(), fetchElapsed);

    if (rows.isEmpty()) {
      LOGGER.debug("No rows to write for batch: {}", batch);
      return;
    }

    // Write using Iceberg's native Parquet writer
    IcebergTableWriter writer = new IcebergTableWriter(table, storageProvider);
    long writeStart = System.currentTimeMillis();
    org.apache.iceberg.DataFile dataFile = writer.writeRecords(rows, partitionValues);
    long writeElapsed = System.currentTimeMillis() - writeStart;

    if (dataFile != null) {
      writer.commitDataFiles(Collections.singletonList(dataFile), typedPartitionFilter);
      LOGGER.info("Wrote and committed {} rows to Iceberg in {}ms", rows.size(), writeElapsed);
    }
  }

  /**
   * Processes data in row batches to prevent OOM for large tables.
   * Uses streaming iteration instead of COUNT to avoid memory-intensive scans.
   */
  private void processWithRowBatchingToIceberg(MaterializationConfig config, Connection conn,
      Table table, String sourcePattern, Map<String, String> batch,
      Map<String, String> partitionValues, Map<String, Object> typedPartitionFilter,
      int rowBatchSize) throws SQLException, IOException {

    LOGGER.info("Row batching enabled: processing in batches of {} for {}", rowBatchSize, batch);

    // Set DuckDB memory limit to prevent OOM
    try (Statement memStmt = conn.createStatement()) {
      memStmt.execute("SET memory_limit='2GB'");
      memStmt.execute("SET temp_directory='/tmp/duckdb_temp'");
    }

    IcebergTableWriter writer = new IcebergTableWriter(table, storageProvider);
    List<org.apache.iceberg.DataFile> dataFiles = new ArrayList<org.apache.iceberg.DataFile>();

    long processedRows = 0;
    int batchNum = 0;
    int commitInterval = 10; // Commit every 10 batches to prevent OOM
    long totalStartTime = System.currentTimeMillis();

    // Iterate with LIMIT/OFFSET until no more rows (avoids expensive COUNT)
    while (true) {
      batchNum++;

      // Build SELECT with LIMIT/OFFSET
      String sql = buildSelectSqlWithPaging(config, sourcePattern, batch, rowBatchSize, (int) processedRows);
      LOGGER.info("Row batch {}: fetching rows {} to {}", batchNum, processedRows, processedRows + rowBatchSize);

      long batchStart = System.currentTimeMillis();

      // Fetch this batch of rows
      List<Map<String, Object>> rows = fetchRows(conn, sql);

      if (rows.isEmpty()) {
        LOGGER.debug("No more rows at offset {}", processedRows);
        break;
      }

      // Write batch to Iceberg
      org.apache.iceberg.DataFile dataFile = writer.writeRecords(rows, partitionValues);
      if (dataFile != null) {
        dataFiles.add(dataFile);
      }

      int rowCount = rows.size();
      long batchElapsed = System.currentTimeMillis() - batchStart;
      LOGGER.info("Row batch {} completed: {} rows in {}ms", batchNum, rowCount, batchElapsed);

      processedRows += rowCount;

      // Clear rows for GC
      rows.clear();
      rows = null; // Help GC

      // Commit incrementally every N batches to prevent OOM from accumulating DataFiles
      if (dataFiles.size() >= commitInterval) {
        writer.commitDataFiles(dataFiles, typedPartitionFilter);
        LOGGER.info("Incremental commit: {} files committed at batch {}", dataFiles.size(), batchNum);
        dataFiles.clear();
        dataFiles = new ArrayList<org.apache.iceberg.DataFile>(); // Fresh list for GC
      }

      // If we got fewer rows than requested, we're done
      if (rowCount < rowBatchSize) {
        break;
      }
    }

    // Commit any remaining data files
    if (!dataFiles.isEmpty()) {
      writer.commitDataFiles(dataFiles, typedPartitionFilter);
      LOGGER.info("Final commit: {} files committed", dataFiles.size());
    }

    long totalElapsed = System.currentTimeMillis() - totalStartTime;
    LOGGER.info("Row batching completed: {} batches, {} rows in {}ms",
        batchNum, processedRows, totalElapsed);
  }

  /**
   * Fetches rows from a SQL query into a list of maps.
   */
  private List<Map<String, Object>> fetchRows(Connection conn, String sql) throws SQLException {
    List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
    try (Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery(sql)) {
      java.sql.ResultSetMetaData meta = rs.getMetaData();
      int colCount = meta.getColumnCount();
      while (rs.next()) {
        Map<String, Object> row = new HashMap<String, Object>();
        for (int i = 1; i <= colCount; i++) {
          String colName = meta.getColumnName(i);
          Object value = rs.getObject(i);
          if (value != null) {
            row.put(colName, value);
          }
        }
        rows.add(row);
      }
    }
    return rows;
  }

  /**
   * Builds a SELECT SQL with LIMIT/OFFSET for row batching.
   */
  private String buildSelectSqlWithPaging(MaterializationConfig config, String sourcePattern,
      Map<String, String> batch, int limit, int offset) {
    StringBuilder sql = new StringBuilder();

    // Build SELECT clause
    Map<String, String> computedCols = config.getComputedColumns();
    if (computedCols == null || computedCols.isEmpty()) {
      sql.append("SELECT * FROM ");
    } else {
      sql.append("SELECT *, ");
      boolean first = true;
      for (Map.Entry<String, String> entry : computedCols.entrySet()) {
        if (!first) {
          sql.append(", ");
        }
        sql.append(entry.getValue()).append(" AS ").append(entry.getKey());
        first = false;
      }
      sql.append(" FROM ");
    }

    // Use appropriate reader
    if (config.getSourceFormat() == SourceFormat.JSON) {
      sql.append("read_json('").append(sourcePattern).append("', union_by_name=true)");
    } else {
      sql.append("read_parquet('").append(sourcePattern).append("', hive_partitioning=true, union_by_name=true)");
    }

    // Add WHERE clause if row filter is specified (e.g., to filter by CIK)
    if (config.getRowFilter() != null && !config.getRowFilter().isEmpty()) {
      sql.append(" WHERE ").append(config.getRowFilter());
    }

    // Add LIMIT/OFFSET
    sql.append(" LIMIT ").append(limit);
    if (offset > 0) {
      sql.append(" OFFSET ").append(offset);
    }

    return sql.toString();
  }

  /**
   * Builds a SELECT SQL statement (not COPY TO) for fetching data into memory.
   */
  private String buildSelectSql(MaterializationConfig config, String sourcePattern,
      Map<String, String> batch) {
    StringBuilder sql = new StringBuilder();

    // Build SELECT clause - include computed columns if present
    Map<String, String> computedCols = config.getComputedColumns();
    if (computedCols == null || computedCols.isEmpty()) {
      sql.append("SELECT * FROM ");
    } else {
      // SELECT * plus computed column expressions
      sql.append("SELECT *");
      for (Map.Entry<String, String> entry : computedCols.entrySet()) {
        sql.append(", ").append(entry.getValue()).append(" AS ").append(entry.getKey());
      }
      sql.append(" FROM ");
    }

    // Use appropriate reader based on source format
    if (config.getSourceFormat() == SourceFormat.JSON) {
      sql.append("read_json('").append(sourcePattern).append("', union_by_name=true)");
    } else {
      sql.append("read_parquet('").append(sourcePattern).append("', hive_partitioning=true, union_by_name=true)");
    }

    // Add WHERE clause if row filter is specified (e.g., to filter by CIK)
    if (config.getRowFilter() != null && !config.getRowFilter().isEmpty()) {
      sql.append(" WHERE ").append(config.getRowFilter());
    }

    return sql.toString();
  }

  /**
   * Processes data using row-level batching to avoid OOM during expensive computations.
   */
  private void processWithRowBatching(MaterializationConfig config, Connection conn,
      String sourcePattern, String stagingPath, Map<String, String> batch, int rowBatchSize)
      throws SQLException {
    // First, count total rows to process
    String countSql = buildCountSql(config, sourcePattern);
    long totalRows = 0;
    try (Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery(countSql)) {
      if (rs.next()) {
        totalRows = rs.getLong(1);
      }
    }

    if (totalRows == 0) {
      LOGGER.debug("No rows to process in batch: {}", batch);
      return;
    }

    LOGGER.info("Row batching: processing {} rows in batches of {} for {}",
        totalRows, rowBatchSize, batch);

    // Process in row batches
    long processedRows = 0;
    int batchNum = 0;
    long totalStartTime = System.currentTimeMillis();

    while (processedRows < totalRows) {
      batchNum++;

      // Build staging path for this row batch - preserve partition structure
      StringBuilder rowBatchPath = new StringBuilder(stagingPath);
      for (Map.Entry<String, String> entry : batch.entrySet()) {
        rowBatchPath.append("/").append(entry.getKey()).append("=").append(entry.getValue());
      }
      rowBatchPath.append("/batch_").append(batchNum).append(".parquet");

      String sql =
          buildDuckDBSql(config, sourcePattern, rowBatchPath.toString(), batch, rowBatchSize, (int) processedRows);
      LOGGER.debug("Row batch {}: rows {} to {} ({})",
          batchNum, processedRows, Math.min(processedRows + rowBatchSize, totalRows), sql);

      long startTime = System.currentTimeMillis();
      try (Statement stmt = conn.createStatement()) {
        stmt.execute(sql);
      }
      long elapsed = System.currentTimeMillis() - startTime;
      LOGGER.debug("Row batch {} completed in {}ms", batchNum, elapsed);

      processedRows += rowBatchSize;
    }

    long totalElapsed = System.currentTimeMillis() - totalStartTime;
    LOGGER.info("Row batching completed: {} batches, {} rows in {}ms",
        batchNum, totalRows, totalElapsed);
  }

  /**
   * Builds a COUNT(*) SQL for getting total rows in source.
   */
  private String buildCountSql(MaterializationConfig config, String sourcePattern) {
    StringBuilder sql = new StringBuilder();
    sql.append("SELECT COUNT(*) FROM ");
    if (config.getSourceFormat() == SourceFormat.JSON) {
      sql.append("read_json('").append(sourcePattern).append("', union_by_name=true)");
    } else {
      sql.append("read_parquet('").append(sourcePattern)
          .append("', hive_partitioning=true, union_by_name=true)");
    }
    // Add WHERE clause if row filter is specified
    if (config.getRowFilter() != null && !config.getRowFilter().isEmpty()) {
      sql.append(" WHERE ").append(config.getRowFilter());
    }
    return sql.toString();
  }

  /**
   * Builds the DuckDB COPY SQL statement.
   *
   * @param config The materialization config
   * @param sourcePattern The source file pattern
   * @param targetPath The target path for output
   * @param batch The batch parameters
   * @param limit Row limit for batching (-1 for no limit)
   * @param offset Row offset for batching (-1 for no offset)
   * @return The SQL statement
   */
  private String buildDuckDBSql(MaterializationConfig config, String sourcePattern,
      String targetPath, Map<String, String> batch, int limit, int offset) {
    StringBuilder sql = new StringBuilder();
    sql.append("COPY (\n");

    // Build SELECT clause - include computed columns if present
    Map<String, String> computedCols = config.getComputedColumns();
    if (computedCols == null || computedCols.isEmpty()) {
      sql.append("  SELECT * FROM ");
    } else {
      // SELECT * plus computed column expressions
      sql.append("  SELECT *");
      for (Map.Entry<String, String> entry : computedCols.entrySet()) {
        sql.append(", ").append(entry.getValue()).append(" AS ").append(entry.getKey());
      }
      sql.append(" FROM ");
    }

    // Use appropriate reader based on source format
    if (config.getSourceFormat() == SourceFormat.JSON) {
      sql.append("read_json('").append(sourcePattern).append("', union_by_name=true)");
    } else {
      sql.append("read_parquet('").append(sourcePattern).append("', hive_partitioning=true, union_by_name=true)");
    }

    // Add WHERE clause if row filter is specified (e.g., to filter by CIK)
    if (config.getRowFilter() != null && !config.getRowFilter().isEmpty()) {
      sql.append(" WHERE ").append(config.getRowFilter());
    }

    // Add LIMIT/OFFSET for row batching
    if (limit > 0) {
      sql.append(" LIMIT ").append(limit);
      if (offset > 0) {
        sql.append(" OFFSET ").append(offset);
      }
    }

    sql.append("\n) TO '").append(targetPath).append("'");
    sql.append(" (FORMAT PARQUET");

    // Skip partition columns and filename pattern for row batching
    // since we're writing to specific file paths
    if (limit <= 0) {
      // Add partition columns
      List<String> partitionCols = config.getPartitionColumnNames();
      if (!partitionCols.isEmpty()) {
        sql.append(", PARTITION_BY (");
        StringBuilder colList = new StringBuilder();
        for (String col : partitionCols) {
          if (colList.length() > 0) {
            colList.append(", ");
          }
          colList.append(col);
        }
        sql.append(colList).append(")");
      }

      // Add filename pattern if batch keys are not in partition columns
      if (needsFilenameEmbedding(config, batch)) {
        String filenamePattern = buildFilenamePattern(batch);
        sql.append(", FILENAME_PATTERN '").append(filenamePattern).append("'");
      }

      sql.append(", OVERWRITE_OR_IGNORE");
    }

    sql.append(");");

    return sql.toString();
  }

  /**
   * Checks if batch keys need to be embedded in filename.
   */
  private boolean needsFilenameEmbedding(MaterializationConfig config, Map<String, String> batch) {
    if (batch.isEmpty()) {
      return false;
    }
    List<String> partitionCols = config.getPartitionColumnNames();
    for (String batchKey : batch.keySet()) {
      if (!partitionCols.contains(batchKey)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Builds filename pattern with batch key values embedded.
   */
  private String buildFilenamePattern(Map<String, String> batch) {
    StringBuilder pattern = new StringBuilder();
    for (Map.Entry<String, String> entry : batch.entrySet()) {
      if (pattern.length() > 0) {
        pattern.append("_");
      }
      pattern.append(entry.getKey()).append("_").append(entry.getValue());
    }
    pattern.append("_{i}");
    return pattern.toString();
  }

  /**
   * Result of ensuring a table exists, including whether it was recreated.
   */
  private static class TableSetupResult {
    final Table table;
    final boolean wasRecreated;

    TableSetupResult(Table table, boolean wasRecreated) {
      this.table = table;
      this.wasRecreated = wasRecreated;
    }
  }

  /**
   * Ensures the target Iceberg table exists, creating it if necessary.
   *
   * <p>Uses tableColumns if provided (full schema from YAML config),
   * otherwise falls back to partition columns only (legacy behavior).
   *
   * <p>If the table exists but has fewer columns than expected (e.g., only
   * partition columns from a previous buggy run), it will be dropped and
   * recreated with the correct schema.
   *
   * @return TableSetupResult containing the table and whether it was recreated
   */
  private TableSetupResult ensureTableExists(MaterializationConfig config) {
    // Use tableColumns if provided, otherwise fall back to partition columns
    List<IcebergCatalogManager.ColumnDef> columns;
    if (!config.getTableColumns().isEmpty()) {
      // Full table schema provided - use it
      columns = new ArrayList<IcebergCatalogManager.ColumnDef>(config.getTableColumns());
      LOGGER.debug("Using {} table columns from config for table '{}'",
          columns.size(), config.getTargetTableId());
    } else if (!config.getPartitionColumns().isEmpty()) {
      // Legacy behavior: use partition columns as schema
      columns = new ArrayList<IcebergCatalogManager.ColumnDef>();
      for (PartitionedTableConfig.ColumnDefinition colDef : config.getPartitionColumns()) {
        columns.add(new IcebergCatalogManager.ColumnDef(colDef.getName(), colDef.getType()));
      }
      LOGGER.debug("Using {} partition columns as schema for table '{}' (legacy mode)",
          columns.size(), config.getTargetTableId());
    } else {
      columns = new ArrayList<IcebergCatalogManager.ColumnDef>();
    }

    // If table exists, check if it has the expected columns
    if (IcebergCatalogManager.tableExists(catalogConfig, config.getTargetTableId())) {
      Table existingTable = IcebergCatalogManager.loadTable(catalogConfig, config.getTargetTableId());

      // Check if existing table has fewer columns than expected (buggy previous run)
      int existingColumnCount = existingTable.schema().columns().size();
      int expectedColumnCount = columns.size();

      if (expectedColumnCount > 0 && existingColumnCount < expectedColumnCount) {
        LOGGER.warn("Existing Iceberg table '{}' has {} columns but expected {}. "
            + "Dropping and recreating with correct schema.",
            config.getTargetTableId(), existingColumnCount, expectedColumnCount);
        IcebergCatalogManager.dropTable(catalogConfig, config.getTargetTableId(), true);
        // Fall through to create new table with wasRecreated = true
      } else {
        LOGGER.debug("Loading existing table: {} ({} columns)",
            config.getTargetTableId(), existingColumnCount);
        return new TableSetupResult(existingTable, false);
      }
    } else {
      // Table doesn't exist - this is a new table, not a recreation
      LOGGER.info("Creating new Iceberg table: {} with {} columns, partitioned by {}",
          config.getTargetTableId(), columns.size(), config.getPartitionColumnNames());
      Table newTable =
          IcebergCatalogManager.createTableFromColumns(catalogConfig,
          config.getTargetTableId(),
          columns,
          config.getPartitionColumnNames());
      return new TableSetupResult(newTable, false);
    }

    // Create new table with partition spec (table was dropped due to schema mismatch)
    LOGGER.info("Recreating Iceberg table: {} with {} columns, partitioned by {}",
        config.getTargetTableId(), columns.size(), config.getPartitionColumnNames());
    Table recreatedTable =
        IcebergCatalogManager.createTableFromColumns(catalogConfig,
        config.getTargetTableId(),
        columns,
        config.getPartitionColumnNames());
    return new TableSetupResult(recreatedTable, true);
  }

  /**
   * Checks if an Iceberg partition already has data (for self-healing).
   *
   * <p>This is used to detect partitions that were materialized in previous runs
   * but not tracked in the incremental tracker (e.g., due to tracking being added
   * after initial materialization).
   *
   * @param table The Iceberg table to check
   * @param partitionValues The partition values to check for (e.g., {year=2026})
   * @return true if the partition has data, false otherwise
   */
  private boolean partitionHasData(Table table, Map<String, String> partitionValues) {
    if (table == null || partitionValues == null || partitionValues.isEmpty()) {
      return false;
    }

    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      for (FileScanTask task : tasks) {
        // Check if this file's partition matches our partition values
        org.apache.iceberg.StructLike partition = task.file().partition();
        org.apache.iceberg.PartitionSpec spec = table.spec();

        boolean matches = true;
        for (Map.Entry<String, String> entry : partitionValues.entrySet()) {
          String colName = entry.getKey();
          String expectedValue = entry.getValue();

          // Find the partition field index
          int fieldIndex = -1;
          for (int i = 0; i < spec.fields().size(); i++) {
            if (spec.fields().get(i).name().equals(colName)) {
              fieldIndex = i;
              break;
            }
          }

          if (fieldIndex < 0) {
            // Partition column not found in spec
            matches = false;
            break;
          }

          Object actualValue = partition.get(fieldIndex, Object.class);
          if (actualValue == null || !actualValue.toString().equals(expectedValue)) {
            matches = false;
            break;
          }
        }

        if (matches) {
          LOGGER.debug("Self-healing: found existing data for partition {} in table {}",
              partitionValues, table.name());
          return true;
        }
      }
    } catch (Exception e) {
      LOGGER.debug("Self-healing check failed for partition {}: {}",
          partitionValues, e.getMessage());
    }

    return false;
  }

  /**
   * Source file watermarking is always enabled - no configuration needed.
   *
   * <p>Watermarking tracks the max lastModified timestamp of source files.
   * On subsequent runs, if any source file has been modified (new files added
   * or existing files updated), the table will be reprocessed.
   *
   * <p>This is essential for document-based ETL (like SEC filings) where new
   * files are continually added to the source directory.
   *
   * @param config The materialization config (unused - watermarking is always enabled)
   * @return always true - source watermarking is mandatory
   */
  private boolean isSourceWatermarkEnabled(MaterializationConfig config) {
    return true; // Source file change detection is always enabled
  }

  /**
   * Computes the max lastModified timestamp from source files.
   *
   * <p>This is used for source file watermarking - detecting when new files
   * have been added or existing files modified since last processing.
   *
   * @param sourcePattern Glob pattern for source files
   * @param sourceFormat Format of source files (JSON or PARQUET)
   * @return Max lastModified timestamp in milliseconds, or 0 if no files or error
   */
  public long getSourceFileWatermark(String sourcePattern, SourceFormat sourceFormat) {
    long maxLastModified = 0;

    try (Connection conn = getDuckDBConnection(1);
         Statement stmt = conn.createStatement()) {
      // Use DuckDB's file_glob function to list matching files
      String ext = sourceFormat == SourceFormat.JSON ? ".json" : ".parquet";

      // For patterns like s3://bucket/path/**/*.parquet, extract the base path
      // and let DuckDB handle the glob
      String globPattern = sourcePattern;
      if (!globPattern.endsWith(ext) && !globPattern.contains("*")) {
        globPattern = globPattern + "/**/*" + ext;
      }

      // DuckDB glob() returns 'file' column; 'last_modified' only available for local files
      // For S3 paths, we skip watermark optimization (always re-check)
      boolean isS3 = globPattern.startsWith("s3://") || globPattern.startsWith("s3a://");
      if (isS3) {
        LOGGER.debug("S3 path detected, skipping watermark (not available): {}", globPattern);
        return 0;  // No watermark for S3 - always check for changes
      }

      String sql = "SELECT file, last_modified FROM glob('" + globPattern + "')";
      LOGGER.debug("Getting source file watermark with pattern: {}", globPattern);

      try (ResultSet rs = stmt.executeQuery(sql)) {
        while (rs.next()) {
          java.sql.Timestamp ts = rs.getTimestamp("last_modified");
          if (ts != null) {
            long lastModified = ts.getTime();
            if (lastModified > maxLastModified) {
              maxLastModified = lastModified;
            }
          }
        }
      }
      LOGGER.debug("Source file watermark: {} (pattern: {})", maxLastModified, sourcePattern);
    } catch (SQLException e) {
      LOGGER.warn("Failed to compute source file watermark for {}: {}",
          sourcePattern, e.getMessage());
    }

    return maxLastModified;
  }

  /**
   * Creates a staging path with timestamp and random suffix.
   *
   * <p>Staging happens under warehousePath/.staging/ to ensure it uses the same
   * storage type (local or S3) as the warehouse. For S3, a lifecycle rule is
   * set up to auto-expire orphaned staging files after 1 day.
   */
  private String createStagingPath() throws IOException {
    String timestamp = new SimpleDateFormat("yyyyMMdd'T'HHmmss'Z'").format(new Date());
    String random = UUID.randomUUID().toString().substring(0, 8);
    String stagingSubpath = ".staging/" + timestamp + "_" + random;
    String stagingPath = storageProvider.resolvePath(warehousePath, stagingSubpath);

    // Set up lifecycle rule for auto-cleanup (S3 only, no-op for local)
    storageProvider.ensureLifecycleRule(".staging/", 1);

    storageProvider.createDirectories(stagingPath);
    LOGGER.debug("Created staging directory: {}", stagingPath);
    return stagingPath;
  }

  /**
   * Cleans up the staging directory using StorageProvider.
   * Works for both local and S3 storage.
   *
   * <p>For S3: After stageFiles() moves files to the data location, the staging
   * directory is empty. We skip cleanup entirely and rely on the lifecycle rule
   * to expire orphaned staging files after 1 day. This saves N API calls per commit.
   *
   * <p>For local: We attempt cleanup to free disk space immediately.
   */
  private void cleanupStagingDirectory(String stagingPath) {
    // For S3 paths, skip cleanup - lifecycle rule handles orphaned staging
    if (stagingPath.startsWith("s3://") || stagingPath.startsWith("s3a://")) {
      LOGGER.debug("Skipping S3 staging cleanup (lifecycle rule handles expiration): {}",
          stagingPath);
      return;
    }

    // For local storage, clean up immediately
    try {
      List<StorageProvider.FileEntry> files = storageProvider.listFiles(stagingPath, true);
      if (!files.isEmpty()) {
        List<String> paths = new ArrayList<String>();
        for (StorageProvider.FileEntry entry : files) {
          paths.add(entry.getPath());
        }
        storageProvider.deleteBatch(paths);
      }
      try {
        storageProvider.delete(stagingPath);
      } catch (IOException ignored) {
        // Directory may not exist, which is fine
      }
    } catch (IOException e) {
      LOGGER.warn("Failed to cleanup staging directory {}: {}", stagingPath, e.getMessage());
    }
  }

  /**
   * Builds batch combinations from batch_partition_columns.
   */
  private List<Map<String, String>> buildBatchCombinations(MaterializationConfig config) {
    List<String> batchColumns = config.getBatchPartitionColumns();
    if (batchColumns.isEmpty()) {
      return Collections.emptyList();
    }

    // Build values for each batch column
    List<List<String>> columnValues = new ArrayList<List<String>>();
    List<String> columnNames = new ArrayList<String>();

    for (String col : batchColumns) {
      List<String> values;
      if ("year".equalsIgnoreCase(col)) {
        // Use configured year range
        values = new ArrayList<String>();
        for (int y = config.getStartYear(); y <= config.getEndYear(); y++) {
          values.add(String.valueOf(y));
        }
      } else {
        // Query distinct values from source
        values = getDistinctValues(config.getSourcePattern(), col, config.getSourceFormat());
      }

      if (!values.isEmpty()) {
        columnNames.add(col);
        columnValues.add(values);
      }
    }

    // Build cartesian product
    List<Map<String, String>> combinations = new ArrayList<Map<String, String>>();
    buildCombinationsRecursive(columnNames, columnValues, 0,
        new LinkedHashMap<String, String>(), combinations);

    LOGGER.debug("Built {} batch combinations", combinations.size());
    return combinations;
  }

  /**
   * Recursively builds cartesian product of column values.
   */
  private void buildCombinationsRecursive(List<String> columnNames,
      List<List<String>> columnValues, int depth, Map<String, String> current,
      List<Map<String, String>> result) {
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
   * Gets distinct values for a column from source files.
   */
  private List<String> getDistinctValues(String sourcePattern, String column, SourceFormat format) {
    Set<String> values = new TreeSet<String>();

    try (Connection conn = getDuckDBConnection(1);
         Statement stmt = conn.createStatement()) {
      String reader = format == SourceFormat.JSON ? "read_json" : "read_parquet";
      String options = format == SourceFormat.JSON ? "union_by_name=true" : "hive_partitioning=true, union_by_name=true";
      String sql =
          String.format("SELECT DISTINCT %s FROM %s('%s', %s) WHERE %s IS NOT NULL ORDER BY %s", column, reader, sourcePattern, options, column, column);

      try (ResultSet rs = stmt.executeQuery(sql)) {
        while (rs.next()) {
          String value = rs.getString(1);
          if (value != null) {
            values.add(value);
          }
        }
      }
    } catch (SQLException e) {
      LOGGER.warn("Failed to get distinct values for {}: {}", column, e.getMessage());
    }

    return new ArrayList<String>(values);
  }

  /**
   * Groups batches by incremental key values.
   */
  private Map<Map<String, String>, List<Map<String, String>>> groupBatchesByIncrementalKey(
      List<Map<String, String>> batches, List<String> incrementalKeys) {
    Map<Map<String, String>, List<Map<String, String>>> grouped =
        new LinkedHashMap<Map<String, String>, List<Map<String, String>>>();

    for (Map<String, String> batch : batches) {
      Map<String, String> keyValues = new LinkedHashMap<String, String>();
      if (incrementalKeys != null) {
        for (String key : incrementalKeys) {
          String value = batch.get(key);
          if (value != null) {
            keyValues.put(key, value);
          }
        }
      }

      List<Map<String, String>> list = grouped.get(keyValues);
      if (list == null) {
        list = new ArrayList<Map<String, String>>();
        grouped.put(keyValues, list);
      }
      list.add(batch);
    }

    return grouped;
  }

  /**
   * Coerces a string value to the appropriate type.
   */
  private Object coerceValue(String value, String type) {
    if (value == null || type == null) {
      return value;
    }
    String upperType = type.toUpperCase();
    switch (upperType) {
      case "INTEGER":
      case "INT":
        return Integer.parseInt(value);
      case "BIGINT":
      case "LONG":
        return Long.parseLong(value);
      case "DOUBLE":
        return Double.parseDouble(value);
      case "BOOLEAN":
        return Boolean.parseBoolean(value);
      default:
        return value;
    }
  }

  /**
   * Finds the type of a column by name.
   */
  private String findColumnType(List<PartitionedTableConfig.ColumnDefinition> columns, String name) {
    for (PartitionedTableConfig.ColumnDefinition col : columns) {
      if (col.getName().equals(name)) {
        return col.getType();
      }
    }
    return "VARCHAR";
  }

  /**
   * Creates a DuckDB connection with configured settings.
   */
  private Connection getDuckDBConnection(int threads) throws SQLException {
    Connection conn = DriverManager.getConnection("jdbc:duckdb:");

    try (Statement stmt = conn.createStatement()) {
      stmt.execute("SET threads=" + threads);
      stmt.execute("SET preserve_insertion_order=false");
      // Limit memory to avoid OOM on memory-constrained systems
      stmt.execute("SET memory_limit='" + DUCKDB_MEMORY_LIMIT + "'");
      if (warehousePath != null) {
        stmt.execute("SET temp_directory='" + warehousePath + "/.duckdb_tmp'");
      }

      // Load extensions
      try {
        stmt.execute("INSTALL parquet");
        stmt.execute("LOAD parquet");
      } catch (SQLException e) {
        LOGGER.debug("Parquet extension already loaded or built-in");
      }

      // Load quackformers extension for embedding functions (embed_jina, etc.)
      try {
        stmt.execute("INSTALL quackformers FROM community");
        stmt.execute("LOAD quackformers");
        LOGGER.debug("Loaded quackformers extension for embedding functions");
      } catch (SQLException e) {
        LOGGER.debug("Quackformers extension not available: {}", e.getMessage());
      }

      // Configure S3 if available
      Map<String, String> s3Config = storageProvider != null ? storageProvider.getS3Config() : null;
      if (s3Config != null && !s3Config.isEmpty()) {
        configureS3(stmt, s3Config);
      }
    }

    return conn;
  }

  /**
   * Configures S3 access for DuckDB.
   */
  private void configureS3(Statement stmt, Map<String, String> s3Config) throws SQLException {
    try {
      stmt.execute("INSTALL httpfs");
      stmt.execute("LOAD httpfs");

      String accessKey = s3Config.get("accessKeyId");
      String secretKey = s3Config.get("secretAccessKey");
      String endpoint = s3Config.get("endpoint");
      String region = s3Config.get("region");

      if (accessKey != null && secretKey != null) {
        stmt.execute("SET s3_access_key_id='" + accessKey + "'");
        stmt.execute("SET s3_secret_access_key='" + secretKey + "'");
      }
      if (endpoint != null) {
        stmt.execute("SET s3_endpoint='" + endpoint + "'");
        stmt.execute("SET s3_url_style='path'");
      }
      if (region != null) {
        stmt.execute("SET s3_region='" + region + "'");
      } else {
        stmt.execute("SET s3_region='auto'");
      }
    } catch (SQLException e) {
      LOGGER.debug("S3 configuration skipped: {}", e.getMessage());
    }
  }
}
