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
// storage-provider-guard:allow-scheme - storage-dispatch layer: inspecting a URI scheme here is the legitimate job (provider dispatch / S3 path handling / endpoint SSL config), not a consumer branching local-vs-remote.

import org.apache.calcite.adapter.file.partition.IncrementalTracker;
import org.apache.calcite.adapter.file.partition.PartitionedTableConfig;
import org.apache.calcite.adapter.file.storage.StorageProvider;

import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.io.CloseableIterable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
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
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

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

  private static final int DEFAULT_MAX_RETRIES = 5;
  private static final long DEFAULT_RETRY_DELAY_MS = 30000;
  private static final int DEFAULT_THREADS = 2;
  private static final int DEFAULT_FILE_CHUNK_SIZE = 5000;

  /**
   * Per-chunk DuckDB query timeout in seconds. DuckDB JDBC does not implement
   * setQueryTimeout(), so we enforce this via a daemon thread + connection close.
   * Observed worst-case chunk time is ~22 min; 30 min gives headroom while
   * catching R2 network stalls that hang indefinitely (4+ hours observed).
   */
  private static final long CHUNK_QUERY_TIMEOUT_SECONDS = 1800L;

  /** DuckDB memory limit - from DUCKDB_MEMORY_LIMIT env var, default 4GB. */
  private static final String DUCKDB_MEMORY_LIMIT =
      System.getenv("DUCKDB_MEMORY_LIMIT") != null
          ? System.getenv("DUCKDB_MEMORY_LIMIT") : "4GB";

  /** DuckDB spill directory - DUCKDB_TEMP_DIR env var, else TMP/TEMP/java.io.tmpdir + "/duckdb". */
  private static final String DUCKDB_TEMP_DIR = resolveDuckDbTempDir();

  private static String resolveDuckDbTempDir() {
    String override = System.getenv("DUCKDB_TEMP_DIR");
    if (override != null && !override.isEmpty()) {
      return override;
    }
    for (String var : new String[]{"TMP", "TEMP"}) {
      String val = System.getenv(var);
      if (val != null && !val.isEmpty()) {
        return val + "/duckdb";
      }
    }
    return System.getProperty("java.io.tmpdir") + "/duckdb";
  }

  private final String warehousePath;
  private final Map<String, Object> catalogConfig;
  private final StorageProvider storageProvider;
  private final IncrementalTracker incrementalTracker;
  private final int maxRetries;
  private final long retryDelayMs;

  /** Tracks total rows written during current materialize() call. Reset per call. */
  private long totalRowsWritten;

  /** Cache of source accessions per year - populated once via S3 LIST, used for all tables.
   *  Key: "year:suffix", Value: Map of CIK to Set of accession numbers. */
  private final Map<String, Map<String, Set<String>>> sourceAccessionsCache =
      new HashMap<String, Map<String, Set<String>>>();

  /** Cache of full S3 paths per accession, populated alongside sourceAccessionsCache.
   *  Key: "year:suffix", Value: Map of accession number to full S3 path. */
  private final Map<String, Map<String, String>> sourcePathsCache =
      new HashMap<String, Map<String, String>>();

  /** Cache of the S3 source-file watermark per (basePath, ext). The non-wildcard base of a
   *  {@code year=*} source pattern collapses to the whole schema prefix (e.g. {@code sec/}), so
   *  every table in a materialize pass would otherwise do an identical full-bucket recursive LIST.
   *  Key: "basePath:ext", Value: max lastModified. Stable within a run (source files are pre-staged). */
  private final Map<String, Long> s3WatermarkCache = new HashMap<String, Long>();

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
    String normalizedPath = org.apache.calcite.adapter.file.storage.StorageProviderFactory
        .normalizeForHadoop(warehousePath);
    this.warehousePath = normalizedPath;
    this.storageProvider = storageProvider;
    this.incrementalTracker = incrementalTracker != null ? incrementalTracker : IncrementalTracker.NOOP;
    this.maxRetries = maxRetries;
    this.retryDelayMs = retryDelayMs;

    // Build catalog config
    this.catalogConfig = new HashMap<String, Object>();
    this.catalogConfig.put("catalog", "hadoop");
    this.catalogConfig.put("warehousePath", normalizedPath);

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
    private final int fileChunkSize;  // Max files per DuckDB query; 0 = use glob (no chunking)
    private final String rowFilter;  // Optional WHERE clause filter (e.g., "cik IN ('0001', '0002')")
    private final String icebergTableLocation;  // Iceberg table location for accession-level dedup
    private final String accessionColumn;  // Column name for accession (default: "accession_number")
    // When true, pre-built parquet (already matching the table schema) is committed to Iceberg by
    // moving files + building DataFile metadata — no rows are read into memory. Streams huge,
    // pre-transformed sources (e.g. the full-market stock_prices bulk).
    private final boolean filePassthrough;

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
      this.fileChunkSize = builder.fileChunkSize > 0 ? builder.fileChunkSize : DEFAULT_FILE_CHUNK_SIZE;
      this.rowFilter = builder.rowFilter;
      this.icebergTableLocation = builder.icebergTableLocation;
      this.accessionColumn = builder.accessionColumn != null ? builder.accessionColumn : "accession_number";
      this.filePassthrough = builder.filePassthrough;
    }

    public boolean isFilePassthrough() {
      return filePassthrough;
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
     * Returns the file chunk size for S3 file-list optimization.
     * When S3 LIST succeeds, new source files are split into explicit chunks of this size
     * instead of using a glob pattern, avoiding full 100k+ file scans per DuckDB query.
     * Defaults to {@value IcebergMaterializer#DEFAULT_FILE_CHUNK_SIZE}.
     */
    public int getFileChunkSize() {
      return fileChunkSize;
    }

    /**
     * Returns the optional row filter (WHERE clause) to apply during materialization.
     * This is used to filter source data, e.g., by CIK list.
     */
    public String getRowFilter() {
      return rowFilter;
    }

    /**
     * Returns the Iceberg table location for accession-level deduplication.
     * When set, the materializer will query this table for existing accessions
     * and skip re-processing them.
     */
    public String getIcebergTableLocation() {
      return icebergTableLocation;
    }

    /**
     * Returns the column name used for accession-level deduplication.
     * Defaults to "accession_number".
     */
    public String getAccessionColumn() {
      return accessionColumn;
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
      private int fileChunkSize;
      private String rowFilter;
      private String icebergTableLocation;
      private String accessionColumn;
      private boolean filePassthrough;

      public Builder sourcePattern(String sourcePattern) {
        this.sourcePattern = sourcePattern;
        return this;
      }

      public Builder filePassthrough(boolean filePassthrough) {
        this.filePassthrough = filePassthrough;
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
       * Sets the file chunk size for S3 file-list optimization.
       * Splits new source files into explicit chunks instead of a glob scan.
       * Defaults to {@value IcebergMaterializer#DEFAULT_FILE_CHUNK_SIZE} if not set.
       */
      public Builder fileChunkSize(int fileChunkSize) {
        this.fileChunkSize = fileChunkSize;
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

      /**
       * Sets the Iceberg table location for accession-level deduplication.
       * When set, enables self-healing: checks Iceberg for existing accessions
       * before re-processing them.
       */
      public Builder icebergTableLocation(String icebergTableLocation) {
        this.icebergTableLocation = icebergTableLocation;
        return this;
      }

      /**
       * Sets the column name used for accession-level deduplication.
       * Defaults to "accession_number" if not specified.
       */
      public Builder accessionColumn(String accessionColumn) {
        this.accessionColumn = accessionColumn;
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
    private final long totalRowsWritten;

    public MaterializationResult(String tableId, int successCount, int failedCount,
        int skippedCount, long durationMs) {
      this(tableId, successCount, failedCount, skippedCount, durationMs, false, 0L);
    }

    public MaterializationResult(String tableId, int successCount, int failedCount,
        int skippedCount, long durationMs, boolean tableRecreated) {
      this(tableId, successCount, failedCount, skippedCount, durationMs, tableRecreated, 0L);
    }

    public MaterializationResult(String tableId, int successCount, int failedCount,
        int skippedCount, long durationMs, boolean tableRecreated, long totalRowsWritten) {
      this.tableId = tableId;
      this.successCount = successCount;
      this.failedCount = failedCount;
      this.skippedCount = skippedCount;
      this.durationMs = durationMs;
      this.tableRecreated = tableRecreated;
      this.totalRowsWritten = totalRowsWritten;
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

    public long getTotalRowsWritten() {
      return totalRowsWritten;
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
    totalRowsWritten = 0;  // Reset per call
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
            java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
                .format(java.time.Instant.ofEpochMilli(cached.completedAt)
                    .atZone(java.time.ZoneOffset.UTC).toLocalDateTime()),
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

    // Streaming file-passthrough: the source parquet already matches the table schema, so move it
    // into the table and commit DataFile metadata — no rows are read into memory. Used for the
    // full-market stock_prices bulk (~27M rows) which cannot be row-materialized in heap.
    if (config.isFilePassthrough()) {
      return materializeViaFilePassthrough(config, table, writer, startTime);
    }

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

    // Process batches with accession-level tracking
    for (Map.Entry<Map<String, String>, List<Map<String, String>>> entry : batchesByIncrementalKey.entrySet()) {
      List<Map<String, String>> batchesForKey = entry.getValue();

      // Process all batches for this incremental key
      for (Map<String, String> batch : batchesForKey) {
        // Get accessions to exclude (already processed)
        Set<String> excludeAccessions = getExcludedAccessions(config, table, batch);

        boolean success = processBatchWithRetry(config, table, batch, excludeAccessions);
        if (success) {
          successCount++;
        } else {
          failedCount++;
        }
      }
    }

    long durationMs = System.currentTimeMillis() - startTime;
    MaterializationResult result =
        new MaterializationResult(config.getTargetTableId(), successCount, failedCount, skippedCount,
            durationMs, tableWasRecreated, totalRowsWritten);

    LOGGER.info("Materialization complete: {}", result);

    // Run maintenance and record completion if we actually wrote data
    if (result.isFullySuccessful() && totalRowsWritten > 0) {
      writer.runMaintenance(7, 365);  // 365 days for orphans = effectively disabled for append-only

      // Compact small files to reduce metadata overhead for iceberg_scan queries
      try {
        long targetSize = 128 * 1024 * 1024; // 128MB
        int minFiles = 10;
        long smallFileSize = 10 * 1024 * 1024; // 10MB
        int compacted = writer.compactSmallFiles(targetSize, minFiles, smallFileSize);
        if (compacted > 0) {
          LOGGER.info("Compacted {} partitions for '{}'", compacted, config.getTargetTableId());
        }
      } catch (Exception e) {
        LOGGER.warn("Compaction failed for '{}': {}", config.getTargetTableId(), e.getMessage());
      }

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
        LOGGER.info("Marked table '{}' complete ({} rows)", config.getTargetTableId(), totalRowsWritten);
      }
    } else if (result.isFullySuccessful()) {
      LOGGER.info("Skipping maintenance for '{}' (0 rows written)", config.getTargetTableId());
    }

    return result;
  }

  /**
   * Streaming materialization for pre-built, schema-matching parquet. Moves the staged parquet
   * files into the Iceberg table's data location and commits DataFile metadata (partition values
   * read from the {@code year=} path, row counts estimated from file size). No rows are read into
   * heap, so arbitrarily large sources (e.g. the full-market stock_prices bulk) stream through.
   * The staging directory is {@code <source-base>__staging} (a sibling of the table location so a
   * recursive walk never picks up the table's own metadata/data files).
   */
  private MaterializationResult materializeViaFilePassthrough(MaterializationConfig config,
      Table table, IcebergTableWriter writer, long startTime) throws IOException {
    String sourcePattern = config.getSourcePattern();
    int yi = sourcePattern.indexOf("/year=");
    String base = (yi > 0 ? sourcePattern.substring(0, yi) : sourcePattern);
    String stagingDir = base + "__staging";
    // Sibling dir holding current-price top-up rows the bulk COPY never clears (see
    // AlphaVantageDownloader); staged alongside the bulk so both land in the same Iceberg commit.
    String topupDir = base + "__topup";
    LOGGER.info("File-passthrough materialization for '{}': staging parquet from {} (+{})",
        config.getTargetTableId(), stagingDir, topupDir);

    // The staged parquet (DuckDB-written) has no Iceberg field IDs, so set a default name mapping
    // — readers (DuckDB iceberg_scan) then resolve columns by name.
    try {
      String mappingJson = org.apache.iceberg.mapping.NameMappingParser.toJson(
          org.apache.iceberg.mapping.MappingUtil.create(table.schema()));
      table.updateProperties()
          .set(org.apache.iceberg.TableProperties.DEFAULT_NAME_MAPPING, mappingJson)
          .commit();
    } catch (Exception e) {
      LOGGER.warn("Could not set name mapping for '{}': {}", config.getTargetTableId(), e.getMessage());
    }

    List<org.apache.iceberg.DataFile> dataFiles = writer.stageFiles(stagingDir);
    // Stage the sibling top-up dir too, if it has files (it may be absent when no top-up ran).
    // Use isDirectory (prefix listing) — exists()/doesObjectExist is false for an S3 "directory".
    try {
      if (storageProvider.isDirectory(topupDir)) {
        List<org.apache.iceberg.DataFile> topupFiles = writer.stageFiles(topupDir);
        if (!topupFiles.isEmpty()) {
          LOGGER.info("File-passthrough '{}': staged {} bulk + {} top-up data files",
              config.getTargetTableId(), dataFiles.size(), topupFiles.size());
          dataFiles.addAll(topupFiles);
        }
      }
    } catch (IOException e) {
      LOGGER.warn("File-passthrough: could not stage top-up dir {} for '{}': {}",
          topupDir, config.getTargetTableId(), e.getMessage());
    }
    long durationMs = System.currentTimeMillis() - startTime;
    if (dataFiles.isEmpty()) {
      LOGGER.warn("File-passthrough: no staged parquet at {} for '{}' — nothing committed",
          stagingDir, config.getTargetTableId());
      return new MaterializationResult(config.getTargetTableId(), 0, 1, 0, durationMs);
    }
    // Replace-partitions (not append): the file-passthrough source is a full snapshot that rewrites
    // every partition each run (e.g. the bulk stock-price ingest writes all year=*/ partitions), so
    // appending would duplicate the whole table on re-run. ReplacePartitions overwrites each touched
    // partition wholesale, making re-materialization idempotent.
    writer.replacePartitionsDataFiles(dataFiles);
    totalRowsWritten += dataFiles.size();
    incrementalTracker.markTableCompleteWithSourceWatermark(
        config.getTargetTableId(), "auto", "", dataFiles.size(), 0);
    LOGGER.info("File-passthrough materialization complete for '{}': committed {} data files in {}ms",
        config.getTargetTableId(), dataFiles.size(), durationMs);
    return new MaterializationResult(
        config.getTargetTableId(), 1, 0, 0, durationMs, false, dataFiles.size());
  }

  /**
   * Processes a single batch with retry logic.
   *
   * @param config The materialization config
   * @param table The Iceberg table
   * @param batch The batch key values
   * @param excludeAccessions Accession numbers to exclude (already processed)
   * @return true if successful
   */
  private boolean processBatchWithRetry(MaterializationConfig config, Table table,
      Map<String, String> batch, Set<String> excludeAccessions) {
    int attempts = 0;
    String checkpointPath = buildChunkProgressPath(config, batch);

    while (attempts < maxRetries) {
      attempts++;
      int startChunkIndex = 0;
      if (attempts > 1) {
        // Re-read excluded accessions so the exclude set reflects anything already committed
        // to Iceberg by a previous attempt (tracker self-heal picks these up on restart).
        excludeAccessions = getExcludedAccessions(config, table, batch);
        LOGGER.info("Retry attempt {}: refreshed exclude set to {} accessions for batch {}",
            attempts, excludeAccessions.size(), batch);
        // Read the chunk-progress checkpoint to resume mid-batch rather than from chunk 1.
        startChunkIndex = readChunkProgress(checkpointPath);
        if (startChunkIndex > 0) {
          LOGGER.info("Retry attempt {}: resuming from chunk {} via checkpoint {}",
              attempts, startChunkIndex, checkpointPath);
        }
      }
      try {
        Set<String> newAccessions = processBatch(config, table, batch, excludeAccessions, startChunkIndex, checkpointPath);
        String accessionCol = config.getAccessionColumn();
        String yearValue = batch.get("year");

        // Mark newly materialized accessions in tracker so we never re-process them
        if (!newAccessions.isEmpty()) {
          for (String accession : newAccessions) {
            Map<String, String> accessionKey = new LinkedHashMap<String, String>();
            if (yearValue != null) {
              accessionKey.put("year", yearValue);
            }
            accessionKey.put(accessionCol, accession);
            incrementalTracker.markProcessed(config.getTargetTableId(),
                config.getSourceTableName(), accessionKey, config.getTargetTableId());
          }
          LOGGER.info("Tracked {} newly materialized accessions for {}/year={}",
              newAccessions.size(), config.getTargetTableId(), yearValue);
        }

        // Batch fully committed — remove the chunk-progress checkpoint so a fresh
        // retry of this batch starts from chunk 0 (no stale resumption point).
        clearChunkProgress(checkpointPath);

        // Self-heal: if DuckDB returned 0 new rows, ensure tracker has entries for
        // all source accessions. This handles the case where data exists in Iceberg
        // but the tracker was reset or never populated (e.g., first run with new tracker).
        if (newAccessions.isEmpty() && yearValue != null) {
          selfHealTracker(config, yearValue, excludeAccessions);
        }

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
          LOGGER.warn("Batch {} has no source files, skipping (table may require different filing type)", batch);
          return true;  // Treat as success - no data to process is OK
        }

        // Enhanced diagnostics for HTTP/S3 errors (especially 404s with HTML responses)
        if (message != null && (message.contains("HTTP") || message.contains("404") || message.contains("<!doctype"))) {
          // Log full stack trace to capture S3 URI and complete error context
          LOGGER.error("Batch {} failed with HTTP/S3 error (attempt {}/{}). Batch parameters: {}. Full exception trace:",
              batch, attempts, maxRetries, batch, e);
          // Also extract and log just the URL/path if possible
          String[] stackElements = e.getStackTrace().length > 0 ? new String[0] : null;
          for (StackTraceElement elem : e.getStackTrace()) {
            if (elem.toString().contains("s3") || elem.toString().contains("httpfs")) {
              LOGGER.error("S3/httpfs context: {}", elem);
            }
          }
        } else {
          LOGGER.warn("Batch {} failed (attempt {}/{}): {}",
              batch, attempts, maxRetries, message);
        }

        if (attempts < maxRetries) {
          try {
            Thread.sleep(retryDelayMs * (1L << (attempts - 1))); // 30s, 60s, 120s, 240s
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
   * Self-heals the tracker by adding source accessions that are missing from the tracker.
   * This handles the case where data already exists in Iceberg (from a previous run)
   * but the tracker was not populated — e.g., first run with a new tracker, or after
   * a tracker reset. Without this, every run would do a full DuckDB query (returning 0 rows)
   * instead of fast-skipping.
   */
  private void selfHealTracker(MaterializationConfig config, String yearValue,
      Set<String> excludeAccessions) {
    Set<String> sourceAccessions =
        getFilteredSourceAccessions(config.getSourcePattern(), yearValue, config.getRowFilter());
    if (sourceAccessions == null || sourceAccessions.isEmpty()) {
      return;
    }

    // Find source accessions not yet in the tracker
    Set<String> untracked = new HashSet<String>();
    for (String acc : sourceAccessions) {
      if (!excludeAccessions.contains(acc)) {
        untracked.add(acc);
      }
    }

    if (untracked.isEmpty()) {
      return;
    }

    // Add untracked accessions to the tracker
    String accessionCol = config.getAccessionColumn();
    for (String accession : untracked) {
      Map<String, String> accessionKey = new LinkedHashMap<String, String>();
      accessionKey.put("year", yearValue);
      accessionKey.put(accessionCol, accession);
      incrementalTracker.markProcessed(config.getTargetTableId(),
          config.getSourceTableName(), accessionKey, config.getTargetTableId());
    }
    LOGGER.info("Self-healed tracker: added {} untracked accessions for {}/year={}",
        untracked.size(), config.getTargetTableId(), yearValue);
  }

  /**
   * Processes a single batch: DuckDB SELECT -> Iceberg native parquet writer.
   *
   * <p>Uses Iceberg's native Parquet writer (via IcebergTableWriter.writeRecords)
   * to embed proper field IDs in the parquet schema. This is required for
   * DuckDB's iceberg_scan extension to correctly map columns.
   *
   * <p>When rowBatchSize > 0, uses row-level batching to prevent OOM for large tables.
   *
   * @param excludeAccessions Accession numbers to skip (already processed)
   * @param startChunkIndex First chunk index to process (0 = start from beginning; >0 = resume)
   * @param checkpointPath S3 path for the chunk-progress checkpoint file
   */
  private Set<String> processBatch(MaterializationConfig config, Table table,
      Map<String, String> batch, Set<String> excludeAccessions,
      int startChunkIndex, String checkpointPath) throws SQLException, IOException {
    LOGGER.info("Processing batch: {} for table={}", batch.isEmpty() ? "(all)" : batch, config.getSourceTableName());

    Set<String> newAccessions = new HashSet<String>();

    try (Connection conn = getDuckDBConnection(config.getThreads())) {
      // Build source pattern with batch filters applied
      String sourcePattern = config.getSourcePattern();
      for (Map.Entry<String, String> entry : batch.entrySet()) {
        sourcePattern =
            sourcePattern.replace(entry.getKey() + "=*", entry.getKey() + "=" + entry.getValue());
      }
      LOGGER.debug("Resolved source pattern for batch: {}", sourcePattern);

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

      // Fast skip: compare tracked accessions against source accessions from S3 LIST cache
      String year = batch.get("year");
      if (year != null && !excludeAccessions.isEmpty()) {
        Set<String> sourceAccessions =
            getFilteredSourceAccessions(config.getSourcePattern(), year, config.getRowFilter());
        if (sourceAccessions != null && !sourceAccessions.isEmpty()) {
          // Check if all source accessions are already tracked
          boolean allTracked = true;
          for (String sourceAcc : sourceAccessions) {
            if (!excludeAccessions.contains(sourceAcc)) {
              allTracked = false;
              break;
            }
          }
          if (allTracked) {
            LOGGER.info("Fast skip: all {} source accessions for year {} already tracked - skipping query",
                sourceAccessions.size(), year);
            return newAccessions;  // Return empty - nothing new to process
          }
          LOGGER.debug("Source has {} accessions, {} tracked - need to query for new data",
              sourceAccessions.size(), excludeAccessions.size());
        }
      }

      // Try file-list chunking: avoids scanning 100k+ files via a single glob query.
      // Falls back to glob-based processing when S3 LIST data is unavailable.
      List<String> newFilePaths =
          getNewSourceFilePaths(config.getSourcePattern(), year, config.getRowFilter(), excludeAccessions);

      if (newFilePaths != null) {
        if (newFilePaths.isEmpty()) {
          LOGGER.info("File-list optimization: 0 new files for year={}, skipping batch", year);
          return newAccessions;
        }
        LOGGER.info("File-list optimization: {} new files for year={} (chunk size={}, startChunk={})",
            newFilePaths.size(), year, config.getFileChunkSize(), startChunkIndex);
        processWithFileChunkingToIceberg(config, conn, table, newFilePaths,
            partitionValues, typedPartitionFilter, newAccessions,
            startChunkIndex, checkpointPath);
      } else {
        // Fall back to glob-based processing
        if (excludeAccessions != null && excludeAccessions.size() > 100) {
          createExclusionTempTable(conn, config.getAccessionColumn(), excludeAccessions);
        }
        int rowBatchSize = config.getRowBatchSize();
        if (rowBatchSize > 0) {
          processWithRowBatchingToIceberg(config, conn, table, sourcePattern, batch,
              partitionValues, typedPartitionFilter, rowBatchSize, excludeAccessions, newAccessions);
        } else {
          processAllRowsToIceberg(config, conn, table, sourcePattern, batch,
              partitionValues, typedPartitionFilter, excludeAccessions, newAccessions);
        }
      }
    }

    return newAccessions;
  }

  /**
   * Processes all rows at once (original behavior for small tables).
   */
  private void processAllRowsToIceberg(MaterializationConfig config, Connection conn, Table table,
      String sourcePattern, Map<String, String> batch, Map<String, String> partitionValues,
      Map<String, Object> typedPartitionFilter, Set<String> excludeAccessions,
      Set<String> newAccessions) throws SQLException, IOException {

    String sql = buildSelectSql(config, sourcePattern, batch, excludeAccessions);
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

    // Collect accession numbers from rows
    String accessionCol = config.getAccessionColumn();
    for (Map<String, Object> row : rows) {
      Object val = row.get(accessionCol);
      if (val != null) {
        newAccessions.add(val.toString());
      }
    }

    // Write using Iceberg's native Parquet writer
    IcebergTableWriter writer = new IcebergTableWriter(table, storageProvider);
    long writeStart = System.currentTimeMillis();
    org.apache.iceberg.DataFile dataFile = writer.writeRecords(rows, partitionValues);
    long writeElapsed = System.currentTimeMillis() - writeStart;

    if (dataFile != null) {
      writer.commitDataFiles(Collections.singletonList(dataFile), typedPartitionFilter);
      totalRowsWritten += rows.size();
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
      int rowBatchSize, Set<String> excludeAccessions, Set<String> newAccessions)
      throws SQLException, IOException {

    LOGGER.info("Row batching enabled: processing in batches of {} for {}", rowBatchSize, batch);

    // Set DuckDB memory limit to prevent OOM
    try (Statement memStmt = conn.createStatement()) {
      memStmt.execute("SET memory_limit='" + DUCKDB_MEMORY_LIMIT + "'");
      String tempDir = warehousePath != null
          ? warehousePath + "/.duckdb_tmp"
          : System.getProperty("java.io.tmpdir");
      memStmt.execute("SET temp_directory='" + tempDir.replace("'", "''") + "'");
    }

    String accessionCol = config.getAccessionColumn();
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
      String sql =
          buildSelectSqlWithPaging(config, sourcePattern, batch, rowBatchSize, (int) processedRows, excludeAccessions);
      LOGGER.info("Row batch {}: fetching rows {} to {}", batchNum, processedRows, processedRows + rowBatchSize);

      long batchStart = System.currentTimeMillis();

      // Fetch this batch of rows
      List<Map<String, Object>> rows = fetchRows(conn, sql);

      if (rows.isEmpty()) {
        LOGGER.debug("No more rows at offset {}", processedRows);
        break;
      }

      // Collect accession numbers from rows
      for (Map<String, Object> row : rows) {
        Object val = row.get(accessionCol);
        if (val != null) {
          newAccessions.add(val.toString());
        }
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
    totalRowsWritten += processedRows;
    LOGGER.info("Row batching completed: {} batches, {} rows in {}ms",
        batchNum, processedRows, totalElapsed);
  }

  /**
   * Processes source files in explicit chunks to avoid full glob scans over 100k+ files.
   * Each DuckDB query targets at most {@code config.getFileChunkSize()} files via
   * {@code read_parquet(ARRAY[...])}, eliminating the need for LIMIT/OFFSET paging
   * and the NOT EXISTS exclusion filter (files are pre-filtered before this method).
   */
  private void processWithFileChunkingToIceberg(MaterializationConfig config, Connection conn,
      Table table, List<String> newFilePaths, Map<String, String> partitionValues,
      Map<String, Object> typedPartitionFilter, Set<String> newAccessions,
      int startChunkIndex, String checkpointPath)
      throws SQLException, IOException {

    int fileChunkSize = config.getFileChunkSize();
    int totalFiles = newFilePaths.size();
    int totalChunks = (totalFiles + fileChunkSize - 1) / fileChunkSize;
    LOGGER.info("File chunking: {} files in {} chunks of {} for table {} (starting at chunk {})",
        totalFiles, totalChunks, fileChunkSize, config.getTargetTableId(), startChunkIndex);

    IcebergTableWriter writer = new IcebergTableWriter(table, storageProvider);
    List<org.apache.iceberg.DataFile> dataFiles = new ArrayList<org.apache.iceberg.DataFile>();
    int chunkNum = 0;
    long processedRows = 0;
    long totalStartTime = System.currentTimeMillis();
    String accessionCol = config.getAccessionColumn();
    // Track accessions committed in this run to prevent cross-chunk duplicates when
    // staging files have been regenerated and the same accession spans multiple files.
    Set<String> committedInThisRun = new HashSet<String>();

    for (int i = 0; i < totalFiles; i += fileChunkSize) {
      chunkNum++;
      if (chunkNum <= startChunkIndex) {
        LOGGER.debug("File chunk {}/{}: skipping (already committed in previous attempt)", chunkNum, totalChunks);
        continue;
      }
      List<String> chunk = newFilePaths.subList(i, Math.min(i + fileChunkSize, totalFiles));
      String sql = buildSelectSqlForFileList(config, chunk);
      LOGGER.info("File chunk {}/{}: querying {} files", chunkNum, totalChunks, chunk.size());

      long chunkStart = System.currentTimeMillis();
      List<Map<String, Object>> rows = fetchRowsWithTimeout(conn, sql, CHUNK_QUERY_TIMEOUT_SECONDS);
      long chunkElapsed = System.currentTimeMillis() - chunkStart;

      if (rows.isEmpty()) {
        LOGGER.debug("File chunk {}: no rows returned", chunkNum);
        continue;
      }

      // Remove rows for accessions already committed in an earlier chunk of this run.
      if (!committedInThisRun.isEmpty() && accessionCol != null) {
        Iterator<Map<String, Object>> it = rows.iterator();
        int before = rows.size();
        while (it.hasNext()) {
          Object val = it.next().get(accessionCol);
          if (val != null && committedInThisRun.contains(val.toString())) {
            it.remove();
          }
        }
        int removed = before - rows.size();
        if (removed > 0) {
          LOGGER.info("File chunk {}/{}: removed {} duplicate rows (accessions already committed this run)",
              chunkNum, totalChunks, removed);
        }
        if (rows.isEmpty()) {
          LOGGER.debug("File chunk {}: all rows duplicates, skipping", chunkNum);
          continue;
        }
      }

      for (Map<String, Object> row : rows) {
        Object val = row.get(accessionCol);
        if (val != null) {
          String acc = val.toString();
          newAccessions.add(acc);
          committedInThisRun.add(acc);
        }
      }

      LOGGER.info("File chunk {}/{} completed: {} rows in {}ms",
          chunkNum, totalChunks, rows.size(), chunkElapsed);

      org.apache.iceberg.DataFile dataFile = writer.writeRecords(rows, partitionValues);
      if (dataFile != null) {
        dataFiles.add(dataFile);
      }
      processedRows += rows.size();
      rows.clear();

      if (!dataFiles.isEmpty()) {
        writer.commitDataFiles(dataFiles, typedPartitionFilter);
        LOGGER.info("Chunk commit: {} Iceberg files at chunk {}/{}", dataFiles.size(), chunkNum, totalChunks);
        dataFiles.clear();
        dataFiles = new ArrayList<org.apache.iceberg.DataFile>();
        // Checkpoint after every commit so retries resume from the exact chunk rather than chunk 0.
        writeChunkProgress(checkpointPath, chunkNum);
      }
    }

    long totalElapsed = System.currentTimeMillis() - totalStartTime;
    totalRowsWritten += processedRows;
    LOGGER.info("File chunking completed: {}/{} chunks, {} rows in {}ms",
        chunkNum, totalChunks, processedRows, totalElapsed);
  }

  /**
   * Returns the S3 path for the chunk-progress checkpoint file for a given batch.
   * Path: {@code <warehousePath>/chunk-progress/<tableId>/<batchKey>.json}
   */
  private String buildChunkProgressPath(MaterializationConfig config, Map<String, String> batch) {
    List<String> sortedKeys = new ArrayList<String>(batch.keySet());
    Collections.sort(sortedKeys);
    List<String> parts = new ArrayList<String>(sortedKeys.size());
    for (String k : sortedKeys) {
      parts.add(k + "=" + batch.get(k));
    }
    String key = parts.isEmpty() ? "all" : join("_", parts);
    return warehousePath + "/chunk-progress/" + config.getTargetTableId() + "/" + key + ".json";
  }

  private static String join(String sep, List<String> parts) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < parts.size(); i++) {
      if (i > 0) {
        sb.append(sep);
      }
      sb.append(parts.get(i));
    }
    return sb.toString();
  }

  /** Writes {@code {"lastCommittedChunk":N}} to the checkpoint path (1 S3 PUT). */
  private void writeChunkProgress(String path, int lastCommittedChunk) {
    if (storageProvider == null) {
      return;
    }
    try {
      byte[] content = ("{\"lastCommittedChunk\":" + lastCommittedChunk + "}")
          .getBytes(StandardCharsets.UTF_8);
      storageProvider.writeFile(path, content);
      LOGGER.debug("Wrote chunk-progress checkpoint: lastCommittedChunk={} to {}", lastCommittedChunk, path);
    } catch (Exception e) {
      LOGGER.warn("Failed to write chunk-progress checkpoint to {}: {}", path, e.getMessage());
    }
  }

  /**
   * Reads the chunk-progress checkpoint and returns {@code lastCommittedChunk}, or 0 if absent.
   * Returns 0 (start from beginning) on any read or parse failure.
   */
  private int readChunkProgress(String path) {
    if (storageProvider == null) {
      return 0;
    }
    try {
      if (!storageProvider.exists(path)) {
        return 0;
      }
      try (BufferedReader reader =
          new BufferedReader(new InputStreamReader(storageProvider.openInputStream(path), StandardCharsets.UTF_8))) {
        String line = reader.readLine();
        if (line == null) {
          return 0;
        }
        // Parse {"lastCommittedChunk":N} — simple extraction, no JSON lib needed
        int colon = line.indexOf(':');
        int brace = line.lastIndexOf('}');
        if (colon >= 0 && brace > colon) {
          String numStr = line.substring(colon + 1, brace).trim();
          return Integer.parseInt(numStr);
        }
      }
    } catch (Exception e) {
      LOGGER.warn("Failed to read chunk-progress checkpoint from {}: {} — starting from chunk 0",
          path, e.getMessage());
    }
    return 0;
  }

  /** Deletes the chunk-progress checkpoint file after a fully successful batch. */
  private void clearChunkProgress(String path) {
    if (storageProvider == null) {
      return;
    }
    try {
      if (storageProvider.exists(path)) {
        storageProvider.delete(path);
        LOGGER.debug("Cleared chunk-progress checkpoint: {}", path);
      }
    } catch (Exception e) {
      LOGGER.warn("Failed to clear chunk-progress checkpoint {}: {}", path, e.getMessage());
    }
  }

  /**
   * Builds a SELECT SQL using an explicit file list ({@code read_parquet(ARRAY[...])})
   * instead of a glob pattern. No LIMIT/OFFSET or NOT EXISTS needed — the file list is
   * pre-filtered to only new files.
   */
  private String buildSelectSqlForFileList(MaterializationConfig config, List<String> filePaths) {
    StringBuilder sql = new StringBuilder();
    Map<String, String> computedCols = config.getComputedColumns();
    // DISTINCT removes exact-duplicate rows that arise when staging writes the same filing more
    // than once (fetch retried/regenerated). It does NOT collapse legitimately distinct rows of
    // a multi-row-per-filing table (e.g. financial_line_items, filing_contexts), which a
    // PARTITION BY accession dedup would. SEC filings are immutable, so a re-fetch yields
    // identical rows — full-row dedup is exact.
    if (computedCols == null || computedCols.isEmpty()) {
      sql.append("SELECT DISTINCT * FROM ");
    } else {
      sql.append("SELECT DISTINCT *, ");
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

    sql.append("read_parquet(ARRAY[");
    for (int i = 0; i < filePaths.size(); i++) {
      if (i > 0) {
        sql.append(", ");
      }
      sql.append("'").append(filePaths.get(i)).append("'");
    }
    sql.append("], hive_partitioning=true, union_by_name=true)");

    if (config.getRowFilter() != null && !config.getRowFilter().isEmpty()) {
      sql.append(" WHERE ").append(config.getRowFilter());
    }

    return sql.toString();
  }

  /**
   * Fetches rows from a SQL query into a list of maps.
   */
  private List<Map<String, Object>> fetchRows(Connection conn, String sql) throws SQLException {
    List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
    try (Statement stmt = conn.createStatement()) {
      try (ResultSet rs = stmt.executeQuery(sql)) {
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
    }
    return rows;
  }

  /**
   * Executes fetchRows on a daemon thread with a hard Java-level timeout.
   * DuckDB JDBC does not implement Statement.setQueryTimeout(), so R2 network stalls
   * can block indefinitely. On timeout, the connection is closed to unblock the DuckDB
   * thread, and a SQLException is thrown so the caller's retry logic can resume from
   * the last tracker-flushed chunk.
   */
  private List<Map<String, Object>> fetchRowsWithTimeout(
      final Connection conn, final String sql, final long timeoutSeconds) throws SQLException {
    final AtomicReference<List<Map<String, Object>>> result =
        new AtomicReference<List<Map<String, Object>>>();
    final AtomicReference<SQLException> error = new AtomicReference<SQLException>();
    final CountDownLatch latch = new CountDownLatch(1);

    Thread worker = new Thread(new Runnable() {
      @Override public void run() {
        try {
          result.set(fetchRows(conn, sql));
        } catch (SQLException e) {
          error.set(e);
        } finally {
          latch.countDown();
        }
      }
    });
    worker.setDaemon(true);
    worker.setName("duckdb-chunk-query");
    worker.start();

    boolean completed;
    try {
      completed = latch.await(timeoutSeconds, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      try { conn.close(); } catch (Exception closeEx) {
        LOGGER.debug("Failed to close DuckDB connection after interrupt: {}", closeEx.getMessage());
      }
      throw new SQLException("DuckDB chunk query interrupted");
    }

    if (!completed) {
      LOGGER.warn("DuckDB chunk query timed out after {}s — closing connection to unblock",
          timeoutSeconds);
      try { conn.close(); } catch (Exception closeEx) {
        LOGGER.debug("Failed to close DuckDB connection after timeout: {}", closeEx.getMessage());
      }
      throw new SQLException(
          "DuckDB chunk query timed out after " + timeoutSeconds + "s (R2 network stall)");
    }

    if (error.get() != null) {
      throw error.get();
    }
    return result.get();
  }

  /**
   * Builds a SELECT SQL with LIMIT/OFFSET for row batching.
   */
  @SuppressWarnings("UnusedVariable")
  private String buildSelectSqlWithPaging(MaterializationConfig config, String sourcePattern,
      Map<String, String> batch, int limit, int offset, Set<String> excludeAccessions) {
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

    // Build WHERE clause
    boolean hasWhere = false;
    if (config.getRowFilter() != null && !config.getRowFilter().isEmpty()) {
      sql.append(" WHERE ").append(config.getRowFilter());
      hasWhere = true;
    }

    // Add exclusion filter for already-processed accessions
    if (excludeAccessions != null && !excludeAccessions.isEmpty()) {
      sql.append(hasWhere ? " AND " : " WHERE ");
      if (excludeAccessions.size() > 100) {
        // Use anti-join against temp table (much faster for large exclusion lists)
        sql.append("NOT EXISTS (SELECT 1 FROM _exclusions e WHERE e.accession = ")
            .append(config.getAccessionColumn()).append(")");
      } else {
        // Small exclusion list - inline NOT IN is fine
        sql.append(config.getAccessionColumn()).append(" NOT IN (");
        boolean first = true;
        for (String accession : excludeAccessions) {
          if (!first) {
            sql.append(", ");
          }
          sql.append("'").append(accession).append("'");
          first = false;
        }
        sql.append(")");
      }
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
  @SuppressWarnings("UnusedVariable")
  private String buildSelectSql(MaterializationConfig config, String sourcePattern,
      Map<String, String> batch, Set<String> excludeAccessions) {
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

    // Build WHERE clause
    boolean hasWhere = false;
    if (config.getRowFilter() != null && !config.getRowFilter().isEmpty()) {
      sql.append(" WHERE ").append(config.getRowFilter());
      hasWhere = true;
    }

    // Add exclusion filter for already-processed accessions
    if (excludeAccessions != null && !excludeAccessions.isEmpty()) {
      sql.append(hasWhere ? " AND " : " WHERE ");
      if (excludeAccessions.size() > 100) {
        // Use anti-join against temp table (much faster for large exclusion lists)
        sql.append("NOT EXISTS (SELECT 1 FROM _exclusions e WHERE e.accession = ")
            .append(config.getAccessionColumn()).append(")");
      } else {
        // Small exclusion list - inline NOT IN is fine
        sql.append(config.getAccessionColumn()).append(" NOT IN (");
        boolean first = true;
        for (String accession : excludeAccessions) {
          if (!first) {
            sql.append(", ");
          }
          sql.append("'").append(accession).append("'");
          first = false;
        }
        sql.append(")");
      }
    }

    return sql.toString();
  }

  /**
   * Processes data using row-level batching to avoid OOM during expensive computations.
   */
  @SuppressWarnings("UnusedMethod")
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
   * Fast count of source records for a batch (used for skip optimization).
   * Returns count WITHOUT exclusion filter - just base source count.
   */
  @SuppressWarnings("UnusedMethod")
  private long getSourceCountForBatch(Connection conn, MaterializationConfig config,
      String sourcePattern, Map<String, String> batch) {
    try {
      StringBuilder sql = new StringBuilder();
      sql.append("SELECT COUNT(*) FROM ");
      if (config.getSourceFormat() == SourceFormat.JSON) {
        sql.append("read_json('").append(sourcePattern).append("', union_by_name=true)");
      } else {
        sql.append("read_parquet('").append(sourcePattern)
            .append("', hive_partitioning=true, union_by_name=true)");
      }
      // Add batch filters (year, etc.) but NOT exclusion filter
      List<String> conditions = new ArrayList<String>();
      if (config.getRowFilter() != null && !config.getRowFilter().isEmpty()) {
        conditions.add(config.getRowFilter());
      }
      for (Map.Entry<String, String> entry : batch.entrySet()) {
        String col = entry.getKey();
        String val = entry.getValue();
        // Numeric columns don't need quotes
        if (val.matches("-?\\d+")) {
          conditions.add(String.format("\"%s\" = %s", col, val));
        } else {
          conditions.add(String.format("\"%s\" = '%s'", col, val));
        }
      }
      if (!conditions.isEmpty()) {
        sql.append(" WHERE ");
        for (int i = 0; i < conditions.size(); i++) {
          if (i > 0) sql.append(" AND ");
          sql.append(conditions.get(i));
        }
      }

      long startTime = System.currentTimeMillis();
      try (Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery(sql.toString())) {
        if (rs.next()) {
          long count = rs.getLong(1);
          long elapsed = System.currentTimeMillis() - startTime;
          LOGGER.debug("Fast count for {}: {} records in {}ms", batch, count, elapsed);
          return count;
        }
      }
    } catch (Exception e) {
      LOGGER.debug("Fast count failed for {}: {}", batch, e.getMessage());
    }
    return -1;  // Unknown, don't skip
  }

  /**
   * Creates a temp table with exclusion accessions for efficient anti-join filtering.
   * This is much faster than a NOT IN clause with thousands of values.
   */
  @SuppressWarnings("UnusedVariable")
  private void createExclusionTempTable(Connection conn, String accessionCol,
      Set<String> excludeAccessions) throws SQLException {
    long startTime = System.currentTimeMillis();
    try (Statement stmt = conn.createStatement()) {
      // Drop if exists from previous batch
      stmt.execute("DROP TABLE IF EXISTS _exclusions");
      // Create temp table
      stmt.execute("CREATE TEMP TABLE _exclusions (accession VARCHAR PRIMARY KEY)");
    }

    // Batch insert using prepared statement for efficiency
    String insertSql = "INSERT INTO _exclusions VALUES (?)";
    try (java.sql.PreparedStatement pstmt = conn.prepareStatement(insertSql)) {
      int batchCount = 0;
      for (String accession : excludeAccessions) {
        pstmt.setString(1, accession);
        pstmt.addBatch();
        batchCount++;
        if (batchCount % 1000 == 0) {
          pstmt.executeBatch();
        }
      }
      if (batchCount % 1000 != 0) {
        pstmt.executeBatch();
      }
    }

    long elapsed = System.currentTimeMillis() - startTime;
    LOGGER.debug("Created exclusion temp table with {} accessions in {}ms",
        excludeAccessions.size(), elapsed);
  }

  /**
   * Gets source accessions for a year, filtered by the CIKs in the rowFilter if present.
   * This ensures fast-skip comparisons only consider accessions relevant to the current job's CIKs.
   *
   * @param sourcePattern Source pattern with year placeholder
   * @param year The year to list accessions for
   * @param rowFilter Optional row filter that may contain CIK IN clause
   * @return Set of accession numbers (filtered by CIK if applicable), or null if listing fails
   */
  private Set<String> getFilteredSourceAccessions(String sourcePattern, String year,
      String rowFilter) {
    Map<String, Set<String>> cikToAccessions = getSourceAccessions(sourcePattern, year);
    if (cikToAccessions == null) {
      return null;
    }

    // Extract CIKs from rowFilter to narrow down accessions
    Set<String> filterCiks = extractCiksFromRowFilter(rowFilter);
    if (filterCiks.isEmpty()) {
      // No CIK filter - return all accessions (flatten the map)
      Set<String> allAccessions = new HashSet<String>();
      for (Set<String> accessions : cikToAccessions.values()) {
        allAccessions.addAll(accessions);
      }
      return allAccessions;
    }

    // Filter accessions to only those from the specified CIKs.
    // Always include batch files (synthetic CIK "_BATCH_") because CIK cannot be pre-filtered
    // from the filename alone — the row-level rowFilter will handle filtering inside the file.
    Set<String> filteredAccessions = new HashSet<String>();
    Set<String> batchAccessions = cikToAccessions.get("_BATCH_");
    if (batchAccessions != null) {
      filteredAccessions.addAll(batchAccessions);
    }
    for (String cik : filterCiks) {
      Set<String> accessions = cikToAccessions.get(cik);
      if (accessions != null) {
        filteredAccessions.addAll(accessions);
      }
    }
    LOGGER.debug("Filtered source accessions by {} CIKs: {} of {} total",
        filterCiks.size(), filteredAccessions.size(), countAllAccessions(cikToAccessions));
    return filteredAccessions;
  }

  private int countAllAccessions(Map<String, Set<String>> cikToAccessions) {
    int count = 0;
    for (Set<String> accessions : cikToAccessions.values()) {
      count += accessions.size();
    }
    return count;
  }

  /**
   * Returns full S3 paths for source files that have NOT yet been materialized.
   * Calls {@link #getFilteredSourceAccessions} to apply any CIK filter from rowFilter,
   * then subtracts excludeAccessions, and looks up each accession's path from the cache.
   *
   * @return ordered list of S3 paths for new files, empty if all processed,
   *         or null if S3 LIST data is unavailable (caller should fall back to glob)
   */
  private List<String> getNewSourceFilePaths(String sourcePattern, String year,
      String rowFilter, Set<String> excludeAccessions) {
    if (year == null) {
      return null;
    }

    // Trigger S3 LIST (populates both sourceAccessionsCache and sourcePathsCache)
    Set<String> sourceAccessions = getFilteredSourceAccessions(sourcePattern, year, rowFilter);
    if (sourceAccessions == null) {
      return null;
    }

    // Derive cache key to look up paths (same logic as getSourceAccessions)
    String fileSuffix = "_metadata.parquet";
    int lastSlashIdx = sourcePattern.lastIndexOf('/');
    if (lastSlashIdx > 0) {
      String filePattern = sourcePattern.substring(lastSlashIdx + 1);
      int starIdx = filePattern.indexOf('*');
      if (starIdx >= 0 && starIdx < filePattern.length() - 1) {
        fileSuffix = filePattern.substring(starIdx + 1);
      }
    }
    String cacheKey = year + ":" + fileSuffix;
    Map<String, String> pathsMap = sourcePathsCache.get(cacheKey);
    if (pathsMap == null) {
      return null;
    }

    // Backward compat: pre-fix runs wrote "batch" as a single sentinel accession key to mean
    // "all batch-style files for this year were processed." Honour that sentinel so years already
    // tracked as done are not reprocessed. Batch-style accessions have a non-numeric first char;
    // real SEC accessions always start with a digit (e.g. "0000320193-24-123456").
    boolean legacyBatchDone = excludeAccessions != null && excludeAccessions.contains("batch");

    List<String> newPaths = new ArrayList<String>();
    for (String accession : sourceAccessions) {
      boolean isBatchStyle = !accession.isEmpty() && !Character.isDigit(accession.charAt(0));
      if (legacyBatchDone && isBatchStyle) {
        continue; // Skip: legacy tracker sentinel covers all batch files for this year
      }
      if (excludeAccessions == null || !excludeAccessions.contains(accession)) {
        String path = pathsMap.get(accession);
        if (path != null) {
          newPaths.add(path);
        }
      }
    }
    return newPaths;
  }

  /**
   * Extracts CIK values from a rowFilter containing a CIK IN clause.
   * Parses patterns like: {@code cik IN ('0000320193', '0000004962', ...)}
   *
   * @param rowFilter The row filter string, may be null
   * @return Set of CIK strings, empty if no CIK filter found
   */
  static Set<String> extractCiksFromRowFilter(String rowFilter) {
    if (rowFilter == null || rowFilter.isEmpty()) {
      return Collections.emptySet();
    }

    // Find "cik IN (" pattern (case-insensitive)
    String upper = rowFilter.toUpperCase();
    int cikInIdx = upper.indexOf("CIK IN");
    if (cikInIdx < 0) {
      // Also try "cik in" with varying whitespace
      cikInIdx = upper.indexOf("CIK  IN");
      if (cikInIdx < 0) {
        return Collections.emptySet();
      }
    }

    // Find the opening paren
    int openParen = rowFilter.indexOf('(', cikInIdx);
    if (openParen < 0) {
      throw new IllegalStateException("Malformed CIK filter in rowFilter: " + rowFilter);
    }

    // Find the closing paren
    int closeParen = rowFilter.indexOf(')', openParen);
    if (closeParen < 0) {
      throw new IllegalStateException("Malformed CIK filter in rowFilter: " + rowFilter);
    }

    // Parse the values between parens: '0000320193', '0000004962', ...
    String valueList = rowFilter.substring(openParen + 1, closeParen);
    Set<String> ciks = new HashSet<String>();
    for (String token : valueList.split(",")) {
      String trimmed = token.trim();
      // Remove surrounding quotes
      if (trimmed.length() >= 2 && trimmed.charAt(0) == '\''
          && trimmed.charAt(trimmed.length() - 1) == '\'') {
        ciks.add(trimmed.substring(1, trimmed.length() - 1));
      }
    }
    return ciks;
  }

  /**
   * Gets source accessions for a year by listing source files via StorageProvider.
   * Results are cached per year+suffix to avoid repeated S3 LIST operations.
   * Returns a map of CIK to accession sets for CIK-aware filtering.
   *
   * @param sourcePattern Source pattern with year placeholder (e.g., year=*\/*_facts.parquet)
   * @param year The year to list accessions for
   * @return Map of CIK to accession number sets, or null if listing fails
   */
  private Map<String, Set<String>> getSourceAccessions(String sourcePattern, String year) {
    // Extract file suffix from source pattern (e.g., "_facts.parquet" from "year=*/*_facts.parquet")
    String fileSuffix = "_metadata.parquet"; // default
    int lastSlashIdx = sourcePattern.lastIndexOf('/');
    if (lastSlashIdx > 0) {
      String filePattern = sourcePattern.substring(lastSlashIdx + 1);
      // Pattern is like "*_facts.parquet" - extract "_facts.parquet"
      int starIdx = filePattern.indexOf('*');
      if (starIdx >= 0 && starIdx < filePattern.length() - 1) {
        fileSuffix = filePattern.substring(starIdx + 1);
      }
    }

    // For multi-wildcard patterns like "*metadata*.parquet" (fileSuffix = "metadata*.parquet"),
    // split into required substrings for sequential-contains matching instead of giving up.
    // Single-wildcard patterns (fileSuffix has no "*") use the fast endsWith check.
    final String[] suffixParts = fileSuffix.contains("*") ? fileSuffix.split("\\*", -1) : null;

    // Cache key includes year and file suffix
    String cacheKey = year + ":" + fileSuffix;
    if (sourceAccessionsCache.containsKey(cacheKey)) {
      return sourceAccessionsCache.get(cacheKey);
    }
    Map<String, String> pathsMap = new HashMap<String, String>();

    if (storageProvider == null) {
      LOGGER.debug("No storage provider available, skipping source accession cache");
      return null;
    }

    long startTime = System.currentTimeMillis();
    Map<String, Set<String>> cikToAccessions = new HashMap<String, Set<String>>();

    try {
      // Build path for year directory
      // Convert source pattern like "s3://bucket/source=sec/year=*/..." to "s3://bucket/source=sec/year=2022/"
      String basePath = sourcePattern;
      int yearWildcardIdx = basePath.indexOf("year=*");
      if (yearWildcardIdx > 0) {
        basePath = basePath.substring(0, yearWildcardIdx) + "year=" + year + "/";
      } else {
        // No year wildcard - can't use this optimization
        LOGGER.debug("Source pattern doesn't have year=* wildcard, skipping source accession cache");
        return null;
      }

      // List files in the year directory
      List<StorageProvider.FileEntry> files = storageProvider.listFiles(basePath, false);
      for (StorageProvider.FileEntry file : files) {
        String fileName = file.getName();
        // Only process files matching the table's suffix (e.g., _facts.parquet, _metadata.parquet)
        if (suffixParts != null) {
          // Multi-wildcard match: all parts must appear sequentially in the filename
          boolean matches = true;
          int searchFrom = 0;
          for (String part : suffixParts) {
            if (part.isEmpty()) {
              continue;
            }
            int idx = fileName.indexOf(part, searchFrom);
            if (idx < 0) {
              matches = false;
              break;
            }
            searchFrom = idx + part.length();
          }
          if (!matches) {
            continue;
          }
        } else if (!fileName.endsWith(fileSuffix)) {
          continue;
        }
        // Extract CIK and accession from filename.
        // Per-CIK format:  0000001750_0001104659-22-081498_facts.parquet
        //                  ^CIK (digits) ^accession              ^suffix
        // Batch format:    metadata_batch_0005.parquet  (CIK prefix is non-numeric)
        //                  → use the strip-suffix filename as the unique accession key
        int underscoreIdx = fileName.indexOf('_');
        String cik;
        String accession;
        if (underscoreIdx > 0) {
          String cikCandidate = fileName.substring(0, underscoreIdx);
          boolean isNumericCik = !cikCandidate.isEmpty() && cikCandidate.chars().allMatch(Character::isDigit);
          if (isNumericCik) {
            // Per-CIK file: {CIK}_{accession}_{suffix}
            int secondUnderscoreIdx = fileName.indexOf('_', underscoreIdx + 1);
            if (secondUnderscoreIdx <= 0) {
              continue;
            }
            cik = cikCandidate;
            accession = fileName.substring(underscoreIdx + 1, secondUnderscoreIdx);
          } else {
            // Batch file: use strip-suffix filename as accession key, "_BATCH_" as synthetic CIK group
            cik = "_BATCH_";
            int dotIdx = fileName.lastIndexOf('.');
            accession = dotIdx > 0 ? fileName.substring(0, dotIdx) : fileName;
          }
        } else {
          continue;
        }
        Set<String> accessions = cikToAccessions.get(cik);
        if (accessions == null) {
          accessions = new HashSet<String>();
          cikToAccessions.put(cik, accessions);
        }
        accessions.add(accession);
        pathsMap.put(accession, basePath + fileName);
      }

      long elapsed = System.currentTimeMillis() - startTime;
      LOGGER.info("Listed {} source accessions across {} CIKs for year {} ({}) in {}ms",
          countAllAccessions(cikToAccessions), cikToAccessions.size(), year, fileSuffix, elapsed);

      // Cache the result
      sourceAccessionsCache.put(cacheKey, cikToAccessions);
      sourcePathsCache.put(cacheKey, pathsMap);
      return cikToAccessions;

    } catch (Exception e) {
      LOGGER.warn("Failed to list source accessions for year {} ({}): {}",
          year, fileSuffix, e.getMessage());
      return null;
    }
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
  @SuppressWarnings("UnusedMethod")
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
   * Gets accessions that should be excluded from processing (already exist).
   *
   * <p>This method implements two-tier checking:
   * 1. First checks the tracker (cheap, local DuckDB) for tracked accessions
   * 2. For untracked accessions, checks Iceberg table (expensive, S3) for self-healing
   *
   * @param config The materialization config
   * @param table The Iceberg table to check for existing data
   * @param batch The batch being processed (contains year, etc.)
   * @return Set of accession numbers to exclude
   */
  private Set<String> getExcludedAccessions(MaterializationConfig config, Table table,
      Map<String, String> batch) {
    Set<String> excludeAccessions = new HashSet<String>();

    // Skip if accession-level tracking is not configured
    String icebergLocation = config.getIcebergTableLocation();
    if (icebergLocation == null || icebergLocation.isEmpty()) {
      return excludeAccessions;
    }

    String accessionCol = config.getAccessionColumn();
    String yearValue = batch.get("year");

    // Iceberg is the source of truth. Scan it directly whenever the table is reachable.
    // The tracker is a fallback for when Iceberg is unavailable — never an authority.
    if (table != null) {
      try {
        Set<String> icebergAccessions =
            getAccessionsFromIceberg(icebergLocation, accessionCol, yearValue);
        // Sync tracker from Iceberg so retries use the cheap path without a second S3 scan.
        // Only mark accessions the tracker does not already have: re-marking entries that are
        // already complete rewrites one tracker file per accession on every --etl-resume run
        // (the write storm + compaction overhead). getTrackedAccessions reads both the bare and
        // composite key formats, so the delta is correct regardless of how entries were stored.
        Set<String> alreadyTracked = getTrackedAccessions(config.getTargetTableId(), yearValue);
        int newlyMarked = 0;
        for (String accession : icebergAccessions) {
          if (alreadyTracked.contains(accession)) {
            continue;
          }
          Map<String, String> accessionKey = new LinkedHashMap<String, String>();
          if (yearValue != null) {
            accessionKey.put("year", yearValue);
          }
          accessionKey.put(accessionCol, accession);
          incrementalTracker.markProcessed(config.getTargetTableId(),
              config.getSourceTableName(), accessionKey, config.getTargetTableId());
          newlyMarked++;
        }
        LOGGER.info("Iceberg scan: {} committed accessions for {}/year={} ({} newly tracked)",
            icebergAccessions.size(), config.getTargetTableId(), yearValue, newlyMarked);
        return icebergAccessions;
      } catch (Exception e) {
        // Iceberg unreachable — fall back to tracker to avoid re-processing known-committed data.
        Set<String> trackedAccessions =
            getTrackedAccessions(config.getTargetTableId(), yearValue);
        LOGGER.warn("Iceberg scan failed for {}/year={}: {} — using {} tracker entries as fallback",
            config.getTargetTableId(), yearValue, e.getMessage(), trackedAccessions.size());
        return trackedAccessions;
      }
    }

    // No Iceberg table exists yet — no committed accessions to exclude.
    // The tracker may hold stale entries from a previous run; ignore them here so that
    // a deleted-and-reset Iceberg table is rebuilt from scratch rather than skipping
    // accessions that were "complete" in the old table.
    LOGGER.info("No Iceberg table at {} — starting fresh (0 excluded accessions)",
        icebergLocation);
    return excludeAccessions;
  }

  /**
   * Gets accessions that are tracked as processed in the partition status store.
   */
  private Set<String> getTrackedAccessions(String tableId, String yearValue) {
    Set<String> accessions = new HashSet<String>();
    try {
      Set<Map<String, String>> processed = incrementalTracker.getProcessedKeyValues(tableId, yearValue);
      for (Map<String, String> keyValues : processed) {
        for (Map.Entry<String, String> entry : keyValues.entrySet()) {
          if (!"year".equals(entry.getKey())) {
            accessions.add(entry.getValue());
            break;
          }
        }
      }
    } catch (Exception e) {
      LOGGER.debug("Error getting tracked accessions: {}", e.getMessage());
    }
    return accessions;
  }

  /**
   * Gets accessions that exist in the Iceberg table for a given year.
   */
  private Set<String> getAccessionsFromIceberg(String icebergLocation, String accessionCol,
      String yearValue) {
    Set<String> accessions = new HashSet<String>();

    try (Connection conn = getDuckDBConnection(1)) {
      // Query Iceberg table for existing accessions
      String sql = "SELECT DISTINCT " + accessionCol + " FROM iceberg_scan('" + icebergLocation + "', allow_moved_paths=true)";
      if (yearValue != null) {
        sql += " WHERE year = " + yearValue;
      }

      try (Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery(sql)) {
        while (rs.next()) {
          String accession = rs.getString(1);
          if (accession != null) {
            accessions.add(accession);
          }
        }
      }
      LOGGER.debug("Found {} existing accessions in Iceberg for year {}", accessions.size(), yearValue);
    } catch (Exception e) {
      // Table may not exist yet or other error - that's OK, return empty set
      LOGGER.debug("Could not query Iceberg for accessions (table may not exist): {}", e.getMessage());
    }

    return accessions;
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
  @SuppressWarnings("UnusedVariable")
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

      boolean isS3 = globPattern.startsWith("s3://") || globPattern.startsWith("s3a://");
      if (isS3) {
        return getS3SourceFileWatermark(globPattern, ext);
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
   * Computes the max lastModified watermark for S3 source files by listing objects
   * under the non-wildcard base prefix of the source pattern.
   *
   * @param globPattern S3 glob pattern (e.g., "s3://bucket/prefix/year=*&#47;*.parquet")
   * @param ext File extension to filter by (e.g., ".parquet")
   * @return Max lastModified timestamp in milliseconds, or 0 on error
   */
  private long getS3SourceFileWatermark(String globPattern, String ext) {
    // Extract the non-wildcard prefix to use as the S3 listing base path
    String basePath = globPattern;
    int wildcardIdx = basePath.indexOf('*');
    if (wildcardIdx >= 0) {
      basePath = basePath.substring(0, wildcardIdx);
      int lastSlash = basePath.lastIndexOf('/');
      if (lastSlash >= 0) {
        basePath = basePath.substring(0, lastSlash + 1);
      }
    }
    // The recursive LIST below walks the whole basePath subtree (for a year=* pattern that is the
    // entire schema prefix, e.g. sec/). The result is identical for every table sharing that
    // basePath, so cache it per (basePath, ext) to collapse N per-table full-bucket walks into one.
    String wmKey = basePath + ":" + ext;
    Long cachedWm = s3WatermarkCache.get(wmKey);
    if (cachedWm != null) {
      return cachedWm;
    }
    LOGGER.debug("Computing S3 source file watermark for base path: {}", basePath);
    long maxLastModified = 0;
    try {
      List<StorageProvider.FileEntry> files = storageProvider.listFiles(basePath, true);
      for (StorageProvider.FileEntry entry : files) {
        if (!entry.isDirectory() && entry.getPath().endsWith(ext)) {
          long lastMod = entry.getLastModified();
          if (lastMod > maxLastModified) {
            maxLastModified = lastMod;
          }
        }
      }
      s3WatermarkCache.put(wmKey, maxLastModified);
      LOGGER.debug("S3 source file watermark: {} ({} objects scanned, base: {})",
          maxLastModified, files.size(), basePath);
    } catch (IOException e) {
      LOGGER.warn("Failed to compute S3 source file watermark for {}: {}", basePath, e.getMessage());
      return 0;
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
  @SuppressWarnings({"UnusedMethod", "JavaUtilDate"})
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
  @SuppressWarnings("UnusedMethod")
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
      return new ArrayList<Map<String, String>>();
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
      stmt.execute("SET temp_directory='" + DUCKDB_TEMP_DIR + "'");

      // Load extensions
      try {
        stmt.execute("INSTALL parquet");
        stmt.execute("LOAD parquet");
      } catch (SQLException e) {
        LOGGER.debug("Parquet extension already loaded or built-in");
      }
    }

    // Each fallible extension/config below runs on its OWN statement. DuckDB JDBC closes a
    // statement when one of its executes throws; reusing it would make every later execute
    // fail with "Statement was closed" — which previously skipped S3 setup silently and dropped
    // DuckDB back to the AWS default endpoint (HTTP 403 on every read).

    // Load Iceberg extension for iceberg_scan() used in self-healing
    try (Statement stmt = conn.createStatement()) {
      stmt.execute("INSTALL iceberg");
      stmt.execute("LOAD iceberg");
      // Enable version guessing for tables without version-hint file
      stmt.execute("SET unsafe_enable_version_guessing = true");
      LOGGER.debug("Loaded DuckDB Iceberg extension for self-healing queries");
    } catch (SQLException e) {
      LOGGER.debug("Iceberg extension not available: {}", e.getMessage());
    }

    // Configure S3 if available — on its own statement so an extension failure above can't
    // silently skip it.
    Map<String, String> s3Config = storageProvider != null ? storageProvider.getS3Config() : null;
    if (s3Config != null && !s3Config.isEmpty()) {
      try (Statement s3Stmt = conn.createStatement()) {
        configureS3(s3Stmt, s3Config);
      }
    }

    // Load quackformers (community) extension for embedding functions (embed_jina, etc.) LAST,
    // since the community-registry fetch is the most likely to fail and must not poison the
    // critical parquet/iceberg/S3 setup above.
    try (Statement stmt = conn.createStatement()) {
      stmt.execute("INSTALL quackformers FROM community");
      stmt.execute("LOAD quackformers");
      LOGGER.debug("Loaded quackformers extension for embedding functions");
    } catch (SQLException e) {
      LOGGER.debug("Quackformers extension not available: {}", e.getMessage());
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
      // Reduce per-request timeout and retries to avoid silent stalls on R2 httpfs reads.
      // Default (30s timeout × 4 attempts with exponential backoff) can stall for 10+ min per file.
      stmt.execute("SET http_timeout=10000");
      stmt.execute("SET http_retries=2");
      stmt.execute("SET http_retry_wait_ms=500");

      String accessKey = s3Config.get("accessKeyId");
      String secretKey = s3Config.get("secretAccessKey");
      String endpoint = s3Config.get("endpoint");
      String region = s3Config.getOrDefault("region", "auto");

      if (accessKey != null && secretKey != null) {
        StringBuilder secret =
            new StringBuilder("CREATE OR REPLACE SECRET calcite_s3 (TYPE S3");
        secret.append(", KEY_ID '").append(accessKey).append("'");
        secret.append(", SECRET '").append(secretKey).append("'");
        // Apply a custom endpoint only when one is genuinely set. An empty string would emit
        // ENDPOINT '' which DuckDB ignores, silently falling back to AWS s3.us-east-1 (HTTP 403
        // against S3-compatible stores like MinIO/R2). Derive USE_SSL from the scheme: http
        // endpoints (e.g. MinIO) must use plaintext, https (e.g. R2) must use TLS — mirroring
        // S3HivePipelineTracker's proven config.
        if (endpoint != null && !endpoint.isEmpty()) {
          String endpointHost = endpoint.replaceFirst("^https?://", "");
          secret.append(", ENDPOINT '").append(endpointHost).append("'");
          secret.append(", URL_STYLE 'path'");
          secret.append(", USE_SSL ").append(endpoint.startsWith("http://") ? "false" : "true");
        }
        secret.append(", REGION '").append(region).append("'");
        secret.append(")");
        stmt.execute(secret.toString());
        LOGGER.info("Configured DuckDB S3 secret (endpoint={}, region={})",
            endpoint != null && !endpoint.isEmpty() ? endpoint : "<aws-default>", region);
      }
    } catch (SQLException e) {
      // Do not swallow: a failed secret leaves DuckDB on the AWS default endpoint, which
      // surfaces later as a confusing HTTP 403 on every read. Surface it here.
      LOGGER.warn("DuckDB S3 secret configuration failed: {}", e.getMessage());
    }
  }
}
