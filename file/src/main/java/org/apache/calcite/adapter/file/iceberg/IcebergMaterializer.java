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

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.types.Types;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
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
    private final List<String> batchPartitionColumns;
    private final List<String> incrementalKeys;
    private final int startYear;
    private final int endYear;
    private final int threads;
    private final String description;

    private MaterializationConfig(Builder builder) {
      this.sourcePattern = builder.sourcePattern;
      this.sourceFormat = builder.sourceFormat != null ? builder.sourceFormat : SourceFormat.PARQUET;
      this.targetTableId = builder.targetTableId;
      this.sourceTableName = builder.sourceTableName;
      this.partitionColumns = builder.partitionColumns != null
          ? builder.partitionColumns : Collections.<PartitionedTableConfig.ColumnDefinition>emptyList();
      this.batchPartitionColumns = builder.batchPartitionColumns != null
          ? builder.batchPartitionColumns : Collections.<String>emptyList();
      this.incrementalKeys = builder.incrementalKeys != null
          ? builder.incrementalKeys : Collections.<String>emptyList();
      this.startYear = builder.startYear;
      this.endYear = builder.endYear;
      this.threads = builder.threads > 0 ? builder.threads : DEFAULT_THREADS;
      this.description = builder.description;
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
      private List<String> batchPartitionColumns;
      private List<String> incrementalKeys;
      private int startYear;
      private int endYear;
      private int threads;
      private String description;

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

    public MaterializationResult(String tableId, int successCount, int failedCount,
        int skippedCount, long durationMs) {
      this.tableId = tableId;
      this.successCount = successCount;
      this.failedCount = failedCount;
      this.skippedCount = skippedCount;
      this.durationMs = durationMs;
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

    @Override
    public String toString() {
      return String.format("MaterializationResult{table=%s, success=%d, failed=%d, skipped=%d, duration=%dms}",
          tableId, successCount, failedCount, skippedCount, durationMs);
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

    // Ensure Iceberg table exists
    Table table = ensureTableExists(config);
    IcebergTableWriter writer = new IcebergTableWriter(table);

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

      // Check if already processed
      if (config.supportsIncremental() && !incrementalKeyValues.isEmpty()) {
        if (incrementalTracker.isProcessed(config.getTargetTableId(),
            config.getSourceTableName(), incrementalKeyValues)) {
          LOGGER.info("Skipping {} batches for incremental key {} (already processed)",
              batchesForKey.size(), incrementalKeyValues);
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
    MaterializationResult result = new MaterializationResult(
        config.getTargetTableId(), successCount, failedCount, skippedCount, durationMs);

    LOGGER.info("Materialization complete: {}", result);

    // Run maintenance if successful
    if (result.isFullySuccessful()) {
      writer.runMaintenance(7, 1);
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
        LOGGER.warn("Batch {} failed (attempt {}/{}): {}",
            batch, attempts, maxRetries, e.getMessage());

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
   * Processes a single batch: DuckDB transform -> staging -> Iceberg commit.
   */
  private void processBatch(MaterializationConfig config, Table table,
      Map<String, String> batch) throws SQLException, IOException {
    LOGGER.info("Processing batch: {}", batch.isEmpty() ? "(all)" : batch);

    // Create staging path
    Path stagingPath = createStagingPath();

    try (Connection conn = getDuckDBConnection(config.getThreads())) {
      // Build source pattern with batch filters applied
      String sourcePattern = config.getSourcePattern();
      for (Map.Entry<String, String> entry : batch.entrySet()) {
        sourcePattern = sourcePattern.replace(entry.getKey() + "=*",
            entry.getKey() + "=" + entry.getValue());
      }

      // Build DuckDB SQL
      String sql = buildDuckDBSql(config, sourcePattern, stagingPath.toString(), batch);
      LOGGER.debug("Executing DuckDB SQL: {}", sql);

      long startTime = System.currentTimeMillis();
      try (Statement stmt = conn.createStatement()) {
        stmt.execute(sql);
      }
      long elapsed = System.currentTimeMillis() - startTime;
      LOGGER.info("DuckDB transformation completed in {}ms", elapsed);

      // Commit from staging to Iceberg
      IcebergTableWriter writer = new IcebergTableWriter(table);
      Map<String, Object> partitionFilter = new HashMap<String, Object>();
      for (Map.Entry<String, String> entry : batch.entrySet()) {
        // Only add to filter if it's a partition column
        if (config.getPartitionColumnNames().contains(entry.getKey())) {
          partitionFilter.put(entry.getKey(), coerceValue(entry.getValue(),
              findColumnType(config.getPartitionColumns(), entry.getKey())));
        }
      }

      writer.commitFromStaging(stagingPath, partitionFilter.isEmpty() ? null : partitionFilter);

    } finally {
      // Cleanup staging directory (files should already be moved)
      cleanupStagingDirectory(stagingPath);
    }
  }

  /**
   * Builds the DuckDB COPY SQL statement.
   */
  private String buildDuckDBSql(MaterializationConfig config, String sourcePattern,
      String targetPath, Map<String, String> batch) {
    StringBuilder sql = new StringBuilder();
    sql.append("COPY (\n");
    sql.append("  SELECT * FROM ");

    // Use appropriate reader based on source format
    if (config.getSourceFormat() == SourceFormat.JSON) {
      sql.append("read_json('").append(sourcePattern).append("', union_by_name=true)");
    } else {
      sql.append("read_parquet('").append(sourcePattern).append("', hive_partitioning=true, union_by_name=true)");
    }

    sql.append("\n) TO '").append(targetPath).append("'");
    sql.append(" (FORMAT PARQUET");

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
   * Ensures the target Iceberg table exists, creating it if necessary.
   */
  private Table ensureTableExists(MaterializationConfig config) {
    // Infer schema from source if no columns specified
    List<IcebergCatalogManager.ColumnDef> columns = new ArrayList<IcebergCatalogManager.ColumnDef>();
    if (!config.getPartitionColumns().isEmpty()) {
      // Use partition columns as schema for now
      // In a full implementation, we would infer the complete schema from source files
      for (PartitionedTableConfig.ColumnDefinition colDef : config.getPartitionColumns()) {
        columns.add(new IcebergCatalogManager.ColumnDef(colDef.getName(), colDef.getType()));
      }
    }

    // If table exists, load it
    if (IcebergCatalogManager.tableExists(catalogConfig, config.getTargetTableId())) {
      LOGGER.debug("Loading existing table: {}", config.getTargetTableId());
      return IcebergCatalogManager.loadTable(catalogConfig, config.getTargetTableId());
    }

    // Create new table with partition spec
    LOGGER.info("Creating new Iceberg table: {}", config.getTargetTableId());
    return IcebergCatalogManager.createTableFromColumns(
        catalogConfig,
        config.getTargetTableId(),
        columns,
        config.getPartitionColumnNames());
  }

  /**
   * Creates a staging path with timestamp and random suffix.
   */
  private Path createStagingPath() throws IOException {
    String timestamp = new SimpleDateFormat("yyyyMMdd'T'HHmmss'Z'").format(new Date());
    String random = UUID.randomUUID().toString().substring(0, 8);
    Path stagingPath = Paths.get(warehousePath, ".staging", timestamp + "_" + random);
    Files.createDirectories(stagingPath);
    LOGGER.debug("Created staging directory: {}", stagingPath);
    return stagingPath;
  }

  /**
   * Cleans up the staging directory.
   */
  private void cleanupStagingDirectory(Path stagingPath) {
    try {
      if (Files.exists(stagingPath)) {
        Files.walkFileTree(stagingPath, new java.nio.file.SimpleFileVisitor<Path>() {
          @Override
          public java.nio.file.FileVisitResult visitFile(Path file,
              java.nio.file.attribute.BasicFileAttributes attrs) throws IOException {
            Files.deleteIfExists(file);
            return java.nio.file.FileVisitResult.CONTINUE;
          }

          @Override
          public java.nio.file.FileVisitResult postVisitDirectory(Path dir,
              IOException exc) throws IOException {
            Files.deleteIfExists(dir);
            return java.nio.file.FileVisitResult.CONTINUE;
          }
        });
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
      String sql = String.format("SELECT DISTINCT %s FROM %s('%s', %s) WHERE %s IS NOT NULL ORDER BY %s",
          column, reader, sourcePattern, options, column, column);

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

      // Load extensions
      try {
        stmt.execute("INSTALL parquet");
        stmt.execute("LOAD parquet");
      } catch (SQLException e) {
        LOGGER.debug("Parquet extension already loaded or built-in");
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
