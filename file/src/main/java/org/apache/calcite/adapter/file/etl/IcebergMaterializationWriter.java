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
package org.apache.calcite.adapter.file.etl;

import org.apache.calcite.adapter.file.iceberg.IcebergCatalogManager;
import org.apache.calcite.adapter.file.iceberg.IcebergTableWriter;
import org.apache.calcite.adapter.file.partition.IncrementalTracker;
import org.apache.calcite.adapter.file.storage.StorageProvider;

import org.apache.iceberg.Table;
import org.apache.iceberg.exceptions.CommitFailedException;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * MaterializationWriter implementation for Iceberg table output.
 *
 * <p>This writer uses a hybrid DuckDB + Iceberg approach:
 * <ol>
 *   <li>Data is staged to temporary JSON files</li>
 *   <li>DuckDB transforms and writes partitioned Parquet to staging directory</li>
 *   <li>Iceberg commits the staged files atomically</li>
 * </ol>
 *
 * <h3>Key Features</h3>
 * <ul>
 *   <li>Atomic commits via Iceberg transactions</li>
 *   <li>Schema evolution support</li>
 *   <li>Time travel and snapshot management</li>
 *   <li>Retry logic for transient failures</li>
 *   <li>Optional maintenance (snapshot expiration, compaction)</li>
 * </ul>
 *
 * @see MaterializationWriter
 * @see MaterializeConfig.Format#ICEBERG
 */
public class IcebergMaterializationWriter implements MaterializationWriter {

  private static final Logger LOGGER = LoggerFactory.getLogger(IcebergMaterializationWriter.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final int DEFAULT_MAX_RETRIES = 3;
  private static final long DEFAULT_RETRY_DELAY_MS = 1000;
  private static final int DEFAULT_BATCH_SIZE = 100000; // Process 100k rows at a time to avoid OOM

  private final StorageProvider storageProvider;
  private final String warehousePath;
  private final IncrementalTracker incrementalTracker;

  private MaterializeConfig config;
  private Map<String, Object> catalogConfig;
  private Table table;
  private IcebergTableWriter tableWriter;
  private long totalRowsWritten;
  private int totalFilesWritten;
  private boolean initialized;
  private int maxRetries;
  private long retryDelayMs;

  /** Pending staged batches awaiting bulk commit. */
  private final List<StagedBatch> pendingStagedBatches = new ArrayList<StagedBatch>();

  /** Accumulated data files for bulk commit. */
  private final List<org.apache.iceberg.DataFile> pendingDataFiles =
      new ArrayList<org.apache.iceberg.DataFile>();

  /**
   * Represents a staged batch ready for commit.
   */
  private static class StagedBatch {
    final String stagingPath;
    final Map<String, Object> partitionFilter;

    StagedBatch(String stagingPath, Map<String, Object> partitionFilter) {
      this.stagingPath = stagingPath;
      this.partitionFilter = partitionFilter;
    }
  }

  /**
   * Creates a new IcebergMaterializationWriter.
   *
   * @param storageProvider Storage provider for file operations
   * @param warehousePath Path to the Iceberg warehouse
   * @param incrementalTracker Tracker for incremental processing
   */
  public IcebergMaterializationWriter(StorageProvider storageProvider, String warehousePath,
      IncrementalTracker incrementalTracker) {
    this.storageProvider = storageProvider;
    this.warehousePath = warehousePath;
    this.incrementalTracker = incrementalTracker != null ? incrementalTracker : IncrementalTracker.NOOP;
    this.totalRowsWritten = 0;
    this.totalFilesWritten = 0;
    this.initialized = false;
    this.maxRetries = DEFAULT_MAX_RETRIES;
    this.retryDelayMs = DEFAULT_RETRY_DELAY_MS;
  }

  @Override public void initialize(MaterializeConfig config) throws IOException {
    if (config == null) {
      throw new IllegalArgumentException("Config cannot be null");
    }
    if (!config.isEnabled()) {
      throw new IOException("Materialization is disabled in config");
    }
    if (config.getFormat() != MaterializeConfig.Format.ICEBERG) {
      throw new IllegalArgumentException("IcebergMaterializationWriter requires ICEBERG format");
    }

    this.config = config;

    // Apply Iceberg-specific config
    MaterializeConfig.IcebergConfig icebergConfig = config.getIceberg();
    if (icebergConfig != null) {
      this.maxRetries = icebergConfig.getMaxRetries();
      this.retryDelayMs = icebergConfig.getRetryDelayMs();
    }

    // Build catalog configuration
    this.catalogConfig = buildCatalogConfig(icebergConfig);

    // Get target table identifier
    String targetTableId = config.getTargetTableId();
    if (targetTableId == null || targetTableId.isEmpty()) {
      targetTableId = config.getName();
    }
    if (targetTableId == null || targetTableId.isEmpty()) {
      throw new IllegalArgumentException("Target table ID is required for Iceberg format");
    }

    // Ensure table exists
    this.table = ensureTableExists(targetTableId);
    this.tableWriter = new IcebergTableWriter(table, storageProvider);

    LOGGER.info("Initialized IcebergMaterializationWriter: table={}, warehouse={}",
        targetTableId, warehousePath);
    this.initialized = true;
  }

  /**
   * Builds catalog configuration from IcebergConfig.
   */
  private Map<String, Object> buildCatalogConfig(MaterializeConfig.IcebergConfig icebergConfig) {
    Map<String, Object> catalogCfg = new HashMap<String, Object>();

    if (icebergConfig != null) {
      MaterializeConfig.IcebergConfig.CatalogType catalogType = icebergConfig.getCatalogType();
      switch (catalogType) {
        case REST:
          catalogCfg.put("catalog", "rest");
          if (icebergConfig.getRestUri() != null) {
            catalogCfg.put("uri", icebergConfig.getRestUri());
          }
          break;
        case HIVE:
          catalogCfg.put("catalog", "hive");
          break;
        case HADOOP:
        default:
          catalogCfg.put("catalog", "hadoop");
          break;
      }

      String effectiveWarehouse = icebergConfig.getWarehousePath();
      if (effectiveWarehouse == null || effectiveWarehouse.isEmpty()) {
        effectiveWarehouse = warehousePath;
      }
      // Convert s3:// to s3a:// for Hadoop S3A FileSystem compatibility
      catalogCfg.put("warehousePath", convertToS3aScheme(effectiveWarehouse));
    } else {
      catalogCfg.put("catalog", "hadoop");
      // Convert s3:// to s3a:// for Hadoop S3A FileSystem compatibility
      catalogCfg.put("warehousePath", convertToS3aScheme(warehousePath));
    }

    // Add S3 credentials as Hadoop configuration if available from storage provider
    Map<String, String> s3Config = storageProvider != null ? storageProvider.getS3Config() : null;
    if (s3Config != null && !s3Config.isEmpty()) {
      Map<String, String> hadoopConfig = buildHadoopS3Config(s3Config);
      catalogCfg.put("hadoopConfig", hadoopConfig);
      LOGGER.info("Added S3 credentials to Hadoop config for Iceberg catalog: {} keys",
          hadoopConfig.size());
    } else {
      LOGGER.warn("No S3 credentials available from storage provider for Iceberg catalog. "
          + "storageProvider={}, s3Config={}",
          storageProvider != null ? storageProvider.getClass().getSimpleName() : "null",
          s3Config);
    }

    return catalogCfg;
  }

  /**
   * Converts s3:// scheme to s3a:// for Hadoop S3A FileSystem compatibility.
   */
  private String convertToS3aScheme(String path) {
    if (path != null && path.startsWith("s3://")) {
      return "s3a://" + path.substring(5);
    }
    return path;
  }

  /**
   * Builds Hadoop S3A configuration from S3 config map.
   */
  private Map<String, String> buildHadoopS3Config(Map<String, String> s3Config) {
    Map<String, String> hadoopConfig = new HashMap<String, String>();

    // S3A FileSystem implementation
    hadoopConfig.put("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");

    // Credentials
    String accessKey = s3Config.get("accessKeyId");
    String secretKey = s3Config.get("secretAccessKey");
    if (accessKey != null) {
      hadoopConfig.put("fs.s3a.access.key", accessKey);
    }
    if (secretKey != null) {
      hadoopConfig.put("fs.s3a.secret.key", secretKey);
    }

    // Custom endpoint for R2, MinIO, etc.
    String endpoint = s3Config.get("endpoint");
    if (endpoint != null) {
      hadoopConfig.put("fs.s3a.endpoint", endpoint);
      hadoopConfig.put("fs.s3a.path.style.access", "true");
    }

    // Region
    String region = s3Config.get("region");
    if (region != null) {
      hadoopConfig.put("fs.s3a.endpoint.region", region);
    }

    return hadoopConfig;
  }

  /**
   * Ensures the target Iceberg table exists with correct schema.
   *
   * <p>If the table exists but is missing expected data columns (e.g., only has
   * partition columns from a previous buggy run), it will be dropped and recreated.
   */
  private Table ensureTableExists(String targetTableId) {
    // Build expected columns from config
    List<IcebergCatalogManager.ColumnDef> expectedColumns =
        new ArrayList<IcebergCatalogManager.ColumnDef>();
    java.util.Set<String> expectedColumnNames = new java.util.HashSet<String>();

    MaterializePartitionConfig partitionConfig = config.getPartition();
    List<String> partitionColumnNames = new ArrayList<String>();
    if (partitionConfig != null && partitionConfig.getColumns() != null) {
      partitionColumnNames = partitionConfig.getColumns();
    }

    // Add columns from config if available
    List<ColumnConfig> columnConfigs = config.getColumns();
    if (columnConfigs != null && !columnConfigs.isEmpty()) {
      for (ColumnConfig colConfig : columnConfigs) {
        expectedColumns.add(
            new IcebergCatalogManager.ColumnDef(
            colConfig.getName(),
            mapToIcebergType(colConfig.getType())));
        expectedColumnNames.add(colConfig.getName());
      }
    }

    // Add partition columns to schema if not already present
    for (String partitionCol : partitionColumnNames) {
      if (!expectedColumnNames.contains(partitionCol)) {
        expectedColumns.add(new IcebergCatalogManager.ColumnDef(partitionCol, "STRING"));
        expectedColumnNames.add(partitionCol);
      }
    }

    if (IcebergCatalogManager.tableExists(catalogConfig, targetTableId)) {
      Table existingTable = IcebergCatalogManager.loadTable(catalogConfig, targetTableId);

      // Check if existing table has all expected data columns
      // This handles the case where a previous run only created partition columns
      java.util.Set<String> existingColumnNames = new java.util.HashSet<String>();
      for (org.apache.iceberg.types.Types.NestedField field : existingTable.schema().columns()) {
        existingColumnNames.add(field.name());
      }

      // Find missing columns (excluding partition columns which are always present)
      java.util.Set<String> missingDataColumns = new java.util.HashSet<String>();
      for (ColumnConfig colConfig : columnConfigs != null ? columnConfigs
          : java.util.Collections.<ColumnConfig>emptyList()) {
        if (!existingColumnNames.contains(colConfig.getName())) {
          missingDataColumns.add(colConfig.getName());
        }
      }

      if (!missingDataColumns.isEmpty()) {
        LOGGER.warn("Existing Iceberg table '{}' is missing {} data columns: {}. "
            + "Dropping and recreating table.",
            targetTableId, missingDataColumns.size(), missingDataColumns);
        IcebergCatalogManager.dropTable(catalogConfig, targetTableId, true);
      } else {
        LOGGER.debug("Loading existing Iceberg table: {} (schema OK)", targetTableId);
        return existingTable;
      }
    }

    // Create new table
    LOGGER.info("Creating new Iceberg table: {} with {} columns",
        targetTableId, expectedColumns.size());
    return IcebergCatalogManager.createTableFromColumns(
        catalogConfig, targetTableId, expectedColumns, partitionColumnNames);
  }

  /**
   * Maps SQL type to Iceberg type string.
   */
  private String mapToIcebergType(String sqlType) {
    if (sqlType == null) {
      return "STRING";
    }
    String upperType = sqlType.toUpperCase();
    if (upperType.startsWith("VARCHAR") || upperType.startsWith("CHAR")) {
      return "STRING";
    }
    switch (upperType) {
      case "INTEGER":
      case "INT":
        return "INT";
      case "BIGINT":
      case "LONG":
        return "LONG";
      case "DOUBLE":
      case "FLOAT":
        return "DOUBLE";
      case "BOOLEAN":
        return "BOOLEAN";
      case "DATE":
        return "DATE";
      case "TIMESTAMP":
        return "TIMESTAMP";
      default:
        return "STRING";
    }
  }

  @Override public long writeBatch(Iterator<Map<String, Object>> data,
      Map<String, String> partitionVariables) throws IOException {

    if (!initialized) {
      throw new IllegalStateException("Writer not initialized. Call initialize() first.");
    }

    if (data == null || !data.hasNext()) {
      LOGGER.debug("Empty batch, skipping write");
      return 0;
    }

    // Process data in chunks to avoid OOM for large datasets
    long totalRows = 0;
    int chunkNumber = 0;
    List<Map<String, Object>> chunk = new ArrayList<Map<String, Object>>(DEFAULT_BATCH_SIZE);

    while (data.hasNext()) {
      chunk.add(data.next());

      // When chunk is full, process it
      if (chunk.size() >= DEFAULT_BATCH_SIZE) {
        chunkNumber++;
        totalRows += processChunk(chunk, partitionVariables, chunkNumber);
        chunk = new ArrayList<Map<String, Object>>(DEFAULT_BATCH_SIZE);
      }
    }

    // Process remaining rows
    if (!chunk.isEmpty()) {
      chunkNumber++;
      totalRows += processChunk(chunk, partitionVariables, chunkNumber);
    }

    if (chunkNumber > 1) {
      LOGGER.info("Completed writing {} total rows in {} chunks with partitions: {}",
          totalRows, chunkNumber, partitionVariables);
    }

    return totalRows;
  }

  /**
   * Processes a single chunk of rows.
   */
  private long processChunk(List<Map<String, Object>> rows,
      Map<String, String> partitionVariables, int chunkNumber) throws IOException {

    if (rows.isEmpty()) {
      return 0;
    }

    LOGGER.info("Writing Iceberg chunk {}: {} rows with partitions: {}",
        chunkNumber, rows.size(), partitionVariables);

    // Process with retry logic
    boolean success = processBatchWithRetry(rows, partitionVariables);
    if (!success) {
      throw new IOException("Failed to write chunk " + chunkNumber + " after " + maxRetries + " attempts");
    }

    totalRowsWritten += rows.size();
    totalFilesWritten++;

    return rows.size();
  }

  /**
   * Processes a batch with retry logic.
   */
  private boolean processBatchWithRetry(List<Map<String, Object>> rows,
      Map<String, String> partitionVariables) {
    int attempts = 0;

    while (attempts < maxRetries) {
      attempts++;
      try {
        processBatch(rows, partitionVariables);
        return true;
      } catch (CommitFailedException e) {
        LOGGER.warn("Batch already committed by another writer, treating as success");
        return true;
      } catch (Exception e) {
        LOGGER.warn("Batch failed (attempt {}/{}): {}",
            attempts, maxRetries, e.getMessage());

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

    LOGGER.error("Batch failed after {} attempts", maxRetries);
    return false;
  }

  /**
   * Processes a single batch: stage to JSON -> DuckDB transform -> queue for bulk commit.
   *
   * <p>This method stages the data but does NOT commit to Iceberg immediately.
   * The staged files are accumulated and committed in bulk during {@link #commit()}.
   * This reduces the number of Iceberg metadata operations from O(batches) to O(1),
   * significantly improving performance for R2/S3 storage.
   */
  private void processBatch(List<Map<String, Object>> rows,
      Map<String, String> partitionVariables) throws IOException, SQLException {

    // Create staging path
    String stagingPath = createStagingPath();

    // Stage data to temp JSON file
    File tempJsonFile = createTempJsonFile(rows);

    try {
      // Use DuckDB to transform JSON to partitioned Parquet in staging
      transformWithDuckDB(tempJsonFile.getAbsolutePath(), stagingPath.toString(), partitionVariables);

      // Stage files to data location and accumulate DataFile objects for bulk commit
      List<org.apache.iceberg.DataFile> stagedFiles = tableWriter.stageFiles(stagingPath);
      pendingDataFiles.addAll(stagedFiles);

      // Track staging path for cleanup after commit
      pendingStagedBatches.add(new StagedBatch(stagingPath, null));

      LOGGER.debug("Staged batch {} ({} files) for bulk commit: {}",
          pendingStagedBatches.size(), stagedFiles.size(), partitionVariables);

    } finally {
      // Cleanup temp JSON file (staging directory cleaned up after commit)
      if (tempJsonFile.exists()) {
        tempJsonFile.delete();
      }
    }
  }

  /**
   * Creates a temporary JSON file with batch data.
   */
  private File createTempJsonFile(List<Map<String, Object>> rows) throws IOException {
    String timestamp = new SimpleDateFormat("yyyyMMdd_HHmmss").format(new Date());
    File tempFile = File.createTempFile("iceberg_batch_" + timestamp + "_", ".json");

    try (BufferedWriter writer = new BufferedWriter(new FileWriter(tempFile))) {
      for (Map<String, Object> row : rows) {
        writer.write(MAPPER.writeValueAsString(row));
        writer.newLine();
      }
    }

    LOGGER.debug("Created temp JSON file: {} ({} rows)", tempFile.getAbsolutePath(), rows.size());
    return tempFile;
  }

  /**
   * Uses DuckDB to transform JSON to partitioned Parquet.
   */
  private void transformWithDuckDB(String jsonPath, String stagingPath,
      Map<String, String> partitionVariables) throws SQLException {
    try (Connection conn = getDuckDBConnection()) {
      String sql = buildDuckDBSql(jsonPath, stagingPath, partitionVariables);
      LOGGER.debug("Executing DuckDB SQL:\n{}", sql);

      long startTime = System.currentTimeMillis();
      try (Statement stmt = conn.createStatement()) {
        stmt.execute(sql);
      }
      long elapsed = System.currentTimeMillis() - startTime;
      LOGGER.debug("DuckDB transformation completed in {}ms", elapsed);
    }
  }

  /**
   * Builds the DuckDB COPY SQL statement.
   *
   * <p>Partition variables (dimension values) are injected as literal columns
   * in the SELECT clause if they're not already present in the source data.
   * This allows Hive-style partitioning where partition values come from
   * the ETL dimension iteration rather than the source data itself.
   *
   * <p>Uses a CTE (WITH clause) to first load JSON data, then SELECT from it.
   * This resolves DuckDB's column reference ambiguity where computed expressions
   * reference source columns that are also being selected (e.g., both selecting
   * TableName and using SUBSTR(TableName, 1, 2) in the same SELECT).
   */
  private String buildDuckDBSql(String jsonPath, String stagingPath,
      Map<String, String> partitionVariables) {
    StringBuilder sql = new StringBuilder();
    sql.append("COPY (\n");
    sql.append("  SELECT ");

    // Build select clause with qualified column references to avoid DuckDB ambiguity
    List<ColumnConfig> columns = config.getColumns();
    if (columns == null || columns.isEmpty()) {
      sql.append("*");
    } else {
      // Build set of source column names (non-computed columns)
      java.util.Set<String> sourceColumns = new java.util.HashSet<String>();
      for (ColumnConfig col : columns) {
        if (!col.isComputed()) {
          sourceColumns.add(col.getEffectiveSource());
        }
      }

      // Build SELECT clause with qualified column references and partition variable substitution
      StringBuilder selectClause = new StringBuilder();
      for (ColumnConfig col : columns) {
        if (selectClause.length() > 0) {
          selectClause.append(", ");
        }
        selectClause.append(col.buildSelectExpression("src", sourceColumns, partitionVariables));
      }
      sql.append(selectClause);
    }

    // Add partition variables as literal columns (if not already in source)
    // These are dimension values that need to be written into the parquet files
    MaterializePartitionConfig partitionConfig = config.getPartition();
    if (partitionConfig != null && partitionVariables != null && !partitionVariables.isEmpty()) {
      for (String partitionCol : partitionConfig.getColumns()) {
        if (partitionVariables.containsKey(partitionCol)) {
          sql.append(", '").append(escapeString(partitionVariables.get(partitionCol)))
              .append("' AS ").append(partitionCol);
        }
      }
    }

    sql.append("\n  FROM read_json('").append(escapeString(jsonPath)).append("') AS src\n");
    sql.append(") TO '").append(escapeString(stagingPath)).append("'");
    sql.append(" (FORMAT PARQUET");

    // Add partition columns
    if (partitionConfig != null && !partitionConfig.getColumns().isEmpty()) {
      sql.append(", PARTITION_BY (");
      List<String> partitionCols = partitionConfig.getColumns();
      for (int i = 0; i < partitionCols.size(); i++) {
        if (i > 0) {
          sql.append(", ");
        }
        sql.append(partitionCols.get(i));
      }
      sql.append(")");
    }

    sql.append(", OVERWRITE_OR_IGNORE");
    sql.append(");");

    return sql.toString();
  }

  /**
   * Builds partition filter from partition variables.
   */
  private Map<String, Object> buildPartitionFilter(Map<String, String> partitionVariables) {
    Map<String, Object> filter = new HashMap<String, Object>();

    if (partitionVariables == null || partitionVariables.isEmpty()) {
      return filter;
    }

    MaterializePartitionConfig partitionConfig = config.getPartition();
    List<String> partitionCols = partitionConfig != null
        ? partitionConfig.getColumns()
        : new ArrayList<String>();

    for (Map.Entry<String, String> entry : partitionVariables.entrySet()) {
      if (partitionCols.contains(entry.getKey())) {
        filter.put(entry.getKey(), entry.getValue());
      }
    }

    return filter;
  }

  /**
   * Creates a staging path with timestamp and random suffix.
   * Uses StorageProvider to resolve and create the path, supporting both local and S3 storage.
   */
  private String createStagingPath() throws IOException {
    String timestamp = new SimpleDateFormat("yyyyMMdd'T'HHmmss'Z'").format(new Date());
    String random = UUID.randomUUID().toString().substring(0, 8);
    String stagingSubpath = ".staging/" + timestamp + "_" + random;
    String stagingPath = storageProvider.resolvePath(warehousePath, stagingSubpath);
    // StorageProvider handles both local (Files.createDirectories) and S3 (marker object)
    storageProvider.createDirectories(stagingPath);
    LOGGER.debug("Created staging path: {}", stagingPath);
    return stagingPath;
  }

  /**
   * Cleans up the staging directory using StorageProvider.
   * Works for both local and S3 storage.
   */
  private void cleanupStagingDirectory(String stagingPath) {
    try {
      if (storageProvider.exists(stagingPath)) {
        // List all files recursively and delete them
        List<StorageProvider.FileEntry> files = storageProvider.listFiles(stagingPath, true);
        List<String> paths = new ArrayList<String>();
        for (StorageProvider.FileEntry entry : files) {
          paths.add(entry.getPath());
        }
        // Delete all files in batch (more efficient for S3)
        if (!paths.isEmpty()) {
          storageProvider.deleteBatch(paths);
        }
        // Delete the staging directory itself
        storageProvider.delete(stagingPath);
      }
    } catch (IOException e) {
      LOGGER.warn("Failed to cleanup staging directory {}: {}", stagingPath, e.getMessage());
    }
  }

  /**
   * Creates a DuckDB connection with required extensions.
   */
  private Connection getDuckDBConnection() throws SQLException {
    Connection conn = DriverManager.getConnection("jdbc:duckdb:");

    MaterializeOptionsConfig optionsConfig = config.getOptions();
    int threads = optionsConfig != null ? optionsConfig.getThreads() : 2;

    try (Statement stmt = conn.createStatement()) {
      stmt.execute("SET threads=" + threads);
      stmt.execute("SET preserve_insertion_order=false");

      try {
        stmt.execute("INSTALL parquet");
        stmt.execute("LOAD parquet");
        stmt.execute("INSTALL json");
        stmt.execute("LOAD json");
      } catch (SQLException e) {
        LOGGER.debug("Extensions already loaded or built-in");
      }

      // Configure S3 if needed
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

  /**
   * Escapes a string for SQL.
   */
  private static String escapeString(String value) {
    return value.replace("'", "''");
  }

  @Override public void commit() throws IOException {
    if (!initialized) {
      throw new IllegalStateException("Writer not initialized");
    }

    // Bulk commit all pending data files in a single Iceberg transaction
    if (!pendingDataFiles.isEmpty()) {
      LOGGER.info("Bulk committing {} data files from {} batches to Iceberg",
          pendingDataFiles.size(), pendingStagedBatches.size());
      long commitStart = System.currentTimeMillis();

      try {
        // Single Iceberg commit for all accumulated files - O(1) metadata operations
        tableWriter.bulkCommitDataFiles(pendingDataFiles);

        long commitElapsed = System.currentTimeMillis() - commitStart;
        LOGGER.info("Bulk commit complete: {} files in {}ms", pendingDataFiles.size(), commitElapsed);
      } catch (Exception e) {
        LOGGER.error("Bulk commit failed: {}", e.getMessage());
        throw new IOException("Bulk commit failed", e);
      } finally {
        // Cleanup all staging directories after commit (success or failure)
        for (StagedBatch batch : pendingStagedBatches) {
          cleanupStagingDirectory(batch.stagingPath);
        }
        pendingStagedBatches.clear();
        pendingDataFiles.clear();
      }
    }

    // Run maintenance if configured
    MaterializeConfig.IcebergConfig icebergConfig = config.getIceberg();
    if (icebergConfig != null && icebergConfig.isRunMaintenance()) {
      int retentionDays = icebergConfig.getSnapshotRetentionDays();
      LOGGER.info("Running Iceberg maintenance with {}d snapshot retention", retentionDays);
      tableWriter.runMaintenance(retentionDays, 1);
    }

    LOGGER.info("Iceberg commit complete: {} rows in {} batches",
        totalRowsWritten, totalFilesWritten);
  }

  @Override public long getTotalRowsWritten() {
    return totalRowsWritten;
  }

  @Override public int getTotalFilesWritten() {
    return totalFilesWritten;
  }

  @Override public MaterializeConfig.Format getFormat() {
    return MaterializeConfig.Format.ICEBERG;
  }

  @Override public String getTableLocation() {
    if (table != null) {
      // Return the Iceberg table location (e.g., s3://bucket/warehouse/table_name)
      // Convert s3a:// back to s3:// for consistency with DuckDB
      String location = table.location();
      if (location != null && location.startsWith("s3a://")) {
        location = "s3://" + location.substring(6);
      }
      return location;
    }
    return null;
  }

  @Override public void close() throws IOException {
    // Cleanup any uncommitted staging directories to prevent disk space leaks
    if (!pendingStagedBatches.isEmpty() || !pendingDataFiles.isEmpty()) {
      LOGGER.warn("Closing writer with {} uncommitted batches ({} files) - cleaning up",
          pendingStagedBatches.size(), pendingDataFiles.size());
      for (StagedBatch batch : pendingStagedBatches) {
        cleanupStagingDirectory(batch.stagingPath);
      }
      pendingStagedBatches.clear();
      pendingDataFiles.clear();
    }

    LOGGER.debug("IcebergMaterializationWriter closed: {} rows in {} files",
        totalRowsWritten, totalFilesWritten);
    initialized = false;
  }
}
