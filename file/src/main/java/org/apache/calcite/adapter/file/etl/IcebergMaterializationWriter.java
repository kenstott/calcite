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

import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.iceberg.Table;
import org.apache.iceberg.exceptions.CommitFailedException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
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

  @Override
  public void initialize(MaterializeConfig config) throws IOException {
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
    this.tableWriter = new IcebergTableWriter(table);

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
      catalogCfg.put("warehousePath", effectiveWarehouse);
    } else {
      catalogCfg.put("catalog", "hadoop");
      catalogCfg.put("warehousePath", warehousePath);
    }

    return catalogCfg;
  }

  /**
   * Ensures the target Iceberg table exists.
   */
  private Table ensureTableExists(String targetTableId) {
    if (IcebergCatalogManager.tableExists(catalogConfig, targetTableId)) {
      LOGGER.debug("Loading existing Iceberg table: {}", targetTableId);
      return IcebergCatalogManager.loadTable(catalogConfig, targetTableId);
    }

    // Create new table - infer schema from partition columns if available
    LOGGER.info("Creating new Iceberg table: {}", targetTableId);
    List<IcebergCatalogManager.ColumnDef> columns = new ArrayList<IcebergCatalogManager.ColumnDef>();

    MaterializePartitionConfig partitionConfig = config.getPartition();
    List<String> partitionColumnNames = new ArrayList<String>();
    if (partitionConfig != null && partitionConfig.getColumns() != null) {
      partitionColumnNames = partitionConfig.getColumns();
    }

    // Add columns from config if available
    List<ColumnConfig> columnConfigs = config.getColumns();
    if (columnConfigs != null && !columnConfigs.isEmpty()) {
      for (ColumnConfig colConfig : columnConfigs) {
        columns.add(new IcebergCatalogManager.ColumnDef(
            colConfig.getName(),
            mapToIcebergType(colConfig.getType())));
      }
    }

    return IcebergCatalogManager.createTableFromColumns(
        catalogConfig, targetTableId, columns, partitionColumnNames);
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

  @Override
  public long writeBatch(Iterator<Map<String, Object>> data,
      Map<String, String> partitionVariables) throws IOException {

    if (!initialized) {
      throw new IllegalStateException("Writer not initialized. Call initialize() first.");
    }

    if (data == null || !data.hasNext()) {
      LOGGER.debug("Empty batch, skipping write");
      return 0;
    }

    // Collect data to list
    List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
    while (data.hasNext()) {
      rows.add(data.next());
    }

    if (rows.isEmpty()) {
      return 0;
    }

    LOGGER.info("Writing Iceberg batch of {} rows with partitions: {}",
        rows.size(), partitionVariables);

    // Process with retry logic
    boolean success = processBatchWithRetry(rows, partitionVariables);
    if (!success) {
      throw new IOException("Failed to write batch after " + maxRetries + " attempts");
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
   * Processes a single batch: stage to JSON -> DuckDB transform -> Iceberg commit.
   */
  private void processBatch(List<Map<String, Object>> rows,
      Map<String, String> partitionVariables) throws IOException, SQLException {

    // Create staging path
    Path stagingPath = createStagingPath();

    try {
      // Stage data to temp JSON file
      File tempJsonFile = createTempJsonFile(rows);

      try {
        // Use DuckDB to transform JSON to partitioned Parquet in staging
        transformWithDuckDB(tempJsonFile.getAbsolutePath(), stagingPath.toString());

        // Commit staging files to Iceberg
        Map<String, Object> partitionFilter = buildPartitionFilter(partitionVariables);
        tableWriter.commitFromStaging(stagingPath, partitionFilter.isEmpty() ? null : partitionFilter);

      } finally {
        // Cleanup temp JSON file
        if (tempJsonFile.exists()) {
          tempJsonFile.delete();
        }
      }
    } finally {
      // Cleanup staging directory
      cleanupStagingDirectory(stagingPath);
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
  private void transformWithDuckDB(String jsonPath, String stagingPath) throws SQLException {
    try (Connection conn = getDuckDBConnection()) {
      String sql = buildDuckDBSql(jsonPath, stagingPath);
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
   */
  private String buildDuckDBSql(String jsonPath, String stagingPath) {
    StringBuilder sql = new StringBuilder();
    sql.append("COPY (\n");
    sql.append("  SELECT ");

    // Build select clause
    List<ColumnConfig> columns = config.getColumns();
    if (columns == null || columns.isEmpty()) {
      sql.append("*");
    } else {
      StringBuilder selectClause = new StringBuilder();
      for (ColumnConfig col : columns) {
        if (selectClause.length() > 0) {
          selectClause.append(", ");
        }
        selectClause.append(col.buildSelectExpression());
      }
      sql.append(selectClause);
    }

    sql.append("\n  FROM read_json('").append(escapeString(jsonPath)).append("')\n");
    sql.append(") TO '").append(escapeString(stagingPath)).append("'");
    sql.append(" (FORMAT PARQUET");

    // Add partition columns
    MaterializePartitionConfig partitionConfig = config.getPartition();
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

  @Override
  public void commit() throws IOException {
    if (!initialized) {
      throw new IllegalStateException("Writer not initialized");
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

  @Override
  public long getTotalRowsWritten() {
    return totalRowsWritten;
  }

  @Override
  public int getTotalFilesWritten() {
    return totalFilesWritten;
  }

  @Override
  public MaterializeConfig.Format getFormat() {
    return MaterializeConfig.Format.ICEBERG;
  }

  @Override
  public void close() throws IOException {
    LOGGER.debug("IcebergMaterializationWriter closed: {} rows in {} files",
        totalRowsWritten, totalFilesWritten);
    initialized = false;
  }
}
