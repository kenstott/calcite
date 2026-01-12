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

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Table;
import org.apache.iceberg.exceptions.CommitFailedException;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
  private static final int DEFAULT_BATCH_SIZE = 10000; // Process 10k rows at a time to avoid OOM

  /** DuckDB memory limit - from DUCKDB_MEMORY_LIMIT env var, default 4GB. */
  private static final String DUCKDB_MEMORY_LIMIT =
      System.getenv("DUCKDB_MEMORY_LIMIT") != null
          ? System.getenv("DUCKDB_MEMORY_LIMIT") : "4GB";

  private final StorageProvider storageProvider;
  private final String warehousePath;
  private final IncrementalTracker incrementalTracker;

  private MaterializeConfig config;
  private Map<String, Object> catalogConfig;
  private Configuration hadoopConfiguration;
  private Table table;
  private IcebergTableWriter tableWriter;
  private long totalRowsWritten;
  private int totalFilesWritten;
  private boolean initialized;
  private int maxRetries;
  private long retryDelayMs;
  private int batchSize;
  private MaterializeOptionsConfig.StagingMode stagingMode;
  private Connection sharedDuckDBConnection;

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

    // Apply options config (batch size, staging mode)
    MaterializeOptionsConfig optionsConfig = config.getOptions();
    if (optionsConfig != null) {
      this.batchSize = optionsConfig.getBatchSize();
      this.stagingMode = optionsConfig.getStagingMode();
    } else {
      this.batchSize = DEFAULT_BATCH_SIZE;
      this.stagingMode = MaterializeOptionsConfig.StagingMode.REMOTE;
    }
    LOGGER.info("Using batchSize={}, stagingMode={}", batchSize, stagingMode);

    // Build catalog configuration
    this.catalogConfig = buildCatalogConfig(icebergConfig);

    // Build Hadoop configuration from catalog config (includes S3 credentials)
    this.hadoopConfiguration = buildHadoopConfiguration();

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
    this.tableWriter = new IcebergTableWriter(table, storageProvider, hadoopConfiguration);

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
   * Builds Hadoop Configuration from catalogConfig.
   *
   * <p>Extracts the hadoopConfig map from catalogConfig and creates
   * a Configuration object with S3 credentials for file access.
   */
  @SuppressWarnings("unchecked")
  private Configuration buildHadoopConfiguration() {
    Configuration conf = new Configuration();

    // Extract hadoopConfig from catalogConfig
    Object hadoopConfigObj = catalogConfig.get("hadoopConfig");
    if (hadoopConfigObj instanceof Map) {
      Map<String, String> hadoopConfigMap = (Map<String, String>) hadoopConfigObj;
      for (Map.Entry<String, String> entry : hadoopConfigMap.entrySet()) {
        conf.set(entry.getKey(), entry.getValue());
      }
      LOGGER.debug("Built Hadoop configuration with {} properties", hadoopConfigMap.size());
    }

    return conf;
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

    // Add columns from config if available, and build maps for partition column lookup
    // Use lowercase keys for case-insensitive matching (DuckDB/Iceberg are case-insensitive)
    List<ColumnConfig> columnConfigs = config.getColumns();
    Map<String, String> columnTypeMap = new java.util.HashMap<String, String>();
    java.util.Set<String> expectedColumnNamesLower = new java.util.HashSet<String>();
    // Map lowercase column name -> actual column name (for partition spec)
    Map<String, String> lowerToActualName = new java.util.HashMap<String, String>();
    if (columnConfigs != null && !columnConfigs.isEmpty()) {
      for (ColumnConfig colConfig : columnConfigs) {
        String icebergType = mapToIcebergType(colConfig.getType());
        expectedColumns.add(
            new IcebergCatalogManager.ColumnDef(
            colConfig.getName(),
            icebergType));
        expectedColumnNames.add(colConfig.getName());
        String lowerName = colConfig.getName().toLowerCase(java.util.Locale.ROOT);
        expectedColumnNamesLower.add(lowerName);
        columnTypeMap.put(lowerName, icebergType);
        lowerToActualName.put(lowerName, colConfig.getName());
      }
    }

    // Build partition column type map from partition config's columnDefinitions
    Map<String, String> partitionColumnTypeMap = new java.util.HashMap<String, String>();
    if (partitionConfig != null && partitionConfig.getColumnDefinitions() != null) {
      for (MaterializePartitionConfig.ColumnDefinition colDef
          : partitionConfig.getColumnDefinitions()) {
        String lowerName = colDef.getName().toLowerCase(java.util.Locale.ROOT);
        partitionColumnTypeMap.put(lowerName, mapToIcebergType(colDef.getType()));
      }
    }

    // Add partition columns to schema if not already present (case-insensitive check)
    // Use the column type from partition columnDefinitions, then data columns, then STRING
    // Skip adding partition columns that already exist in source data to avoid duplicates
    // Also build the actual partition column names list for Iceberg partition spec
    List<String> actualPartitionColumnNames = new ArrayList<String>();
    for (String partitionCol : partitionColumnNames) {
      String partitionColLower = partitionCol.toLowerCase(java.util.Locale.ROOT);
      if (!expectedColumnNamesLower.contains(partitionColLower)) {
        // Partition column doesn't exist in source - add it
        // Priority: partitionColumnTypeMap > columnTypeMap > STRING
        String partitionType;
        if (partitionColumnTypeMap.containsKey(partitionColLower)) {
          partitionType = partitionColumnTypeMap.get(partitionColLower);
        } else if (columnTypeMap.containsKey(partitionColLower)) {
          partitionType = columnTypeMap.get(partitionColLower);
        } else {
          partitionType = "STRING";
        }
        expectedColumns.add(new IcebergCatalogManager.ColumnDef(partitionCol, partitionType));
        expectedColumnNames.add(partitionCol);
        expectedColumnNamesLower.add(partitionColLower);
        lowerToActualName.put(partitionColLower, partitionCol);
        actualPartitionColumnNames.add(partitionCol);
        LOGGER.debug("Adding partition column '{}' with type '{}' to Iceberg schema",
            partitionCol, partitionType);
      } else {
        // Partition column exists in source (case-insensitive) - use source column's actual name
        String actualName = lowerToActualName.get(partitionColLower);
        actualPartitionColumnNames.add(actualName);
        LOGGER.debug("Partition column '{}' maps to source column '{}' (case-insensitive match)",
            partitionCol, actualName);
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
    LOGGER.info("Creating new Iceberg table: {} with {} columns, partitioned by {}",
        targetTableId, expectedColumns.size(), actualPartitionColumnNames);
    return IcebergCatalogManager.createTableFromColumns(
        catalogConfig, targetTableId, expectedColumns, actualPartitionColumnNames);
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

  /**
   * Maps YAML column types to DuckDB types.
   * Used for generating CAST expressions in SQL queries.
   */
  private String mapToDuckDBType(String yamlType) {
    if (yamlType == null) {
      return "VARCHAR";
    }
    String upperType = yamlType.toUpperCase();
    if (upperType.startsWith("VARCHAR") || upperType.startsWith("CHAR")
        || upperType.equals("STRING")) {
      return "VARCHAR";
    }
    switch (upperType) {
      case "INTEGER":
      case "INT":
        return "INTEGER";
      case "BIGINT":
      case "LONG":
        return "BIGINT";
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
        return "VARCHAR";
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
    List<Map<String, Object>> chunk = new ArrayList<Map<String, Object>>(batchSize);

    while (data.hasNext()) {
      chunk.add(data.next());

      // When chunk is full, process it
      if (chunk.size() >= batchSize) {
        chunkNumber++;
        totalRows += processChunk(chunk, partitionVariables, chunkNumber);
        chunk = new ArrayList<Map<String, Object>>(batchSize);
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
   * Processes a single batch using Iceberg's native Parquet writer.
   *
   * <p>This method writes data using Iceberg's GenericParquetWriter which embeds
   * proper field IDs in the Parquet schema. This is required for Iceberg readers
   * (including DuckDB's iceberg_scan) to correctly map columns.
   *
   * <p>The staged files are accumulated and committed in bulk during {@link #commit()}.
   * This reduces the number of Iceberg metadata operations from O(batches) to O(1).
   */
  private void processBatch(List<Map<String, Object>> rows,
      Map<String, String> partitionVariables) throws IOException, SQLException {

    // Transform rows: map source field names to target column names
    List<Map<String, Object>> transformedRows = transformRows(rows, partitionVariables);

    // Use Iceberg's native Parquet writer with proper field IDs
    org.apache.iceberg.DataFile dataFile = tableWriter.writeRecords(transformedRows, partitionVariables);

    if (dataFile != null) {
      pendingDataFiles.add(dataFile);
      LOGGER.debug("Staged batch {} (1 file) for bulk commit: {}",
          pendingStagedBatches.size() + 1, partitionVariables);
    }
  }

  /**
   * Transforms rows from source field names to target column names.
   *
   * <p>Applies column mappings defined in the config:
   * <ul>
   *   <li>Direct columns: maps source field name to target column name</li>
   *   <li>Computed columns: evaluates simple expressions (partition variable substitution)</li>
   *   <li>Partition columns: injects partition variable values</li>
   * </ul>
   */
  private List<Map<String, Object>> transformRows(List<Map<String, Object>> rows,
      Map<String, String> partitionVariables) {
    List<ColumnConfig> columns = config.getColumns();
    if (columns == null || columns.isEmpty()) {
      return rows;  // No transformation needed
    }

    List<Map<String, Object>> transformed = new ArrayList<Map<String, Object>>(rows.size());
    for (Map<String, Object> row : rows) {
      Map<String, Object> newRow = new HashMap<String, Object>();

      for (ColumnConfig col : columns) {
        String targetName = col.getName();
        Object value = null;

        if (col.isComputed()) {
          // For computed columns, evaluate expressions that reference source columns
          String expr = col.getExpression();
          if (expr != null) {
            // First check for partition variable substitution
            if (partitionVariables != null) {
              for (Map.Entry<String, String> pv : partitionVariables.entrySet()) {
                String placeholder = "{" + pv.getKey() + "}";
                if (expr.equals(placeholder) || expr.equals("'" + placeholder + "'")) {
                  value = pv.getValue();
                  break;
                }
              }
            }

            // If not a partition variable, try to evaluate the expression
            if (value == null) {
              value = evaluateExpression(expr, row);
            }
          }
        } else {
          // Direct column: look up by source name
          String sourceName = col.getEffectiveSource();
          value = getValueCaseInsensitive(row, sourceName);

          // If not found in row, check partition variables
          if (value == null && partitionVariables != null) {
            value = getValueCaseInsensitive(partitionVariables, targetName);
            if (value == null) {
              value = getValueCaseInsensitive(partitionVariables, sourceName);
            }
          }
        }

        if (value != null) {
          newRow.put(targetName, value);
        }
      }

      // Also add partition variables directly if not already present
      if (partitionVariables != null) {
        for (Map.Entry<String, String> pv : partitionVariables.entrySet()) {
          if (!newRow.containsKey(pv.getKey())) {
            newRow.put(pv.getKey(), pv.getValue());
          }
        }
      }

      transformed.add(newRow);
    }
    return transformed;
  }

  /**
   * Gets a value from a map using case-insensitive key lookup.
   */
  private Object getValueCaseInsensitive(Map<String, ?> map, String key) {
    if (map == null || key == null) {
      return null;
    }
    // First try exact match
    Object value = map.get(key);
    if (value != null) {
      return value;
    }
    // Then try case-insensitive match
    for (Map.Entry<String, ?> entry : map.entrySet()) {
      if (entry.getKey().equalsIgnoreCase(key)) {
        return entry.getValue();
      }
    }
    return null;
  }

  /**
   * Evaluates a simple SQL expression against a source row.
   *
   * <p>Supports expressions like:
   * <ul>
   *   <li>{@code src."FIELDNAME"} - simple column reference</li>
   *   <li>{@code TRY_CAST(src."FIELDNAME" AS BIGINT)} - type conversion</li>
   *   <li>{@code TRY_CAST(src."FIELDNAME" AS DOUBLE)} - numeric conversion</li>
   * </ul>
   *
   * @param expr The SQL expression to evaluate
   * @param row The source data row
   * @return The evaluated value, or null if expression cannot be evaluated
   */
  private Object evaluateExpression(String expr, Map<String, Object> row) {
    if (expr == null || expr.isEmpty()) {
      return null;
    }

    // Pattern: src."FIELDNAME" or src.FIELDNAME
    java.util.regex.Pattern srcFieldPattern = java.util.regex.Pattern.compile(
        "^\\s*src\\.\"?([A-Za-z0-9_]+)\"?\\s*$");
    java.util.regex.Matcher srcFieldMatcher = srcFieldPattern.matcher(expr);
    if (srcFieldMatcher.matches()) {
      String fieldName = srcFieldMatcher.group(1);
      return getValueCaseInsensitive(row, fieldName);
    }

    // Pattern: TRY_CAST(src."FIELDNAME" AS TYPE) or CAST(src."FIELDNAME" AS TYPE)
    java.util.regex.Pattern castPattern = java.util.regex.Pattern.compile(
        "^\\s*(?:TRY_)?CAST\\s*\\(\\s*src\\.\"?([A-Za-z0-9_]+)\"?\\s+AS\\s+(\\w+)\\s*\\)\\s*$",
        java.util.regex.Pattern.CASE_INSENSITIVE);
    java.util.regex.Matcher castMatcher = castPattern.matcher(expr);
    if (castMatcher.matches()) {
      String fieldName = castMatcher.group(1);
      String targetType = castMatcher.group(2).toUpperCase(java.util.Locale.ROOT);
      Object sourceValue = getValueCaseInsensitive(row, fieldName);
      return castValue(sourceValue, targetType);
    }

    // Pattern: REPLACE(src."FIELDNAME", 'old', 'new') - for comma handling in numbers
    java.util.regex.Pattern replacePattern = java.util.regex.Pattern.compile(
        "^\\s*REPLACE\\s*\\(\\s*src\\.\"?([A-Za-z0-9_]+)\"?\\s*,\\s*'([^']*)'\\s*,\\s*'([^']*)'\\s*\\)\\s*$",
        java.util.regex.Pattern.CASE_INSENSITIVE);
    java.util.regex.Matcher replaceMatcher = replacePattern.matcher(expr);
    if (replaceMatcher.matches()) {
      String fieldName = replaceMatcher.group(1);
      String oldStr = replaceMatcher.group(2);
      String newStr = replaceMatcher.group(3);
      Object sourceValue = getValueCaseInsensitive(row, fieldName);
      if (sourceValue != null) {
        return sourceValue.toString().replace(oldStr, newStr);
      }
      return null;
    }

    // Pattern: TRY_CAST(REPLACE(src."FIELDNAME", 'old', 'new') AS TYPE)
    java.util.regex.Pattern castReplacePattern = java.util.regex.Pattern.compile(
        "^\\s*(?:TRY_)?CAST\\s*\\(\\s*REPLACE\\s*\\(\\s*src\\.\"?([A-Za-z0-9_]+)\"?\\s*,\\s*'([^']*)'\\s*,\\s*'([^']*)'\\s*\\)\\s+AS\\s+(\\w+)\\s*\\)\\s*$",
        java.util.regex.Pattern.CASE_INSENSITIVE);
    java.util.regex.Matcher castReplaceMatcher = castReplacePattern.matcher(expr);
    if (castReplaceMatcher.matches()) {
      String fieldName = castReplaceMatcher.group(1);
      String oldStr = castReplaceMatcher.group(2);
      String newStr = castReplaceMatcher.group(3);
      String targetType = castReplaceMatcher.group(4).toUpperCase(java.util.Locale.ROOT);
      Object sourceValue = getValueCaseInsensitive(row, fieldName);
      if (sourceValue != null) {
        String replaced = sourceValue.toString().replace(oldStr, newStr);
        return castValue(replaced, targetType);
      }
      return null;
    }

    // Pattern: COALESCE(src."FIELD1", src."FIELD2", ...) - returns first non-null value
    java.util.regex.Pattern coalescePattern = java.util.regex.Pattern.compile(
        "^\\s*COALESCE\\s*\\((.+)\\)\\s*$",
        java.util.regex.Pattern.CASE_INSENSITIVE);
    java.util.regex.Matcher coalesceMatcher = coalescePattern.matcher(expr);
    if (coalesceMatcher.matches()) {
      String argsStr = coalesceMatcher.group(1);
      // Split by comma, but handle quoted field names
      java.util.regex.Pattern argPattern = java.util.regex.Pattern.compile(
          "src\\.\"?([A-Za-z0-9_]+)\"?");
      java.util.regex.Matcher argMatcher = argPattern.matcher(argsStr);
      while (argMatcher.find()) {
        String fieldName = argMatcher.group(1);
        Object value = getValueCaseInsensitive(row, fieldName);
        if (value != null) {
          return value;
        }
      }
      return null;
    }

    // If expression cannot be evaluated, return null (like TRY_CAST behavior)
    LOGGER.debug("Cannot evaluate expression locally: {}", expr);
    return null;
  }

  /**
   * Casts a value to the specified target type.
   * Returns null on conversion failure (like TRY_CAST behavior).
   */
  private Object castValue(Object value, String targetType) {
    if (value == null) {
      return null;
    }
    String strValue = value.toString().trim();
    if (strValue.isEmpty()) {
      return null;
    }

    try {
      switch (targetType) {
        case "BIGINT":
        case "INT64":
        case "LONG":
          return Long.parseLong(strValue);
        case "INTEGER":
        case "INT":
        case "INT32":
          return Integer.parseInt(strValue);
        case "DOUBLE":
        case "FLOAT8":
          return Double.parseDouble(strValue);
        case "FLOAT":
        case "FLOAT4":
        case "REAL":
          return Float.parseFloat(strValue);
        case "VARCHAR":
        case "STRING":
        case "TEXT":
          return strValue;
        case "BOOLEAN":
        case "BOOL":
          return Boolean.parseBoolean(strValue);
        default:
          // Unknown type - return as string
          return strValue;
      }
    } catch (NumberFormatException e) {
      // Like TRY_CAST - return null on parse failure
      LOGGER.debug("Failed to cast '{}' to {}: {}", strValue, targetType, e.getMessage());
      return null;
    }
  }

  /**
   * Uploads locally staged Parquet files to remote storage and returns DataFile objects.
   *
   * <p>For LOCAL staging mode, DuckDB writes Parquet to local filesystem.
   * This method uploads those files to the Iceberg data location on S3/remote.
   */
  private List<org.apache.iceberg.DataFile> uploadLocalStagingToRemote(String localStagingPath)
      throws IOException {

    String dataLocation = table.location() + "/data";
    // Normalize data location to s3:// for DuckDB compatibility
    if (dataLocation.startsWith("s3a://")) {
      dataLocation = "s3://" + dataLocation.substring(6);
    }
    storageProvider.createDirectories(dataLocation);

    List<org.apache.iceberg.DataFile> dataFiles = new ArrayList<org.apache.iceberg.DataFile>();
    java.nio.file.Path localPath = java.nio.file.Paths.get(localStagingPath);

    if (!java.nio.file.Files.exists(localPath)) {
      LOGGER.warn("Local staging directory does not exist: {}", localStagingPath);
      return dataFiles;
    }

    // Walk local directory and upload Parquet files
    java.util.stream.Stream<java.nio.file.Path> fileStream = java.nio.file.Files.walk(localPath);
    try {
      java.util.Iterator<java.nio.file.Path> iterator = fileStream.iterator();
      while (iterator.hasNext()) {
        java.nio.file.Path file = iterator.next();
        if (java.nio.file.Files.isRegularFile(file) && file.toString().endsWith(".parquet")) {
          // Compute relative path from staging root
          String relativePath = localPath.relativize(file).toString();

          // Compute final remote path
          String finalPath = storageProvider.resolvePath(dataLocation, relativePath);

          // Create parent directories on remote
          String parentPath = getRemoteParentPath(finalPath);
          if (parentPath != null) {
            storageProvider.createDirectories(parentPath);
          }

          // Upload file
          long fileSize = java.nio.file.Files.size(file);
          try (java.io.InputStream in = java.nio.file.Files.newInputStream(file)) {
            storageProvider.writeFile(finalPath, in);
          }
          LOGGER.debug("Uploaded local {} to remote {} ({} bytes)",
              file, finalPath, fileSize);

          // Build DataFile pointing to remote location
          org.apache.iceberg.DataFile dataFile = buildDataFileFromPath(finalPath, fileSize);
          dataFiles.add(dataFile);
        }
      }
    } finally {
      fileStream.close();
    }

    LOGGER.info("Uploaded {} files from local staging to remote", dataFiles.size());
    return dataFiles;
  }

  /**
   * Gets the parent path for a remote path.
   */
  private String getRemoteParentPath(String path) {
    int lastSlash = path.lastIndexOf('/');
    if (lastSlash <= 0) {
      return null;
    }
    // Handle s3:// prefix
    if (path.startsWith("s3://") && lastSlash <= 5) {
      return null;
    }
    if (path.startsWith("s3a://") && lastSlash <= 6) {
      return null;
    }
    return path.substring(0, lastSlash);
  }

  /**
   * Builds a DataFile from a remote path and file size.
   * Extracts partition values from Hive-style path components.
   */
  private org.apache.iceberg.DataFile buildDataFileFromPath(String pathStr, long fileSize) {
    org.apache.iceberg.PartitionSpec spec = table.spec();

    // Extract partition values from Hive-style path
    org.apache.iceberg.PartitionData partitionData =
        new org.apache.iceberg.PartitionData(spec.partitionType());
    int dataIdx = pathStr.indexOf("/data/");
    String relativePath = dataIdx >= 0 ? pathStr.substring(dataIdx + 6) : pathStr;
    String[] pathParts = relativePath.split("/");

    for (int i = 0; i < pathParts.length - 1; i++) { // Exclude filename
      String part = pathParts[i];
      if (part.contains("=")) {
        String[] kv = part.split("=", 2);
        String columnName = kv[0];
        String value = kv[1];

        // Find field index in partition spec
        for (int fieldIdx = 0; fieldIdx < spec.fields().size(); fieldIdx++) {
          if (spec.fields().get(fieldIdx).name().equals(columnName)) {
            partitionData.set(fieldIdx, coercePartitionValue(value, spec.fields().get(fieldIdx)));
            break;
          }
        }
      }
    }

    // Build the DataFile
    org.apache.iceberg.DataFiles.Builder builder = org.apache.iceberg.DataFiles.builder(spec)
        .withPath(pathStr)
        .withFileSizeInBytes(fileSize)
        .withFormat(org.apache.iceberg.FileFormat.PARQUET)
        .withRecordCount(Math.max(1, fileSize / 100)); // Rough estimate

    if (spec.fields().size() > 0) {
      builder.withPartition(partitionData);
    }

    return builder.build();
  }

  /**
   * Coerces a string partition value to the appropriate type based on schema.
   * Handles null indicators like "-" (used by BLS for missing values).
   */
  private Object coercePartitionValue(String value, org.apache.iceberg.PartitionField field) {
    // Handle null/missing value indicators
    if (value == null || value.isEmpty() || "-".equals(value.trim())) {
      return null;
    }

    org.apache.iceberg.types.Type sourceType = table.schema().findType(field.sourceId());
    if (sourceType == null) {
      return value;
    }

    try {
      switch (sourceType.typeId()) {
        case INTEGER:
          return Integer.parseInt(value);
        case LONG:
          return Long.parseLong(value);
        case FLOAT:
          return Float.parseFloat(value);
        case DOUBLE:
          return Double.parseDouble(value);
        case BOOLEAN:
          return Boolean.parseBoolean(value);
        default:
          return value;
      }
    } catch (NumberFormatException e) {
      // If parsing fails, log and return null
      LOGGER.debug("Could not parse partition value '{}' as {}: {}",
          value, sourceType.typeId(), e.getMessage());
      return null;
    }
  }

  /**
   * Creates a JSON staging file with batch data.
   *
   * <p>For LOCAL staging mode, writes directly to local filesystem (fast).
   * For REMOTE staging mode, writes via storage provider (may be S3).
   *
   * @param rows The data rows to write
   * @param stagingPath The staging directory path
   * @return The path to the JSON file (local path or S3 URI)
   */
  private String createStagingJsonFile(List<Map<String, Object>> rows, String stagingPath)
      throws IOException {
    String timestamp = new SimpleDateFormat("yyyyMMdd_HHmmss").format(new Date());
    String jsonFileName = "batch_" + timestamp + "_" + UUID.randomUUID().toString().substring(0, 8) + ".json";
    String jsonPath = stagingPath + "/" + jsonFileName;

    // Build JSON content
    StringBuilder jsonContent = new StringBuilder();
    for (Map<String, Object> row : rows) {
      jsonContent.append(MAPPER.writeValueAsString(row));
      jsonContent.append("\n");
    }

    // Write to local filesystem or remote storage depending on staging mode
    if (stagingMode == MaterializeOptionsConfig.StagingMode.LOCAL) {
      java.nio.file.Files.write(java.nio.file.Paths.get(jsonPath),
          jsonContent.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8));
    } else {
      storageProvider.writeFile(jsonPath, jsonContent.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8));
    }

    LOGGER.debug("Created staging JSON file: {} ({} rows)", jsonPath, rows.size());
    return jsonPath;
  }

  /**
   * Cleans up the staging JSON file.
   */
  private void cleanupJsonFile(String jsonPath) {
    try {
      if (stagingMode == MaterializeOptionsConfig.StagingMode.LOCAL) {
        java.nio.file.Files.deleteIfExists(java.nio.file.Paths.get(jsonPath));
        LOGGER.debug("Cleaned up local JSON file: {}", jsonPath);
      } else {
        if (storageProvider.delete(jsonPath)) {
          LOGGER.debug("Cleaned up remote JSON file: {}", jsonPath);
        }
      }
    } catch (IOException e) {
      LOGGER.warn("Failed to cleanup JSON file {}: {}", jsonPath, e.getMessage());
    }
  }

  /**
   * Uses DuckDB to transform JSON to partitioned Parquet.
   * Reuses a shared DuckDB connection across batches for efficiency.
   */
  private void transformWithDuckDB(String jsonPath, String stagingPath,
      Map<String, String> partitionVariables) throws SQLException {
    // Reuse connection for efficiency
    if (sharedDuckDBConnection == null || sharedDuckDBConnection.isClosed()) {
      sharedDuckDBConnection = createDuckDBConnection();
    }

    String sql = buildDuckDBSql(jsonPath, stagingPath, partitionVariables);
    LOGGER.debug("Executing DuckDB SQL:\n{}", sql);

    long startTime = System.currentTimeMillis();
    try (Statement stmt = sharedDuckDBConnection.createStatement()) {
      stmt.execute(sql);
    }
    long elapsed = System.currentTimeMillis() - startTime;
    LOGGER.debug("DuckDB transformation completed in {}ms", elapsed);
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
    MaterializePartitionConfig partitionConfig = config.getPartition();

    if (columns == null || columns.isEmpty()) {
      sql.append("*");
    } else {
      // Build set of partition column names - these come from dimension iteration,
      // not from source data, so they shouldn't be in sourceColumns
      java.util.Set<String> partitionColumnNames = new java.util.HashSet<String>();
      if (partitionConfig != null) {
        partitionColumnNames.addAll(partitionConfig.getColumns());
      }

      // Build set of source column names (non-computed, non-partition, non-dimension columns)
      // Columns that are in partitionVariables are dimension values injected from iteration,
      // not from source data, so they shouldn't be in sourceColumns
      java.util.Set<String> sourceColumns = new java.util.HashSet<String>();
      java.util.Set<String> dimensionKeys = partitionVariables != null
          ? partitionVariables.keySet() : java.util.Collections.<String>emptySet();
      for (ColumnConfig col : columns) {
        if (!col.isComputed()
            && !partitionColumnNames.contains(col.getName())
            && !dimensionKeys.contains(col.getName())) {
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

    // Add partition variables as literal columns for partition-only columns
    // (columns that are in partition config but NOT in the regular column list).
    // Columns that ARE in the column list are already handled by buildSelectExpression.
    // Use case-insensitive matching since DuckDB/Iceberg treat columns as case-insensitive.
    if (partitionConfig != null && partitionVariables != null && !partitionVariables.isEmpty()) {
      // Build set of column names (lowercase) to check for duplicates
      java.util.Set<String> columnNamesLower = new java.util.HashSet<String>();
      if (columns != null) {
        for (ColumnConfig col : columns) {
          columnNamesLower.add(col.getName().toLowerCase(java.util.Locale.ROOT));
        }
      }

      // Build map of partition column types from columnDefinitions
      Map<String, String> partitionColTypes = new java.util.HashMap<String, String>();
      if (partitionConfig.getColumnDefinitions() != null) {
        for (MaterializePartitionConfig.ColumnDefinition colDef
            : partitionConfig.getColumnDefinitions()) {
          partitionColTypes.put(
              colDef.getName().toLowerCase(java.util.Locale.ROOT), colDef.getType());
        }
      }

      for (String partitionCol : partitionConfig.getColumns()) {
        String partitionColLower = partitionCol.toLowerCase(java.util.Locale.ROOT);
        // Only add if: has a value AND not already in column list (case-insensitive)
        if (partitionVariables.containsKey(partitionCol)
            && !columnNamesLower.contains(partitionColLower)) {
          // Use the partition column type from config, defaulting to VARCHAR
          String colType = partitionColTypes.get(partitionColLower);
          String duckdbType = mapToDuckDBType(colType);
          sql.append(", CAST('").append(escapeString(partitionVariables.get(partitionCol)))
              .append("' AS ").append(duckdbType).append(") AS ").append(partitionCol);
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
   *
   * <p>Staging location depends on stagingMode:
   * <ul>
   *   <li>REMOTE: Under warehousePath/.staging/ (same storage as warehouse)</li>
   *   <li>LOCAL: Under system temp directory (faster for transforms)</li>
   * </ul>
   *
   * <p>For remote S3 staging, a lifecycle rule auto-expires orphaned files after 1 day.
   */
  private String createStagingPath() throws IOException {
    String timestamp = new SimpleDateFormat("yyyyMMdd'T'HHmmss'Z'").format(new Date());
    String random = UUID.randomUUID().toString().substring(0, 8);
    String stagingSubpath = ".staging/" + timestamp + "_" + random;

    String stagingPath;
    if (stagingMode == MaterializeOptionsConfig.StagingMode.LOCAL) {
      // Use local temp directory for faster staging
      java.io.File tempDir = new java.io.File(System.getProperty("java.io.tmpdir"),
          "iceberg-staging/" + stagingSubpath);
      tempDir.mkdirs();
      stagingPath = tempDir.getAbsolutePath();
      LOGGER.debug("Created local staging path: {}", stagingPath);
    } else {
      // Use remote staging (same storage as warehouse)
      stagingPath = storageProvider.resolvePath(warehousePath, stagingSubpath);
      // Set up lifecycle rule for auto-cleanup (S3 only, no-op for local)
      storageProvider.ensureLifecycleRule(".staging/", 1);
      storageProvider.createDirectories(stagingPath);
      LOGGER.debug("Created remote staging path: {}", stagingPath);
    }

    return stagingPath;
  }

  /**
   * Cleans up the staging directory.
   *
   * <p>For LOCAL staging mode: Deletes local temp directory to free disk space.
   *
   * <p>For REMOTE staging mode with S3: Skip cleanup and rely on lifecycle rule
   * to expire orphaned staging files after 1 day. This saves API calls.
   */
  private void cleanupStagingDirectory(String stagingPath) {
    if (stagingMode == MaterializeOptionsConfig.StagingMode.LOCAL) {
      // Clean up local staging directory
      try {
        java.nio.file.Path localPath = java.nio.file.Paths.get(stagingPath);
        if (java.nio.file.Files.exists(localPath)) {
          // Delete all files recursively
          java.util.stream.Stream<java.nio.file.Path> walkStream = java.nio.file.Files.walk(localPath);
          try {
            java.util.List<java.nio.file.Path> pathsToDelete = new ArrayList<java.nio.file.Path>();
            java.util.Iterator<java.nio.file.Path> it = walkStream.iterator();
            while (it.hasNext()) {
              pathsToDelete.add(it.next());
            }
            // Sort in reverse order to delete files before directories
            java.util.Collections.sort(pathsToDelete, java.util.Collections.reverseOrder());
            for (java.nio.file.Path p : pathsToDelete) {
              java.nio.file.Files.deleteIfExists(p);
            }
          } finally {
            walkStream.close();
          }
          LOGGER.debug("Cleaned up local staging directory: {}", stagingPath);
        }
      } catch (IOException e) {
        LOGGER.warn("Failed to cleanup local staging directory {}: {}", stagingPath, e.getMessage());
      }
    } else {
      // For remote S3 paths, skip cleanup - lifecycle rule handles orphaned staging
      if (stagingPath.startsWith("s3://") || stagingPath.startsWith("s3a://")) {
        LOGGER.debug("Skipping S3 staging cleanup (lifecycle rule handles expiration): {}",
            stagingPath);
        return;
      }

      // For local storage via remote mode, clean up immediately
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
  }

  /**
   * Creates a new DuckDB connection with required extensions.
   * The connection is configured based on MaterializeOptionsConfig.
   */
  private Connection createDuckDBConnection() throws SQLException {
    Connection conn = DriverManager.getConnection("jdbc:duckdb:");

    MaterializeOptionsConfig optionsConfig = config.getOptions();
    int threads = optionsConfig != null ? optionsConfig.getThreads() : 2;

    try (Statement stmt = conn.createStatement()) {
      stmt.execute("SET threads=" + threads);
      stmt.execute("SET preserve_insertion_order=false");
      // Limit memory to avoid OOM on memory-constrained systems
      stmt.execute("SET memory_limit='" + DUCKDB_MEMORY_LIMIT + "'");
      if (warehousePath != null) {
        stmt.execute("SET temp_directory='" + warehousePath + "/.duckdb_tmp'");
      }

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

    // Run compaction if configured - consolidates many small files into fewer large files
    MaterializeConfig.IcebergConfig icebergConfig = config.getIceberg();
    if (icebergConfig != null && icebergConfig.isRunCompaction()) {
      long targetSize = icebergConfig.getCompactionTargetFileSizeBytes();
      int minFiles = icebergConfig.getCompactionMinFiles();
      long smallSize = icebergConfig.getCompactionSmallFileSizeBytes();
      LOGGER.info("Running automatic compaction: targetSize={}MB, minFiles={}, smallSize={}MB",
          targetSize / (1024 * 1024), minFiles, smallSize / (1024 * 1024));
      try {
        int compacted = tableWriter.compactSmallFiles(targetSize, minFiles, smallSize);
        if (compacted > 0) {
          LOGGER.info("Compaction complete: {} partitions consolidated", compacted);
        }
      } catch (Exception e) {
        LOGGER.warn("Compaction failed (non-fatal): {}", e.getMessage());
      }
    }

    // Run maintenance if configured
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
    // Close shared DuckDB connection
    if (sharedDuckDBConnection != null) {
      try {
        sharedDuckDBConnection.close();
        sharedDuckDBConnection = null;
      } catch (SQLException e) {
        LOGGER.warn("Failed to close DuckDB connection: {}", e.getMessage());
      }
    }

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

  /**
   * Queries existing partition combinations from an Iceberg table.
   *
   * <p>This enables "self-healing" of the incremental tracker when the cache DB
   * is deleted but Iceberg data exists. By reading existing partitions from the
   * table metadata, we can rebuild the cache without re-downloading data.
   *
   * @param catalogConfig Catalog configuration
   * @param tableId Table identifier
   * @param partitionColumns List of partition column names
   * @return Set of existing partition value combinations, or empty set if table doesn't exist
   */
  public static java.util.Set<Map<String, String>> getExistingPartitions(
      Map<String, Object> catalogConfig, String tableId, List<String> partitionColumns) {

    java.util.Set<Map<String, String>> partitions = new java.util.LinkedHashSet<Map<String, String>>();

    if (!IcebergCatalogManager.tableExists(catalogConfig, tableId)) {
      LOGGER.debug("Table {} does not exist, no existing partitions", tableId);
      return partitions;
    }

    try {
      Table table = IcebergCatalogManager.loadTable(catalogConfig, tableId);
      org.apache.iceberg.Snapshot currentSnapshot = table.currentSnapshot();

      if (currentSnapshot == null) {
        LOGGER.debug("Table {} has no snapshots, no existing partitions", tableId);
        return partitions;
      }

      // Get partition spec
      org.apache.iceberg.PartitionSpec spec = table.spec();
      if (spec.isUnpartitioned()) {
        LOGGER.debug("Table {} is unpartitioned", tableId);
        return partitions;
      }

      // Build field index map for partition columns
      Map<Integer, String> fieldIdToName = new java.util.HashMap<Integer, String>();
      for (org.apache.iceberg.PartitionField field : spec.fields()) {
        fieldIdToName.put(field.fieldId(), field.name());
      }

      // Scan manifest files to extract partition values
      for (org.apache.iceberg.ManifestFile manifest : currentSnapshot.allManifests(table.io())) {
        try (org.apache.iceberg.ManifestReader<org.apache.iceberg.DataFile> reader =
            org.apache.iceberg.ManifestFiles.read(manifest, table.io())) {
          for (org.apache.iceberg.DataFile dataFile : reader) {
            org.apache.iceberg.StructLike partition = dataFile.partition();

            Map<String, String> partitionValues = new java.util.LinkedHashMap<String, String>();
            for (int i = 0; i < spec.fields().size(); i++) {
              org.apache.iceberg.PartitionField field = spec.fields().get(i);
              Object value = partition.get(i, Object.class);
              if (value != null) {
                partitionValues.put(field.name(), value.toString());
              }
            }

            if (!partitionValues.isEmpty()) {
              partitions.add(partitionValues);
            }
          }
        }
      }

      LOGGER.info("Found {} existing partition combinations in Iceberg table {}",
          partitions.size(), tableId);
      return partitions;

    } catch (Exception e) {
      LOGGER.warn("Failed to read existing partitions from {}: {}", tableId, e.getMessage());
      return partitions;
    }
  }
}
