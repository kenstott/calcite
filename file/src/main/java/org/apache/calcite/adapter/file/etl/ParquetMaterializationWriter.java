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
package org.apache.calcite.adapter.file.etl;

import org.apache.calcite.adapter.file.storage.StorageProvider;

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
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * MaterializationWriter implementation for hive-partitioned Parquet output.
 *
 * <p>This writer uses DuckDB to transform data and write partitioned Parquet files.
 * Data is first staged to a temporary JSON file, then DuckDB reads and writes
 * it to the final partitioned Parquet structure.
 *
 * <h3>Write Process</h3>
 * <ol>
 *   <li>Data is buffered to a temporary JSON file</li>
 *   <li>DuckDB reads the JSON and writes partitioned Parquet</li>
 *   <li>Temporary files are cleaned up</li>
 * </ol>
 *
 * @see MaterializationWriter
 * @see MaterializeConfig.Format#PARQUET
 */
public class ParquetMaterializationWriter implements MaterializationWriter {

  private static final Logger LOGGER = LoggerFactory.getLogger(ParquetMaterializationWriter.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final StorageProvider storageProvider;
  private final String baseDirectory;

  private MaterializeConfig config;
  private String outputPath;
  private long totalRowsWritten;
  private int totalFilesWritten;
  private boolean initialized;

  /**
   * Creates a new ParquetMaterializationWriter.
   *
   * @param storageProvider Storage provider for file operations
   * @param baseDirectory Base directory for output files
   */
  public ParquetMaterializationWriter(StorageProvider storageProvider, String baseDirectory) {
    this.storageProvider = storageProvider;
    this.baseDirectory = baseDirectory;
    this.totalRowsWritten = 0;
    this.totalFilesWritten = 0;
    this.initialized = false;
  }

  @Override public void initialize(MaterializeConfig config) throws IOException {
    if (config == null) {
      throw new IllegalArgumentException("Config cannot be null");
    }
    if (!config.isEnabled()) {
      throw new IOException("Materialization is disabled in config");
    }

    this.config = config;

    // Use baseDirectory directly - location in output config is optional and deprecated
    MaterializeOutputConfig outputConfig = config.getOutput();
    String location = (outputConfig != null) ? outputConfig.getLocation() : null;
    if (location == null || location.isEmpty() || "{baseDirectory}".equals(location)) {
      this.outputPath = baseDirectory;
    } else {
      this.outputPath = storageProvider.resolvePath(baseDirectory, location);
    }
    LOGGER.info("Initialized ParquetMaterializationWriter: output={}", outputPath);
    this.initialized = true;
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

    // Collect data to list for counting and staging
    List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
    while (data.hasNext()) {
      rows.add(data.next());
    }

    if (rows.isEmpty()) {
      return 0;
    }

    LOGGER.info("Writing batch of {} rows with partitions: {}", rows.size(), partitionVariables);

    // Stage data to temp JSON file
    File tempFile = createTempJsonFile(rows);

    try {
      // Use DuckDB to convert JSON to partitioned Parquet
      writeToDuckDB(tempFile.getAbsolutePath(), partitionVariables);
      totalRowsWritten += rows.size();
      totalFilesWritten++;

      LOGGER.debug("Batch write complete: {} rows", rows.size());
      return rows.size();

    } finally {
      // Cleanup temp file
      if (tempFile.exists()) {
        boolean deleted = tempFile.delete();
        if (!deleted) {
          LOGGER.warn("Failed to delete temp file: {}", tempFile);
        }
      }
    }
  }

  /**
   * Creates a temporary JSON file with the batch data.
   */
  private File createTempJsonFile(List<Map<String, Object>> rows) throws IOException {
    String timestamp = new SimpleDateFormat("yyyyMMdd_HHmmss").format(new Date());
    File tempFile = File.createTempFile("etl_batch_" + timestamp + "_", ".json");

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
   * Uses DuckDB to read JSON and write partitioned Parquet.
   */
  private void writeToDuckDB(String jsonPath, Map<String, String> partitionVariables)
      throws IOException {

    try (Connection conn = getDuckDBConnection()) {
      String sql = buildCopySql(jsonPath, partitionVariables);
      LOGGER.debug("Executing DuckDB SQL:\n{}", sql);

      try (Statement stmt = conn.createStatement()) {
        stmt.execute(sql);
      }
    } catch (SQLException e) {
      throw new IOException("DuckDB write failed: " + e.getMessage(), e);
    }
  }

  /**
   * Builds the DuckDB COPY SQL statement.
   */
  private String buildCopySql(String jsonPath, Map<String, String> partitionVariables) {
    StringBuilder sql = new StringBuilder();
    sql.append("COPY (\n");
    sql.append("  SELECT ");

    // Build select clause with column definitions or wildcard
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
    sql.append(") TO '").append(escapeString(outputPath)).append("'");
    sql.append(buildCopyOptions());
    sql.append(";");

    return sql.toString();
  }

  /**
   * Builds COPY options including FORMAT, PARTITION_BY, and COMPRESSION.
   */
  private String buildCopyOptions() {
    StringBuilder options = new StringBuilder();
    options.append(" (FORMAT PARQUET");

    // Add partition columns
    MaterializePartitionConfig partitionConfig = config.getPartition();
    if (partitionConfig != null && !partitionConfig.getColumns().isEmpty()) {
      options.append(", PARTITION_BY (");
      List<String> partitionCols = partitionConfig.getColumns();
      for (int i = 0; i < partitionCols.size(); i++) {
        if (i > 0) {
          options.append(", ");
        }
        options.append(partitionCols.get(i));
      }
      options.append(")");
    }

    // Add compression
    MaterializeOutputConfig outputConfig = config.getOutput();
    String compression = outputConfig != null ? outputConfig.getCompression() : null;
    if (compression != null && !compression.isEmpty() && !"none".equalsIgnoreCase(compression)) {
      options.append(", COMPRESSION ").append(compression.toUpperCase());
    }

    // Add row group size
    MaterializeOptionsConfig optionsConfig = config.getOptions();
    if (optionsConfig != null) {
      int rowGroupSize = optionsConfig.getRowGroupSize();
      if (rowGroupSize > 0) {
        options.append(", ROW_GROUP_SIZE ").append(rowGroupSize);
      }
    }

    options.append(", OVERWRITE_OR_IGNORE)");

    return options.toString();
  }

  /**
   * Creates a DuckDB connection with required extensions loaded.
   */
  private Connection getDuckDBConnection() throws SQLException {
    Connection conn = DriverManager.getConnection("jdbc:duckdb:");
    loadExtensions(conn);
    configureSettings(conn);
    configureS3Access(conn);
    return conn;
  }

  /**
   * Loads required DuckDB extensions.
   */
  private void loadExtensions(Connection conn) {
    try (Statement stmt = conn.createStatement()) {
      stmt.execute("INSTALL parquet");
      stmt.execute("LOAD parquet");
      stmt.execute("INSTALL json");
      stmt.execute("LOAD json");
    } catch (SQLException e) {
      LOGGER.debug("Extensions already loaded or built-in: {}", e.getMessage());
    }
  }

  /**
   * Configures DuckDB settings for optimal performance.
   */
  private void configureSettings(Connection conn) throws SQLException {
    MaterializeOptionsConfig optionsConfig = config.getOptions();
    int threads = optionsConfig != null ? optionsConfig.getThreads() : 2;
    boolean preserveOrder = optionsConfig != null && optionsConfig.isPreserveInsertionOrder();

    try (Statement stmt = conn.createStatement()) {
      stmt.execute("SET threads=" + threads);
      stmt.execute("SET preserve_insertion_order=" + preserveOrder);
    }
  }

  /**
   * Configures S3 access for DuckDB connection.
   */
  private void configureS3Access(Connection conn) {
    try {
      conn.createStatement().execute("INSTALL httpfs");
      conn.createStatement().execute("LOAD httpfs");

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
    } catch (SQLException e) {
      LOGGER.debug("S3 configuration skipped: {}", e.getMessage());
    }
  }

  /**
   * Escapes a string for use in SQL.
   */
  private static String escapeString(String value) {
    return value.replace("'", "''");
  }

  @Override public void commit() throws IOException {
    // For Parquet format, files are written directly - no commit needed
    LOGGER.debug("Commit called (no-op for Parquet format)");
  }

  @Override public long getTotalRowsWritten() {
    return totalRowsWritten;
  }

  @Override public int getTotalFilesWritten() {
    return totalFilesWritten;
  }

  @Override public MaterializeConfig.Format getFormat() {
    return MaterializeConfig.Format.PARQUET;
  }

  @Override public String getTableLocation() {
    // For Parquet format, return the output path (directory pattern)
    return outputPath != null ? outputPath : baseDirectory;
  }

  @Override public void close() throws IOException {
    LOGGER.debug("ParquetMaterializationWriter closed: {} rows in {} files",
        totalRowsWritten, totalFilesWritten);
    initialized = false;
  }
}
