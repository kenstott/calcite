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
package org.apache.calcite.adapter.file;

import org.apache.calcite.adapter.file.duckdb.DuckDBJdbcSchemaFactory;
import org.apache.calcite.adapter.file.execution.ExecutionEngineConfig;
import org.apache.calcite.adapter.file.execution.duckdb.DuckDBConfig;
import org.apache.calcite.adapter.file.metadata.InformationSchema;
import org.apache.calcite.adapter.file.metadata.PostgresMetadataSchema;
import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.materialize.MaterializationService;
import org.apache.calcite.model.JsonTable;
import org.apache.calcite.model.ModelHandler;
import org.apache.calcite.schema.ConstraintCapableSchemaFactory;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.lookup.LikePattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Factory that creates a {@link FileSchema}.
 *
 * <p>Allows a custom schema to be included in a model.json file.
 * See <a href="http://calcite.apache.org/docs/file_adapter.html">File adapter</a>.
 */
@SuppressWarnings("UnusedDeclaration")
public class FileSchemaFactory implements ConstraintCapableSchemaFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(FileSchemaFactory.class);

  static {
    LOGGER.debug("[FileSchemaFactory] Class loaded and static initializer running");
  }

  /** Public singleton, per factory contract. */
  public static final FileSchemaFactory INSTANCE = new FileSchemaFactory();

  // Store constraint metadata from model files
  private Map<String, Map<String, Object>> tableConstraints;
  private List<JsonTable> tableDefinitions;

  /** Name of the column that is implicitly created in a CSV stream table
   * to hold the data arrival time. */
  public static final String ROWTIME_COLUMN_NAME = "ROWTIME";

  private FileSchemaFactory() {
  }

  @Override public Schema create(SchemaPlus parentSchema, String name,
      Map<String, Object> operand) {
    // Validate that schema name is unique within parent schema
    validateUniqueSchemaName(parentSchema, name);
    LOGGER.info("[FileSchemaFactory] ==> create() called for schema: '{}'", name);
    LOGGER.info("[FileSchemaFactory] ==> Parent schema: '{}'", parentSchema != null ? parentSchema.getName() : "null");
    LOGGER.info("[FileSchemaFactory] ==> Operand keys: {}", operand.keySet());
    LOGGER.info("[FileSchemaFactory] ==> Thread: {}", Thread.currentThread().getName());
    @SuppressWarnings("unchecked") List<Map<String, Object>> tables =
        (List) operand.get("tables");

    // Model file location (automatically set by Calcite's ModelHandler)
    // This can be null for inline models (model provided as a string)
    // Convert to string immediately - avoid File object usage
    String modelFileDirPath = null;
    Object baseDirectoryObj = operand.get(ModelHandler.ExtraOperand.BASE_DIRECTORY.camelName);
    if (baseDirectoryObj instanceof File) {
      modelFileDirPath = ((File) baseDirectoryObj).getAbsolutePath();
    } else if (baseDirectoryObj instanceof String) {
      modelFileDirPath = new File((String) baseDirectoryObj).getAbsolutePath();
    }

    // Get ephemeralCache option (default to false for backward compatibility)
    final Boolean ephemeralCache = parseBooleanValue(operand.get("ephemeralCache"))
        != null ? parseBooleanValue(operand.get("ephemeralCache"))
        : parseBooleanValue(operand.get("ephemeral_cache")) != null  // Support snake_case too
            ? parseBooleanValue(operand.get("ephemeral_cache"))
            : Boolean.FALSE;  // Default to persistent cache

    // Handle ephemeralCache and baseDirectory configuration
    // baseConfigDirectory is the root directory (without .aperio/<schema>)
    // baseDirectory is the full path including .aperio/<schema>
    File baseConfigDirectory = null;
    File baseDirectory = null;

    if (ephemeralCache) {
      // Create a temp directory for this test instance
      try {
        String tempDir = System.getProperty("java.io.tmpdir");
        String uniqueId = UUID.randomUUID().toString();
        baseConfigDirectory = new File(tempDir, uniqueId);
        baseConfigDirectory.mkdirs();
        LOGGER.info("Using ephemeral cache directory for schema '{}': {}",
            name, baseConfigDirectory.getAbsolutePath());
        // baseDirectory will be computed from baseConfigDirectory later
      } catch (Exception e) {
        LOGGER.error("Failed to create ephemeral cache directory", e);
      }
    } else {
      // User-configurable baseDirectory for cache/conversions (optional)
      // Handle both String and File types for baseDirectory
      final Object baseDirObj = operand.get("baseDirectory");
      final String baseDirConfig;
      if (baseDirObj instanceof String) {
        baseDirConfig = (String) baseDirObj;
      } else if (baseDirObj instanceof File) {
        baseDirConfig = ((File) baseDirObj).getPath();
      } else {
        baseDirConfig = null;
      }

      if (baseDirConfig != null && !baseDirConfig.isEmpty()) {
        // User explicitly configured baseDirectory - respect their choice
        baseConfigDirectory = new File(baseDirConfig);
        if (!baseConfigDirectory.isAbsolute() && modelFileDirPath != null) {
          // If relative path, resolve against model.json location
          baseConfigDirectory = new File(modelFileDirPath, baseDirConfig);
        }
        baseConfigDirectory = baseConfigDirectory.getAbsoluteFile();
        LOGGER.info("Using user-configured baseConfigDirectory: {}", baseConfigDirectory.getAbsolutePath());
      }
      // If no explicit config and not ephemeral, baseConfigDirectory remains null
      // and FileSchema will use its default (working directory)
    }

    // Compute the full baseDirectory path (with .aperio/<schema>) if baseConfigDirectory is set
    if (baseConfigDirectory != null) {
      baseDirectory = baseConfigDirectory;  // For now, keep it as the root for backward compatibility
      LOGGER.debug("baseConfigDirectory={}, baseDirectory={}",
          baseConfigDirectory.getAbsolutePath(), baseDirectory.getAbsolutePath());
    }

    // Extract schema comment from operand
    String comment = (String) operand.get("comment");

    // Schema-specific sourceDirectory operand (for reading source files)
    // Support both "directory" and "sourceDirectory" for backward compatibility
    final String directory = (String) operand.get("directory") != null
        ? (String) operand.get("directory")
        : (String) operand.get("sourceDirectory");
    LOGGER.debug("[FileSchemaFactory] directory from operand: '{}' (checked both 'directory' and 'sourceDirectory')", directory);
    LOGGER.debug("[FileSchemaFactory] modelFileDirPath: '{}'", modelFileDirPath);
    LOGGER.debug("[FileSchemaFactory] ephemeralCache: {}, baseDirectory: {}", ephemeralCache, baseDirectory);

    // Execution engine configuration
    // Priority: 1. Schema-specific operand, 2. Environment variable, 3. System property, 4. Default
    String executionEngine = (String) operand.get("executionEngine");
    String source = "schema operand";

    if (executionEngine == null || executionEngine.isEmpty()) {
      executionEngine = System.getenv("CALCITE_FILE_ENGINE_TYPE");
      source = "environment variable";
    }
    if (executionEngine == null || executionEngine.isEmpty()) {
      executionEngine = System.getProperty("calcite.file.engine.type");
      source = "system property";
    }
    if (executionEngine == null || executionEngine.isEmpty()) {
      executionEngine = ExecutionEngineConfig.DEFAULT_EXECUTION_ENGINE;
      source = "default";
    }

    LOGGER.info("[FileSchemaFactory] ==> executionEngine: '{}' for schema: '{}' (source: {})",
        executionEngine, name, source);
    final Object batchSizeObj = operand.get("batchSize");
    final int batchSize = batchSizeObj instanceof Number
        ? ((Number) batchSizeObj).intValue()
        : ExecutionEngineConfig.DEFAULT_BATCH_SIZE;
    final Object memoryThresholdObj = operand.get("memoryThreshold");
    final long memoryThreshold = memoryThresholdObj instanceof Number
        ? ((Number) memoryThresholdObj).longValue()
        : ExecutionEngineConfig.DEFAULT_MEMORY_THRESHOLD;

    // Get DuckDB configuration if provided
    @SuppressWarnings("unchecked") Map<String, Object> duckdbConfigMap =
        (Map<String, Object>) operand.get("duckdbConfig");
    final DuckDBConfig duckdbConfig = duckdbConfigMap != null
        ? new DuckDBConfig(duckdbConfigMap)
        : null;

    // Get custom Parquet cache directory if provided
    final String parquetCacheDirectory = (String) operand.get("parquetCacheDirectory");

    final ExecutionEngineConfig engineConfig =
        new ExecutionEngineConfig(executionEngine, batchSize, memoryThreshold, null, duckdbConfig, parquetCacheDirectory);

    // Get recursive parameter (default to false for backward compatibility)
    final boolean recursive = operand.get("recursive") == Boolean.TRUE;

    // Get directory pattern for glob-based file discovery
    // Support both "directoryPattern" and "glob" for backward compatibility
    final String directoryPattern = (String) operand.get("directoryPattern") != null
        ? (String) operand.get("directoryPattern")
        : (String) operand.get("glob");

    // Get materialized views configuration
    @SuppressWarnings("unchecked") List<Map<String, Object>> materializations =
        (List<Map<String, Object>>) operand.get("materializations");

    // Get views configuration
    @SuppressWarnings("unchecked") List<Map<String, Object>> views =
        (List<Map<String, Object>>) operand.get("views");

    // Get partitioned tables configuration
    @SuppressWarnings("unchecked") List<Map<String, Object>> partitionedTables =
        (List<Map<String, Object>>) operand.get("partitionedTables");

    // Get table constraints configuration
    @SuppressWarnings("unchecked") Map<String, Map<String, Object>> operandTableConstraints =
        (Map<String, Map<String, Object>>) operand.get("tableConstraints");

    // Get storage provider configuration
    String storageType = (String) operand.get("storageType");
    @SuppressWarnings("unchecked") Map<String, Object> storageConfig =
        (Map<String, Object>) operand.get("storageConfig");

    // Auto-detect storage type from directory path
    if (storageType == null && directory != null) {
      if (directory.startsWith("s3://")) {
        storageType = "s3";
        LOGGER.info("Auto-detected S3 storage from directory path: {}", directory);
      } else if (directory.startsWith("http://") || directory.startsWith("https://")) {
        storageType = "http";
        LOGGER.info("Auto-detected HTTP storage from directory path: {}", directory);
      } else if (directory.startsWith("hdfs://")) {
        storageType = "hdfs";
        LOGGER.info("Auto-detected HDFS storage from directory path: {}", directory);
      }
    }

    // Auto-detect storage type from baseDirectory path for cache/parquet locations
    final Object baseDirObj = operand.get("baseDirectory");
    if (storageType == null && baseDirObj instanceof String) {
      String baseDirStr = (String) baseDirObj;
      if (baseDirStr.startsWith("s3://")) {
        storageType = "s3";
        LOGGER.info("Auto-detected S3 storage from baseDirectory path: {}", baseDirStr);
      } else if (baseDirStr.startsWith("hdfs://")) {
        storageType = "hdfs";
        LOGGER.info("Auto-detected HDFS storage from baseDirectory path: {}", baseDirStr);
      }
    }

    // Get refresh interval for schema (default for all tables)
    final String refreshInterval = (String) operand.get("refreshInterval");
    LOGGER.info("FileSchemaFactory: refreshInterval from operand: '{}'", refreshInterval);

    // Get flatten option for JSON/YAML files
    final Boolean flatten = (Boolean) operand.get("flatten");

    // Get table name casing configuration (default to SMART_CASING)
    // Support both camelCase (model.json) and snake_case (JDBC URL) naming conventions
    String tableNameCasing = (String) operand.get("tableNameCasing");
    if (tableNameCasing == null) {
      tableNameCasing = (String) operand.getOrDefault("table_name_casing", "SMART_CASING");
    }

    // Get column name casing configuration (default to SMART_CASING)
    // Support both camelCase (model.json) and snake_case (JDBC URL) naming conventions
    String columnNameCasing = (String) operand.get("columnNameCasing");
    if (columnNameCasing == null) {
      columnNameCasing = (String) operand.getOrDefault("column_name_casing", "SMART_CASING");
    }

    // Get CSV type inference configuration
    @SuppressWarnings("unchecked") Map<String, Object> csvTypeInference =
        (Map<String, Object>) operand.get("csvTypeInference");

    // Get prime_cache option (default to true for optimal performance)
    final Boolean primeCache = operand.get("primeCache") != null
        ? (Boolean) operand.get("primeCache")
        : operand.get("prime_cache") != null
            ? (Boolean) operand.get("prime_cache")
            : Boolean.TRUE;  // Default to true

    // All paths are strings - StorageProvider handles both local and cloud storage
    String directoryPath = null;

    // Set storageType = "local" if not specified and we have a local directory
    if (storageType == null && directory != null && !directory.startsWith("s3://") && !directory.startsWith("hdfs://")
        && !directory.startsWith("http://") && !directory.startsWith("https://")) {
      storageType = "local";
      LOGGER.info("Auto-detected local storage from directory path: {}", directory);
    }

    // Validate storageType is set
    if (storageType == null) {
      throw new IllegalStateException(
          String.format("storageType must be configured for schema '%s'. " +
              "Set storageType='local' for local files or 's3' for S3 storage in model.json operand.", name));
    }

    // Resolve directory path
    if (directory != null) {
      // Check if it's a relative path (for local storage only)
      boolean isAbsolute = directory.startsWith("/") || directory.matches("^[a-zA-Z]:[\\\\/].*"); // Unix or Windows absolute
      if ("local".equals(storageType) && modelFileDirPath != null && !isAbsolute) {
        // Relative path - resolve against model file directory
        directoryPath = modelFileDirPath + (modelFileDirPath.endsWith("/") ? "" : "/") + directory;
        LOGGER.info("Resolved relative path against model directory: {} -> {}", directory, directoryPath);
      } else {
        // Absolute path or cloud URI - use as-is
        directoryPath = directory;
        LOGGER.info("Using directory path: {}", directoryPath);
      }
    } else if (modelFileDirPath != null) {
      // No directory specified - use model file directory
      directoryPath = modelFileDirPath;
      LOGGER.info("Using model directory as default: {}", directoryPath);
    } else {
      // Default to current working directory
      directoryPath = System.getProperty("user.dir");
      LOGGER.info("Using current directory as default: {}", directoryPath);
    }

    // If DuckDB engine is selected, first create FileSchema with PARQUET engine for conversions
    LOGGER.debug("FileSchemaFactory: Checking DuckDB conditions for schema '{}': engineConfig.getEngineType()={}, directoryPath={}, storageType={}",
                name, engineConfig.getEngineType(), directoryPath, storageType);

    // Prepare constraint metadata - used for both DuckDB and regular FileSchema
    // Support both instance field (from setTableConstraints) and operand
    Map<String, Map<String, Object>> constraintsToPass = this.tableConstraints;
    LOGGER.info("FileSchemaFactory: instance tableConstraints = {}",
        this.tableConstraints != null ? this.tableConstraints.size() + " tables" : "null");
    LOGGER.info("FileSchemaFactory: operand tableConstraints = {}",
        operandTableConstraints != null ? operandTableConstraints.size() + " tables" : "null");
    if (operandTableConstraints != null && !operandTableConstraints.isEmpty()) {
      // Operand constraints take precedence
      constraintsToPass = operandTableConstraints;
    }

    // Check if we're using DuckDB engine
    boolean isDuckDB = engineConfig.getEngineType() == ExecutionEngineConfig.ExecutionEngineType.DUCKDB;
    LOGGER.info("[FileSchemaFactory] ==> DuckDB analysis for schema '{}': ", name);
    LOGGER.info("[FileSchemaFactory] ==> - engineConfig.getEngineType(): {}", engineConfig.getEngineType());
    LOGGER.info("[FileSchemaFactory] ==> - ExecutionEngineType.DUCKDB: {}", ExecutionEngineConfig.ExecutionEngineType.DUCKDB);
    LOGGER.info("[FileSchemaFactory] ==> - isDuckDB: {}", isDuckDB);
    LOGGER.info("[FileSchemaFactory] ==> - storageType: '{}'", storageType);
    LOGGER.info("[FileSchemaFactory] ==> - directoryPath: '{}'", directoryPath);

    // DuckDB supports both local and cloud storage via StorageProvider
    if (isDuckDB && directoryPath != null) {
      LOGGER.info("[FileSchemaFactory] ==> *** ENTERING DUCKDB PATH FOR SCHEMA: {} (storageType: {}) ***", name, storageType);
      LOGGER.info("Using DuckDB: Running conversions first, then creating JDBC adapter for schema: {}", name);

      // Step 1: Create FileSchema with PARQUET engine
      // Use the same baseDirectory that was computed for DuckDB (ephemeral or not)
      ExecutionEngineConfig conversionConfig =
          new ExecutionEngineConfig("PARQUET", engineConfig.getBatchSize(), engineConfig.getMemoryThreshold(),
          engineConfig.getMaterializedViewStoragePath(), engineConfig.getDuckDBConfig(),
          engineConfig.getParquetCacheDirectory());

      LOGGER.info("DuckDB: Creating internal Parquet FileSchema with baseConfigDirectory: {}", baseConfigDirectory);

      // Create internal FileSchema for DuckDB processing
      // Pass null for File parameters - all file access goes through StorageProvider with directoryPath
      FileSchema fileSchema =
          new FileSchema(parentSchema, name, null, baseConfigDirectory, directoryPath, directoryPattern, tables, conversionConfig, recursive, materializations, views,
          partitionedTables, refreshInterval, tableNameCasing, columnNameCasing,
          storageType, storageConfig, flatten, csvTypeInference, primeCache, comment);

      // Set constraint metadata on FileSchema if available
      if (constraintsToPass != null && !constraintsToPass.isEmpty()) {
        LOGGER.info("FileSchemaFactory: Setting {} constraint configs on FileSchema for DuckDB", constraintsToPass.size());
        fileSchema.setConstraintMetadata(constraintsToPass);
      }

      // Pass conversion records (with viewScanPatterns) to FileSchema for DuckDB optimization
      @SuppressWarnings("unchecked")
      Map<String, org.apache.calcite.adapter.file.metadata.ConversionMetadata.ConversionRecord> conversionRecords =
          (Map<String, org.apache.calcite.adapter.file.metadata.ConversionMetadata.ConversionRecord>)
          operand.get("conversionRecords");
      if (conversionRecords != null && !conversionRecords.isEmpty()) {
        LOGGER.info("FileSchemaFactory: Setting {} conversion records on internal FileSchema for DuckDB", conversionRecords.size());
        fileSchema.setConversionRecords(conversionRecords);
      }

      // Force initialization to run conversions and populate the FileSchema for DuckDB
      LOGGER.info("DuckDB: About to call fileSchema.getTableMap() for table discovery");
      LOGGER.info("DuckDB: Internal FileSchema created successfully: {}", fileSchema.getClass().getSimpleName());
      LOGGER.info("DuckDB: Internal FileSchema directory: {}", directoryPath);

      Map<String, Table> tableMap;
      try {
        tableMap = fileSchema.getTableMap();
        LOGGER.info("DuckDB: Internal FileSchema discovered {} tables: {}", tableMap.size(), tableMap.keySet());
      } catch (Exception e) {
        LOGGER.error("ERROR calling fileSchema.getTableMap(): {}", e.getMessage(), e);
        throw new RuntimeException("Failed to discover tables in FileSchema for DuckDB", e);
      }

      if (tableMap.containsKey("sales_custom")) {
        LOGGER.info("DuckDB: Found sales_custom table in internal FileSchema!");
      } else {
        LOGGER.warn("DuckDB: sales_custom table NOT found in internal FileSchema");
      }

      // Check the conversion metadata immediately after table discovery
      if (fileSchema.getConversionMetadata() != null) {
        java.util.Map<String, org.apache.calcite.adapter.file.metadata.ConversionMetadata.ConversionRecord> records =
            fileSchema.getAllTableRecords();
        LOGGER.info("FileSchemaFactory: After getTableMap(), conversion metadata has {} records", records.size());
        for (String key : records.keySet()) {
          LOGGER.debug("FileSchemaFactory: Conversion record key: {}", key);
        }
      } else {
        LOGGER.warn("FileSchemaFactory: FileSchema has no conversion metadata!");
      }

      // Check if any JSON files were processed by the internal FileSchema
      LOGGER.debug("FileSchemaFactory: Checking if internal FileSchema processed JSON files from HTML conversion...");

      // Parquet conversion should happen automatically when tables are accessed
      LOGGER.debug("FileSchemaFactory: Parquet conversion will happen on-demand via FileSchema");

      // Step 2: Now create DuckDB JDBC schema that reads the files
      // Pass the FileSchema so it stays alive for refresh handling
      // Pass directoryPath as String to support both local and S3 URIs
      // Pass the operand map to support database_filename configuration
      LOGGER.debug("FileSchemaFactory: Now creating DuckDB JDBC schema");
      JdbcSchema duckdbSchema = DuckDBJdbcSchemaFactory.create(parentSchema, name, directoryPath, recursive, fileSchema, operand);
      LOGGER.info("FileSchemaFactory: DuckDB JDBC schema created successfully");

      // Wrap the schema with constraint metadata if available
      Schema schemaToRegister = duckdbSchema;
      if (constraintsToPass != null && !constraintsToPass.isEmpty()) {
        LOGGER.info("FileSchemaFactory: Wrapping DuckDB schema with constraint metadata for {} tables",
                    constraintsToPass.size());
        schemaToRegister = new ConstraintAwareJdbcSchema(duckdbSchema, constraintsToPass);
      }

      // Register the schema with the parent so SQL queries can find the tables
      // This is critical for Calcite's SQL validator to see the tables
      // Note: Schema uniqueness already validated at method start
      parentSchema.add(name, schemaToRegister);
      LOGGER.info("FileSchemaFactory: Registered {} schema '{}' with parent schema for SQL validation",
                  schemaToRegister.getClass().getSimpleName(), name);

      // Add metadata schemas as sibling schemas
      addMetadataSchemas(parentSchema);

      return schemaToRegister;
    }

    // Otherwise use regular FileSchema
    LOGGER.info("[FileSchemaFactory] ==> *** USING REGULAR FILESCHEMA FOR SCHEMA: {} ***", name);
    LOGGER.info("[FileSchemaFactory] ==> - directoryPath={}, storageType='{}'",
               directoryPath != null ? directoryPath : "null", storageType);
    // Pass user-configured baseDirectory or null to let FileSchema use its default
    // FileSchema will default to {working_directory}/.aperio/<schema_name> if null
    // Pass null for File parameter - all file access goes through StorageProvider with directoryPath
    FileSchema fileSchema =
        new FileSchema(parentSchema, name, null, baseDirectory, directoryPath, directoryPattern, tables, engineConfig, recursive,
        materializations, views, partitionedTables, refreshInterval, tableNameCasing,
        columnNameCasing, storageType, storageConfig, flatten, csvTypeInference, primeCache, comment);

    // Pass constraint metadata to FileSchema BEFORE table discovery
    // This ensures tables are created with constraint configuration available
    if (constraintsToPass != null && !constraintsToPass.isEmpty()) {
      LOGGER.info("FileSchemaFactory: Setting {} constraint configs on FileSchema BEFORE table discovery", constraintsToPass.size());
      fileSchema.setConstraintMetadata(constraintsToPass);
    } else {
      LOGGER.info("FileSchemaFactory: No constraints to pass to FileSchema");
    }

    // Pass conversion records (with viewScanPatterns) to FileSchema for DuckDB optimization
    // This is provided by sub-schema factories like SEC adapter
    @SuppressWarnings("unchecked")
    Map<String, org.apache.calcite.adapter.file.metadata.ConversionMetadata.ConversionRecord> conversionRecords =
        (Map<String, org.apache.calcite.adapter.file.metadata.ConversionMetadata.ConversionRecord>)
        operand.get("conversionRecords");
    if (conversionRecords != null && !conversionRecords.isEmpty()) {
      LOGGER.info("FileSchemaFactory: Setting {} conversion records on FileSchema for DuckDB", conversionRecords.size());
      fileSchema.setConversionRecords(conversionRecords);
    }

    // Force table discovery to populate the schema - tables will now be created with constraints
    LOGGER.debug("FileSchemaFactory: About to call fileSchema.getTableMap() for table discovery (constraints already set)");
    Map<String, Table> tableMap = fileSchema.getTableMap();
    LOGGER.info("FileSchemaFactory: FileSchema discovered {} tables: {}", tableMap.size(), tableMap.keySet());

    // Register the FileSchema with the parent so metadata queries can find the tables
    // This is critical for DatabaseMetaData.getColumns() to work
    // Note: Schema uniqueness already validated at method start
    SchemaPlus schemaPlus = parentSchema.add(name, fileSchema);
    LOGGER.info("FileSchemaFactory: Registered FileSchema '{}' with parent schema for metadata visibility", name);

    // Add metadata schemas (information_schema, pg_catalog) for PARQUET engine
    // This allows SQL queries against information_schema.TABLE_CONSTRAINTS
    if (engineConfig.getEngineType() == ExecutionEngineConfig.ExecutionEngineType.PARQUET) {
      LOGGER.info("FileSchemaFactory: Adding metadata schemas for PARQUET engine");
      addMetadataSchemas(parentSchema);
    }

    // Register text similarity functions if enabled
    @SuppressWarnings("unchecked")
    Map<String, Object> textSimilarity = (Map<String, Object>) operand.get("textSimilarity");
    if (textSimilarity != null) {
      Boolean enabled = (Boolean) textSimilarity.get("enabled");
      if (Boolean.TRUE.equals(enabled)) {
        LOGGER.info("FileSchemaFactory: Registering text similarity functions for schema '{}'", name);
        try {
          // Register functions on the parent schema
          org.apache.calcite.adapter.file.similarity.SimilarityFunctions.registerFunctions(schemaPlus);

          // Also register functions directly on the FileSchema instance for direct schema queries
          com.google.common.collect.ImmutableMultimap.Builder<String, org.apache.calcite.schema.Function> functionBuilder =
              com.google.common.collect.ImmutableMultimap.builder();

          // Add similarity functions to the FileSchema
          functionBuilder.put("COSINE_SIMILARITY",
              org.apache.calcite.schema.impl.ScalarFunctionImpl.create(
                  org.apache.calcite.adapter.file.similarity.SimilarityFunctions.class, "cosineSimilarity"));
          functionBuilder.put("COSINE_DISTANCE",
              org.apache.calcite.schema.impl.ScalarFunctionImpl.create(
                  org.apache.calcite.adapter.file.similarity.SimilarityFunctions.class, "cosineDistance"));
          functionBuilder.put("EUCLIDEAN_DISTANCE",
              org.apache.calcite.schema.impl.ScalarFunctionImpl.create(
                  org.apache.calcite.adapter.file.similarity.SimilarityFunctions.class, "euclideanDistance"));
          functionBuilder.put("DOT_PRODUCT",
              org.apache.calcite.schema.impl.ScalarFunctionImpl.create(
                  org.apache.calcite.adapter.file.similarity.SimilarityFunctions.class, "dotProduct"));
          functionBuilder.put("VECTORS_SIMILAR",
              org.apache.calcite.schema.impl.ScalarFunctionImpl.create(
                  org.apache.calcite.adapter.file.similarity.SimilarityFunctions.class, "vectorsSimilar"));
          functionBuilder.put("VECTOR_NORM",
              org.apache.calcite.schema.impl.ScalarFunctionImpl.create(
                  org.apache.calcite.adapter.file.similarity.SimilarityFunctions.class, "vectorNorm"));
          functionBuilder.put("NORMALIZE_VECTOR",
              org.apache.calcite.schema.impl.ScalarFunctionImpl.create(
                  org.apache.calcite.adapter.file.similarity.SimilarityFunctions.class, "normalizeVector"));
          functionBuilder.put("TEXT_SIMILARITY",
              org.apache.calcite.schema.impl.ScalarFunctionImpl.create(
                  org.apache.calcite.adapter.file.similarity.SimilarityFunctions.class, "textSimilarity"));
          functionBuilder.put("EMBED",
              org.apache.calcite.schema.impl.ScalarFunctionImpl.create(
                  org.apache.calcite.adapter.file.similarity.SimilarityFunctions.class, "embed"));

          fileSchema.setFunctionMultimap(functionBuilder.build());

          LOGGER.info("FileSchemaFactory: Successfully registered text similarity functions on both parent schema and FileSchema instance");
        } catch (Exception e) {
          LOGGER.warn("FileSchemaFactory: Failed to register similarity functions: " + e.getMessage(), e);
        }
      }
    }

    // Add metadata schemas as sibling schemas (not sub-schemas)
    // This makes them available at the same level as the file schema
    // Get the root schema to access all schemas for metadata
    SchemaPlus rootSchema = parentSchema;
    while (rootSchema.getParentSchema() != null) {
      rootSchema = rootSchema.getParentSchema();
    }

    // Only add metadata schemas if they don't already exist
    if (parentSchema.subSchemas().get("information_schema") == null) {
      LOGGER.info("FileSchemaFactory: Creating InformationSchema with parentSchema containing tables: {}",
                  parentSchema.tables().getNames(LikePattern.any()));
      InformationSchema infoSchema = new InformationSchema(parentSchema, "CALCITE");
      parentSchema.add("information_schema", infoSchema);
      LOGGER.info("FileSchemaFactory: Added InformationSchema to parent schema");
    } else {
      LOGGER.info("FileSchemaFactory: InformationSchema already exists, not creating new one");
    }

    if (parentSchema.subSchemas().get("pg_catalog") == null) {
      PostgresMetadataSchema pgSchema = new PostgresMetadataSchema(parentSchema, "CALCITE");
      parentSchema.add("pg_catalog", pgSchema);
    }

    // Ensure the standard Calcite metadata schema is preserved
    // It should already exist at the root level from CalciteConnectionImpl
    if (rootSchema.subSchemas().get("metadata") != null && parentSchema.subSchemas().get("metadata") == null) {
      // The metadata schema exists at root but not at current level, so reference it
      SchemaPlus metadataSchema = rootSchema.subSchemas().get("metadata");
      parentSchema.add("metadata", metadataSchema.unwrap(Schema.class));
    }

    // Register the FileSchema so we can add views to it
    // This is similar to what DuckDB does at line 453
    parentSchema.add(name, fileSchema);
    LOGGER.info("FileSchemaFactory: Registered FileSchema '{}' with parent for PARQUET engine", name);

    // Now register SQL views from table definitions for PARQUET engine
    // (DuckDB engine handles views separately as native DuckDB views)
    registerSqlViews(parentSchema, name, tables, operand);

    // Register materializations with MaterializationService for optimizer substitution
    // This enables Calcite's optimizer to automatically substitute materialized views
    // (e.g., trend tables) when they provide better performance
    if (materializations != null && !materializations.isEmpty()) {
      registerMaterializations(parentSchema, name, materializations);
    }

    return fileSchema;
  }

  /**
   * Validates that a schema with the given name does not already exist in the parent schema.
   * This prevents configuration errors and silent schema replacement.
   *
   * @param parentSchema the parent schema to check
   * @param name the schema name to validate
   * @throws IllegalArgumentException if a schema with the same name already exists
   */
  private static void validateUniqueSchemaName(SchemaPlus parentSchema, String name) {
    if (parentSchema == null || name == null) {
      return;
    }

    // Check if schema with this name already exists
    if (parentSchema.subSchemas().get(name) != null) {
      String errorMessage =
          String.format("Schema with name '%s' already exists in parent schema. " +
          "Each schema must have a unique name within the same connection. " +
          "Existing schemas: %s",
          name,
          parentSchema.subSchemas().getNames(LikePattern.any()));
      LOGGER.error("Duplicate schema name detected: {}", errorMessage);
      throw new IllegalArgumentException(errorMessage);
    }

    LOGGER.debug("Schema name '{}' is unique within parent schema", name);
  }

  /**
   * Registers materialized views with Calcite's MaterializationService for optimizer substitution.
   *
   * <p>This enables automatic query rewriting where the optimizer can substitute materialized
   * views (e.g., trend tables) when they provide better performance than the original query.
   *
   * <p>Each materialization entry should contain:
   * <ul>
   *   <li>table: The materialized view table name</li>
   *   <li>sql: The query that defines the materialization (e.g., "SELECT * FROM detail_table")</li>
   *   <li>viewSchemaPath: Optional schema path for the view (defaults to current schema)</li>
   *   <li>existing: Whether the table pre-exists (default: true)</li>
   * </ul>
   *
   * @param parentSchema the parent schema containing the file schema
   * @param schemaName the name of the file schema
   * @param materializations list of materialization definitions
   */
  @SuppressWarnings("deprecation") // getSubSchema is deprecated but needed for schema access
  private static void registerMaterializations(SchemaPlus parentSchema, String schemaName,
      List<Map<String, Object>> materializations) {

    // Get the CalciteSchema for materialization registration
    SchemaPlus fileSchema = parentSchema.getSubSchema(schemaName);
    if (fileSchema == null) {
      LOGGER.warn("Could not access schema '{}' - materializations not registered", schemaName);
      return;
    }

    CalciteSchema calciteSchema = CalciteSchema.from(fileSchema);
    if (calciteSchema == null) {
      LOGGER.warn("Could not access CalciteSchema for '{}' - materializations not registered",
          schemaName);
      return;
    }

    // Register each materialization with MaterializationService
    for (Map<String, Object> mv : materializations) {
      String table = (String) mv.get("table");
      String sql = (String) mv.get("sql");
      @SuppressWarnings("unchecked")
      List<String> viewSchemaPath = (List<String>) mv.get("viewSchemaPath");
      Boolean existing = (Boolean) mv.get("existing");

      if (table == null || sql == null) {
        LOGGER.warn("Skipping materialization with missing table or sql: {}", mv);
        continue;
      }

      try {
        MaterializationService.instance().defineMaterialization(
            calciteSchema,
            null,  // tileKey - not used for simple materializations
            sql,
            viewSchemaPath != null ? viewSchemaPath : Collections.singletonList(schemaName),
            table,
            false, // create = false (don't create table, just register)
            existing != null ? existing : true  // existing = true by default
        );
        LOGGER.info("Registered materialization: {} -> {}", table, sql);
      } catch (Exception e) {
        LOGGER.error("Failed to register materialization for table '{}': {}",
            table, e.getMessage());
      }
    }
  }

  private static void addMetadataSchemas(SchemaPlus parentSchema) {
    // Get the root schema to access all schemas for metadata
    SchemaPlus rootSchema = parentSchema;
    while (rootSchema.getParentSchema() != null) {
      rootSchema = rootSchema.getParentSchema();
    }

    // Only add metadata schemas if they don't already exist
    if (parentSchema.subSchemas().get("information_schema") == null) {
      InformationSchema infoSchema = new InformationSchema(parentSchema, "CALCITE");
      parentSchema.add("information_schema", infoSchema);
    }

    if (parentSchema.subSchemas().get("pg_catalog") == null) {
      PostgresMetadataSchema pgSchema = new PostgresMetadataSchema(parentSchema, "CALCITE");
      parentSchema.add("pg_catalog", pgSchema);
    }

    // Ensure the standard Calcite metadata schema is preserved
    if (rootSchema.subSchemas().get("metadata") != null && parentSchema.subSchemas().get("metadata") == null) {
      SchemaPlus metadataSchema = rootSchema.subSchemas().get("metadata");
      parentSchema.add("metadata", metadataSchema.unwrap(Schema.class));
    }
  }

  /**
   * Parse boolean value from operand map, handling both Boolean and String types.
   * This is needed because environment variable substitution can produce either type.
   */
  private static Boolean parseBooleanValue(Object value) {
    if (value == null) {
      return null;
    }
    if (value instanceof Boolean) {
      return (Boolean) value;
    }
    if (value instanceof String) {
      return Boolean.parseBoolean((String) value);
    }
    return null;
  }

  @Override public boolean supportsConstraints() {
    // Enable constraint support for file-based schemas
    return true;
  }

  @Override public void setTableConstraints(Map<String, Map<String, Object>> tableConstraints,
      List<JsonTable> tableDefinitions) {
    this.tableConstraints = tableConstraints;
    this.tableDefinitions = tableDefinitions;
    LOGGER.debug("Received constraint metadata for {} tables",
        tableConstraints != null ? tableConstraints.size() : 0);
  }

  /**
   * Registers SQL views as Calcite ViewTableMacro objects in the schema.
   * This enables views to work with the PARQUET execution engine.
   *
   * @param parentSchema Parent schema containing the file schema
   * @param schemaName Name of the file schema
   * @param tables List of table definitions (may include views)
   * @param operand Schema operand containing declaredSchemaName for rewriting
   */
  private static void registerSqlViews(SchemaPlus parentSchema, String schemaName,
                                      List<Map<String, Object>> tables,
                                      Map<String, Object> operand) {
    if (tables == null || tables.isEmpty()) {
      LOGGER.debug("No tables provided, skipping SQL view registration");
      return;
    }

    // Get the registered schema
    SchemaPlus schema = parentSchema.subSchemas().get(schemaName);
    if (schema == null) {
      LOGGER.warn("Could not find registered schema '{}' to add views", schemaName);
      return;
    }

    // Extract declared schema name for rewriting
    String declaredSchemaName = (String) operand.get("declaredSchemaName");

    int viewCount = 0;
    for (Map<String, Object> table : tables) {
      String tableType = (String) table.get("type");
      if (!"view".equals(tableType)) {
        continue;
      }

      String viewName = (String) table.get("name");
      // Try "sql" first (used by econ-schema.json), then "viewDef" as fallback
      String viewSql = (String) table.get("sql");
      if (viewSql == null) {
        viewSql = (String) table.get("viewDef");
      }

      if (viewName == null || viewSql == null) {
        LOGGER.warn("View definition missing name or sql/viewDef, skipping: {}", table);
        continue;
      }

      try {
        // Rewrite schema references if needed (same logic as DuckDB)
        String rewrittenViewSql = viewSql;
        if (declaredSchemaName != null && !declaredSchemaName.equalsIgnoreCase(schemaName)) {
          rewrittenViewSql = rewriteSchemaReferencesInSql(viewSql, declaredSchemaName, schemaName);
        }

        // Create ViewTableMacro and add to schema
        // Use schema path = [schemaName] and viewPath = [schemaName, viewName]
        java.util.List<String> schemaPath = java.util.Collections.singletonList(schemaName);
        java.util.List<String> viewPath = java.util.Arrays.asList(schemaName, viewName);

        org.apache.calcite.schema.impl.ViewTableMacro viewMacro =
            org.apache.calcite.schema.impl.ViewTable.viewMacro(schema, rewrittenViewSql,
                schemaPath, viewPath, null);

        schema.add(viewName, viewMacro);
        viewCount++;

        LOGGER.info("✅ Registered Calcite view: {}.{}", schemaName, viewName);

      } catch (Exception e) {
        LOGGER.error("Failed to register Calcite view '{}': {}", viewName, e.getMessage());
        LOGGER.error("View definition was: {}", viewSql);
      }
    }

    LOGGER.info("Calcite view registration complete: {} views registered", viewCount);
  }

  /**
   * Rewrites schema references in SQL view definitions from declared schema name to actual schema name.
   * Same logic as DuckDBJdbcSchemaFactory.rewriteSchemaReferencesInSql().
   *
   * @param viewSql Original SQL view definition
   * @param declaredSchemaName Canonical schema name from JSON (e.g., "econ")
   * @param actualSchemaName User-provided schema name from model.json (e.g., "ECON")
   * @return Rewritten SQL with schema names updated
   */
  private static String rewriteSchemaReferencesInSql(String viewSql, String declaredSchemaName,
                                                     String actualSchemaName) {
    if (viewSql == null || declaredSchemaName == null || actualSchemaName == null) {
      return viewSql;
    }

    // If schema names match (case-insensitive), no rewriting needed
    if (declaredSchemaName.equalsIgnoreCase(actualSchemaName)) {
      LOGGER.debug("Schema names match (case-insensitive), no SQL rewriting needed: {} = {}",
                  declaredSchemaName, actualSchemaName);
      return viewSql;
    }

    LOGGER.debug("Rewriting SQL view definition: '{}' -> '{}'", declaredSchemaName, actualSchemaName);

    // Pattern matches: schemaName. (word boundaries ensure we don't match partial names)
    // Case-insensitive matching for flexibility
    String pattern = "(?i)\\b" + java.util.regex.Pattern.quote(declaredSchemaName) + "\\.";
    String replacement = actualSchemaName + ".";

    String rewritten = viewSql.replaceAll(pattern, replacement);

    if (!rewritten.equals(viewSql)) {
      LOGGER.debug("SQL rewriting applied successfully");
    }

    return rewritten;
  }
}
