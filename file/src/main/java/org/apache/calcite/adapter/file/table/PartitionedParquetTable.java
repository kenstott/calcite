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
package org.apache.calcite.adapter.file.table;

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.file.execution.ExecutionEngineConfig;
import org.apache.calcite.adapter.file.metadata.TableConstraints;
import org.apache.calcite.adapter.file.partition.PartitionDetector;
import org.apache.calcite.adapter.file.partition.PartitionedTableConfig;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.CommentableTable;
import org.apache.calcite.schema.FilterableTable;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

import com.google.common.collect.ImmutableList;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Table implementation for partitioned Parquet datasets.
 * Represents multiple Parquet files as a single logical table.
 */
public class PartitionedParquetTable extends AbstractTable implements ScannableTable, FilterableTable, CommentableTable, org.apache.calcite.adapter.file.statistics.StatisticsProvider {
  private static final Logger LOGGER = LoggerFactory.getLogger(PartitionedParquetTable.class);

  private final List<String> filePaths;
  private final PartitionDetector.PartitionInfo partitionInfo;
  private final ExecutionEngineConfig engineConfig;
  private final org.apache.calcite.adapter.file.storage.StorageProvider storageProvider;
  private final PartitionedTableConfig config;
  private RelProtoDataType protoRowType;
  private final List<String> partitionColumns;
  private List<String> addedPartitionColumns;  // Partition columns actually added to schema
  private final Map<String, String> partitionColumnTypes;
  private Map<String, String> columnTypes;  // Column types from Parquet file
  private List<String> parquetColumnNames;  // Column names from Parquet file
  private final String customRegex;
  private final List<PartitionedTableConfig.ColumnMapping> columnMappings;
  private final Map<String, Object> constraintConfig;
  private final String schemaName;
  private final String tableName;
  private String tableComment;
  private Map<String, String> columnComments;

  public PartitionedParquetTable(List<String> filePaths,
                                 PartitionDetector.PartitionInfo partitionInfo,
                                 ExecutionEngineConfig engineConfig) {
    this(filePaths, partitionInfo, engineConfig, null, null, null, null, null, null, null);
  }

  public PartitionedParquetTable(List<String> filePaths,
                                 PartitionDetector.PartitionInfo partitionInfo,
                                 ExecutionEngineConfig engineConfig,
                                 Map<String, String> partitionColumnTypes) {
    this(filePaths, partitionInfo, engineConfig, partitionColumnTypes, null, null, null, null, null, null);
  }

  public PartitionedParquetTable(List<String> filePaths,
                                 PartitionDetector.PartitionInfo partitionInfo,
                                 ExecutionEngineConfig engineConfig,
                                 Map<String, String> partitionColumnTypes,
                                 String customRegex,
                                 List<PartitionedTableConfig.ColumnMapping> columnMappings) {
    this(filePaths, partitionInfo, engineConfig, partitionColumnTypes, customRegex, columnMappings, null, null, null, null);
  }

  public PartitionedParquetTable(List<String> filePaths,
                                 PartitionDetector.PartitionInfo partitionInfo,
                                 ExecutionEngineConfig engineConfig,
                                 Map<String, String> partitionColumnTypes,
                                 String customRegex,
                                 List<PartitionedTableConfig.ColumnMapping> columnMappings,
                                 Map<String, Object> constraintConfig) {
    this(filePaths, partitionInfo, engineConfig, partitionColumnTypes, customRegex, columnMappings, constraintConfig, null, null, null);
  }

  public PartitionedParquetTable(List<String> filePaths,
                                 PartitionDetector.PartitionInfo partitionInfo,
                                 ExecutionEngineConfig engineConfig,
                                 Map<String, String> partitionColumnTypes,
                                 String customRegex,
                                 List<PartitionedTableConfig.ColumnMapping> columnMappings,
                                 Map<String, Object> constraintConfig,
                                 String schemaName,
                                 String tableName) {
    this(filePaths, partitionInfo, engineConfig, partitionColumnTypes, customRegex, columnMappings, constraintConfig, schemaName, tableName, null);
  }

  public PartitionedParquetTable(List<String> filePaths,
                                 PartitionDetector.PartitionInfo partitionInfo,
                                 ExecutionEngineConfig engineConfig,
                                 Map<String, String> partitionColumnTypes,
                                 String customRegex,
                                 List<PartitionedTableConfig.ColumnMapping> columnMappings,
                                 Map<String, Object> constraintConfig,
                                 String schemaName,
                                 String tableName,
                                 org.apache.calcite.adapter.file.storage.StorageProvider storageProvider) {
    this(filePaths, partitionInfo, engineConfig, partitionColumnTypes, customRegex,
        columnMappings, constraintConfig, schemaName, tableName, storageProvider, null);
  }

  public PartitionedParquetTable(List<String> filePaths,
                                 PartitionDetector.PartitionInfo partitionInfo,
                                 ExecutionEngineConfig engineConfig,
                                 Map<String, String> partitionColumnTypes,
                                 String customRegex,
                                 List<PartitionedTableConfig.ColumnMapping> columnMappings,
                                 Map<String, Object> constraintConfig,
                                 String schemaName,
                                 String tableName,
                                 org.apache.calcite.adapter.file.storage.StorageProvider storageProvider,
                                 PartitionedTableConfig config) {
    this.filePaths = filePaths;
    this.partitionInfo = partitionInfo;
    this.engineConfig = engineConfig;
    this.storageProvider = storageProvider;
    this.config = config;
    this.partitionColumnTypes = partitionColumnTypes;
    this.customRegex = customRegex;
    this.columnMappings = columnMappings;
    this.constraintConfig = constraintConfig;
    this.schemaName = schemaName;
    this.tableName = tableName;

    // Initialize partition columns
    if (partitionInfo != null && partitionInfo.getPartitionColumns() != null) {
      this.partitionColumns = partitionInfo.getPartitionColumns();
      LOGGER.debug("PartitionedParquetTable initialized with partition columns: {}", this.partitionColumns);
    } else {
      this.partitionColumns = new ArrayList<>();
      LOGGER.debug("PartitionedParquetTable initialized with no partition columns");
    }

    // Log partition column types for debugging
    if (partitionColumnTypes != null && !partitionColumnTypes.isEmpty()) {
      LOGGER.debug("Partition column types: {}", partitionColumnTypes);
    }

    // Extract comments from Parquet metadata
    extractComments();

    // Eagerly initialize the row type to ensure it's available for SQL validation
    // This is critical for JOIN queries where the validator needs to resolve column references
    initializeRowType();

    // Register in PartitionInfoRegistry so JDBC/DuckDB rules can look up partition info
    if (schemaName != null && tableName != null && !partitionColumns.isEmpty()) {
      org.apache.calcite.adapter.file.partition.PartitionInfoRegistry.getInstance()
          .register(schemaName, tableName, this);
    }
  }

  /**
   * Eagerly initialize the row type from the first Parquet file.
   * This ensures the schema is available during SQL validation for JOIN queries.
   */
  private void initializeRowType() {
    if (filePaths == null || filePaths.isEmpty()) {
      LOGGER.debug("No files available to initialize row type");
      return;
    }

    try {
      // Store as a proto that will compute the row type on demand
      // The key is that this proto is initialized NOW, not lazily in getRowType()
      this.protoRowType = new RelProtoDataType() {
        private RelDataType cachedRowType = null;

        @Override public RelDataType apply(RelDataTypeFactory factory) {
          // Cache the result per type factory to avoid recomputation
          if (cachedRowType != null && cachedRowType.getSqlTypeName() != null) {
            return cachedRowType;
          }

          // Compute the row type
          try {
            cachedRowType = computeRowType(factory);
            return cachedRowType;
          } catch (Exception e) {
            LOGGER.error("Error computing row type: {}", e.getMessage(), e);
            return factory.builder().build();
          }
        }
      };

      LOGGER.debug("Initialized proto row type for table {}",
          tableName != null ? tableName : "unnamed");

    } catch (Exception e) {
      LOGGER.warn("Failed to eagerly initialize row type: {}", e.getMessage());
      // Don't fail - row type will be computed on first access
    }
  }

  /**
   * Compute the row type from the first Parquet file and partition columns.
   */
  private RelDataType computeRowType(RelDataTypeFactory typeFactory) {
    // Get schema from first Parquet file
    if (filePaths.isEmpty()) {
      return typeFactory.builder().build();
    }

    try {
      String firstFile = filePaths.get(0);
      RelDataType fileSchema = getParquetSchema(firstFile, typeFactory);

      // Add partition columns to schema
      RelDataTypeFactory.Builder builder = typeFactory.builder();

      // Add all fields from Parquet file
      fileSchema.getFieldList().forEach(field ->
          builder.add(field.getName(), field.getType()));

      // Track which partition columns are actually added
      if (addedPartitionColumns == null) {
        addedPartitionColumns = new ArrayList<>();
      }

      // Add partition columns to the schema
      LOGGER.debug("Adding {} partition columns to schema for table '{}'",
          partitionColumns.size(), tableName);

      for (String partCol : partitionColumns) {
        if (!containsField(fileSchema, partCol)) {
          // This partition column is not in the file, so add it
          if (!addedPartitionColumns.contains(partCol)) {
            addedPartitionColumns.add(partCol);
          }

          // Use specified type or default to VARCHAR
          SqlTypeName sqlType = SqlTypeName.VARCHAR;
          if (partitionColumnTypes != null && partitionColumnTypes.containsKey(partCol)) {
            String typeStr = partitionColumnTypes.get(partCol);
            try {
              sqlType = SqlTypeName.valueOf(typeStr.toUpperCase(java.util.Locale.ROOT));
              LOGGER.debug("Partition column '{}' will have type: {}", partCol, sqlType);
            } catch (IllegalArgumentException e) {
              LOGGER.warn("Unknown type '{}' for partition column '{}', defaulting to VARCHAR",
                  typeStr, partCol);
            }
          }
          builder.add(
              partCol, typeFactory.createTypeWithNullability(
              typeFactory.createSqlType(sqlType), true));
          LOGGER.debug("Added partition column '{}' to schema", partCol);
        } else {
          // Partition column is already in the file
          // This can happen when data files include partition key values as regular columns
          // Skip adding it since it's already in the schema from the Parquet file
          LOGGER.debug("Partition column '{}' already present in Parquet file for table '{}', skipping",
              partCol, tableName);
        }
      }

      return builder.build();
    } catch (Exception e) {
      LOGGER.error("Error computing row type: {}", e.getMessage(), e);
      return typeFactory.builder().build();
    }
  }

  /**
   * Extract table and column comments from config (primary) and Parquet file metadata (fallback).
   * Config-based comments take precedence over Parquet metadata.
   */
  private void extractComments() {
    // STEP 1: Initialize from config (primary source)
    if (config != null) {
      this.tableComment = config.getComment();
      this.columnComments = config.getColumnComments() != null
          ? new LinkedHashMap<>(config.getColumnComments())
          : new LinkedHashMap<>();

      if (tableComment != null) {
        LOGGER.debug("Loaded table comment from config: {}", tableComment);
      }
      if (!columnComments.isEmpty()) {
        LOGGER.debug("Loaded {} column comments from config", columnComments.size());
      }
    } else {
      this.columnComments = new LinkedHashMap<>();
    }

    // STEP 2: Extract from Parquet metadata (fallback)
    if (filePaths == null || filePaths.isEmpty()) {
      LOGGER.debug("No files to extract comments from");
      return;
    }

    try {
      // Read metadata from the first Parquet file using StorageProvider
      String firstFile = filePaths.get(0);

      if (storageProvider == null) {
        LOGGER.debug("No StorageProvider available, skipping Parquet comment extraction");
        return;
      }

      LOGGER.debug("Using StorageProvider to extract comments from: {}", firstFile);
      InputFile inputFile = new org.apache.calcite.adapter.file.storage.StorageProviderInputFile(storageProvider, firstFile);

      // Read Parquet metadata using the non-deprecated method
      ParquetMetadata metadata;
      try (ParquetFileReader reader = ParquetFileReader.open(inputFile)) {
        metadata = reader.getFooter();
      }
      FileMetaData fileMetaData = metadata.getFileMetaData();

      // Extract table comment from file key-value metadata (only if not already set from config)
      if (tableComment == null) {
        Map<String, String> keyValueMetaData = fileMetaData.getKeyValueMetaData();
        if (keyValueMetaData != null) {
          // Look for table comment in various possible keys
          String parquetTableComment = keyValueMetaData.get("table.comment");
          if (parquetTableComment == null) {
            parquetTableComment = keyValueMetaData.get("comment");
          }
          if (parquetTableComment == null) {
            parquetTableComment = keyValueMetaData.get("description");
          }

          if (parquetTableComment != null) {
            tableComment = parquetTableComment;
            LOGGER.debug("Found table comment from Parquet metadata: {}", tableComment);
          }
        }
      } else {
        LOGGER.debug("Table comment from config takes precedence over Parquet metadata");
      }

      // Extract column comments from schema (only add comments not already in config)
      MessageType schema = fileMetaData.getSchema();

      // Extract column names from Parquet schema for constraint mapping
      parquetColumnNames = new ArrayList<>();

      Map<String, String> keyValueMetaData = fileMetaData.getKeyValueMetaData();
      int parquetCommentsAdded = 0;
      int parquetCommentsSkipped = 0;

      for (Type field : schema.getFields()) {
        String fieldName = field.getName();
        parquetColumnNames.add(fieldName);

        // Skip if comment already exists from config (config wins)
        if (columnComments.containsKey(fieldName)) {
          parquetCommentsSkipped++;
          continue;
        }

        // Try to get comment from field's original type (if it has one)
        String fieldComment = null;

        // Check if there's a column-specific comment in key-value metadata
        if (keyValueMetaData != null) {
          fieldComment = keyValueMetaData.get("column." + fieldName + ".comment");
          if (fieldComment == null) {
            fieldComment = keyValueMetaData.get(fieldName + ".comment");
          }
        }

        // Store the comment if found
        if (fieldComment != null) {
          columnComments.put(fieldName, fieldComment);
          parquetCommentsAdded++;
          LOGGER.debug("Added comment for column '{}' from Parquet metadata: {}", fieldName, fieldComment);
        }
      }

      if (parquetCommentsSkipped > 0) {
        LOGGER.debug("Skipped {} column comments from Parquet (config takes precedence)", parquetCommentsSkipped);
      }
      if (parquetCommentsAdded > 0) {
        LOGGER.debug("Added {} column comments from Parquet metadata (not in config)", parquetCommentsAdded);
      }

      // Add partition columns to the column names list
      if (partitionColumns != null && !partitionColumns.isEmpty()) {
        parquetColumnNames.addAll(partitionColumns);
      }

      LOGGER.debug("Extracted {} column names from Parquet schema (including {} partition columns)",
          parquetColumnNames.size(),
          partitionColumns != null ? partitionColumns.size() : 0);
      LOGGER.debug("Comment extraction complete. Table comment: {}, Column comments: {}",
          tableComment != null ? "present" : "absent", columnComments.size());

    } catch (Exception e) {
      // Log but don't fail - comments are optional metadata
      LOGGER.warn("Failed to extract comments from Parquet file: {}", e.getMessage());
      LOGGER.debug("Comment extraction error details:", e);
    }
  }

  /**
   * Get the list of parquet file paths for this partitioned table.
   * Used by conversion metadata to register with DuckDB.
   */
  public List<String> getFilePaths() {
    return filePaths;
  }

  @Override public Statistic getStatistic() {
    LOGGER.debug("getStatistic called for table '{}': constraintConfig={}, fileCount={}",
        tableName, constraintConfig != null ? constraintConfig.keySet() : "null",
        filePaths != null ? filePaths.size() : 0);

    // Calculate row count estimate based on partition count
    // More partitions → higher cost due to S3 list API overhead
    // This helps optimizer prefer trend tables (fewer partitions) for full table scans
    double rowCountEstimate = estimateRowCount();

    if (constraintConfig == null || constraintConfig.isEmpty()) {
      // No constraints, but still report row count for cost calculation
      if (rowCountEstimate > 0) {
        LOGGER.debug("Table '{}': {} partitions, estimated {} rows", tableName,
            filePaths != null ? filePaths.size() : 0, rowCountEstimate);
        return Statistics.of(rowCountEstimate, ImmutableList.of());
      }
      return Statistics.UNKNOWN;
    }

    // Build the table configuration with "constraints" key as expected by TableConstraints.fromConfig
    Map<String, Object> tableConfig = new LinkedHashMap<>();
    tableConfig.put("constraints", constraintConfig);  // Wrap in "constraints" key

    // Use the column names extracted from Parquet schema during initialization
    List<String> columnNames = new ArrayList<>();
    if (parquetColumnNames != null && !parquetColumnNames.isEmpty()) {
      columnNames.addAll(parquetColumnNames);
      LOGGER.debug("Using {} column names from Parquet schema for constraints in table '{}'",
                   columnNames.size(), tableName);
    } else if (columnTypes != null && !columnTypes.isEmpty()) {
      // Fallback to columnTypes map if parquetColumnNames not available
      columnNames.addAll(columnTypes.keySet());
      LOGGER.debug("Got {} column names from columnTypes for table '{}'", columnNames.size(), tableName);
    } else {
      LOGGER.warn("No column names available for table '{}' - constraints won't work", tableName);
    }

    // Create statistic with constraints AND row count, passing schema and table names
    Statistic constraintStatistic =
        TableConstraints.fromConfig(tableConfig, columnNames, null, schemaName, tableName);

    // Enhance with row count if we have an estimate
    if (rowCountEstimate > 0) {
      return Statistics.of(rowCountEstimate, constraintStatistic.getKeys());
    }

    return constraintStatistic;
  }

  /**
   * Estimate row count based on partition count and typical partition size.
   * More partitions implies more S3 API overhead, so we inflate the cost
   * to encourage optimizer to prefer trend tables with fewer partitions.
   */
  private double estimateRowCount() {
    if (filePaths == null || filePaths.isEmpty()) {
      return 0;
    }

    int partitionCount = filePaths.size();

    // Estimate based on partition count:
    // - Each partition typically has 1000-10000 rows
    // - Add S3 API overhead factor (100 rows per partition for list cost)
    // This makes tables with many partitions appear more expensive
    double rowsPerPartition = 5000;  // Conservative estimate
    double s3ApiOverhead = 100;      // Cost of listing each partition

    double estimate = (partitionCount * rowsPerPartition) + (partitionCount * s3ApiOverhead);

    LOGGER.debug("Table '{}' row count estimate: {} partitions × {} rows + {} API cost = {}",
        tableName, partitionCount, rowsPerPartition, s3ApiOverhead, estimate);

    return estimate;
  }

  @Override public RelDataType getRowType(@NonNull RelDataTypeFactory typeFactory) {
    if (protoRowType != null) {
      return protoRowType.apply(typeFactory);
    }

    // Get schema from first Parquet file
    if (filePaths.isEmpty()) {
      return typeFactory.builder().build();
    }

    try {
      String firstFile = filePaths.get(0);
      RelDataType fileSchema = getParquetSchema(firstFile, typeFactory);

      // Add partition columns to schema
      RelDataTypeFactory.Builder builder = typeFactory.builder();

      // Add all fields from Parquet file
      fileSchema.getFieldList().forEach(field ->
          builder.add(field.getName(), field.getType()));

      // Track which partition columns are actually added
      addedPartitionColumns = new ArrayList<>();

      // Add partition columns
      for (String partCol : partitionColumns) {
        if (!containsField(fileSchema, partCol)) {
          // This partition column is not in the file, so add it
          addedPartitionColumns.add(partCol);

          // Use specified type or default to VARCHAR
          SqlTypeName sqlType = SqlTypeName.VARCHAR;
          if (partitionColumnTypes != null && partitionColumnTypes.containsKey(partCol)) {
            String typeStr = partitionColumnTypes.get(partCol);
            try {
              sqlType = SqlTypeName.valueOf(typeStr.toUpperCase(java.util.Locale.ROOT));
              LOGGER.debug("Partition column '{}' will have type: {} (from '{}')",
                          partCol, sqlType, typeStr);
            } catch (IllegalArgumentException e) {
              LOGGER.warn("Unknown type '{}' for partition column '{}', defaulting to VARCHAR",
                          typeStr, partCol);
            }
          } else {
            LOGGER.debug("No type specified for partition column '{}', defaulting to VARCHAR", partCol);
          }
          // All Parquet fields should be nullable
          builder.add(
              partCol, typeFactory.createTypeWithNullability(
              typeFactory.createSqlType(sqlType), true));
        } else {
          // Partition column found in file - this violates Hive partitioning standards
          throw new IllegalStateException(
              String.format("Partition column '%s' found in Parquet file. " +
                  "Hive-style partitioned files should NOT contain partition columns in the file content. " +
                  "Partition values should only be encoded in the directory structure.", partCol));
        }
      }

      return builder.build();

    } catch (Exception e) {
      LOGGER.error("Failed to get schema from Parquet files", e);
      return typeFactory.builder().build();
    }
  }

  private boolean containsField(RelDataType rowType, String fieldName) {
    return rowType.getFieldList().stream()
        .anyMatch(field -> field.getName().equalsIgnoreCase(fieldName));
  }

  private RelDataType getParquetSchema(String filePath, RelDataTypeFactory typeFactory)
      throws IOException {
    InputFile inputFile;

    // Use StorageProvider for S3/cloud storage, Hadoop FileSystem for local files
    if (storageProvider != null && (filePath.startsWith("s3://") || filePath.startsWith("http://") || filePath.startsWith("https://"))) {
      // Use StorageProvider for cloud storage
      LOGGER.debug("Using StorageProvider to read Parquet schema from: {}", filePath);
      inputFile = new org.apache.calcite.adapter.file.storage.StorageProviderInputFile(storageProvider, filePath);
    } else {
      // Use Hadoop FileSystem for local files
      LOGGER.debug("Using Hadoop FileSystem to read Parquet schema from: {}", filePath);
      Configuration conf = new Configuration();
      Path hadoopPath = new Path(filePath);
      inputFile = HadoopInputFile.fromPath(hadoopPath, conf);
    }

    // Use the non-deprecated method
    ParquetMetadata metadata;
    try (ParquetFileReader reader = ParquetFileReader.open(inputFile)) {
      metadata = reader.getFooter();
    }

    MessageType messageType = metadata.getFileMetaData().getSchema();

    // Map Parquet types to SQL types properly
    RelDataTypeFactory.Builder builder = typeFactory.builder();
    messageType.getFields().forEach(field -> {
      SqlTypeName sqlType = mapParquetTypeToSqlType(field);
      // All Parquet fields should be nullable
      builder.add(
          field.getName(), typeFactory.createTypeWithNullability(
          typeFactory.createSqlType(sqlType), true));
    });

    return builder.build();
  }

  private SqlTypeName mapParquetTypeToSqlType(org.apache.parquet.schema.Type parquetType) {
    if (!parquetType.isPrimitive()) {
      // Complex types default to VARCHAR
      return SqlTypeName.VARCHAR;
    }

    org.apache.parquet.schema.PrimitiveType primitiveType = parquetType.asPrimitiveType();
    org.apache.parquet.schema.LogicalTypeAnnotation logicalType =
        primitiveType.getLogicalTypeAnnotation();

    switch (primitiveType.getPrimitiveTypeName()) {
    case INT32:
      // Check for logical types
      if (logicalType
          instanceof org.apache.parquet.schema.LogicalTypeAnnotation.IntLogicalTypeAnnotation) {
        org.apache.parquet.schema.LogicalTypeAnnotation.IntLogicalTypeAnnotation intType =
            (org.apache.parquet.schema.LogicalTypeAnnotation.IntLogicalTypeAnnotation)
                logicalType;
        if (intType.getBitWidth() <= 8) {
          return SqlTypeName.TINYINT;
        } else if (intType.getBitWidth() <= 16) {
          return SqlTypeName.SMALLINT;
        }
      }
      return SqlTypeName.INTEGER;
    case INT64:
      return SqlTypeName.BIGINT;
    case FLOAT:
      return SqlTypeName.REAL;
    case DOUBLE:
      return SqlTypeName.DOUBLE;
    case BOOLEAN:
      return SqlTypeName.BOOLEAN;
    case BINARY:
    case FIXED_LEN_BYTE_ARRAY:
      // Check if it's a string
      if (logicalType
          instanceof org.apache.parquet.schema.LogicalTypeAnnotation.StringLogicalTypeAnnotation) {
        return SqlTypeName.VARCHAR;
      }
      // Check using ConvertedType instead of deprecated OriginalType
      org.apache.parquet.schema.Type.Repetition repetition = primitiveType.getRepetition();
      if (primitiveType.getId() != null
          && primitiveType.getName() != null
          && primitiveType.getPrimitiveTypeName()
          == org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY) {
        // Most BINARY types in Parquet files are strings unless explicitly marked otherwise
        return SqlTypeName.VARCHAR;
      }
      return SqlTypeName.VARBINARY;
    case INT96:
      // INT96 is typically used for timestamps
      return SqlTypeName.TIMESTAMP;
    default:
      return SqlTypeName.VARCHAR;
    }
  }

  @Override public Enumerable<Object[]> scan(DataContext root) {
    return scan(root, null);
  }

  @Override public Enumerable<Object[]> scan(DataContext root, List<RexNode> filters) {
    final AtomicBoolean cancelFlag = DataContext.Variable.CANCEL_FLAG.get(root);
    final JavaTypeFactory typeFactory = root.getTypeFactory();

    // Apply partition pruning if filters are provided
    List<String> prunedFilePaths = filePaths;
    if (filters != null && !filters.isEmpty() && partitionColumns != null && !partitionColumns.isEmpty()) {
      prunedFilePaths = prunePartitions(filePaths, filters, typeFactory);
      LOGGER.debug("Partition pruning: {} files before pruning, {} files after pruning",
          filePaths.size(), prunedFilePaths.size());
    }

    final List<String> finalFilePaths = prunedFilePaths;
    return new AbstractEnumerable<Object[]>() {
      @Override public Enumerator<Object[]> enumerator() {
        return new PartitionedParquetEnumerator(
            finalFilePaths, partitionInfo, partitionColumns, addedPartitionColumns, partitionColumnTypes,
            cancelFlag, engineConfig.getMemoryThreshold(), customRegex, columnMappings);
      }
    };
  }

  /**
   * Prune partitions based on filter predicates.
   * Extracts predicates on partition columns and filters the file list.
   */
  private List<String> prunePartitions(List<String> allFiles, List<RexNode> filters,
      RelDataTypeFactory typeFactory) {
    // Get the row type to map column indices to names
    RelDataType rowType = getRowType(typeFactory);
    List<RelDataTypeField> fields = rowType.getFieldList();

    // Extract partition column predicates from filters
    Map<String, Object> partitionPredicates = new LinkedHashMap<>();

    for (RexNode filter : filters) {
      extractPartitionPredicates(filter, fields, partitionPredicates);
    }

    if (partitionPredicates.isEmpty()) {
      LOGGER.debug("No partition predicates found in filters");
      return allFiles;
    }

    LOGGER.debug("Partition predicates extracted: {}", partitionPredicates);

    // Filter files based on partition predicates
    List<String> filteredFiles = new ArrayList<>();
    for (String filePath : allFiles) {
      if (matchesPartitionPredicates(filePath, partitionPredicates)) {
        filteredFiles.add(filePath);
      }
    }

    return filteredFiles;
  }

  /**
   * Extract equality predicates on partition columns from a filter expression.
   */
  private void extractPartitionPredicates(RexNode filter, List<RelDataTypeField> fields,
      Map<String, Object> predicates) {
    if (filter instanceof RexCall) {
      RexCall call = (RexCall) filter;

      if (call.getKind() == SqlKind.EQUALS) {
        // Handle: column = literal
        if (call.getOperands().size() == 2) {
          RexNode left = call.getOperands().get(0);
          RexNode right = call.getOperands().get(1);

          if (left instanceof RexInputRef && right instanceof RexLiteral) {
            RexInputRef inputRef = (RexInputRef) left;
            RexLiteral literal = (RexLiteral) right;

            String columnName = fields.get(inputRef.getIndex()).getName();
            if (partitionColumns.contains(columnName)) {
              Object value = literal.getValue();
              predicates.put(columnName, value);
              LOGGER.debug("Extracted partition predicate: {} = {}", columnName, value);
            }
          }
        }
      } else if (call.getKind() == SqlKind.AND) {
        // Handle: predicate1 AND predicate2
        for (RexNode operand : call.getOperands()) {
          extractPartitionPredicates(operand, fields, predicates);
        }
      }
      // Note: We don't handle OR, IN, or other complex predicates yet
      // Those would require more sophisticated pruning logic
    }
  }

  /**
   * Check if a file matches the partition predicates.
   */
  private boolean matchesPartitionPredicates(String filePath, Map<String, Object> predicates) {
    // Extract partition values from the file path
    PartitionDetector.PartitionInfo filePartitionInfo;

    if (partitionInfo.isHiveStyle()) {
      filePartitionInfo = PartitionDetector.extractHivePartitions(filePath);
    } else if (customRegex != null && columnMappings != null) {
      filePartitionInfo =
          PartitionDetector.extractCustomPartitions(filePath, customRegex, columnMappings);
    } else {
      filePartitionInfo = PartitionDetector.extractDirectoryPartitions(filePath, partitionColumns);
    }

    Map<String, String> filePartitionValues = filePartitionInfo != null
        ? filePartitionInfo.getPartitionValues()
        : new LinkedHashMap<>();

    // Check if all predicates match
    for (Map.Entry<String, Object> predicate : predicates.entrySet()) {
      String partitionColumn = predicate.getKey();
      Object expectedValue = predicate.getValue();

      String fileValue = filePartitionValues.get(partitionColumn);

      if (fileValue == null) {
        return false;  // Partition column not found in file path
      }

      // Convert expectedValue to string for comparison
      // Handle NlsString (which has charset prefix like _ISO-8859-1'value')
      String expectedStr;
      if (expectedValue instanceof org.apache.calcite.util.NlsString) {
        expectedStr = ((org.apache.calcite.util.NlsString) expectedValue).getValue();
      } else {
        expectedStr = expectedValue.toString();
      }

      if (!fileValue.equals(expectedStr)) {
        return false;  // Value doesn't match
      }
    }

    return true;  // All predicates match
  }

  /**
   * Enumerator that reads from multiple Parquet files with partition support.
   */
  private static class PartitionedParquetEnumerator implements Enumerator<Object[]> {
    private final List<String> filePaths;
    private final PartitionDetector.PartitionInfo globalPartitionInfo;
    private final List<String> partitionColumns;
    private final List<String> effectivePartitionColumns;  // Pre-computed columns to add
    private final Map<String, String> partitionColumnTypes;
    private final AtomicBoolean cancelFlag;
    private final long memoryThreshold;
    private final String customRegex;
    private final List<PartitionedTableConfig.ColumnMapping> columnMappings;

    private Iterator<String> fileIterator;
    private String currentFile;
    private ParquetReader<GenericRecord> currentReader;
    private GenericRecord currentRecord;
    private Object[] currentRow;
    private Map<String, String> currentPartitionValues;
    private Schema recordSchema;
    private int fileFieldCount;

    PartitionedParquetEnumerator(List<String> filePaths,
                                 PartitionDetector.PartitionInfo partitionInfo,
                                 List<String> partitionColumns,
                                 List<String> addedPartitionColumns,
                                 Map<String, String> partitionColumnTypes,
                                 AtomicBoolean cancelFlag,
                                 long memoryThreshold,
                                 String customRegex,
                                 List<PartitionedTableConfig.ColumnMapping> columnMappings) {
      this.filePaths = filePaths;
      this.globalPartitionInfo = partitionInfo;
      this.partitionColumns = partitionColumns;
      // Use addedPartitionColumns if provided, otherwise use all partition columns
      this.effectivePartitionColumns = addedPartitionColumns != null ? addedPartitionColumns : partitionColumns;
      this.partitionColumnTypes = partitionColumnTypes;
      this.cancelFlag = cancelFlag;
      this.memoryThreshold = memoryThreshold;
      this.customRegex = customRegex;
      this.columnMappings = columnMappings;
      this.fileIterator = filePaths.iterator();
    }

    @Override public Object[] current() {
      return currentRow;
    }

    @Override public boolean moveNext() {
      if (cancelFlag != null && cancelFlag.get()) {
        return false;
      }

      try {
        // Try to read next record from current file
        if (currentReader != null) {
          currentRecord = currentReader.read();
          if (currentRecord != null) {
            currentRow = convertToRow(currentRecord);
            return true;
          }
        }

        // Current file exhausted, move to next file
        while (fileIterator.hasNext()) {
          closeCurrentReader();
          currentFile = fileIterator.next();

          if (!openNextFile()) {
            continue;
          }

          currentRecord = currentReader.read();
          if (currentRecord != null) {
            currentRow = convertToRow(currentRecord);
            return true;
          }
        }

        // No more files
        return false;

      } catch (Exception e) {
        throw new RuntimeException("Error reading partitioned Parquet data", e);
      }
    }

    private boolean openNextFile() throws IOException {
      Path hadoopPath = new Path(currentFile);
      Configuration conf = new Configuration();

      // Extract partition values for this file
      currentPartitionValues = extractPartitionValues(currentFile);

      try {
        @SuppressWarnings("deprecation")
        ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(hadoopPath)
            .withConf(conf)
            .build();
        currentReader = reader;

        // Read first record to get schema
        GenericRecord firstRecord = currentReader.read();
        if (firstRecord != null) {
          recordSchema = firstRecord.getSchema();
          fileFieldCount = recordSchema.getFields().size();
          // Put the record back by creating a new reader
          closeCurrentReader();
          @SuppressWarnings("deprecation")
          ParquetReader<GenericRecord> newReader =
              AvroParquetReader.<GenericRecord>builder(hadoopPath)
              .withConf(conf)
              .build();
          currentReader = newReader;
          return true;
        }
      } catch (Exception e) {
        LOGGER.warn("Failed to open Parquet file: {}", currentFile, e);
      }

      return false;
    }

    private Map<String, String> extractPartitionValues(String filePath) {
      if (globalPartitionInfo == null) {
        return new LinkedHashMap<>();
      }

      PartitionDetector.PartitionInfo filePartitionInfo = null;

      if (globalPartitionInfo.isHiveStyle()) {
        filePartitionInfo =
            PartitionDetector.extractHivePartitions(filePath);
      } else if (customRegex != null && columnMappings != null) {
        // Custom regex partitions
        filePartitionInfo =
            PartitionDetector.extractCustomPartitions(filePath, customRegex,
                columnMappings);
      } else if (partitionColumns != null && !partitionColumns.isEmpty()) {
        filePartitionInfo =
            PartitionDetector.extractDirectoryPartitions(filePath,
                partitionColumns);
      }

      Map<String, String> values = filePartitionInfo != null
          ? filePartitionInfo.getPartitionValues()
          : new LinkedHashMap<>();

      if (LOGGER.isDebugEnabled() && !values.isEmpty()) {
        LOGGER.debug("Extracted partition values from path: {}", filePath);
        LOGGER.debug("  Partition values: {}", values);
        LOGGER.debug("  Expected columns: {}", partitionColumns);
      }

      return values;
    }

    private Object[] convertToRow(GenericRecord record) {
      // Create array with space for file fields + effective partition fields
      Object[] row = new Object[fileFieldCount + effectivePartitionColumns.size()];

      // Copy file data
      for (int i = 0; i < fileFieldCount; i++) {
        Object value = record.get(i);
        // Convert Avro UTF8 to String
        if (value != null && value.getClass().getName().equals("org.apache.avro.util.Utf8")) {
          value = value.toString();
        }
        row[i] = value;
      }

      // Debug: Log partition mapping
      if (LOGGER.isDebugEnabled() && currentPartitionValues != null) {
        LOGGER.debug("Current file: {}", currentFile);
        LOGGER.debug("Partition columns: {}", partitionColumns);
        LOGGER.debug("Current partition values: {}", currentPartitionValues);
      }

      // Add partition values with type conversion (only for columns added to schema)
      for (int i = 0; i < effectivePartitionColumns.size(); i++) {
        String partCol = effectivePartitionColumns.get(i);
        String strValue = currentPartitionValues.get(partCol);


        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug("Adding partition column[{}] '{}' with value '{}' at row index {}",
                      i, partCol, strValue, fileFieldCount + i);
          if (i == 0) {
            LOGGER.debug("Full partition mapping - columns: {}, values: {}",
                        partitionColumns, currentPartitionValues);
          }
        }

        // Convert based on specified type
        Object value = strValue;
        if (partitionColumnTypes != null && partitionColumnTypes.containsKey(partCol)) {
          String typeStr = partitionColumnTypes.get(partCol);
          try {
            SqlTypeName sqlType = SqlTypeName.valueOf(typeStr.toUpperCase(java.util.Locale.ROOT));
            value = convertPartitionValue(strValue, sqlType);
            LOGGER.debug("Converted partition column '{}' value '{}' to type {} -> {}",
                        partCol, strValue, sqlType, value.getClass().getSimpleName());
          } catch (Exception e) {
            // Keep as string if conversion fails
            LOGGER.debug("Failed to convert partition value '{}' to type {}: {}",
                        strValue, typeStr, e.getMessage());
          }
        } else {
          LOGGER.debug("No type conversion for partition column '{}', keeping as string: '{}'",
                      partCol, strValue);
        }

        row[fileFieldCount + i] = value;
      }

      return row;
    }

    private Object convertPartitionValue(String strValue, SqlTypeName sqlType) {
      if (strValue == null) {
        return null;
      }

      switch (sqlType) {
      case INTEGER:
        return Integer.parseInt(strValue);
      case BIGINT:
        return Long.parseLong(strValue);
      case SMALLINT:
        return Short.parseShort(strValue);
      case TINYINT:
        return Byte.parseByte(strValue);
      case DOUBLE:
        return Double.parseDouble(strValue);
      case FLOAT:
      case REAL:
        return Float.parseFloat(strValue);
      case DECIMAL:
        return new java.math.BigDecimal(strValue);
      case BOOLEAN:
        return Boolean.parseBoolean(strValue);
      case DATE:
        // Simple date parsing for yyyy-MM-dd format
        if (strValue.matches("\\d{4}-\\d{2}-\\d{2}")) {
          return java.sql.Date.valueOf(strValue);
        }
        return strValue;
      case VARCHAR:
      case CHAR:
      default:
        return strValue;
      }
    }

    private void closeCurrentReader() {
      if (currentReader != null) {
        try {
          currentReader.close();
        } catch (IOException e) {
          // Ignore
        }
        currentReader = null;
      }
    }

    @Override public void reset() {
      closeCurrentReader();
      fileIterator = filePaths.iterator();
      currentFile = null;
      currentRecord = null;
      currentRow = null;
    }

    @Override public void close() {
      closeCurrentReader();
    }
  }

  // CommentableTable implementation

  @Override public @Nullable String getTableComment() {
    return tableComment;
  }

  @Override public @Nullable String getColumnComment(String columnName) {
    if (columnComments == null) {
      return null;
    }
    return columnComments.get(columnName);
  }

  // StatisticsProvider implementation

  // Cached table statistics
  private org.apache.calcite.adapter.file.statistics.TableStatistics cachedStatistics;
  private volatile boolean statisticsLoaded = false;

  @Override public org.apache.calcite.adapter.file.statistics.TableStatistics getTableStatistics(
      org.apache.calcite.plan.RelOptTable table) {
    if (!statisticsLoaded) {
      synchronized (this) {
        if (!statisticsLoaded) {
          cachedStatistics = computeStatistics();
          statisticsLoaded = true;
        }
      }
    }
    return cachedStatistics;
  }

  @Override public org.apache.calcite.adapter.file.statistics.ColumnStatistics getColumnStatistics(
      org.apache.calcite.plan.RelOptTable table, String columnName) {
    org.apache.calcite.adapter.file.statistics.TableStatistics stats = getTableStatistics(table);
    if (stats == null || stats.getColumnStatistics() == null) {
      return null;
    }
    return stats.getColumnStatistics().get(columnName);
  }

  @Override public double getSelectivity(org.apache.calcite.plan.RelOptTable table,
      org.apache.calcite.rex.RexNode predicate) {
    // Default selectivity estimate
    return 0.25;
  }

  @Override public long getDistinctCount(org.apache.calcite.plan.RelOptTable table, String columnName) {
    org.apache.calcite.adapter.file.statistics.ColumnStatistics colStats = getColumnStatistics(table, columnName);
    if (colStats != null && colStats.getHllSketch() != null) {
      return colStats.getHllSketch().getEstimate();
    }
    return -1;
  }

  @Override public boolean hasStatistics(org.apache.calcite.plan.RelOptTable table) {
    return getTableStatistics(table) != null;
  }

  @Override public void scheduleStatisticsGeneration(org.apache.calcite.plan.RelOptTable table) {
    // Compute statistics synchronously for now
    getTableStatistics(table);
  }

  /**
   * Compute statistics for this partitioned table using DuckDB.
   * Uses approx_count_distinct for efficient HLL sketch generation.
   */
  private org.apache.calcite.adapter.file.statistics.TableStatistics computeStatistics() {
    if (filePaths == null || filePaths.isEmpty()) {
      LOGGER.debug("No files to compute statistics for table {}", tableName);
      return null;
    }

    // Build the parquet pattern for DuckDB
    String pattern = buildParquetPattern();
    if (pattern == null) {
      LOGGER.debug("Could not build parquet pattern for table {}", tableName);
      return null;
    }

    LOGGER.debug("Computing statistics for partitioned table {} using pattern: {}", tableName, pattern);

    try (java.sql.Connection conn = java.sql.DriverManager.getConnection("jdbc:duckdb:")) {
      java.sql.Statement stmt = conn.createStatement();

      // Get column names from the first file or use stored column names
      List<String> columns = getColumnsForStatistics();
      if (columns.isEmpty()) {
        LOGGER.debug("No columns found for statistics computation in table {}", tableName);
        return null;
      }

      // Compute row count
      long rowCount = 0;
      try (java.sql.ResultSet rs = stmt.executeQuery(
          "SELECT COUNT(*) FROM read_parquet('" + pattern + "', hive_partitioning=true)")) {
        if (rs.next()) {
          rowCount = rs.getLong(1);
        }
      }

      // Build column statistics with HLL sketches
      java.util.Map<String, org.apache.calcite.adapter.file.statistics.ColumnStatistics> columnStats =
          new java.util.HashMap<>();

      for (String column : columns) {
        try {
          // Compute approx_count_distinct using DuckDB's HLL implementation
          String sql = "SELECT approx_count_distinct(\"" + column + "\") FROM read_parquet('"
              + pattern + "', hive_partitioning=true)";
          try (java.sql.ResultSet rs = stmt.executeQuery(sql)) {
            if (rs.next()) {
              long distinctCount = rs.getLong(1);
              if (distinctCount > 0) {
                // Create HLL sketch from pre-computed estimate
                org.apache.calcite.adapter.file.statistics.HyperLogLogSketch hll =
                    org.apache.calcite.adapter.file.statistics.HyperLogLogSketch.fromEstimate(distinctCount);

                org.apache.calcite.adapter.file.statistics.ColumnStatistics colStats =
                    new org.apache.calcite.adapter.file.statistics.ColumnStatistics(
                        column, null, null, 0, rowCount, hll);
                columnStats.put(column, colStats);

                LOGGER.debug("Computed HLL for {}.{}: distinct={}", tableName, column, distinctCount);
              }
            }
          }
        } catch (Exception e) {
          LOGGER.debug("Failed to compute distinct count for column {}: {}", column, e.getMessage());
        }
      }

      LOGGER.info("Statistics computed for table {}: rowCount={}, columns={}",
          tableName, rowCount, columnStats.size());

      return new org.apache.calcite.adapter.file.statistics.TableStatistics(
          rowCount, 0, columnStats, null);

    } catch (Exception e) {
      LOGGER.warn("Failed to compute statistics for table {}: {}", tableName, e.getMessage());
      return null;
    }
  }

  /**
   * Returns the list of partition columns for this table.
   */
  public List<String> getPartitionColumns() {
    return partitionColumns != null ? partitionColumns : new ArrayList<>();
  }

  /**
   * Checks if the given column is a partition column.
   */
  public boolean isPartitionColumn(String columnName) {
    return partitionColumns != null && partitionColumns.contains(columnName);
  }

  /**
   * Gets distinct values for a partition column by listing directories.
   * This is much faster than scanning parquet files since values are encoded in directory names.
   *
   * @param columnName The partition column name
   * @return List of distinct values, or empty list if column is not a partition column
   */
  public List<String> getDistinctPartitionValues(String columnName) {
    if (!isPartitionColumn(columnName)) {
      LOGGER.debug("Column '{}' is not a partition column for table '{}'", columnName, tableName);
      return new ArrayList<>();
    }

    java.util.Set<String> values = new java.util.TreeSet<>();

    // Determine base path for listing
    String basePath = getBasePathForPartitionListing();
    if (basePath == null) {
      LOGGER.debug("Could not determine base path for partition listing in table '{}'", tableName);
      return new ArrayList<>();
    }

    LOGGER.debug("Getting distinct {} values via directory listing from: {}", columnName, basePath);

    try {
      // List files/directories under the base path
      List<org.apache.calcite.adapter.file.storage.StorageProvider.FileEntry> entries;
      if (storageProvider != null) {
        entries = storageProvider.listFiles(basePath, true);
      } else {
        // Local file system fallback
        entries = listLocalFiles(basePath);
      }

      // Extract partition values from paths
      // Look for paths like ".../{columnName}={value}/..."
      java.util.regex.Pattern valuePattern =
          java.util.regex.Pattern.compile(columnName + "=([^/]+)");

      for (org.apache.calcite.adapter.file.storage.StorageProvider.FileEntry entry : entries) {
        java.util.regex.Matcher matcher = valuePattern.matcher(entry.getPath());
        if (matcher.find()) {
          values.add(matcher.group(1));
        }
      }

      LOGGER.debug("Found {} distinct {} values via directory listing: {}",
          values.size(), columnName, values);

    } catch (IOException e) {
      LOGGER.warn("Failed to list directories for partition column {}: {}", columnName, e.getMessage());
    }

    return new ArrayList<>(values);
  }

  /**
   * Gets the base path for partition listing by finding the directory containing partition columns.
   */
  private String getBasePathForPartitionListing() {
    // Use configured pattern if available
    if (config != null && config.getPattern() != null && !filePaths.isEmpty()) {
      String firstFile = filePaths.get(0);
      // Find common base directory (up to type=)
      int typeIndex = firstFile.indexOf("type=");
      if (typeIndex > 0) {
        return firstFile.substring(0, typeIndex);
      }
    }

    // Fallback to first file's directory structure
    if (!filePaths.isEmpty()) {
      String firstFile = filePaths.get(0);
      // Find the first partition column in the path
      if (partitionColumns != null && !partitionColumns.isEmpty()) {
        String firstPartCol = partitionColumns.get(0);
        int partIndex = firstFile.indexOf(firstPartCol + "=");
        if (partIndex > 0) {
          return firstFile.substring(0, partIndex);
        }
      }
      // Just use parent directory
      int lastSlash = firstFile.lastIndexOf('/');
      if (lastSlash > 0) {
        return firstFile.substring(0, lastSlash);
      }
    }

    return null;
  }

  /**
   * Lists files in a local directory (fallback when no StorageProvider).
   */
  private List<org.apache.calcite.adapter.file.storage.StorageProvider.FileEntry> listLocalFiles(
      String basePath) throws IOException {
    List<org.apache.calcite.adapter.file.storage.StorageProvider.FileEntry> entries = new ArrayList<>();
    java.nio.file.Path base = java.nio.file.Paths.get(basePath);

    if (java.nio.file.Files.exists(base) && java.nio.file.Files.isDirectory(base)) {
      java.nio.file.Files.walkFileTree(base, new java.nio.file.SimpleFileVisitor<java.nio.file.Path>() {
        @Override
        public java.nio.file.FileVisitResult visitFile(java.nio.file.Path file,
            java.nio.file.attribute.BasicFileAttributes attrs) {
          String filePath = file.toString();
          String fileName = file.getFileName().toString();
          entries.add(new org.apache.calcite.adapter.file.storage.StorageProvider.FileEntry(
              filePath, fileName, false, attrs.size(), attrs.lastModifiedTime().toMillis()));
          return java.nio.file.FileVisitResult.CONTINUE;
        }

        @Override
        public java.nio.file.FileVisitResult preVisitDirectory(java.nio.file.Path dir,
            java.nio.file.attribute.BasicFileAttributes attrs) {
          // Also capture directories for partition value extraction
          String dirPath = dir.toString();
          String dirName = dir.getFileName() != null ? dir.getFileName().toString() : "";
          entries.add(new org.apache.calcite.adapter.file.storage.StorageProvider.FileEntry(
              dirPath, dirName, true, 0, attrs.lastModifiedTime().toMillis()));
          return java.nio.file.FileVisitResult.CONTINUE;
        }
      });
    }

    return entries;
  }

  /**
   * Build parquet glob pattern from file paths or config.
   */
  private String buildParquetPattern() {
    if (config != null && config.getPattern() != null) {
      // Use the configured pattern, converting wildcards to DuckDB glob syntax
      String pattern = config.getPattern();
      // If we have a base directory from the first file path, use it
      if (!filePaths.isEmpty()) {
        String firstFile = filePaths.get(0);
        // Find common base directory
        int typeIndex = firstFile.indexOf("type=");
        if (typeIndex > 0) {
          String baseDir = firstFile.substring(0, typeIndex);
          return baseDir + pattern.replace("*", "**");
        }
      }
    }

    // Fallback: use first file's directory with glob
    if (!filePaths.isEmpty()) {
      String firstFile = filePaths.get(0);
      int lastSlash = firstFile.lastIndexOf('/');
      if (lastSlash > 0) {
        String dir = firstFile.substring(0, lastSlash);
        // Go up to find type= directory for proper partitioning
        int typeIndex = dir.indexOf("type=");
        if (typeIndex > 0) {
          return dir.substring(0, typeIndex) + "type=*/**/*.parquet";
        }
        return dir + "/**/*.parquet";
      }
    }

    return null;
  }

  /**
   * Get column names for statistics computation.
   */
  private List<String> getColumnsForStatistics() {
    List<String> columns = new ArrayList<>();

    // Use parquet column names if available
    if (parquetColumnNames != null && !parquetColumnNames.isEmpty()) {
      columns.addAll(parquetColumnNames);
    }

    // Add partition columns
    if (partitionColumns != null) {
      for (String partCol : partitionColumns) {
        if (!columns.contains(partCol)) {
          columns.add(partCol);
        }
      }
    }

    return columns;
  }
}
