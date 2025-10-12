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
public class PartitionedParquetTable extends AbstractTable implements ScannableTable, FilterableTable, CommentableTable {
  private static final Logger LOGGER = LoggerFactory.getLogger(PartitionedParquetTable.class);

  private final List<String> filePaths;
  private final PartitionDetector.PartitionInfo partitionInfo;
  private final ExecutionEngineConfig engineConfig;
  private final org.apache.calcite.adapter.file.storage.StorageProvider storageProvider;
  private RelProtoDataType protoRowType;
  private List<String> partitionColumns;
  private List<String> addedPartitionColumns;  // Partition columns actually added to schema
  private Map<String, String> partitionColumnTypes;
  private Map<String, String> columnTypes;  // Column types from Parquet file
  private List<String> parquetColumnNames;  // Column names from Parquet file
  private String customRegex;
  private List<PartitionedTableConfig.ColumnMapping> columnMappings;
  private Map<String, Object> constraintConfig;
  private String schemaName;
  private String tableName;
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
    this.filePaths = filePaths;
    this.partitionInfo = partitionInfo;
    this.engineConfig = engineConfig;
    this.storageProvider = storageProvider;
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
   * Extract table and column comments from Parquet file metadata.
   */
  private void extractComments() {
    if (filePaths == null || filePaths.isEmpty()) {
      LOGGER.debug("No files to extract comments from");
      return;
    }

    try {
      // Read metadata from the first Parquet file
      String firstFile = filePaths.get(0);
      Configuration conf = new Configuration();
      Path path = new Path(firstFile);

      // Read Parquet metadata
      @SuppressWarnings("deprecation")
      ParquetMetadata metadata = ParquetFileReader.readFooter(conf, path);
      FileMetaData fileMetaData = metadata.getFileMetaData();

      // Extract table comment from file key-value metadata
      Map<String, String> keyValueMetaData = fileMetaData.getKeyValueMetaData();
      if (keyValueMetaData != null) {
        // Look for table comment in various possible keys
        tableComment = keyValueMetaData.get("table.comment");
        if (tableComment == null) {
          tableComment = keyValueMetaData.get("comment");
        }
        if (tableComment == null) {
          tableComment = keyValueMetaData.get("description");
        }

        if (tableComment != null) {
          LOGGER.debug("Found table comment: {}", tableComment);
        }
      }

      // Extract column comments from schema
      MessageType schema = fileMetaData.getSchema();
      columnComments = new LinkedHashMap<>();

      // Extract column names from Parquet schema for constraint mapping
      parquetColumnNames = new ArrayList<>();

      for (Type field : schema.getFields()) {
        String fieldName = field.getName();
        parquetColumnNames.add(fieldName);

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
          LOGGER.debug("Found comment for column '{}': {}", fieldName, fieldComment);
        }
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
    LOGGER.debug("getStatistic called for table '{}': constraintConfig={}", tableName,
        constraintConfig != null ? constraintConfig.keySet() : "null");
    if (constraintConfig == null || constraintConfig.isEmpty()) {
      // No constraints configured, return default
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

    // Create statistic with constraints, passing schema and table names
    return TableConstraints.fromConfig(tableConfig, columnNames, null, schemaName, tableName);
  }

  @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
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
      String expectedStr = expectedValue.toString();

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
}
