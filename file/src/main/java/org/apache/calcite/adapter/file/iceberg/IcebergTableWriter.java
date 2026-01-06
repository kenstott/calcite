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

import org.apache.calcite.adapter.file.storage.StorageProvider;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Writes data files to Iceberg tables.
 *
 * <p>Implements the staging directory strategy:
 * <ol>
 *   <li>DuckDB writes Parquet files to staging directory</li>
 *   <li>Files are moved to Iceberg data location</li>
 *   <li>DataFiles are built with final paths and committed atomically</li>
 * </ol>
 *
 * <p>This class handles the move-and-commit pattern for efficient Iceberg writes.
 */
public class IcebergTableWriter {
  private static final Logger LOGGER = LoggerFactory.getLogger(IcebergTableWriter.class);

  private final Table table;
  private final StorageProvider storageProvider;
  private final Configuration hadoopConf;

  /**
   * Creates a writer for the specified Iceberg table.
   *
   * @param table The Iceberg table to write to
   * @param storageProvider Storage provider for file operations (local/S3)
   */
  public IcebergTableWriter(Table table, StorageProvider storageProvider) {
    this.table = table;
    this.storageProvider = storageProvider;
    this.hadoopConf = new Configuration();
  }

  /**
   * Commits files from a staging directory to the Iceberg table.
   *
   * <p>This method:
   * <ol>
   *   <li>Moves files from staging to Iceberg data location</li>
   *   <li>Builds DataFile metadata for each moved file</li>
   *   <li>Commits files atomically to the table</li>
   * </ol>
   *
   * @param stagingPath The staging directory containing Parquet files
   * @param partitionFilter Optional filter for partition overwrite (null for append)
   * @throws IOException if file operations fail
   */
  public void commitFromStaging(String stagingPath,
      Map<String, Object> partitionFilter) throws IOException {
    List<DataFile> dataFiles = stageFiles(stagingPath);

    if (dataFiles.isEmpty()) {
      return;
    }

    commitDataFiles(dataFiles, partitionFilter);
  }

  /**
   * Stages files from a staging directory without committing.
   *
   * <p>This method moves files from staging to Iceberg data location and
   * returns DataFile objects that can be accumulated for bulk commit.
   * Use with {@link #bulkCommitDataFiles(List)} for reduced metadata operations.
   *
   * @param stagingPath The staging directory containing Parquet files
   * @return List of DataFile objects ready for commit
   * @throws IOException if file operations fail
   */
  public List<DataFile> stageFiles(String stagingPath) throws IOException {
    String dataLocation = table.location() + "/data";

    // Ensure data directory exists using StorageProvider
    storageProvider.createDirectories(dataLocation);

    // Walk staging directory and move files using StorageProvider
    List<DataFile> dataFiles = new ArrayList<DataFile>();
    moveFilesAndBuildDataFiles(stagingPath, dataLocation, dataFiles);

    if (dataFiles.isEmpty()) {
      LOGGER.warn("No data files found in staging directory: {}", stagingPath);
    } else {
      LOGGER.debug("Staged {} data files from {}", dataFiles.size(), stagingPath);
    }

    return dataFiles;
  }

  /**
   * Commits data files to Iceberg with optional partition filter.
   *
   * @param dataFiles The data files to commit
   * @param partitionFilter Optional filter for partition overwrite (null for append)
   */
  public void commitDataFiles(List<DataFile> dataFiles, Map<String, Object> partitionFilter) {
    if (dataFiles.isEmpty()) {
      return;
    }

    // Commit to Iceberg
    if (partitionFilter != null && !partitionFilter.isEmpty()) {
      // Overwrite partition
      LOGGER.info("Overwriting partition with filter: {}", partitionFilter);
      org.apache.iceberg.OverwriteFiles overwrite = table.newOverwrite();

      // Build filter expression from partition values
      org.apache.iceberg.expressions.Expression filter = Expressions.alwaysTrue();
      for (Map.Entry<String, Object> entry : partitionFilter.entrySet()) {
        filter =
            Expressions.and(filter, Expressions.equal(entry.getKey(), entry.getValue()));
      }
      overwrite.overwriteByRowFilter(filter);

      for (DataFile dataFile : dataFiles) {
        overwrite.addFile(dataFile);
      }
      overwrite.commit();
    } else {
      // Simple append
      LOGGER.info("Appending {} data files to table", dataFiles.size());
      org.apache.iceberg.AppendFiles append = table.newAppend();
      for (DataFile dataFile : dataFiles) {
        append.appendFile(dataFile);
      }
      append.commit();
    }

    LOGGER.info("Successfully committed {} files to Iceberg table {}", dataFiles.size(), table.name());
  }

  /**
   * Bulk commits multiple data files in a single Iceberg append operation.
   *
   * <p>This is more efficient than individual commits for batch operations
   * as it reduces the number of metadata updates to S3/R2 from O(n) to O(1).
   *
   * <p>Note: This uses append mode, not overwrite. For idempotent writes,
   * consider using partition-level deduplication or calling
   * {@link #commitDataFiles(List, Map)} per partition group.
   *
   * @param allDataFiles All data files to commit in a single transaction
   */
  public void bulkCommitDataFiles(List<DataFile> allDataFiles) {
    if (allDataFiles.isEmpty()) {
      LOGGER.debug("No data files to bulk commit");
      return;
    }

    LOGGER.info("Bulk committing {} data files to Iceberg table {}",
        allDataFiles.size(), table.name());
    long startTime = System.currentTimeMillis();

    org.apache.iceberg.AppendFiles append = table.newAppend();
    for (DataFile dataFile : allDataFiles) {
      append.appendFile(dataFile);
    }
    append.commit();

    long elapsed = System.currentTimeMillis() - startTime;
    LOGGER.info("Bulk commit complete: {} files in {}ms", allDataFiles.size(), elapsed);
  }

  /**
   * Moves files from staging to data location and builds DataFile metadata.
   * Uses StorageProvider for all file operations, supporting both local and S3.
   */
  private void moveFilesAndBuildDataFiles(String stagingPath,
      String dataPath, List<DataFile> dataFiles) throws IOException {

    // List all files in staging directory
    List<StorageProvider.FileEntry> stagingFiles = storageProvider.listFiles(stagingPath, true);

    for (StorageProvider.FileEntry entry : stagingFiles) {
      if (entry.isDirectory()) {
        continue;
      }

      String filePath = entry.getPath();
      if (filePath.endsWith(".parquet")) {
        // Compute relative path from staging
        String relativePath = computeRelativePath(stagingPath, filePath);

        // Compute final path in data directory
        String finalPath = storageProvider.resolvePath(dataPath, relativePath);

        // Create parent directories
        String parentPath = getParentPath(finalPath);
        if (parentPath != null) {
          storageProvider.createDirectories(parentPath);
        }

        // Copy file from staging to data location
        try (InputStream in = storageProvider.openInputStream(filePath)) {
          storageProvider.writeFile(finalPath, in);
        }
        LOGGER.debug("Copied {} to {}", filePath, finalPath);

        // Delete the source file from staging
        storageProvider.delete(filePath);

        // Build DataFile for the final file
        DataFile dataFile = buildDataFile(finalPath, entry.getSize());
        dataFiles.add(dataFile);
      }
    }
  }

  /**
   * Computes the relative path from a base path.
   */
  private String computeRelativePath(String basePath, String fullPath) {
    // Normalize paths - ensure base ends with /
    String normalizedBase = basePath.endsWith("/") ? basePath : basePath + "/";

    if (fullPath.startsWith(normalizedBase)) {
      return fullPath.substring(normalizedBase.length());
    }

    // Handle case where paths differ in trailing slash
    if (fullPath.startsWith(basePath)) {
      String remainder = fullPath.substring(basePath.length());
      return remainder.startsWith("/") ? remainder.substring(1) : remainder;
    }

    // Fallback - return just the filename
    int lastSlash = fullPath.lastIndexOf('/');
    return lastSlash >= 0 ? fullPath.substring(lastSlash + 1) : fullPath;
  }

  /**
   * Gets the parent path of a file path.
   */
  private String getParentPath(String path) {
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
   * Builds a DataFile for a Parquet file with partition information extracted from path.
   */
  private DataFile buildDataFile(String pathStr, long fileSize) {
    // Normalize path - fix Hadoop's s3a:/ to s3a://
    pathStr = normalizeS3Path(pathStr);
    PartitionSpec spec = table.spec();

    // Extract partition values from Hive-style path
    org.apache.iceberg.PartitionData partitionData = new org.apache.iceberg.PartitionData(spec.partitionType());
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
            // Set partition value
            partitionData.set(fieldIdx, coercePartitionValue(value, spec.fields().get(fieldIdx)));
            break;
          }
        }
      }
    }

    // Build the DataFile
    DataFiles.Builder builder = DataFiles.builder(spec)
        .withPath(pathStr)
        .withFileSizeInBytes(fileSize)
        .withFormat(FileFormat.PARQUET)
        .withRecordCount(estimateRecordCount(fileSize));

    if (spec.fields().size() > 0) {
      builder.withPartition(partitionData);
    }

    return builder.build();
  }

  /**
   * Coerces a string partition value to the appropriate type.
   */
  private Object coercePartitionValue(String value, org.apache.iceberg.PartitionField field) {
    // For identity transforms, look at the source field type
    org.apache.iceberg.types.Type sourceType = table.schema().findType(field.sourceId());
    if (sourceType == null) {
      return value;
    }

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
  }

  /**
   * Estimates record count from file size (rough heuristic).
   * This is a placeholder - for accurate counts, you would need to read the Parquet footer.
   */
  private long estimateRecordCount(long fileSize) {
    // Rough estimate: ~100 bytes per row on average
    return Math.max(1, fileSize / 100);
  }

  /**
   * Deletes files from a partition before overwriting.
   * Uses Iceberg's delete API for atomic operations.
   *
   * @param partitionFilter The partition filter to match files for deletion
   * @throws IOException if deletion fails
   */
  public void deletePartition(Map<String, Object> partitionFilter) throws IOException {
    if (partitionFilter == null || partitionFilter.isEmpty()) {
      throw new IllegalArgumentException("Partition filter is required for deletePartition");
    }

    org.apache.iceberg.expressions.Expression filter = Expressions.alwaysTrue();
    for (Map.Entry<String, Object> entry : partitionFilter.entrySet()) {
      filter =
          Expressions.and(filter, Expressions.equal(entry.getKey(), entry.getValue()));
    }

    LOGGER.info("Deleting partition with filter: {}", partitionFilter);
    table.newDelete()
        .deleteFromRowFilter(filter)
        .commit();
  }

  /**
   * Writes records to Iceberg using the native Parquet writer with proper field IDs.
   *
   * <p>This method creates Parquet files with Iceberg field IDs embedded in the schema,
   * which is required for Iceberg readers (including DuckDB's iceberg_scan) to properly
   * map columns. Using DuckDB's COPY TO PARQUET does not include these field IDs.
   *
   * @param records The records to write (as Map objects)
   * @param partitionValues The partition values for these records
   * @return DataFile object ready for commit
   * @throws IOException if writing fails
   */
  public DataFile writeRecords(List<Map<String, Object>> records,
      Map<String, String> partitionValues) throws IOException {
    if (records == null || records.isEmpty()) {
      return null;
    }

    Schema schema = table.schema();
    PartitionSpec spec = table.spec();

    // Generate unique file path in data location
    String dataLocation = table.location() + "/data";
    String partitionPath = buildPartitionPath(partitionValues);
    String filePath = dataLocation + "/" + partitionPath + "/data_"
        + java.util.UUID.randomUUID().toString().substring(0, 8) + ".parquet";

    // Normalize to s3a:// for Iceberg/Hadoop compatibility
    if (filePath.startsWith("s3://")) {
      filePath = "s3a://" + filePath.substring(5);
    }

    LOGGER.debug("Writing {} records to {} with partition {}", records.size(), filePath, partitionValues);

    // Create output file using table's FileIO
    OutputFile outputFile = table.io().newOutputFile(filePath);

    // Build partition key
    PartitionKey partitionKey = new PartitionKey(spec, schema);
    setPartitionKeyValues(partitionKey, spec, schema, partitionValues);

    // Convert Map records to GenericRecord
    List<Record> icebergRecords = new ArrayList<Record>(records.size());
    for (Map<String, Object> row : records) {
      GenericRecord record = GenericRecord.create(schema);
      for (Types.NestedField field : schema.columns()) {
        String fieldName = field.name();
        Object value = getFieldValue(row, fieldName, partitionValues);
        if (value != null) {
          record.setField(fieldName, coerceValue(value, field.type()));
        }
      }
      icebergRecords.add(record);
    }

    // Write using Iceberg's Parquet writer which includes field IDs
    DataWriter<Record> writer = null;
    try {
      writer = Parquet.writeData(outputFile)
          .schema(schema)
          .withSpec(spec)
          .withPartition(partitionKey)
          .createWriterFunc(GenericParquetWriter::buildWriter)
          .overwrite()
          .build();

      for (Record record : icebergRecords) {
        writer.write(record);
      }
    } finally {
      if (writer != null) {
        writer.close();
      }
    }

    // Build and return DataFile
    DataFile dataFile = writer.toDataFile();
    LOGGER.debug("Created data file: {} ({} records, {} bytes)",
        dataFile.path(), dataFile.recordCount(), dataFile.fileSizeInBytes());

    return dataFile;
  }

  /**
   * Builds a Hive-style partition path from partition values.
   */
  private String buildPartitionPath(Map<String, String> partitionValues) {
    if (partitionValues == null || partitionValues.isEmpty()) {
      return "";
    }
    StringBuilder path = new StringBuilder();
    PartitionSpec spec = table.spec();
    for (org.apache.iceberg.PartitionField field : spec.fields()) {
      String value = partitionValues.get(field.name());
      if (value != null) {
        if (path.length() > 0) {
          path.append("/");
        }
        path.append(field.name()).append("=").append(value);
      }
    }
    return path.toString();
  }

  /**
   * Sets partition key values from the partition variables map.
   */
  private void setPartitionKeyValues(PartitionKey partitionKey, PartitionSpec spec,
      Schema schema, Map<String, String> partitionValues) {
    if (partitionValues == null) {
      return;
    }
    for (int i = 0; i < spec.fields().size(); i++) {
      org.apache.iceberg.PartitionField field = spec.fields().get(i);
      String stringValue = partitionValues.get(field.name());
      if (stringValue != null) {
        Object value = coercePartitionValue(stringValue, field);
        partitionKey.set(i, value);
      }
    }
  }

  /**
   * Gets field value from row map, falling back to partition values for partition columns.
   */
  private Object getFieldValue(Map<String, Object> row, String fieldName,
      Map<String, String> partitionValues) {
    // First check the row data (case-insensitive lookup)
    for (Map.Entry<String, Object> entry : row.entrySet()) {
      if (entry.getKey().equalsIgnoreCase(fieldName)) {
        return entry.getValue();
      }
    }
    // Fall back to partition values for partition columns
    if (partitionValues != null) {
      for (Map.Entry<String, String> entry : partitionValues.entrySet()) {
        if (entry.getKey().equalsIgnoreCase(fieldName)) {
          return entry.getValue();
        }
      }
    }
    return null;
  }

  /**
   * Coerces a value to the appropriate Iceberg type.
   */
  private Object coerceValue(Object value, org.apache.iceberg.types.Type type) {
    if (value == null) {
      return null;
    }

    switch (type.typeId()) {
      case INTEGER:
        if (value instanceof Number) {
          return ((Number) value).intValue();
        }
        return Integer.parseInt(value.toString());
      case LONG:
        if (value instanceof Number) {
          return ((Number) value).longValue();
        }
        return Long.parseLong(value.toString());
      case FLOAT:
        if (value instanceof Number) {
          return ((Number) value).floatValue();
        }
        return Float.parseFloat(value.toString());
      case DOUBLE:
        if (value instanceof Number) {
          return ((Number) value).doubleValue();
        }
        return Double.parseDouble(value.toString());
      case BOOLEAN:
        if (value instanceof Boolean) {
          return value;
        }
        return Boolean.parseBoolean(value.toString());
      case STRING:
        return value.toString();
      case DATE:
        if (value instanceof java.time.LocalDate) {
          return (int) ((java.time.LocalDate) value).toEpochDay();
        }
        if (value instanceof java.sql.Date) {
          return (int) ((java.sql.Date) value).toLocalDate().toEpochDay();
        }
        if (value instanceof String) {
          return (int) java.time.LocalDate.parse((String) value).toEpochDay();
        }
        return value;
      case TIMESTAMP:
        if (value instanceof java.time.Instant) {
          return ((java.time.Instant) value).toEpochMilli() * 1000;
        }
        if (value instanceof java.sql.Timestamp) {
          return ((java.sql.Timestamp) value).getTime() * 1000;
        }
        return value;
      default:
        return value;
    }
  }

  /**
   * Runs maintenance operations on the table.
   * Should be called at the end of ingestion.
   *
   * @param expireSnapshotsDays Number of days after which to expire snapshots (default: 7)
   * @param orphanFilesDays Number of days after which to remove orphan files (default: 1)
   */
  public void runMaintenance(int expireSnapshotsDays, int orphanFilesDays) {
    long expireSnapshotsMillis = System.currentTimeMillis()
        - TimeUnit.DAYS.toMillis(expireSnapshotsDays);
    long orphanFilesMillis = System.currentTimeMillis()
        - TimeUnit.DAYS.toMillis(orphanFilesDays);

    LOGGER.info("Running Iceberg maintenance for table {}", table.name());

    // Expire old snapshots
    try {
      table.expireSnapshots()
          .expireOlderThan(expireSnapshotsMillis)
          .commit();
      LOGGER.info("Expired snapshots older than {} days", expireSnapshotsDays);
    } catch (Exception e) {
      LOGGER.warn("Failed to expire snapshots: {}", e.getMessage());
    }

    // Note: removeOrphanFiles requires iceberg-spark or iceberg-flink
    // For the core API, we rely on staging directory lifecycle rules
    LOGGER.debug("Orphan file cleanup is handled by staging directory lifecycle rules");
  }

  /**
   * Gets the table being written to.
   *
   * @return The Iceberg table
   */
  public Table getTable() {
    return table;
  }

  /**
   * Normalizes S3 paths to fix Hadoop's malformed URIs.
   * Hadoop's Path.toString() can return "s3a:/bucket" instead of "s3a://bucket".
   */
  private String normalizeS3Path(String path) {
    if (path == null) {
      return null;
    }
    // Fix s3a:/ (single slash) to s3a:// (double slashes)
    if (path.startsWith("s3a:/") && !path.startsWith("s3a://")) {
      return "s3a://" + path.substring(5);
    }
    // Fix s3:/ (single slash) to s3:// (double slashes)
    if (path.startsWith("s3:/") && !path.startsWith("s3://")) {
      return "s3://" + path.substring(4);
    }
    return path;
  }
}
