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

import org.apache.calcite.adapter.file.storage.StorageProvider;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.ReplacePartitions;
import org.apache.iceberg.RewriteFiles;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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

  /**
   * Runs a table-mutating operation under both serialization layers — the in-JVM per-table monitor
   * and the host-local {@link CrossProcessCommitLock} — so at most one mutator touches a given table
   * per host. Every commit (append, replace-partitions, delete, snapshot expiry, compaction rewrite)
   * AND every non-transactional file mutation (orphan-file deletion) runs under it. The
   * Hadoop/filesystem catalog tracks the current table via a non-atomic {@code version-hint.text}
   * pointer and is unsafe under concurrent mutation: interleaving a commit with the compaction's
   * expire-snapshots + orphan deletion can leave the live snapshot referencing a physically-deleted
   * data file (a dangling ref that 404s on read). Keyed by {@link Table#location()} so all writer
   * instances and processes for the same table share one lock.
   */
  private void underCommitLock(Runnable commitBody) {
    CrossProcessCommitLock.runExclusive(table.location(), commitBody);
  }

  /**
   * Creates a writer for the specified Iceberg table.
   */
  public IcebergTableWriter(Table table, StorageProvider storageProvider) {
    this.table = table;
    this.storageProvider = storageProvider;
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
   * <p>NOTE: Always uses APPEND mode now. With accession-level deduplication,
   * duplicates are filtered in the SQL query, so we should append new data
   * to existing partitions rather than overwriting them.
   *
   * @param dataFiles The data files to commit
   * @param partitionFilter Partition info for logging (no longer used for overwrite)
   */
  public void commitDataFiles(List<DataFile> dataFiles, Map<String, Object> partitionFilter) {
    if (dataFiles.isEmpty()) {
      return;
    }

    // Always use append mode - accession-level deduplication handles duplicates in SQL
    if (partitionFilter != null && !partitionFilter.isEmpty()) {
      LOGGER.info("Appending {} data files to partition: {}", dataFiles.size(), partitionFilter);
    } else {
      LOGGER.info("Appending {} data files to table", dataFiles.size());
    }

    org.apache.iceberg.AppendFiles append = table.newAppend();
    for (DataFile dataFile : dataFiles) {
      append.appendFile(dataFile);
    }
    underCommitLock(() -> {
      append.commit();
      // Ensure version-hint.text exists after commit (repairs orphaned tables)
      ensureVersionHint();
    });

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
    underCommitLock(() -> {
      append.commit();
      // Ensure version-hint.text exists after commit (repairs orphaned tables)
      ensureVersionHint();
    });

    long elapsed = System.currentTimeMillis() - startTime;
    LOGGER.info("Bulk commit complete: {} files in {}ms", allDataFiles.size(), elapsed);
  }

  /**
   * Replaces all data in the affected partitions with the given files.
   * Use for reference tables that should be fully overwritten each run.
   */
  public void replacePartitionsDataFiles(List<DataFile> allDataFiles) {
    if (allDataFiles.isEmpty()) {
      LOGGER.debug("No data files to replace partitions");
      return;
    }
    LOGGER.info("Replace-partitions committing {} data files to Iceberg table {}",
        allDataFiles.size(), table.name());
    long startTime = System.currentTimeMillis();
    ReplacePartitions replace = table.newReplacePartitions();
    for (DataFile dataFile : allDataFiles) {
      replace.addFile(dataFile);
    }
    underCommitLock(() -> {
      replace.commit();
      ensureVersionHint();
    });
    long elapsed = System.currentTimeMillis() - startTime;
    LOGGER.info("Replace-partitions commit complete: {} files in {}ms", allDataFiles.size(), elapsed);
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
        // Compute relative path (partition dirs + basename) from staging
        String relativePath = computeRelativePath(stagingPath, filePath);

        // Iceberg data files must be write-once and uniquely named. DuckDB's partitioned COPY
        // emits deterministic basenames (data_0.parquet per partition), so preserving them here
        // overwrites the prior snapshot's object in place on every re-ingest — violating
        // immutability (corrupts time-travel, and lets a concurrent reader/cache observe a
        // mutating object). Keep the partition directory, but give each moved file a unique name,
        // matching writeRecords()/compaction.
        int relLastSlash = relativePath.lastIndexOf('/');
        String partitionDir = relLastSlash >= 0 ? relativePath.substring(0, relLastSlash + 1) : "";
        String uniqueName = "data_"
            + java.util.UUID.randomUUID().toString().substring(0, 8) + ".parquet";
        String finalPath = storageProvider.resolvePath(dataPath, partitionDir + uniqueName);

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

    // Read real Iceberg metrics (record count, null counts, bounds) from the parquet FOOTER —
    // no rows are read. A NameMapping resolves the externally-written parquet's columns by name
    // (DuckDB parquet has no Iceberg field IDs). Without proper metrics, DuckDB's iceberg_scan
    // asserts ("GetValueInternal on a value that is NULL").
    org.apache.iceberg.Metrics metrics = null;
    try {
      org.apache.iceberg.mapping.NameMapping nameMapping =
          org.apache.iceberg.mapping.MappingUtil.create(table.schema());
      org.apache.iceberg.io.InputFile inputFile = table.io().newInputFile(pathStr);
      metrics = org.apache.iceberg.parquet.ParquetUtil.fileMetrics(
          inputFile, org.apache.iceberg.MetricsConfig.forTable(table), nameMapping);
    } catch (Exception e) {
      LOGGER.warn("Could not read parquet footer metrics for {}: {} — falling back to size estimate",
          pathStr, e.getMessage());
    }

    // Build the DataFile
    DataFiles.Builder builder = DataFiles.builder(spec)
        .withPath(pathStr)
        .withFileSizeInBytes(fileSize)
        .withFormat(FileFormat.PARQUET);
    if (metrics != null) {
      builder.withMetrics(metrics);
    } else {
      builder.withRecordCount(estimateRecordCount(fileSize));
    }

    if (spec.fields().size() > 0) {
      builder.withPartition(partitionData);
    }

    return builder.build();
  }

  /**
   * Coerces a string partition value to the appropriate type.
   * Handles null indicators like "-" (used by BLS for missing values).
   */
  private Object coercePartitionValue(String value, org.apache.iceberg.PartitionField field) {
    // Handle null/missing value indicators
    if (value == null || value.isEmpty() || "-".equals(value.trim())) {
      return null;
    }

    // For identity transforms, look at the source field type
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
      return null;
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
   * Finds parquet files that exist on storage for the given partition but are not registered
   * in the current Iceberg snapshot. Used for tracker self-healing: if a partition's data
   * files exist on S3 but the catalog was cleared, we can re-register them without re-fetching
   * from the source.
   *
   * @param partitionVariables dimension variables for the partition (e.g. {year=2025, month=4})
   * @return DataFile list of orphaned files ready to commit; empty if none found
   */
  public List<DataFile> findOrphanedDataFiles(Map<String, String> partitionVariables)
      throws IOException {
    String hivePath = buildHivePartitionPath(partitionVariables);
    String dataDir = table.location() + "/data"
        + (hivePath.isEmpty() ? "" : "/" + hivePath);

    List<StorageProvider.FileEntry> entries;
    try {
      entries = storageProvider.listFiles(dataDir, true);
    } catch (IOException e) {
      LOGGER.debug("Self-heal: no files at {}: {}", dataDir, e.getMessage());
      return new ArrayList<DataFile>();
    }
    if (entries == null || entries.isEmpty()) {
      return new ArrayList<DataFile>();
    }

    // Build set of paths already tracked by the current snapshot so we don't double-register.
    Set<String> catalogPaths = new HashSet<String>();
    if (table.currentSnapshot() != null) {
      try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
        for (FileScanTask task : tasks) {
          String p = task.file().path().toString();
          int sl = p.lastIndexOf('/');
          catalogPaths.add(sl >= 0 ? p.substring(sl + 1) : p);
        }
      } catch (Exception e) {
        LOGGER.debug("Self-heal: could not scan catalog for {}: {}", dataDir, e.getMessage());
      }
    }

    List<DataFile> orphaned = new ArrayList<DataFile>();
    for (StorageProvider.FileEntry entry : entries) {
      String path = entry.getPath();
      if (!path.endsWith(".parquet")) {
        continue;
      }
      String fileName = path.substring(path.lastIndexOf('/') + 1);
      if (!catalogPaths.contains(fileName)) {
        orphaned.add(buildDataFile(path, entry.getSize()));
      }
    }
    return orphaned;
  }

  /**
   * Builds a Hive-style partition path (e.g. {@code year=2025/month=4}) from partition
   * variables, using the table's partition spec field order.
   */
  private String buildHivePartitionPath(Map<String, String> partitionVariables) {
    if (partitionVariables == null || partitionVariables.isEmpty()) {
      return "";
    }
    PartitionSpec spec = table.spec();
    StringBuilder sb = new StringBuilder();
    for (org.apache.iceberg.PartitionField field : spec.fields()) {
      String name = field.name();
      String val = partitionVariables.get(name);
      if (val != null) {
        if (sb.length() > 0) {
          sb.append("/");
        }
        sb.append(name).append("=").append(val);
      }
    }
    return sb.toString();
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
    final org.apache.iceberg.expressions.Expression deleteFilter = filter;
    underCommitLock(() -> {
      table.newDelete()
          .deleteFromRowFilter(deleteFilter)
          .commit();
    });
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
  @SuppressWarnings("UnusedVariable")
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

    // Handle null indicators like "-" (used by BLS for missing values)
    if (value instanceof String) {
      String strVal = ((String) value).trim();
      if (strVal.isEmpty() || "-".equals(strVal)) {
        return null;
      }
    }

    switch (type.typeId()) {
      case INTEGER:
        if (value instanceof Number) {
          return ((Number) value).intValue();
        }
        try {
          return Integer.parseInt(value.toString());
        } catch (NumberFormatException e) {
          return null;  // Return null for unparseable values
        }
      case LONG:
        if (value instanceof Number) {
          return ((Number) value).longValue();
        }
        try {
          return Long.parseLong(value.toString());
        } catch (NumberFormatException e) {
          return null;
        }
      case FLOAT:
        if (value instanceof Number) {
          return ((Number) value).floatValue();
        }
        try {
          return Float.parseFloat(value.toString());
        } catch (NumberFormatException e) {
          return null;
        }
      case DOUBLE:
        if (value instanceof Number) {
          return ((Number) value).doubleValue();
        }
        try {
          return Double.parseDouble(value.toString());
        } catch (NumberFormatException e) {
          return null;
        }
      case BOOLEAN:
        if (value instanceof Boolean) {
          return value;
        }
        return Boolean.parseBoolean(value.toString());
      case STRING:
        return value.toString();
      case DATE:
        if (value instanceof java.time.LocalDate) {
          return value;
        }
        if (value instanceof Number) {
          return java.time.LocalDate.ofEpochDay(((Number) value).longValue());
        }
        if (value instanceof java.sql.Date) {
          return ((java.sql.Date) value).toLocalDate();
        }
        if (value instanceof String) {
          try {
            return java.time.LocalDate.parse((String) value);
          } catch (Exception e) {
            return null;
          }
        }
        return value;
      case TIMESTAMP:
        if (value instanceof LocalDateTime) {
          return ((LocalDateTime) value).toInstant(ZoneOffset.UTC).toEpochMilli() * 1000;
        }
        if (value instanceof Instant) {
          return ((Instant) value).toEpochMilli() * 1000;
        }
        if (value instanceof java.sql.Timestamp) {
          return ((java.sql.Timestamp) value).getTime() * 1000;
        }
        if (value instanceof Long) {
          // DuckDB JDBC returns TIMESTAMP as microseconds since epoch — convert to LocalDateTime
          // for Iceberg GenericRecord (TIMESTAMP WITHOUT TIMEZONE expects LocalDateTime)
          return LocalDateTime.ofInstant(
              Instant.ofEpochMilli(((Long) value) / 1000), ZoneOffset.UTC);
        }
        if (value instanceof String) {
          String s = ((String) value).trim();
          if (s.isEmpty()) {
            return null;
          }
          try {
            // ISO 8601 with Z suffix (e.g. "2026-06-04T13:38:30Z")
            return LocalDateTime.ofInstant(Instant.parse(s), ZoneOffset.UTC);
          } catch (Exception e1) {
            try {
              // ISO 8601 without timezone (e.g. "2026-06-04T13:38:30")
              return LocalDateTime.parse(s);
            } catch (Exception e2) {
              LOGGER.warn("Could not parse timestamp string '{}': {}", s, e2.getMessage());
              return null;
            }
          }
        }
        return value;
      case LIST:
        // Convert SQL arrays (including DuckDBArray) to Java List for Iceberg
        org.apache.iceberg.types.Types.ListType listType =
            (org.apache.iceberg.types.Types.ListType) type;
        org.apache.iceberg.types.Type elementType = listType.elementType();

        if (value instanceof java.sql.Array) {
          try {
            Object arrayData = ((java.sql.Array) value).getArray();
            return convertArrayToList(arrayData, elementType);
          } catch (java.sql.SQLException e) {
            LOGGER.warn("Failed to convert SQL Array: {}", e.getMessage());
            return null;
          }
        }
        if (value instanceof java.util.List) {
          // Already a list, just coerce elements
          java.util.List<?> inputList = (java.util.List<?>) value;
          java.util.List<Object> result = new java.util.ArrayList<>(inputList.size());
          for (Object elem : inputList) {
            result.add(coerceValue(elem, elementType));
          }
          return result;
        }
        if (value.getClass().isArray()) {
          return convertArrayToList(value, elementType);
        }
        return value;
      default:
        return value;
    }
  }

  /**
   * Converts a Java array to a List, coercing elements to the target Iceberg type.
   */
  private java.util.List<Object> convertArrayToList(Object arrayData,
      org.apache.iceberg.types.Type elementType) {
    int length = java.lang.reflect.Array.getLength(arrayData);
    java.util.List<Object> result = new java.util.ArrayList<>(length);
    for (int i = 0; i < length; i++) {
      Object elem = java.lang.reflect.Array.get(arrayData, i);
      result.add(coerceValue(elem, elementType));
    }
    return result;
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
      underCommitLock(() -> {
        table.expireSnapshots()
            .expireOlderThan(expireSnapshotsMillis)
            .commit();
      });
      LOGGER.info("Expired snapshots older than {} days", expireSnapshotsDays);
    } catch (Exception e) {
      LOGGER.warn("Failed to expire snapshots: {}", e.getMessage());
    }

    // Remove orphan files using core Iceberg API
    // Orphans are data files not referenced by any snapshot
    // Skip if threshold is > 30 days (effectively disabled - scan is expensive)
    if (orphanFilesDays <= 30) {
      try {
        int orphansRemoved = removeOrphanFiles(orphanFilesMillis);
        if (orphansRemoved > 0) {
          LOGGER.info("Removed {} orphan files older than {} days", orphansRemoved, orphanFilesDays);
        }
      } catch (Exception e) {
        LOGGER.warn("Failed to remove orphan files: {}", e.getMessage());
      }
    } else {
      LOGGER.debug("Skipping orphan file detection (threshold {} days > 30)", orphanFilesDays);
    }
  }

  /**
   * Removes orphan data files under the per-table commit lock, against freshly-refreshed metadata.
   *
   * <p>Orphan cleanup diffs the physical data directory against the set of files referenced by any
   * snapshot and raw-deletes the difference — a non-transactional delete that Iceberg's optimistic
   * concurrency does NOT guard. It must therefore hold the same lock as commits and read the current
   * committed metadata: otherwise a sibling writer's just-committed data file is absent from this
   * process's stale in-memory snapshot view and is deleted as a false orphan, leaving the live
   * snapshot pointing at a deleted file (a dangling ref that 404s on read). Holding the lock across
   * the whole scan+delete also blocks concurrent commits from adding a new file mid-scan, which is
   * what makes the 0ms threshold used right after compaction safe.
   *
   * @param olderThanMillis Only delete orphans older than this timestamp
   * @return Number of orphan files removed
   */
  private int removeOrphanFiles(long olderThanMillis) {
    final int[] deleted = new int[1];
    underCommitLock(() -> {
      table.refresh();
      deleted[0] = removeOrphanFilesLocked(olderThanMillis);
    });
    return deleted[0];
  }

  /**
   * Body of {@link #removeOrphanFiles(long)}; must be called while holding the per-table commit lock
   * and after {@link Table#refresh()}. See that method for why.
   *
   * <ol>
   *   <li>Collect all data file paths referenced by any snapshot</li>
   *   <li>List all parquet files in the table's data directory</li>
   *   <li>Delete files that are not referenced and older than the threshold</li>
   * </ol>
   */
  private int removeOrphanFilesLocked(long olderThanMillis) {
    // Collect all data files referenced by ANY snapshot (including ancestors). This "keep" set
    // is what protects live data from deletion, so it MUST be complete: if planning any single
    // snapshot fails, its files are absent from the set and every one of them not also referenced
    // by another snapshot is deleted as a false orphan — leaving the live snapshot pointing at a
    // physically-deleted file (404 on read). A partial keep-set must therefore abort the whole
    // cleanup (delete nothing) rather than silently proceed; orphan reclamation is best-effort and
    // retried on the next maintenance pass, but a wrong deletion is unrecoverable data loss.
    Set<String> referencedFiles = new HashSet<>();

    for (org.apache.iceberg.Snapshot snapshot : table.snapshots()) {
      try (CloseableIterable<FileScanTask> tasks =
          table.newScan().useSnapshot(snapshot.snapshotId()).planFiles()) {
        for (FileScanTask task : tasks) {
          referencedFiles.add(task.file().path().toString());
        }
      } catch (Exception e) {
        throw new IllegalStateException(
            "Aborting orphan-file cleanup for table " + table.name() + ": failed to plan snapshot "
            + snapshot.snapshotId() + " (" + e.getMessage() + "). Refusing to delete against an "
            + "incomplete referenced-file set to avoid deleting live data.", e);
      }
    }

    LOGGER.debug("Found {} files referenced by snapshots", referencedFiles.size());

    // Get the data directory path
    String tableLocation = table.location();
    String dataDir = tableLocation + "/data";

    // List all parquet files in data directory
    List<String> allDataFiles = new ArrayList<>();
    try {
      List<StorageProvider.FileEntry> entries = storageProvider.listFiles(dataDir, true);
      for (StorageProvider.FileEntry entry : entries) {
        if (entry.getPath().endsWith(".parquet")) {
          allDataFiles.add(entry.getPath());
        }
      }
    } catch (IOException e) {
      LOGGER.warn("Failed to list data files in {}: {}", dataDir, e.getMessage());
      return 0;
    }

    // Build set of referenced file names for fast lookup
    // Use just the filename (last path component) to avoid s3:// vs s3a:// issues
    Set<String> referencedFileNames = new HashSet<>();
    for (String refPath : referencedFiles) {
      int lastSlash = refPath.lastIndexOf('/');
      String fileName = lastSlash >= 0 ? refPath.substring(lastSlash + 1) : refPath;
      referencedFileNames.add(fileName);
    }

    // Find orphans (files not referenced by any snapshot)
    List<String> orphanFiles = new ArrayList<>();
    for (String filePath : allDataFiles) {
      int lastSlash = filePath.lastIndexOf('/');
      String fileName = lastSlash >= 0 ? filePath.substring(lastSlash + 1) : filePath;
      boolean isReferenced = referencedFileNames.contains(fileName);

      if (!isReferenced) {
        // Check file age before adding to orphan list
        try {
          StorageProvider.FileMetadata metadata = storageProvider.getMetadata(filePath);
          if (metadata.getLastModified() < olderThanMillis) {
            orphanFiles.add(filePath);
          }
        } catch (IOException e) {
          // Age unknown: do NOT delete. An unreferenced-but-recent file is typically an in-flight
          // commit's data file not yet visible in a snapshot; deleting it on a transient metadata
          // read failure would corrupt that write. Skip it — the next pass reclaims it once its
          // age is readable and it is confirmed past the retention window.
          LOGGER.debug("Skipping orphan candidate {} (age unreadable: {})", filePath, e.getMessage());
        }
      }
    }

    if (orphanFiles.isEmpty()) {
      LOGGER.debug("No orphan files found");
      return 0;
    }

    LOGGER.info("Found {} orphan files to remove", orphanFiles.size());

    // Delete orphan files in batches
    int deleted = 0;
    try {
      storageProvider.deleteBatch(orphanFiles);
      deleted = orphanFiles.size();
    } catch (IOException e) {
      LOGGER.warn("Failed to delete orphan files: {}", e.getMessage());
    }

    return deleted;
  }

  /**
   * Reader-drain window (days) for post-compaction snapshot expiry + orphan deletion when the
   * caller does not supply the table's {@code snapshotRetentionDays}. Compaction must retain the
   * pre-compaction snapshots and their (now-superseded) small files until any concurrent scan that
   * planned against them has drained; deleting at {@code now} pulls files out from under an
   * in-flight reader. Seven days matches the default {@code snapshotRetentionDays}, so the retained
   * history the config already promises is exactly what keeps readers safe.
   */
  private static final int DEFAULT_POST_COMPACTION_RETENTION_DAYS = 7;

  /**
   * Compacts small files in the table into larger files, retaining pre-compaction snapshots/files
   * for {@link #DEFAULT_POST_COMPACTION_RETENTION_DAYS} so concurrent readers stay safe.
   */
  public int compactSmallFiles(long targetFileSizeBytes, int minFilesToCompact,
      long smallFileSizeBytes) throws IOException {
    return compactSmallFiles(targetFileSizeBytes, minFilesToCompact, smallFileSizeBytes,
        DEFAULT_POST_COMPACTION_RETENTION_DAYS);
  }

  /**
   * Compacts small files in the table into larger files.
   *
   * <p>This method scans all data files, groups them by partition, and rewrites
   * partitions that have many small files into fewer larger files targeting the
   * specified file size.
   *
   * <p>The compaction rewrite itself is an atomic Iceberg commit (a new snapshot that references
   * the compacted files); the pre-compaction files remain referenced by the prior snapshots. The
   * post-compaction snapshot-expiry and orphan-file deletion below therefore run against a
   * <b>retention window</b> ({@code retentionDays}), never {@code now}: a query that resolved an
   * older snapshot and is still scanning its manifests would otherwise find those files
   * physically deleted mid-read (a dangling ref / partial-metadata read). Space from the
   * superseded small files is reclaimed on a later pass once they age past the window.
   *
   * @param targetFileSizeBytes Target size for compacted files (default: 128MB)
   * @param minFilesToCompact Minimum number of small files to trigger compaction (default: 10)
   * @param smallFileSizeBytes Files smaller than this are considered "small" (default: 10MB)
   * @param retentionDays Days to retain pre-compaction snapshots/files before expiry+orphan delete
   * @return Number of partitions compacted
   */
  public int compactSmallFiles(long targetFileSizeBytes, int minFilesToCompact,
      long smallFileSizeBytes, int retentionDays) throws IOException {
    LOGGER.info("Starting compaction for table {} (target={}MB, minFiles={}, smallSize={}MB)",
        table.name(), targetFileSizeBytes / (1024 * 1024), minFilesToCompact,
        smallFileSizeBytes / (1024 * 1024));

    if (table.currentSnapshot() == null) {
      LOGGER.info("Table has no snapshots, nothing to compact");
      return 0;
    }

    // Group files by partition
    Map<String, List<FileScanTask>> partitionFiles = new HashMap<>();
    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      for (FileScanTask task : tasks) {
        String partitionKey = task.file().partition().toString();
        partitionFiles.computeIfAbsent(partitionKey, k -> new ArrayList<>()).add(task);
      }
    }

    int compactedPartitions = 0;
    for (Map.Entry<String, List<FileScanTask>> entry : partitionFiles.entrySet()) {
      String partitionKey = entry.getKey();
      List<FileScanTask> tasks = entry.getValue();

      // Count small files in this partition
      List<FileScanTask> smallFiles = new ArrayList<>();
      for (FileScanTask task : tasks) {
        if (task.file().fileSizeInBytes() < smallFileSizeBytes) {
          smallFiles.add(task);
        }
      }

      if (smallFiles.size() >= minFilesToCompact) {
        LOGGER.info("Compacting partition {} with {} small files", partitionKey, smallFiles.size());
        try {
          compactPartition(smallFiles, targetFileSizeBytes);
          compactedPartitions++;
        } catch (Throwable e) {
          LOGGER.warn("Failed to compact partition {}: {}", partitionKey, e.getMessage());
        }
      }
    }

    if (compactedPartitions > 0) {
      LOGGER.info("Compaction complete: {} partitions compacted, retaining superseded files for "
          + "{} days before cleanup", compactedPartitions, retentionDays);
      // Reclaim only files whose owning snapshots have aged past the retention window. The
      // compaction rewrite already committed a new snapshot; the pre-compaction snapshots and
      // their small files stay live until `retentionCutoff` so an in-flight reader that planned
      // against an older snapshot still finds every file it references. Expiring/deleting at `now`
      // (the previous behavior) raced concurrent readers and could yank files mid-scan.
      long retentionCutoff = System.currentTimeMillis()
          - TimeUnit.DAYS.toMillis(retentionDays);
      try {
        int expiredCount = 0;
        for (org.apache.iceberg.Snapshot snapshot : table.snapshots()) {
          if (snapshot.snapshotId() != table.currentSnapshot().snapshotId()
              && snapshot.timestampMillis() < retentionCutoff) {
            expiredCount++;
          }
        }
        if (expiredCount > 0) {
          underCommitLock(() -> {
            table.expireSnapshots()
                .expireOlderThan(retentionCutoff)
                .retainLast(1)
                .commit();
          });
          LOGGER.info("Expired {} snapshots older than {} days after compaction",
              expiredCount, retentionDays);
        }
      } catch (Exception e) {
        LOGGER.warn("Failed to expire snapshots after compaction: {}", e.getMessage());
      }

      // Remove only orphans older than the retention window; the just-superseded small files
      // (age ~0) are intentionally left for a later pass so concurrent readers keep working.
      try {
        int orphansRemoved = removeOrphanFiles(retentionCutoff);
        if (orphansRemoved > 0) {
          LOGGER.info("Removed {} orphan files older than {} days after compaction",
              orphansRemoved, retentionDays);
        }
      } catch (Exception e) {
        LOGGER.warn("Failed to remove orphan files after compaction: {}", e.getMessage());
      }
    } else {
      LOGGER.info("Compaction complete: no partitions needed compaction");
    }
    return compactedPartitions;
  }

  /**
   * Compacts files within a single partition using streaming to avoid OOM.
   *
   * <p>This method streams records from input files directly to output files
   * without loading all records into memory. This allows compaction of partitions
   * with millions of records that would otherwise cause OutOfMemoryError.
   */
  private void compactPartition(List<FileScanTask> smallFiles, long targetFileSizeBytes)
      throws IOException {
    if (smallFiles.isEmpty()) {
      return;
    }

    Schema schema = table.schema();
    PartitionSpec spec = table.spec();

    long totalRecords = 0;
    long totalBytes = 0;
    Set<DataFile> filesToDelete = new HashSet<>();

    for (FileScanTask task : smallFiles) {
      DataFile dataFile = task.file();
      filesToDelete.add(dataFile);
      totalRecords += dataFile.recordCount();
      totalBytes += dataFile.fileSizeInBytes();
    }

    if (totalRecords == 0) {
      return;
    }

    // Java reader: uses table.io() FileIO which has S3 credentials already configured
    StructLike partitionData = smallFiles.get(0).file().partition();
    PartitionKey partitionKey = new PartitionKey(spec, schema);
    copyPartitionValues(partitionKey, partitionData, spec);

    double avgBytesPerRecord = (double) totalBytes / totalRecords;
    int recordsPerFile = Math.max(1000, (int) (targetFileSizeBytes / avgBytesPerRecord));

    LOGGER.info("Java compaction: {} records from {} files, ~{} records per output file",
        totalRecords, smallFiles.size(), recordsPerFile);

    String dataLocation = table.location() + "/data";
    String partitionPath = buildPartitionPathFromKey(partitionKey, spec);

    List<DataFile> newFiles = new ArrayList<>();
    DataWriter<Record> currentWriter = null;
    int currentRecordCount = 0;
    int totalWritten = 0;

    try {
      for (FileScanTask task : smallFiles) {
        DataFile dataFile = task.file();
        InputFile inputFile = table.io().newInputFile(dataFile.path().toString());

        try (CloseableIterable<Record> records = Parquet.read(inputFile)
            .project(schema)
            .createReaderFunc(fileSchema ->
                org.apache.iceberg.data.parquet.GenericParquetReaders.buildReader(
                    schema, fileSchema))
            .build()) {

          for (Record record : records) {
            if (currentWriter == null) {
              String outputPath = dataLocation + "/" + partitionPath + "/compacted_"
                  + java.util.UUID.randomUUID().toString().substring(0, 8) + ".parquet";
              if (outputPath.startsWith("s3://")) {
                outputPath = "s3a://" + outputPath.substring(5);
              }
              OutputFile outputFile = table.io().newOutputFile(outputPath);
              currentWriter = Parquet.writeData(outputFile)
                  .schema(schema)
                  .withSpec(spec)
                  .withPartition(partitionKey)
                  .createWriterFunc(GenericParquetWriter::buildWriter)
                  .overwrite()
                  .build();
              currentRecordCount = 0;
            }
            currentWriter.write(record);
            currentRecordCount++;
            totalWritten++;
            if (currentRecordCount >= recordsPerFile) {
              currentWriter.close();
              newFiles.add(currentWriter.toDataFile());
              currentWriter = null;
            }
          }
        } catch (Exception e) {
          LOGGER.warn("Failed to read file {}: {}", dataFile.path(), e.getMessage());
        }
      }

      if (currentWriter != null) {
        currentWriter.close();
        newFiles.add(currentWriter.toDataFile());
        currentWriter = null;
      }
    } finally {
      if (currentWriter != null) {
        try {
          currentWriter.close();
        } catch (Exception e) {
          LOGGER.warn("Failed to close writer: {}", e.getMessage());
        }
      }
    }

    if (newFiles.isEmpty()) {
      LOGGER.warn("No records written during compaction");
      return;
    }

    LOGGER.info("Java compaction wrote {} records to {} files", totalWritten, newFiles.size());

    // Commit the rewrite: delete old files, add new files
    RewriteFiles rewrite = table.newRewrite();
    // Set validation snapshot to current to avoid lineage errors
    // when old snapshots have been expired
    if (table.currentSnapshot() != null) {
      rewrite.validateFromSnapshot(table.currentSnapshot().snapshotId());
    }
    for (DataFile oldFile : filesToDelete) {
      rewrite.deleteFile(oldFile);
    }
    for (DataFile newFile : newFiles) {
      rewrite.addFile(newFile);
    }
    underCommitLock(() -> {
      rewrite.commit();
    });

    LOGGER.info("Compacted {} files into {} files", filesToDelete.size(), newFiles.size());
  }


  /**
   * Copies partition values from StructLike to PartitionKey.
   */
  private void copyPartitionValues(PartitionKey key, StructLike source, PartitionSpec spec) {
    for (int i = 0; i < spec.fields().size(); i++) {
      Object value = source.get(i, Object.class);
      key.set(i, value);
    }
  }

  /**
   * Builds partition path from PartitionKey.
   */
  private String buildPartitionPathFromKey(PartitionKey key, PartitionSpec spec) {
    StringBuilder path = new StringBuilder();
    List<org.apache.iceberg.PartitionField> fields = spec.fields();
    for (int i = 0; i < fields.size(); i++) {
      if (path.length() > 0) {
        path.append("/");
      }
      path.append(fields.get(i).name()).append("=").append(key.get(i, Object.class));
    }
    return path.toString();
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
   * Ensures that version-hint.text exists in the metadata directory.
   *
   * <p>This method repairs Iceberg tables that are missing their version-hint.text file,
   * which can occur due to partial commits, interrupted writes, or previous buggy code.
   * Without version-hint.text, DuckDB's iceberg_scan (without unsafe_enable_version_guessing)
   * and other Iceberg readers cannot discover the latest metadata.
   *
   * <p>The method:
   * <ol>
   *   <li>Lists all files in the metadata directory</li>
   *   <li>Finds the highest-versioned metadata JSON file (e.g., v4.metadata.json)</li>
   *   <li>Checks if version-hint.text exists</li>
   *   <li>If missing, creates version-hint.text with the correct version number</li>
   * </ol>
   */
  private void ensureVersionHint() {
    try {
      String metadataDir = table.location() + "/metadata";
      String versionHintPath = metadataDir + "/version-hint.text";

      // Check if version-hint.text already exists
      try {
        StorageProvider.FileMetadata metadata = storageProvider.getMetadata(versionHintPath);
        if (metadata != null && metadata.getSize() > 0) {
          LOGGER.debug("version-hint.text already exists at {}", versionHintPath);
          return;
        }
      } catch (IOException e) {
        // File doesn't exist, need to create it
        LOGGER.debug("version-hint.text not found, will create: {}", e.getMessage());
      }

      // List metadata directory to find latest version
      List<StorageProvider.FileEntry> metadataFiles;
      try {
        metadataFiles = storageProvider.listFiles(metadataDir, false);
      } catch (IOException e) {
        LOGGER.warn("Cannot list metadata directory {}: {}", metadataDir, e.getMessage());
        return;
      }

      // Find the highest version number from metadata files (v1.metadata.json, v2.metadata.json, etc.)
      int maxVersion = 0;
      java.util.regex.Pattern versionPattern = java.util.regex.Pattern.compile("v(\\d+)\\.metadata\\.json$");
      for (StorageProvider.FileEntry entry : metadataFiles) {
        String fileName = entry.getPath();
        int lastSlash = fileName.lastIndexOf('/');
        if (lastSlash >= 0) {
          fileName = fileName.substring(lastSlash + 1);
        }
        java.util.regex.Matcher matcher = versionPattern.matcher(fileName);
        if (matcher.find()) {
          int version = Integer.parseInt(matcher.group(1));
          if (version > maxVersion) {
            maxVersion = version;
          }
        }
      }

      if (maxVersion == 0) {
        LOGGER.warn("No metadata files found in {}, cannot create version-hint.text", metadataDir);
        return;
      }

      // Create version-hint.text with the version number (no trailing newline)
      String versionContent = String.valueOf(maxVersion);
      storageProvider.writeFile(versionHintPath, versionContent.getBytes(java.nio.charset.StandardCharsets.UTF_8));
      LOGGER.info("Created missing version-hint.text with version {} at {}", maxVersion, versionHintPath);

    } catch (Exception e) {
      // Non-fatal - log and continue
      LOGGER.warn("Failed to ensure version-hint.text: {}", e.getMessage());
    }
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
