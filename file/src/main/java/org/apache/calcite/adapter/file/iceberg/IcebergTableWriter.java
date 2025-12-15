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

import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
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
  private final Configuration hadoopConf;

  /**
   * Creates a writer for the specified Iceberg table.
   *
   * @param table The Iceberg table to write to
   */
  public IcebergTableWriter(Table table) {
    this.table = table;
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
  public void commitFromStaging(java.nio.file.Path stagingPath,
      Map<String, Object> partitionFilter) throws IOException {
    String dataLocation = table.location() + "/data";
    java.nio.file.Path dataPath = java.nio.file.Paths.get(dataLocation.replace("file:", ""));

    // Ensure data directory exists
    Files.createDirectories(dataPath);

    // Walk staging directory and move files
    List<DataFile> dataFiles = new ArrayList<DataFile>();
    moveFilesAndBuildDataFiles(stagingPath, dataPath, dataFiles);

    if (dataFiles.isEmpty()) {
      LOGGER.warn("No data files found in staging directory: {}", stagingPath);
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
        filter = Expressions.and(filter,
            Expressions.equal(entry.getKey(), entry.getValue()));
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
   * Moves files from staging to data location and builds DataFile metadata.
   */
  private void moveFilesAndBuildDataFiles(java.nio.file.Path stagingPath,
      java.nio.file.Path dataPath, List<DataFile> dataFiles) throws IOException {

    Files.walkFileTree(stagingPath, new SimpleFileVisitor<java.nio.file.Path>() {
      @Override
      public FileVisitResult visitFile(java.nio.file.Path file,
          BasicFileAttributes attrs) throws IOException {
        if (file.toString().endsWith(".parquet")) {
          // Preserve partition path: staging/year=2020/data.parquet -> data/year=2020/data.parquet
          java.nio.file.Path relativePath = stagingPath.relativize(file);
          java.nio.file.Path finalPath = dataPath.resolve(relativePath);

          // Create parent directories
          Files.createDirectories(finalPath.getParent());

          // Move file (atomic on same filesystem)
          Files.move(file, finalPath, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
          LOGGER.debug("Moved {} to {}", file, finalPath);

          // Build DataFile for the moved file
          DataFile dataFile = buildDataFile(finalPath, attrs.size());
          dataFiles.add(dataFile);
        }
        return FileVisitResult.CONTINUE;
      }

      @Override
      public FileVisitResult postVisitDirectory(java.nio.file.Path dir,
          IOException exc) throws IOException {
        // Clean up empty directories in staging
        if (!dir.equals(stagingPath)) {
          try {
            Files.deleteIfExists(dir);
          } catch (IOException e) {
            // Directory not empty, ignore
          }
        }
        return FileVisitResult.CONTINUE;
      }
    });
  }

  /**
   * Builds a DataFile for a Parquet file with partition information extracted from path.
   */
  private DataFile buildDataFile(java.nio.file.Path filePath, long fileSize) {
    String pathStr = filePath.toString();
    PartitionSpec spec = table.spec();

    // Extract partition values from Hive-style path
    org.apache.iceberg.PartitionData partitionData = new org.apache.iceberg.PartitionData(spec.partitionType());
    String relativePath = pathStr.substring(pathStr.indexOf("/data/") + 6);
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
      filter = Expressions.and(filter,
          Expressions.equal(entry.getKey(), entry.getValue()));
    }

    LOGGER.info("Deleting partition with filter: {}", partitionFilter);
    table.newDelete()
        .deleteFromRowFilter(filter)
        .commit();
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
}
