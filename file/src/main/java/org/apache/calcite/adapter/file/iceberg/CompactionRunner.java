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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.StaticTableOperations;
import org.apache.iceberg.Table;
import org.apache.iceberg.hadoop.HadoopFileIO;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Standalone compaction runner for Iceberg tables.
 *
 * <p>Connects to an Iceberg table via Hadoop catalog, scans for small files,
 * and compacts them into larger files. Also expires old snapshots and removes
 * orphan files after compaction.
 *
 * <p>Usage:
 * <pre>{@code
 * java -cp govdata-all.jar org.apache.calcite.adapter.file.iceberg.CompactionRunner \
 *   --warehouse s3://bucket/warehouse \
 *   --table table_name \
 *   --target-file-size 134217728 \
 *   --min-files 3 \
 *   --small-file-size 10485760
 * }</pre>
 */
public class CompactionRunner {

  private static final Logger LOGGER = LoggerFactory.getLogger(CompactionRunner.class);

  public static void main(String[] args) {
    String warehouse = null;
    String tableName = null;
    long targetFileSize = 128L * 1024 * 1024;
    int minFiles = 3;
    long smallFileSize = 10L * 1024 * 1024;

    for (int i = 0; i < args.length; i++) {
      switch (args[i]) {
      case "--warehouse":
        warehouse = args[++i];
        break;
      case "--table":
        tableName = args[++i];
        break;
      case "--target-file-size":
        targetFileSize = Long.parseLong(args[++i]);
        break;
      case "--min-files":
        minFiles = Integer.parseInt(args[++i]);
        break;
      case "--small-file-size":
        smallFileSize = Long.parseLong(args[++i]);
        break;
      default:
        System.err.println("Unknown argument: " + args[i]);
        System.exit(1);
      }
    }

    if (warehouse == null || tableName == null) {
      System.err.println("Usage: CompactionRunner --warehouse <path> --table <name>"
          + " [--target-file-size <bytes>] [--min-files <n>] [--small-file-size <bytes>]");
      System.exit(1);
    }

    // Convert s3:// to s3a:// for Hadoop compatibility
    String hadoopWarehouse = warehouse.replace("s3://", "s3a://");

    Configuration conf = new Configuration();
    // S3 credentials from environment
    String accessKey = System.getenv("AWS_ACCESS_KEY_ID");
    String secretKey = System.getenv("AWS_SECRET_ACCESS_KEY");
    String endpoint = System.getenv("AWS_ENDPOINT_OVERRIDE");

    if (accessKey != null) {
      conf.set("fs.s3a.access.key", accessKey);
    }
    if (secretKey != null) {
      conf.set("fs.s3a.secret.key", secretKey);
    }
    if (endpoint != null) {
      conf.set("fs.s3a.endpoint", endpoint);
      conf.set("fs.s3a.path.style.access", "true");
      // R2/MinIO compatibility: disable change detection (returns 403 not 404 for missing objects)
      conf.set("fs.s3a.change.detection.mode", "none");
      conf.set("fs.s3a.change.detection.version.required", "false");
    }
    conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
    conf.set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");

    try {
      String tablePath = hadoopWarehouse.replace("s3://", "s3a://") + "/" + tableName;

      // Load table by finding the latest metadata file directly.
      // This bypasses version-hint.text which R2 returns 403 for when missing.
      Table table = loadTableDirect(conf, tablePath);

      // Report pre-compaction state
      long fileCount = 0;
      long totalSize = 0;
      long smallCount = 0;
      final long smallThreshold = smallFileSize;
      try (org.apache.iceberg.io.CloseableIterable<org.apache.iceberg.FileScanTask> tasks
               = table.newScan().planFiles()) {
        for (org.apache.iceberg.FileScanTask task : tasks) {
          fileCount++;
          totalSize += task.file().fileSizeInBytes();
          if (task.file().fileSizeInBytes() < smallThreshold) {
            smallCount++;
          }
        }
      }

      System.out.println("    Before: " + fileCount + " files, "
          + (totalSize / (1024 * 1024)) + " MB total, "
          + smallCount + " small files (<" + (smallFileSize / (1024 * 1024)) + "MB)");

      if (smallCount < minFiles) {
        System.out.println("    Skipping: only " + smallCount
            + " small files (need " + minFiles + ")");
        return;
      }

      // Run compaction
      IcebergTableWriter writer = new IcebergTableWriter(table, null);
      int compacted = writer.compactSmallFiles(targetFileSize, minFiles, smallFileSize);

      // Report post-compaction state
      long afterFileCount = 0;
      long afterTotalSize = 0;
      try (org.apache.iceberg.io.CloseableIterable<org.apache.iceberg.FileScanTask> tasks
               = table.newScan().planFiles()) {
        for (org.apache.iceberg.FileScanTask task : tasks) {
          afterFileCount++;
          afterTotalSize += task.file().fileSizeInBytes();
        }
      }

      System.out.println("    After:  " + afterFileCount + " files, "
          + (afterTotalSize / (1024 * 1024)) + " MB total");
      System.out.println("    Compacted " + compacted + " partitions ("
          + fileCount + " -> " + afterFileCount + " files)");

    } catch (Exception e) {
      System.err.println("    ERROR: " + e.getMessage());
      LOGGER.error("Compaction failed for {}", tableName, e);
      System.exit(1);
    }
  }

  /**
   * Loads an Iceberg table by scanning the metadata directory for the latest
   * version file directly. This bypasses HadoopTableOperations.findVersion()
   * which fails on R2/MinIO because missing objects return 403 (not 404).
   */
  private static Table loadTableDirect(Configuration conf, String tablePath)
      throws Exception {
    FileSystem fs = FileSystem.get(new java.net.URI(tablePath), conf);
    Path metadataDir = new Path(tablePath + "/metadata");

    // Find highest version metadata file
    int maxVersion = 0;
    String latestMetadataPath = null;
    FileStatus[] files = fs.listStatus(metadataDir);
    for (FileStatus file : files) {
      String name = file.getPath().getName();
      if (name.startsWith("v") && name.endsWith(".metadata.json")) {
        String numStr = name.substring(1, name.indexOf('.'));
        try {
          int v = Integer.parseInt(numStr);
          if (v > maxVersion) {
            maxVersion = v;
            latestMetadataPath = file.getPath().toString();
          }
        } catch (NumberFormatException nfe) {
          // skip
        }
      }
    }

    if (latestMetadataPath == null) {
      throw new IllegalStateException("No metadata files found in " + metadataDir);
    }

    System.out.println("    Loading metadata: v" + maxVersion);

    // Parse metadata and create read-write table via StaticTableOperations
    HadoopFileIO fileIO = new HadoopFileIO(conf);
    StaticTableOperations ops = new StaticTableOperations(latestMetadataPath, fileIO);
    return new BaseTable(ops, tableName(tablePath));
  }

  private static String tableName(String tablePath) {
    int lastSlash = tablePath.lastIndexOf('/');
    return lastSlash >= 0 ? tablePath.substring(lastSlash + 1) : tablePath;
  }
}
