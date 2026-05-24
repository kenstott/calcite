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
package org.apache.calcite.adapter.govdata.etl;

import org.apache.calcite.adapter.file.iceberg.IcebergCatalogManager;
import org.apache.calcite.adapter.file.iceberg.IcebergTableWriter;
import org.apache.calcite.adapter.file.storage.StorageProvider;
import org.apache.calcite.adapter.file.storage.StorageProviderFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Table;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Standalone Iceberg maintenance runner for compaction and cleanup.
 *
 * <p>Compacts small files into larger ones to reduce metadata overhead,
 * which dramatically improves query performance for tools like DuckDB
 * iceberg_scan() that must read all manifest files.
 *
 * <p>Usage:
 * <pre>
 * # Compact a table on S3/R2
 * java -cp "build/libs/*" org.apache.calcite.adapter.govdata.etl.IcebergMaintenanceRunner \
 *   --warehouse s3a://govdata-parquet-v1/sec \
 *   --table vectorized_chunks \
 *   --s3-access-key $AWS_ACCESS_KEY_ID \
 *   --s3-secret-key $AWS_SECRET_ACCESS_KEY \
 *   --s3-endpoint $AWS_ENDPOINT_OVERRIDE
 *
 * # Dry run (report only)
 * java -cp "build/libs/*" org.apache.calcite.adapter.govdata.etl.IcebergMaintenanceRunner \
 *   --warehouse s3a://govdata-parquet-v1/sec \
 *   --table vectorized_chunks \
 *   --s3-access-key $AWS_ACCESS_KEY_ID \
 *   --s3-secret-key $AWS_SECRET_ACCESS_KEY \
 *   --s3-endpoint $AWS_ENDPOINT_OVERRIDE \
 *   --dry-run
 * </pre>
 *
 * <p>Exit codes:
 * <ul>
 *   <li>0 - SUCCESS</li>
 *   <li>1 - PARTIAL (compaction ran but some partitions failed)</li>
 *   <li>2 - FAILED (critical error)</li>
 * </ul>
 */
@SuppressWarnings("UnusedVariable")
public class IcebergMaintenanceRunner {

  private static final Logger LOGGER = LoggerFactory.getLogger(IcebergMaintenanceRunner.class);

  private static final long DEFAULT_TARGET_FILE_SIZE = 128 * 1024 * 1024; // 128MB
  private static final int DEFAULT_MIN_FILES_TO_COMPACT = 10;
  private static final long DEFAULT_SMALL_FILE_SIZE = 10 * 1024 * 1024; // 10MB
  private static final int DEFAULT_EXPIRE_SNAPSHOTS_DAYS = 3;
  private static final int DEFAULT_ORPHAN_FILES_DAYS = 1;

  public static void main(String[] args) {
    int exitCode = 2;
    try {
      Config config = Config.fromArgs(args);
      exitCode = new IcebergMaintenanceRunner().run(config);
    } catch (IllegalArgumentException e) {
      System.err.println("Error: " + e.getMessage());
      System.err.println();
      printUsage();
      exitCode = 2;
    } catch (Exception e) {
      System.err.println("Fatal error: " + e.getMessage());
      e.printStackTrace(System.err);
      exitCode = 2;
    }
    System.exit(exitCode);
  }

  public int run(Config config) throws Exception {
    System.out.println("=".repeat(60));
    System.out.println("Iceberg Maintenance: " + config.tableName);
    System.out.println("=".repeat(60));
    System.out.println("  Warehouse: " + config.warehouse);
    System.out.println("  Target file size: " + (config.targetFileSize / (1024 * 1024)) + " MB");
    System.out.println("  Min files to compact: " + config.minFilesToCompact);
    System.out.println("  Small file threshold: " + (config.smallFileSize / (1024 * 1024)) + " MB");
    System.out.println("  Dry run: " + config.dryRun);
    System.out.println();

    // Build Hadoop configuration for S3/R2
    Configuration hadoopConf = new Configuration();
    Map<String, String> hadoopConfig = new HashMap<>();

    if (config.s3AccessKey != null) {
      hadoopConf.set("fs.s3a.access.key", config.s3AccessKey);
      hadoopConf.set("fs.s3a.secret.key", config.s3SecretKey);
      hadoopConf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
      hadoopConf.set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");

      hadoopConfig.put("fs.s3a.access.key", config.s3AccessKey);
      hadoopConfig.put("fs.s3a.secret.key", config.s3SecretKey);

      if (config.s3Endpoint != null) {
        String endpoint = config.s3Endpoint;
        hadoopConf.set("fs.s3a.endpoint", endpoint);
        hadoopConf.set("fs.s3a.path.style.access", "true");
        hadoopConfig.put("fs.s3a.endpoint", endpoint);
        hadoopConfig.put("fs.s3a.path.style.access", "true");
      }
    }

    // Load the Iceberg table
    System.out.println("Loading table...");
    Map<String, Object> catalogConfig = new HashMap<>();
    catalogConfig.put("catalog", "hadoop");
    catalogConfig.put("warehouse", config.warehouse);
    catalogConfig.put("hadoopConfig", hadoopConfig);

    Table table = IcebergCatalogManager.loadTable(catalogConfig, config.tableName);
    System.out.println("  Table loaded: " + table.name());
    System.out.println("  Location: " + table.location());
    System.out.println("  Snapshots: " + countSnapshots(table));
    System.out.println(
        "  Current snapshot: " + (table.currentSnapshot() != null
        ? table.currentSnapshot().snapshotId() : "none"));

    // Count data files
    int totalFiles = 0;
    long totalSize = 0;
    Map<String, Integer> filesPerPartition = new HashMap<>();
    Map<String, Long> sizePerPartition = new HashMap<>();

    for (org.apache.iceberg.FileScanTask task : table.newScan().planFiles()) {
      totalFiles++;
      totalSize += task.file().fileSizeInBytes();
      String partition = task.file().partition().toString();
      filesPerPartition.merge(partition, 1, Integer::sum);
      sizePerPartition.merge(partition, task.file().fileSizeInBytes(), Long::sum);
    }

    System.out.println("  Data files: " + totalFiles);
    System.out.println("  Total size: " + (totalSize / (1024 * 1024)) + " MB");
    System.out.println();

    // Report per-partition stats
    System.out.println("Partition statistics:");
    int partitionsNeedingCompaction = 0;
    for (Map.Entry<String, Integer> entry
        : filesPerPartition.entrySet().stream()
            .sorted(Map.Entry.<String, Integer>comparingByValue().reversed())
            .collect(java.util.stream.Collectors.toList())) {
      String partition = entry.getKey();
      int files = entry.getValue();
      long size = sizePerPartition.getOrDefault(partition, 0L);
      String marker = files >= config.minFilesToCompact ? " <-- COMPACT" : "";
      System.out.printf("  %-30s %5d files  %6d MB%s%n",
          partition, files, size / (1024 * 1024), marker);
      if (files >= config.minFilesToCompact) {
        partitionsNeedingCompaction++;
      }
    }
    System.out.println();
    System.out.println("Partitions needing compaction: " + partitionsNeedingCompaction);

    if (config.dryRun) {
      System.out.println("\n[DRY RUN] Would compact " + partitionsNeedingCompaction + " partitions");
      return 0;
    }

    if (partitionsNeedingCompaction == 0) {
      System.out.println("No partitions need compaction.");
      return 0;
    }

    // Create storage provider for S3/R2
    Map<String, Object> s3Config = new HashMap<>();
    if (config.s3AccessKey != null) {
      s3Config.put("accessKeyId", config.s3AccessKey);
      s3Config.put("secretAccessKey", config.s3SecretKey);
      if (config.s3Endpoint != null) {
        s3Config.put("endpoint", config.s3Endpoint);
      }
      s3Config.put("region", "us-east-1");
    }

    StorageProvider storageProvider = config.s3AccessKey != null
        ? StorageProviderFactory.createFromType("s3", s3Config)
        : StorageProviderFactory.createFromType("local", null);

    IcebergTableWriter writer = new IcebergTableWriter(table, storageProvider);

    // First expire old snapshots to clean up lineage
    // (RewriteFiles needs valid snapshot lineage to commit)
    System.out.println("\nExpiring old snapshots first (required for compaction)...");
    writer.runMaintenance(config.expireSnapshotsDays, config.orphanFilesDays);
    table.refresh();
    System.out.println("  Snapshots after cleanup: " + countSnapshots(table));

    // Run compaction
    System.out.println("\nRunning compaction...");
    int compacted =
        writer.compactSmallFiles(config.targetFileSize, config.minFilesToCompact, config.smallFileSize);

    System.out.println("\nCompacted " + compacted + " partitions");

    // Final stats
    table.refresh();
    int finalFiles = 0;
    for (org.apache.iceberg.FileScanTask task : table.newScan().planFiles()) {
      finalFiles++;
    }

    System.out.println("\n"
  + "=".repeat(60));
    System.out.println("Maintenance Complete");
    System.out.println("=".repeat(60));
    System.out.println("  Files before: " + totalFiles);
    System.out.println("  Files after:  " + finalFiles);
    System.out.println("  Partitions compacted: " + compacted);
    System.out.println("  Snapshots: " + countSnapshots(table));

    return compacted > 0 ? 0 : 1;
  }

  private int countSnapshots(Table table) {
    int count = 0;
    for (org.apache.iceberg.Snapshot s : table.snapshots()) {
      count++;
    }
    return count;
  }

  private static void printUsage() {
    System.err.println("Usage: IcebergMaintenanceRunner [options]");
    System.err.println();
    System.err.println("Required:");
    System.err.println("  --warehouse PATH        Iceberg warehouse path (e.g., s3a://bucket/warehouse)");
    System.err.println("  --table NAME            Table name");
    System.err.println();
    System.err.println("S3/R2 options:");
    System.err.println("  --s3-access-key KEY     S3 access key ID");
    System.err.println("  --s3-secret-key KEY     S3 secret access key");
    System.err.println("  --s3-endpoint URL       S3 endpoint (for R2/MinIO)");
    System.err.println();
    System.err.println("Compaction options:");
    System.err.println("  --target-size-mb N      Target compacted file size (default: 128)");
    System.err.println("  --min-files N           Min files to trigger compaction (default: 10)");
    System.err.println("  --small-size-mb N       Small file threshold in MB (default: 10)");
    System.err.println("  --expire-days N         Expire snapshots older than N days (default: 3)");
    System.err.println("  --orphan-days N         Remove orphans older than N days (default: 1)");
    System.err.println("  --dry-run               Report only, no changes");
  }

  static class Config {
    String warehouse;
    String tableName;
    String s3AccessKey;
    String s3SecretKey;
    String s3Endpoint;
    long targetFileSize = DEFAULT_TARGET_FILE_SIZE;
    int minFilesToCompact = DEFAULT_MIN_FILES_TO_COMPACT;
    long smallFileSize = DEFAULT_SMALL_FILE_SIZE;
    int expireSnapshotsDays = DEFAULT_EXPIRE_SNAPSHOTS_DAYS;
    int orphanFilesDays = DEFAULT_ORPHAN_FILES_DAYS;
    boolean dryRun = false;

    static Config fromArgs(String[] args) {
      Config config = new Config();
      for (int i = 0; i < args.length; i++) {
        switch (args[i]) {
        case "--warehouse":
          config.warehouse = args[++i];
          break;
        case "--table":
          config.tableName = args[++i];
          break;
        case "--s3-access-key":
          config.s3AccessKey = args[++i];
          break;
        case "--s3-secret-key":
          config.s3SecretKey = args[++i];
          break;
        case "--s3-endpoint":
          config.s3Endpoint = args[++i];
          break;
        case "--target-size-mb":
          config.targetFileSize = Long.parseLong(args[++i]) * 1024 * 1024;
          break;
        case "--min-files":
          config.minFilesToCompact = Integer.parseInt(args[++i]);
          break;
        case "--small-size-mb":
          config.smallFileSize = Long.parseLong(args[++i]) * 1024 * 1024;
          break;
        case "--expire-days":
          config.expireSnapshotsDays = Integer.parseInt(args[++i]);
          break;
        case "--orphan-days":
          config.orphanFilesDays = Integer.parseInt(args[++i]);
          break;
        case "--dry-run":
          config.dryRun = true;
          break;
        case "--help":
          printUsage();
          System.exit(0);
          break;
        default:
          throw new IllegalArgumentException("Unknown argument: " + args[i]);
        }
      }

      if (config.warehouse == null) {
        throw new IllegalArgumentException("--warehouse is required");
      }
      if (config.tableName == null) {
        throw new IllegalArgumentException("--table is required");
      }
      return config;
    }
  }
}
