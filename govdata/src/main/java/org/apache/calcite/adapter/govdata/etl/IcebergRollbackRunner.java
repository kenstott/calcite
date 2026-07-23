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
package org.apache.calcite.adapter.govdata.etl;

import org.apache.calcite.adapter.file.iceberg.IcebergCatalogManager;
import org.apache.calcite.adapter.file.storage.StorageProvider;
import org.apache.calcite.adapter.file.storage.StorageProviderFactory;

import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.CloseableIterable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Repairs an Iceberg table whose current snapshot references data files that no longer exist in
 * the object store (a dangling reference that 404s on read) by rolling the table's current pointer
 * back to the newest ancestor snapshot whose every data file is physically present.
 *
 * <p>This is a non-destructive recovery: it writes a new metadata version whose current snapshot is
 * a prior, fully-present one. No data files are deleted. If no snapshot in the lineage is fully
 * present, the table cannot be recovered by rollback and must be re-ingested — the runner reports
 * this with exit code 3 and changes nothing.
 *
 * <p>File existence is checked by basename against a single recursive listing of the table's
 * {@code /data} directory, which avoids {@code s3://} vs {@code s3a://} scheme mismatches between
 * Iceberg's recorded paths and the storage provider's own listing (the same approach the orphan
 * scanner in {@code IcebergTableWriter} uses).
 *
 * <p>Usage mirrors {@link IcebergMaintenanceRunner}:
 * <pre>
 * java -cp build/libs/sih-govdata.jar \
 *   org.apache.calcite.adapter.govdata.etl.IcebergRollbackRunner \
 *   --warehouse s3a://govdata-parquet-v1/econ --table state_personal_income \
 *   --s3-access-key $AWS_ACCESS_KEY_ID --s3-secret-key $AWS_SECRET_ACCESS_KEY \
 *   --s3-endpoint $AWS_ENDPOINT_OVERRIDE [--dry-run]
 * </pre>
 *
 * <p>Exit codes:
 * <ul>
 *   <li>0 - OK: current snapshot already fully present, or rolled back to one that is</li>
 *   <li>2 - FAILED (bad args / could not load table / commit failed)</li>
 *   <li>3 - NEEDS_REINGEST: no snapshot in the lineage is fully present</li>
 * </ul>
 */
public class IcebergRollbackRunner {

  private static final Logger LOGGER = LoggerFactory.getLogger(IcebergRollbackRunner.class);
  private static final String BAR =
      "============================================================";

  public static void main(String[] args) {
    int exitCode = 2;
    try {
      Config config = Config.fromArgs(args);
      exitCode = new IcebergRollbackRunner().run(config);
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
    System.out.println(BAR);
    System.out.println("Iceberg Rollback Repair: " + config.tableName);
    System.out.println(BAR);
    System.out.println("  Warehouse: " + config.warehouse);
    System.out.println("  Dry run: " + config.dryRun);
    System.out.println();

    Map<String, String> hadoopConfig = new HashMap<>();
    Map<String, Object> s3Config = new HashMap<>();
    if (config.s3AccessKey != null) {
      hadoopConfig.put("fs.s3a.access.key", config.s3AccessKey);
      hadoopConfig.put("fs.s3a.secret.key", config.s3SecretKey);
      s3Config.put("accessKeyId", config.s3AccessKey);
      s3Config.put("secretAccessKey", config.s3SecretKey);
      if (config.s3Endpoint != null) {
        hadoopConfig.put("fs.s3a.endpoint", config.s3Endpoint);
        hadoopConfig.put("fs.s3a.path.style.access", "true");
        s3Config.put("endpoint", config.s3Endpoint);
      }
      s3Config.put("region", "us-east-1");
    }

    Map<String, Object> catalogConfig = new HashMap<>();
    catalogConfig.put("catalog", "hadoop");
    catalogConfig.put("warehouse", config.warehouse);
    catalogConfig.put("hadoopConfig", hadoopConfig);

    Table table = IcebergCatalogManager.loadTable(catalogConfig, config.tableName);
    table.refresh();
    System.out.println("  Table loaded: " + table.name());
    System.out.println("  Location: " + table.location());

    Snapshot current = table.currentSnapshot();
    if (current == null) {
      System.out.println("  No current snapshot — nothing to roll back.");
      return 0;
    }
    System.out.println("  Current snapshot: " + current.snapshotId()
        + " @ " + current.timestampMillis());

    StorageProvider storageProvider = config.s3AccessKey != null
        ? StorageProviderFactory.createFromType("s3", s3Config)
        : StorageProviderFactory.createFromType("local", null);

    // Single recursive listing of /data — the authoritative set of physically-present files.
    Set<String> presentBasenames = listPresentBasenames(storageProvider, table.location());
    System.out.println("  Physical parquet files present in /data: " + presentBasenames.size());

    // Walk snapshots newest -> oldest; the first fully-present one is the recovery target.
    List<Snapshot> snapshots = new ArrayList<>();
    for (Snapshot s : table.snapshots()) {
      snapshots.add(s);
    }
    snapshots.sort(Comparator.comparingLong(Snapshot::timestampMillis).reversed());

    System.out.println();
    System.out.println("Scanning " + snapshots.size() + " snapshots (newest first)...");

    Long targetSnapshotId = null;
    for (Snapshot s : snapshots) {
      MissingReport report = missingFilesForSnapshot(table, s, presentBasenames);
      String verdict = report.missing.isEmpty() ? "OK (all present)"
          : (report.missing.size() + " MISSING");
      System.out.printf("  snapshot %-20d @ %d  files=%-6d  %s%n",
          s.snapshotId(), s.timestampMillis(), report.total, verdict);
      if (report.missing.isEmpty()) {
        targetSnapshotId = s.snapshotId();
        break;
      }
    }

    if (targetSnapshotId == null) {
      System.out.println();
      System.out.println(BAR);
      System.out.println("NEEDS_REINGEST: no snapshot in the lineage is fully present.");
      System.out.println("  Rollback cannot recover this table; re-ingest it.");
      System.out.println(BAR);
      return 3;
    }

    if (targetSnapshotId == current.snapshotId()) {
      System.out.println();
      System.out.println("Current snapshot is already fully present — no rollback needed.");
      return 0;
    }

    System.out.println();
    System.out.println("Newest fully-present snapshot: " + targetSnapshotId);
    if (config.dryRun) {
      System.out.println("[DRY RUN] Would roll back current pointer to " + targetSnapshotId);
      return 0;
    }

    System.out.println("Rolling back current pointer to " + targetSnapshotId + "...");
    table.manageSnapshots().rollbackTo(targetSnapshotId).commit();
    table.refresh();

    Snapshot now = table.currentSnapshot();
    System.out.println();
    System.out.println(BAR);
    System.out.println("Rollback complete");
    System.out.println(BAR);
    System.out.println("  Current snapshot now: "
        + (now != null ? now.snapshotId() : "none"));
    if (now == null || now.snapshotId() != targetSnapshotId) {
      System.err.println("  WARNING: current snapshot is not the rollback target after commit.");
      return 2;
    }
    return 0;
  }

  /** Lists every {@code .parquet} basename under {@code <location>/data}. */
  private Set<String> listPresentBasenames(StorageProvider storageProvider, String tableLocation)
      throws Exception {
    Set<String> present = new HashSet<>();
    String dataDir = tableLocation + "/data";
    List<StorageProvider.FileEntry> entries = storageProvider.listFiles(dataDir, true);
    for (StorageProvider.FileEntry entry : entries) {
      String path = entry.getPath();
      if (path.endsWith(".parquet")) {
        present.add(basename(path));
      }
    }
    return present;
  }

  /** Data files referenced by a snapshot that are NOT in the present set. */
  private MissingReport missingFilesForSnapshot(Table table, Snapshot snapshot,
      Set<String> presentBasenames) throws Exception {
    MissingReport report = new MissingReport();
    try (CloseableIterable<FileScanTask> tasks =
        table.newScan().useSnapshot(snapshot.snapshotId()).planFiles()) {
      for (FileScanTask task : tasks) {
        report.total++;
        String name = basename(task.file().path().toString());
        if (!presentBasenames.contains(name)) {
          report.missing.add(name);
        }
      }
    }
    return report;
  }

  private static String basename(String path) {
    int lastSlash = path.lastIndexOf('/');
    return lastSlash >= 0 ? path.substring(lastSlash + 1) : path;
  }

  private static final class MissingReport {
    int total;
    final List<String> missing = new ArrayList<>();
  }

  private static void printUsage() {
    System.err.println("Usage: IcebergRollbackRunner [options]");
    System.err.println();
    System.err.println("Required:");
    System.err.println("  --warehouse PATH     Iceberg warehouse path (e.g., s3a://bucket/schema)");
    System.err.println("  --table NAME         Table name");
    System.err.println();
    System.err.println("S3/R2 options:");
    System.err.println("  --s3-access-key KEY  S3 access key ID");
    System.err.println("  --s3-secret-key KEY  S3 secret access key");
    System.err.println("  --s3-endpoint URL    S3 endpoint (for R2/MinIO)");
    System.err.println();
    System.err.println("  --dry-run            Report the target snapshot, commit nothing");
  }

  static class Config {
    String warehouse;
    String tableName;
    String s3AccessKey;
    String s3SecretKey;
    String s3Endpoint;
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
