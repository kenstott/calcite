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

import org.apache.calcite.adapter.file.etl.VariableResolver;
import org.apache.calcite.adapter.file.iceberg.CrossProcessCommitLock;
import org.apache.calcite.adapter.file.iceberg.IcebergCatalogManager;
import org.apache.calcite.adapter.file.iceberg.IcebergTableWriter;
import org.apache.calcite.adapter.file.storage.StorageProvider;
import org.apache.calcite.adapter.file.storage.StorageProviderFactory;
import org.apache.calcite.adapter.govdata.YamlUtils;

import com.fasterxml.jackson.databind.JsonNode;

import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Scheduled snapshot expiry for Iceberg tables that are no longer being written.
 *
 * <p>Materialization expires snapshots only at the end of a write to the same table, and
 * compaction's own expiry sees only snapshots that were seconds old when it ran. A table that
 * compacts on the last write of a cycle and then idles therefore keeps every file the compaction
 * replaced until some later write arrives. This pass visits each table whose schema declares
 * {@code iceberg.runMaintenance: true}, with that table's own {@code snapshotRetentionDays}, and
 * runs the same {@link IcebergTableWriter#runMaintenance} a write would have.
 *
 * <p>Expiry removes only files unreachable from the retained snapshots, so it cannot touch the
 * current snapshot's data. A table whose commit lock is held is mid-commit and is skipped; its own
 * writer expires at the end of its run.
 *
 * <p>Usage: {@code IcebergIdleMaintenance --schema-resource econ/econ-schema.yaml [...]
 * [--s3-access-key K --s3-secret-key S --s3-endpoint URL] [--dry-run]}. Exit code 0 when every
 * visited table was handled, 1 when any table failed.
 */
public final class IcebergIdleMaintenance {

  private static final Logger LOGGER = LoggerFactory.getLogger(IcebergIdleMaintenance.class);

  private IcebergIdleMaintenance() {
  }

  /** What one table's pass did. */
  public static final class Outcome {
    public final String table;
    public final boolean skippedBusy;
    public final int snapshotsBefore;
    public final int snapshotsAfter;
    public final int liveFiles;
    public final int filesOnDiskBefore;
    public final int filesOnDiskAfter;

    Outcome(String table, boolean skippedBusy, int snapshotsBefore, int snapshotsAfter,
        int liveFiles, int filesOnDiskBefore, int filesOnDiskAfter) {
      this.table = table;
      this.skippedBusy = skippedBusy;
      this.snapshotsBefore = snapshotsBefore;
      this.snapshotsAfter = snapshotsAfter;
      this.liveFiles = liveFiles;
      this.filesOnDiskBefore = filesOnDiskBefore;
      this.filesOnDiskAfter = filesOnDiskAfter;
    }

    public int filesDeleted() {
      return filesOnDiskBefore - filesOnDiskAfter;
    }

    @Override public String toString() {
      if (skippedBusy) {
        return table + ": skipped, commit lock held";
      }
      return table + ": snapshots " + snapshotsBefore + " -> " + snapshotsAfter
          + ", live files " + liveFiles + ", files on disk " + filesOnDiskBefore + " -> "
          + filesOnDiskAfter + " (" + filesDeleted() + " deleted)";
    }
  }

  /**
   * Expires snapshots older than {@code retentionDays} on one table, unless it is mid-commit.
   */
  public static Outcome expireIdleTable(String name, Table table, StorageProvider storage,
      int retentionDays) throws IOException {
    CrossProcessCommitLock.Handle probe = CrossProcessCommitLock.tryAcquire(table.location());
    if (probe == null) {
      return new Outcome(name, true, 0, 0, 0, 0, 0);
    }
    probe.close();

    table.refresh();
    int snapshotsBefore = countSnapshots(table);
    int onDiskBefore = countDataFilesOnDisk(table, storage);
    new IcebergTableWriter(table, storage).runMaintenance(retentionDays);
    table.refresh();
    int liveFiles = 0;
    for (FileScanTask task : table.newScan().planFiles()) {
      liveFiles++;
    }
    return new Outcome(name, false, snapshotsBefore, countSnapshots(table), liveFiles,
        onDiskBefore, countDataFilesOnDisk(table, storage));
  }

  private static int countSnapshots(Table table) {
    int count = 0;
    for (org.apache.iceberg.Snapshot ignored : table.snapshots()) {
      count++;
    }
    return count;
  }

  private static int countDataFilesOnDisk(Table table, StorageProvider storage)
      throws IOException {
    int count = 0;
    for (StorageProvider.FileEntry entry : storage.listFiles(table.location(), true)) {
      if (!entry.isDirectory() && !entry.getPath().contains("/metadata/")
          && !entry.getName().endsWith(".crc")) {
        count++;
      }
    }
    return count;
  }

  /** A table the schema marks for maintenance, with where it lives and how long to retain. */
  static final class Target {
    final String name;
    final String warehouse;
    final int retentionDays;

    Target(String name, String warehouse, int retentionDays) {
      this.name = name;
      this.warehouse = warehouse;
      this.retentionDays = retentionDays;
    }
  }

  /**
   * Reads the tables a schema YAML marks {@code iceberg.runMaintenance: true}, each with its own
   * warehouse path and {@code snapshotRetentionDays}.
   */
  static List<Target> targetsOf(JsonNode schemaRoot, String source) {
    List<Target> targets = new ArrayList<>();
    JsonNode tables = schemaRoot.path("partitionedTables");
    if (!tables.isArray()) {
      return targets;
    }
    for (JsonNode tableNode : tables) {
      JsonNode iceberg = tableNode.path("materialize").path("iceberg");
      if (iceberg.isMissingNode()) {
        iceberg = tableNode.path("iceberg");
      }
      if (!iceberg.path("runMaintenance").asBoolean(false)) {
        continue;
      }
      String name = tableNode.path("name").asText();
      String warehouse = VariableResolver.resolveEnvVars(iceberg.path("warehousePath").asText());
      if (name.isEmpty() || warehouse.isEmpty() || warehouse.contains("${")) {
        throw new IllegalStateException("Table '" + name + "' in " + source
            + " has runMaintenance but no resolvable iceberg.warehousePath: '" + warehouse + "'");
      }
      targets.add(new Target(name, warehouse, iceberg.path("snapshotRetentionDays").asInt(7)));
    }
    return targets;
  }

  public static void main(String[] args) throws Exception {
    List<String> resources = new ArrayList<>();
    String accessKey = null;
    String secretKey = null;
    String endpoint = null;
    boolean dryRun = false;
    for (int i = 0; i < args.length; i++) {
      switch (args[i]) {
      case "--schema-resource":
        resources.add(args[++i]);
        break;
      case "--s3-access-key":
        accessKey = args[++i];
        break;
      case "--s3-secret-key":
        secretKey = args[++i];
        break;
      case "--s3-endpoint":
        endpoint = args[++i];
        break;
      case "--dry-run":
        dryRun = true;
        break;
      default:
        throw new IllegalArgumentException("Unknown argument: " + args[i]);
      }
    }
    if (resources.isEmpty()) {
      throw new IllegalArgumentException("at least one --schema-resource is required");
    }

    Map<String, String> hadoopConfig = new HashMap<>();
    Map<String, Object> s3Config = new HashMap<>();
    if (accessKey != null) {
      hadoopConfig.put("fs.s3a.access.key", accessKey);
      hadoopConfig.put("fs.s3a.secret.key", secretKey);
      s3Config.put("accessKeyId", accessKey);
      s3Config.put("secretAccessKey", secretKey);
      s3Config.put("region", "us-east-1");
      if (endpoint != null) {
        hadoopConfig.put("fs.s3a.endpoint", endpoint);
        hadoopConfig.put("fs.s3a.path.style.access", "true");
        hadoopConfig.put("fs.s3a.endpoint.region", "auto");
        hadoopConfig.put("fs.s3a.change.detection.mode", "none");
        hadoopConfig.put("fs.s3a.change.detection.version.required", "false");
        s3Config.put("endpoint", endpoint);
      }
    }
    StorageProvider storage = accessKey != null
        ? StorageProviderFactory.createFromType("s3", s3Config)
        : StorageProviderFactory.createFromType("local", null);

    int failed = 0;
    for (String resource : resources) {
      JsonNode root;
      try (InputStream in = IcebergIdleMaintenance.class.getResourceAsStream("/" + resource)) {
        if (in == null) {
          throw new IllegalArgumentException("schema resource not found: " + resource);
        }
        root = YamlUtils.parseYamlOrJson(in, resource);
      }
      for (Target target : targetsOf(root, resource)) {
        Map<String, Object> catalogConfig = new HashMap<>();
        catalogConfig.put("catalog", "hadoop");
        catalogConfig.put("warehouse", target.warehouse);
        catalogConfig.put("hadoopConfig", hadoopConfig);
        try {
          if (!IcebergCatalogManager.tableExists(catalogConfig, target.name)) {
            LOGGER.info("{}: not materialized, nothing to expire", target.name);
            continue;
          }
          Table table = IcebergCatalogManager.loadTable(catalogConfig, target.name);
          if (dryRun) {
            LOGGER.info("{}: [dry-run] would expire snapshots older than {} days "
                + "(snapshots now: {})", target.name, target.retentionDays, countSnapshots(table));
            continue;
          }
          LOGGER.info("{}", expireIdleTable(target.name, table, storage, target.retentionDays));
        } catch (Exception e) {
          failed++;
          LOGGER.error("{}: idle maintenance failed", target.name, e);
        }
      }
    }
    System.exit(failed == 0 ? 0 : 1);
  }
}
