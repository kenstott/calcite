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

import org.apache.calcite.adapter.file.iceberg.CrossProcessCommitLock;
import org.apache.calcite.adapter.file.iceberg.IcebergCatalogManager;
import org.apache.calcite.adapter.file.storage.StorageProvider;
import org.apache.calcite.adapter.file.storage.StorageProviderFactory;
import org.apache.calcite.adapter.govdata.YamlUtils;

import com.fasterxml.jackson.databind.JsonNode;

import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestReader;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Finds, and optionally deletes, data files that no retained snapshot of an Iceberg table
 * references.
 *
 * <p>Snapshot expiry reclaims only files reachable from an expired snapshot. Files left behind by
 * earlier rewrites whose snapshots were expired without a delete hook, and by writes that never
 * committed, stay on disk. This pass lists the table directory, subtracts every data and delete
 * file named by any retained snapshot's manifests, and treats what remains as orphans.
 *
 * <p>It is a separate step from {@link IcebergIdleMaintenance} and is a dry run unless
 * {@code --delete} is given. A file is deleted only when it is referenced by no retained snapshot
 * <em>and</em> is older than the safety window ({@code --min-age-days}, default 3, never below 1),
 * so a file written by a commit still in flight cannot be removed. A table whose commit lock is
 * held is skipped, and so is every schema with a live ETL writer (the launch script excludes those
 * schemas).
 *
 * <p>Usage: {@code IcebergOrphanCleanup --schema-resource econ/econ-schema.yaml [...]
 * [--s3-access-key K --s3-secret-key S --s3-endpoint URL] [--min-age-days N] [--delete]}. Exit code
 * 0 when every visited table was handled, 1 when any table failed.
 */
public final class IcebergOrphanCleanup {

  private static final Logger LOGGER = LoggerFactory.getLogger(IcebergOrphanCleanup.class);

  /** Default safety window, in days. */
  public static final int DEFAULT_MIN_AGE_DAYS = 3;

  /** Smallest safety window the pass accepts, in days. */
  public static final int MINIMUM_MIN_AGE_DAYS = 1;

  private IcebergOrphanCleanup() {
  }

  /** What one table's pass found and did. */
  public static final class Outcome {
    public final String table;
    public final boolean skippedBusy;
    public final boolean skippedNoSnapshot;
    public final boolean deleted;
    public final int onDisk;
    public final int heldByRetention;
    public final int trueOrphans;
    public final int withinSafetyWindow;
    public final int deletable;
    public final int deletedCount;

    Outcome(String table, boolean skippedBusy, boolean skippedNoSnapshot, boolean deleted,
        int onDisk, int heldByRetention, int trueOrphans, int withinSafetyWindow, int deletable,
        int deletedCount) {
      this.table = table;
      this.skippedBusy = skippedBusy;
      this.skippedNoSnapshot = skippedNoSnapshot;
      this.deleted = deleted;
      this.onDisk = onDisk;
      this.heldByRetention = heldByRetention;
      this.trueOrphans = trueOrphans;
      this.withinSafetyWindow = withinSafetyWindow;
      this.deletable = deletable;
      this.deletedCount = deletedCount;
    }

    @Override public String toString() {
      if (skippedBusy) {
        return table + ": skipped, commit lock held";
      }
      if (skippedNoSnapshot) {
        return table + ": skipped, table has no snapshot to anchor the reference set";
      }
      return table + ": on disk " + onDisk + ", held by retained non-current snapshots "
          + heldByRetention + ", true orphans " + trueOrphans + ", inside safety window "
          + withinSafetyWindow + ", " + (deleted ? "deleted " + deletedCount
          : "would delete " + deletable);
    }
  }

  /**
   * Reports, and when {@code delete} is true removes, the orphan data files of one table.
   *
   * @param minAgeDays safety window; files modified more recently than this are never deleted
   * @param nowMillis the clock the window is measured against
   */
  public static Outcome cleanTable(String name, Table table, StorageProvider storage,
      int minAgeDays, boolean delete, long nowMillis) throws IOException {
    if (minAgeDays < MINIMUM_MIN_AGE_DAYS) {
      throw new IllegalArgumentException("minAgeDays must be at least " + MINIMUM_MIN_AGE_DAYS
          + ", got " + minAgeDays);
    }
    CrossProcessCommitLock.Handle probe = CrossProcessCommitLock.tryAcquire(table.location());
    if (probe == null) {
      return new Outcome(name, true, false, delete, 0, 0, 0, 0, 0, 0);
    }
    probe.close();

    table.refresh();
    Snapshot current = table.currentSnapshot();
    if (current == null) {
      return new Outcome(name, false, true, delete, 0, 0, 0, 0, 0, 0);
    }
    Set<String> currentRefs = referencedBy(table, current);
    Set<String> retainedRefs = new HashSet<>(currentRefs);
    for (Snapshot snapshot : table.snapshots()) {
      if (snapshot.snapshotId() != current.snapshotId()) {
        retainedRefs.addAll(referencedBy(table, snapshot));
      }
    }

    long cutoff = nowMillis - TimeUnit.DAYS.toMillis(minAgeDays);
    String location = table.location();
    String directory = location.endsWith("/") ? location : location + "/";
    int onDisk = 0;
    int held = 0;
    int orphans = 0;
    int young = 0;
    List<String> deletable = new ArrayList<>();
    for (StorageProvider.FileEntry entry : storage.listFiles(directory, true)) {
      if (entry.isDirectory() || entry.getPath().contains("/metadata/")
          || entry.getName().endsWith(".crc")) {
        continue;
      }
      onDisk++;
      String key = normalize(entry.getPath());
      if (currentRefs.contains(key)) {
        continue;
      }
      if (retainedRefs.contains(key)) {
        held++;
        continue;
      }
      orphans++;
      if (entry.getLastModified() > cutoff) {
        young++;
      } else {
        deletable.add(entry.getPath());
      }
    }
    int removed = 0;
    if (delete && !deletable.isEmpty()) {
      removed = storage.deleteBatch(deletable);
    }
    return new Outcome(name, false, false, delete, onDisk, held, orphans, young,
        deletable.size(), removed);
  }

  /** Every data and delete file the snapshot's manifests list as added or existing. */
  private static Set<String> referencedBy(Table table, Snapshot snapshot) throws IOException {
    FileIO io = table.io();
    Set<String> paths = new HashSet<>();
    for (ManifestFile manifest : snapshot.dataManifests(io)) {
      try (ManifestReader<org.apache.iceberg.DataFile> reader = ManifestFiles.read(manifest, io)) {
        collect(reader, paths);
      }
    }
    for (ManifestFile manifest : snapshot.deleteManifests(io)) {
      try (ManifestReader<DeleteFile> reader =
          ManifestFiles.readDeleteManifest(manifest, io, table.specs())) {
        collect(reader, paths);
      }
    }
    return paths;
  }

  private static <F extends ContentFile<F>> void collect(CloseableIterable<F> files,
      Set<String> into) {
    for (F file : files) {
      into.add(normalize(file.path().toString()));
    }
  }

  /** Drops the URI scheme so {@code s3a://b/k} and {@code s3://b/k} compare equal. */
  static String normalize(String path) {
    int scheme = path.indexOf("://");
    return scheme < 0 ? path : path.substring(scheme + 3);
  }

  public static void main(String[] args) throws Exception {
    List<String> resources = new ArrayList<>();
    String accessKey = null;
    String secretKey = null;
    String endpoint = null;
    int minAgeDays = DEFAULT_MIN_AGE_DAYS;
    boolean delete = false;
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
      case "--min-age-days":
        minAgeDays = Integer.parseInt(args[++i]);
        break;
      case "--delete":
        delete = true;
        break;
      default:
        throw new IllegalArgumentException("Unknown argument: " + args[i]);
      }
    }
    if (resources.isEmpty()) {
      throw new IllegalArgumentException("at least one --schema-resource is required");
    }
    if (minAgeDays < MINIMUM_MIN_AGE_DAYS) {
      throw new IllegalArgumentException("--min-age-days must be at least "
          + MINIMUM_MIN_AGE_DAYS);
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

    LOGGER.info("orphan cleanup mode: {}, safety window {} days",
        delete ? "DELETE" : "dry run (no files deleted)", minAgeDays);
    int failed = 0;
    for (String resource : resources) {
      JsonNode root;
      try (InputStream in = IcebergOrphanCleanup.class.getResourceAsStream("/" + resource)) {
        if (in == null) {
          throw new IllegalArgumentException("schema resource not found: " + resource);
        }
        root = YamlUtils.parseYamlOrJson(in, resource);
      }
      for (IcebergIdleMaintenance.Target target : IcebergIdleMaintenance.targetsOf(root, resource)) {
        Map<String, Object> catalogConfig = new HashMap<>();
        catalogConfig.put("catalog", "hadoop");
        catalogConfig.put("warehouse", target.warehouse);
        catalogConfig.put("hadoopConfig", hadoopConfig);
        try {
          if (!IcebergCatalogManager.tableExists(catalogConfig, target.name)) {
            LOGGER.info("{}: not materialized, nothing to clean", target.name);
            continue;
          }
          Table table = IcebergCatalogManager.loadTable(catalogConfig, target.name);
          LOGGER.info("{}", cleanTable(target.name, table, storage, minAgeDays, delete,
              System.currentTimeMillis()));
        } catch (Exception e) {
          failed++;
          LOGGER.error("{}: orphan cleanup failed", target.name, e);
        }
      }
    }
    System.exit(failed == 0 ? 0 : 1);
  }
}
