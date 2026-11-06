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
 * 0 when every table was handled, 1 when any table failed. A final summary names the failed
 * tables and, when the run is stopped early, the tables not reached.
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
      if (!isDataFileEntry(entry)) {
        continue;
      }
      String key = normalize(entry.getPath());
      onDisk++;
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

  /**
   * Refuses a table whose metadata records a location outside the warehouse being cleaned. Iceberg
   * resolves manifests and data files from the absolute paths in its metadata, so a table whose
   * metadata was copied from another bucket would be listed, and deleted from, in that other bucket.
   */
  static void requireUnderWarehouse(String name, String tableLocation, String warehouse) {
    String root = normalize(warehouse);
    if (!root.endsWith("/")) {
      root = root + "/";
    }
    String location = normalize(tableLocation);
    if (!(location + "/").startsWith(root)) {
      throw new IllegalStateException(name + ": table location " + tableLocation
          + " is outside the warehouse " + warehouse + "; its metadata points at another store");
    }
  }

  /**
   * True when a listed entry is a table data file: not a directory (an object store lists a
   * zero-byte {@code data/} placeholder key as a file), not under {@code metadata/}, not a
   * {@code .crc} checksum.
   */
  static boolean isDataFileEntry(StorageProvider.FileEntry entry) {
    String path = normalize(entry.getPath());
    return !entry.isDirectory() && !path.endsWith("/") && !path.contains("/metadata/")
        && !entry.getName().endsWith(".crc");
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

  /**
   * Canonical form for comparing a listed path with a path Iceberg records: forward slashes, no URI
   * scheme ({@code s3a://b/k} equals {@code s3://b/k}), and {@code file:/C:/x} equals
   * {@code C:/x} on Windows.
   */
  static String normalize(String path) {
    String p = path.replace('\\', '/');
    if (p.startsWith("file:")) {
      p = p.substring("file:".length());
      int start = 0;
      while (start < p.length() && p.charAt(start) == '/') {
        start++;
      }
      boolean drive = p.length() >= start + 2 && p.charAt(start + 1) == ':'
          && Character.isLetter(p.charAt(start));
      return drive ? p.substring(start) : "/" + p.substring(start);
    }
    int scheme = p.indexOf("://");
    return scheme < 0 ? p : p.substring(scheme + 3);
  }

  /** Per-table tally of a run: completed, failed with reason, and not yet reached. */
  static final class RunSummary {
    final List<String> pending = new java.util.concurrent.CopyOnWriteArrayList<>();
    private final List<String> done = new java.util.concurrent.CopyOnWriteArrayList<>();
    private final Map<String, String> failures =
        new java.util.concurrent.ConcurrentHashMap<>();

    void completed(String table) {
      pending.remove(table);
      done.add(table);
    }

    void failed(String table, Exception cause) {
      pending.remove(table);
      failures.put(table, cause.getClass().getSimpleName() + ": " + cause.getMessage());
    }

    int failedCount() {
      return failures.size();
    }

    String render() {
      StringBuilder out = new StringBuilder();
      out.append("completed ").append(done.size()).append(", failed ").append(failures.size())
          .append(", not reached ").append(pending.size());
      for (Map.Entry<String, String> failure : failures.entrySet()) {
        out.append("\n  FAILED ").append(failure.getKey()).append(": ").append(failure.getValue());
      }
      for (String table : pending) {
        out.append("\n  NOT REACHED ").append(table);
      }
      return out.toString();
    }
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
    List<IcebergIdleMaintenance.Target> targets = new ArrayList<>();
    for (String resource : resources) {
      JsonNode root;
      try (InputStream in = IcebergOrphanCleanup.class.getResourceAsStream("/" + resource)) {
        if (in == null) {
          throw new IllegalArgumentException("schema resource not found: " + resource);
        }
        root = YamlUtils.parseYamlOrJson(in, resource);
      }
      targets.addAll(IcebergIdleMaintenance.targetsOf(root, resource));
    }
    final RunSummary summary = new RunSummary();
    for (IcebergIdleMaintenance.Target target : targets) {
      summary.pending.add(target.name);
    }
    // A wall-clock cap (timeout, SIGTERM) must not hide the tables it cut off.
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      if (!summary.pending.isEmpty()) {
        System.err.println("orphan cleanup interrupted\n" + summary.render());
      }
    }));
    for (IcebergIdleMaintenance.Target target : targets) {
      Map<String, Object> catalogConfig = new HashMap<>();
      catalogConfig.put("catalog", "hadoop");
      catalogConfig.put("warehouse", target.warehouse);
      catalogConfig.put("hadoopConfig", hadoopConfig);
      try {
        if (!IcebergCatalogManager.tableExists(catalogConfig, target.name)) {
          LOGGER.info("{}: not materialized, nothing to clean", target.name);
          summary.completed(target.name);
          continue;
        }
        Table table = IcebergCatalogManager.loadTable(catalogConfig, target.name);
        requireUnderWarehouse(target.name, table.location(), target.warehouse);
        LOGGER.info("{}", cleanTable(target.name, table, storage, minAgeDays, delete,
            System.currentTimeMillis()));
        summary.completed(target.name);
      } catch (Exception e) {
        summary.failed(target.name, e);
        LOGGER.error("{}: orphan cleanup failed", target.name, e);
      }
    }
    LOGGER.info("orphan cleanup finished\n{}", summary.render());
    int failed = summary.failedCount();
    System.exit(failed == 0 ? 0 : 1);
  }
}
