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
import org.apache.iceberg.ReachableFileUtil;
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
 * [--s3-credentials-from-env --s3-endpoint URL] [--min-age-days N] [--metadata
 * [--metadata-min-age-days N]] [--metadata-only] [--tables schema/table[,schema/table]]
 * [--delete]}. {@code --metadata} adds the {@code metadata/} pass (see {@link #cleanMetadata});
 * its window defaults to 14 days. {@code --metadata-only} runs that pass alone, without listing
 * data files. {@code --tables} restricts the run to the named tables, each given as the schema
 * directory of its {@code --schema-resource} and the table name; a name that matches no table is an
 * error. {@code --list-orphans FILE} writes every metadata/ candidate with its class, size and
 * modification time (see {@link #cleanMetadata}) and requires {@code --metadata}; it is refused
 * together with {@code --delete}, so a listing is always read-only. Exit code
 * 0 when every table was handled, 1 when any table failed. A final summary names the failed
 * tables and, when the run is stopped early, the tables not reached.
 */
public final class IcebergOrphanCleanup {

  private static final Logger LOGGER = LoggerFactory.getLogger(IcebergOrphanCleanup.class);

  /** Default safety window, in days. */
  public static final int DEFAULT_MIN_AGE_DAYS = 3;

  /** Smallest safety window the pass accepts, in days. */
  public static final int MINIMUM_MIN_AGE_DAYS = 1;

  /** Default safety window for files under {@code metadata/}, in days. */
  public static final int DEFAULT_METADATA_MIN_AGE_DAYS = 14;

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

  /** What one table's metadata/ pass found and did. */
  public static final class MetadataOutcome {
    public final String table;
    public final boolean skippedBusy;
    public final boolean deleted;
    public final int onDisk;
    public final int reachable;
    public final int orphans;
    public final int withinSafetyWindow;
    public final int deletable;
    public final long deletableBytes;
    public final int deletedCount;

    MetadataOutcome(String table, boolean skippedBusy, boolean deleted, int onDisk, int reachable,
        int orphans, int withinSafetyWindow, int deletable, long deletableBytes,
        int deletedCount) {
      this.table = table;
      this.skippedBusy = skippedBusy;
      this.deleted = deleted;
      this.onDisk = onDisk;
      this.reachable = reachable;
      this.orphans = orphans;
      this.withinSafetyWindow = withinSafetyWindow;
      this.deletable = deletable;
      this.deletableBytes = deletableBytes;
      this.deletedCount = deletedCount;
    }

    @Override public String toString() {
      if (skippedBusy) {
        return table + " metadata: skipped, commit lock held";
      }
      return table + " metadata: on disk " + onDisk + ", reachable " + reachable + ", orphans "
          + orphans + ", inside safety window " + withinSafetyWindow + ", "
          + (deleted ? "deleted " + deletedCount : "would delete " + deletable) + " ("
          + deletableBytes + " bytes)";
    }
  }

  /**
   * Reports, and when {@code delete} is true removes, the orphan files directly under the table's
   * {@code metadata/} directory: manifests, manifest lists, table-metadata versions and statistics
   * files that the current table state does not reach.
   *
   * <p>Reachability is Iceberg's own: {@link ReachableFileUtil} supplies the table-metadata chain,
   * the version hint, every snapshot's manifest list and the statistics files, and each retained
   * snapshot supplies its manifests. Only names Iceberg writes ({@code .avro},
   * {@code .metadata.json}, {@code .stats}) are candidates; nothing else under {@code metadata/} is
   * touched. Expiry cannot reclaim manifests that a failed or retried commit wrote without
   * reaching any snapshot, so they stay until this pass removes them once past the safety window.
   *
   * @param minAgeDays safety window; files modified more recently than this are never deleted
   */
  public static MetadataOutcome cleanMetadata(String name, Table table, StorageProvider storage,
      int minAgeDays, boolean delete, long nowMillis) throws IOException {
    return cleanMetadata(name, table, storage, minAgeDays, delete, nowMillis, null);
  }

  /**
   * As {@link #cleanMetadata(String, Table, StorageProvider, int, boolean, long)}, and when
   * {@code listing} is not null writes one tab-separated line per candidate file before anything is
   * deleted: {@code table, class, size, modified (ISO-8601 UTC), path}. The class is
   * {@code ORPHAN_DELETABLE}, {@code ORPHAN_YOUNG} (inside the safety window, never deleted) or
   * {@code REACHABLE:<role>}, so every file the pass kept can be accounted for as well as every file
   * it would remove.
   *
   * <p>A commit still in flight is protected by the safety window, not by the lock probe: the probe
   * only skips a table whose commit lock is held at that instant, whereas a manifest a commit has
   * written but not yet published is unreachable and young, and the window (at least one day) keeps
   * it out of the deletable set.
   */
  public static MetadataOutcome cleanMetadata(String name, Table table, StorageProvider storage,
      int minAgeDays, boolean delete, long nowMillis, Appendable listing) throws IOException {
    if (minAgeDays < MINIMUM_MIN_AGE_DAYS) {
      throw new IllegalArgumentException("minAgeDays must be at least " + MINIMUM_MIN_AGE_DAYS
          + ", got " + minAgeDays);
    }
    CrossProcessCommitLock.Handle probe = CrossProcessCommitLock.tryAcquire(table.location());
    if (probe == null) {
      return new MetadataOutcome(name, true, delete, 0, 0, 0, 0, 0, 0L, 0);
    }
    probe.close();

    table.refresh();
    Map<String, String> reachable = reachableMetadataFiles(table);

    long cutoff = nowMillis - TimeUnit.DAYS.toMillis(minAgeDays);
    String location = table.location();
    String directory = (location.endsWith("/") ? location : location + "/") + "metadata/";
    int onDisk = 0;
    int held = 0;
    int orphans = 0;
    int young = 0;
    long bytes = 0L;
    List<String> deletable = new ArrayList<>();
    for (StorageProvider.FileEntry entry : storage.listFiles(directory, false)) {
      if (!isIcebergMetadataFile(entry)) {
        continue;
      }
      onDisk++;
      String role = reachable.get(normalize(entry.getPath()));
      if (role != null) {
        held++;
        listEntry(listing, name, "REACHABLE:" + role, entry);
        continue;
      }
      orphans++;
      if (entry.getLastModified() > cutoff) {
        young++;
        listEntry(listing, name, "ORPHAN_YOUNG", entry);
      } else {
        deletable.add(entry.getPath());
        bytes += entry.getSize();
        listEntry(listing, name, "ORPHAN_DELETABLE", entry);
      }
    }
    int removed = 0;
    if (delete && !deletable.isEmpty()) {
      removed = storage.deleteBatch(deletable);
    }
    return new MetadataOutcome(name, false, delete, onDisk, held, orphans, young,
        deletable.size(), bytes, removed);
  }

  /**
   * Every file under {@code metadata/} that the table's current state reaches. Fails closed: when
   * the current metadata file or the current snapshot's manifest list is not in the set, the set is
   * not trusted and nothing may be deleted against it.
   */
  static Map<String, String> reachableMetadataFiles(Table table) throws IOException {
    Map<String, String> paths = new HashMap<>();
    for (String path : ReachableFileUtil.metadataFileLocations(table, true)) {
      paths.putIfAbsent(normalize(path), "METADATA_JSON");
    }
    String hint = ReachableFileUtil.versionHintLocation(table);
    if (hint != null) {
      paths.putIfAbsent(normalize(hint), "VERSION_HINT");
    }
    for (String path : ReachableFileUtil.manifestListLocations(table)) {
      paths.putIfAbsent(normalize(path), "MANIFEST_LIST");
    }
    for (String path : ReachableFileUtil.statisticsFilesLocations(table)) {
      paths.putIfAbsent(normalize(path), "STATISTICS");
    }
    FileIO io = table.io();
    for (Snapshot snapshot : table.snapshots()) {
      for (ManifestFile manifest : snapshot.allManifests(io)) {
        paths.putIfAbsent(normalize(manifest.path()), "MANIFEST");
      }
    }
    Snapshot current = table.currentSnapshot();
    if (current != null && !paths.containsKey(normalize(current.manifestListLocation()))) {
      throw new IllegalStateException("current snapshot manifest list "
          + current.manifestListLocation() + " is not in the reachable set of " + table.name());
    }
    return paths;
  }

  private static void listEntry(Appendable listing, String table, String kind,
      StorageProvider.FileEntry entry) throws IOException {
    if (listing == null) {
      return;
    }
    listing.append(table).append('\t').append(kind).append('\t')
        .append(Long.toString(entry.getSize())).append('\t')
        .append(java.time.Instant.ofEpochMilli(entry.getLastModified()).toString()).append('\t')
        .append(entry.getPath()).append('\n');
  }

  /**
   * True when a listed entry is a file Iceberg writes under {@code metadata/}: a manifest or
   * manifest list ({@code .avro}), a table-metadata version ({@code .metadata.json}, optionally
   * gzipped) or a statistics file ({@code .stats}). The version hint and anything else are not
   * candidates.
   */
  static boolean isIcebergMetadataFile(StorageProvider.FileEntry entry) {
    String path = normalize(entry.getPath());
    if (entry.isDirectory() || path.endsWith("/") || !path.contains("/metadata/")) {
      return false;
    }
    String file = entry.getName();
    return file.endsWith(".avro") || file.endsWith(".metadata.json")
        || file.endsWith(".metadata.json.gz") || file.endsWith(".stats");
  }

  /** The {@code --tables} key of a table: its schema resource's directory and its name. */
  static String scopeKey(String resource, String table) {
    int slash = resource.indexOf('/');
    if (slash <= 0) {
      throw new IllegalArgumentException("schema resource has no schema directory: " + resource);
    }
    return resource.substring(0, slash) + "/" + table;
  }

  /**
   * Keeps the targets named in {@code scope}, recording each match in {@code matched}. An empty
   * scope keeps every target.
   */
  static List<IcebergIdleMaintenance.Target> scopeTargets(String resource,
      List<IcebergIdleMaintenance.Target> targets, Set<String> scope, Set<String> matched) {
    if (scope.isEmpty()) {
      return targets;
    }
    List<IcebergIdleMaintenance.Target> kept = new ArrayList<>();
    for (IcebergIdleMaintenance.Target target : targets) {
      String key = scopeKey(resource, target.name);
      if (scope.contains(key)) {
        kept.add(target);
        matched.add(key);
      }
    }
    return kept;
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
    boolean metadata = false;
    boolean metadataOnly = false;
    String listOrphans = null;
    Set<String> scope = new java.util.LinkedHashSet<>();
    int metadataMinAgeDays = DEFAULT_METADATA_MIN_AGE_DAYS;
    for (int i = 0; i < args.length; i++) {
      switch (args[i]) {
      case "--schema-resource":
        resources.add(args[++i]);
        break;
      case S3EnvCredentials.FLAG:
        S3EnvCredentials creds = S3EnvCredentials.read();
        accessKey = creds.accessKey;
        secretKey = creds.secretKey;
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
      case "--metadata":
        metadata = true;
        break;
      case "--metadata-only":
        metadata = true;
        metadataOnly = true;
        break;
      case "--tables":
        for (String key : args[++i].split(",")) {
          if (!key.matches("[^/,\\s]+/[^/,\\s]+")) {
            throw new IllegalArgumentException("--tables entries must be schema/table, got: " + key);
          }
          scope.add(key);
        }
        break;
      case "--list-orphans":
        listOrphans = args[++i];
        break;
      case "--metadata-min-age-days":
        metadataMinAgeDays = Integer.parseInt(args[++i]);
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
    if (metadataMinAgeDays < MINIMUM_MIN_AGE_DAYS) {
      throw new IllegalArgumentException("--metadata-min-age-days must be at least "
          + MINIMUM_MIN_AGE_DAYS);
    }
    if (listOrphans != null && !metadata) {
      throw new IllegalArgumentException("--list-orphans requires --metadata or --metadata-only");
    }
    if (listOrphans != null && delete) {
      throw new IllegalArgumentException("--list-orphans is read-only and cannot be combined with "
          + "--delete");
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
    Set<String> matched = new java.util.HashSet<>();
    for (String resource : resources) {
      JsonNode root;
      try (InputStream in = IcebergOrphanCleanup.class.getResourceAsStream("/" + resource)) {
        if (in == null) {
          throw new IllegalArgumentException("schema resource not found: " + resource);
        }
        root = YamlUtils.parseYamlOrJson(in, resource);
      }
      targets.addAll(scopeTargets(resource, IcebergIdleMaintenance.targetsOf(root, resource),
          scope, matched));
    }
    if (!matched.containsAll(scope)) {
      Set<String> missing = new java.util.LinkedHashSet<>(scope);
      missing.removeAll(matched);
      throw new IllegalArgumentException("--tables names no maintained table in the given "
          + "--schema-resource files: " + missing);
    }
    java.io.Writer listing = listOrphans == null ? null
        : java.nio.file.Files.newBufferedWriter(java.nio.file.Paths.get(listOrphans),
            java.nio.charset.StandardCharsets.UTF_8);
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
        if (!metadataOnly) {
          LOGGER.info("{}", cleanTable(target.name, table, storage, minAgeDays, delete,
              System.currentTimeMillis()));
        }
        if (metadata) {
          LOGGER.info("{}", cleanMetadata(target.name, table, storage, metadataMinAgeDays, delete,
              System.currentTimeMillis(), listing));
          if (listing != null) {
            listing.flush();
          }
        }
        summary.completed(target.name);
      } catch (Exception e) {
        summary.failed(target.name, e);
        LOGGER.error("{}: orphan cleanup failed", target.name, e);
      }
    }
    if (listing != null) {
      listing.close();
    }
    LOGGER.info("orphan cleanup finished\n{}", summary.render());
    int failed = summary.failedCount();
    System.exit(failed == 0 ? 0 : 1);
  }
}
