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

import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotParser;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;

import java.io.InputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Re-attaches a committed-but-unreachable snapshot chain to an Iceberg table whose
 * {@code vN.metadata.json} pointers were lost while the manifest lists, manifests and data files
 * survived.
 *
 * <p>Each snapshot's manifest list is a cumulative view of the table at that snapshot, so the
 * manifest list with the highest {@code sequence-number} (read from the Avro header Iceberg writes
 * into every {@code snap-*.avro}) describes the full surviving table. The runner verifies that
 * every manifest and data file that list references is physically present, then commits a new
 * metadata version whose current snapshot is that list. Nothing is deleted or rewritten.
 *
 * <p>The re-attached snapshot has no parent (the intermediate snapshots cannot be reconstructed)
 * and its summary carries record and file totals recomputed from the manifests. Its timestamp is
 * the repair time unless {@code --timestamp-ms} is given.
 *
 * <pre>
 * java -cp build/libs/sih-govdata.jar \
 *   org.apache.calcite.adapter.govdata.etl.IcebergSnapshotReattachRunner \
 *   --warehouse s3a://govdata-parquet-v1/sec --table insider_transactions \
 *   --s3-access-key $AWS_ACCESS_KEY_ID --s3-secret-key $AWS_SECRET_ACCESS_KEY \
 *   --s3-endpoint $AWS_ENDPOINT_OVERRIDE [--dry-run]
 * </pre>
 *
 * <p>Exit codes: 0 OK (re-attached, dry run, or nothing newer to attach), 2 FAILED,
 * 3 NEEDS_REINGEST (the newest manifest list references files that are not present).
 */
public class IcebergSnapshotReattachRunner {

  private static final String BAR =
      "============================================================";

  public static void main(String[] args) {
    int exitCode;
    try {
      exitCode = new IcebergSnapshotReattachRunner().run(Config.fromArgs(args));
    } catch (IllegalArgumentException e) {
      System.err.println("Error: " + e.getMessage());
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
    System.out.println("Iceberg Snapshot Re-attach: " + config.tableName);
    System.out.println(BAR);
    System.out.println("  Warehouse: " + config.warehouse);
    System.out.println("  Dry run: " + config.dryRun);

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
        hadoopConfig.put("fs.s3a.endpoint.region", "auto");
        hadoopConfig.put("fs.s3a.change.detection.mode", "none");
        hadoopConfig.put("fs.s3a.change.detection.version.required", "false");
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
    System.out.println("  Location: " + table.location());
    Snapshot current = table.currentSnapshot();
    long currentSeq = current == null ? 0 : current.sequenceNumber();
    System.out.println("  Current snapshot: "
        + (current == null ? "none" : current.snapshotId() + " seq=" + currentSeq));

    StorageProvider storage = config.s3AccessKey != null
        ? StorageProviderFactory.createFromType("s3", s3Config)
        : StorageProviderFactory.createFromType("local", null);
    FileIO io = table.io();
    // Anchor every path on the warehouse the operator named, not the location recorded inside the
    // metadata, so a copied table is repaired in place and never reaches back to its source.
    String root = config.warehouse.replaceFirst("^s3://", "s3a://") + "/" + config.tableName;
    System.out.println("  Table root: " + root);

    // Newest manifest list = highest sequence-number among snap-*.avro headers.
    String headList = null;
    long headSeq = -1;
    long headSnapshotId = 0;
    int scanned = 0;
    for (StorageProvider.FileEntry entry : storage.listFiles(root + "/metadata/", false)) {
      String name = basename(entry.getPath());
      if (!name.startsWith("snap-") || !name.endsWith(".avro")) {
        continue;
      }
      scanned++;
      String location = root + "/metadata/" + name;
      try (InputStream in = io.newInputFile(location).newStream();
          DataFileStream<GenericRecord> stream =
              new DataFileStream<>(in, new GenericDatumReader<GenericRecord>())) {
        long seq = Long.parseLong(stream.getMetaString("sequence-number"));
        if (seq > headSeq) {
          headSeq = seq;
          headList = location;
          headSnapshotId = Long.parseLong(stream.getMetaString("snapshot-id"));
        }
      }
    }
    System.out.println("  Manifest lists scanned: " + scanned);
    if (headList == null) {
      throw new IllegalStateException("no snap-*.avro manifest list under " + root);
    }
    System.out.println("  Newest manifest list: " + headList + " seq=" + headSeq
        + " snapshot=" + headSnapshotId);
    if (headSeq <= currentSeq) {
      System.out.println("Current snapshot is already the newest — nothing to attach.");
      return 0;
    }

    // Closure check: every manifest and data file of the newest list must exist.
    Set<String> present = new HashSet<>();
    for (StorageProvider.FileEntry entry : storage.listFiles(root + "/data/", true)) {
      if (entry.getPath().endsWith(".parquet")) {
        present.add(basename(entry.getPath()));
      }
    }
    TableMetadata base = ((HasTableOperations) table).operations().current();
    List<ManifestFile> manifests = SnapshotParser.fromJson(
        snapshotJson(headSnapshotId, headSeq, System.currentTimeMillis(), headList,
            base.currentSchemaId(), 0, 0)).allManifests(io);
    long records = 0;
    long files = 0;
    int missing = 0;
    for (ManifestFile manifest : manifests) {
      try (CloseableIterable<DataFile> dataFiles = ManifestFiles.read(manifest, io)) {
        for (DataFile file : dataFiles) {
          files++;
          records += file.recordCount();
          if (!present.contains(basename(file.path().toString()))) {
            missing++;
            System.out.println("  MISSING data file: " + file.path());
          }
        }
      }
    }
    System.out.println("  Manifests: " + manifests.size() + "  data files: " + files
        + "  records: " + records + "  missing: " + missing);
    if (missing > 0) {
      System.out.println("NEEDS_REINGEST: newest manifest list references absent data files.");
      return 3;
    }
    if (config.dryRun) {
      System.out.println("[DRY RUN] Would attach snapshot " + headSnapshotId);
      return 0;
    }

    long timestamp = config.timestampMs != null ? config.timestampMs : System.currentTimeMillis();
    Snapshot snapshot = SnapshotParser.fromJson(
        snapshotJson(headSnapshotId, headSeq, timestamp, headList, base.currentSchemaId(),
            records, files));
    TableOperations ops = ((HasTableOperations) table).operations();
    ops.commit(base, TableMetadata.buildFrom(base).setBranchSnapshot(snapshot, "main").build());
    table.refresh();
    Snapshot now = table.currentSnapshot();
    if (now == null || now.snapshotId() != headSnapshotId) {
      System.err.println("  WARNING: current snapshot is not the attached snapshot after commit.");
      return 2;
    }
    System.out.println(BAR);
    System.out.println("Re-attached snapshot " + headSnapshotId + " (seq " + headSeq + ")");
    System.out.println(BAR);
    return 0;
  }

  private static String snapshotJson(long id, long seq, long timestampMs, String manifestList,
      int schemaId, long records, long files) {
    return "{\"snapshot-id\":" + id + ",\"sequence-number\":" + seq
        + ",\"timestamp-ms\":" + timestampMs
        + ",\"summary\":{\"operation\":\"append\",\"total-records\":\"" + records
        + "\",\"total-data-files\":\"" + files + "\"}"
        + ",\"manifest-list\":\"" + manifestList + "\",\"schema-id\":" + schemaId + "}";
  }

  private static String basename(String path) {
    int lastSlash = path.lastIndexOf('/');
    return lastSlash >= 0 ? path.substring(lastSlash + 1) : path;
  }

  private static void printUsage() {
    System.err.println("Usage: IcebergSnapshotReattachRunner --warehouse PATH --table NAME");
    System.err.println("  [--s3-access-key KEY --s3-secret-key KEY --s3-endpoint URL]");
    System.err.println("  [--timestamp-ms MS] [--dry-run]");
  }

  static class Config {
    String warehouse;
    String tableName;
    String s3AccessKey;
    String s3SecretKey;
    String s3Endpoint;
    Long timestampMs;
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
        case "--timestamp-ms":
          config.timestampMs = Long.valueOf(args[++i]);
          break;
        case "--dry-run":
          config.dryRun = true;
          break;
        default:
          throw new IllegalArgumentException("Unknown argument: " + args[i]);
        }
      }
      if (config.warehouse == null || config.tableName == null) {
        throw new IllegalArgumentException("--warehouse and --table are required");
      }
      return config;
    }
  }
}
