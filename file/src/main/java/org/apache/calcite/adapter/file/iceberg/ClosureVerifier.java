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
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StaticTableOperations;
import org.apache.iceberg.Table;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;

/**
 * Read-only check that a table's CURRENT snapshot is actually complete on the given warehouse:
 * every manifest the manifest-list names, and every data file each manifest names, physically
 * exists. Loads the table the same way {@link CompactionRunner} does (list the metadata
 * directory directly rather than relying on {@code HadoopTableOperations.findVersion()}, which
 * fails on R2/MinIO because a missing key returns 403, not 404) so this is safe to point at R2.
 *
 * <p>Never writes anything. Existence-only — this does not read data file contents or verify
 * row counts, only that every file the committed snapshot depends on is present.
 *
 * <p>Usage:
 * <pre>{@code
 * java -cp sih-govdata.jar org.apache.calcite.adapter.file.iceberg.ClosureVerifier \
 *   --warehouse s3://bucket/sec --table filing_metadata
 * }</pre>
 */
public final class ClosureVerifier {

  private ClosureVerifier() {}

  public static void main(String[] args) throws Exception {
    String warehouse = null;
    String tableName = null;
    for (int i = 0; i < args.length; i++) {
      switch (args[i]) {
      case "--warehouse":
        warehouse = args[++i];
        break;
      case "--table":
        tableName = args[++i];
        break;
      default:
        System.err.println("Unknown argument: " + args[i]);
        System.exit(1);
      }
    }
    if (warehouse == null || tableName == null) {
      System.err.println("Usage: ClosureVerifier --warehouse <path> --table <name>");
      System.exit(1);
    }

    Configuration conf = buildHadoopConf();
    String hadoopWarehouse = warehouse.replace("s3://", "s3a://");
    String tablePath = hadoopWarehouse + "/" + tableName;

    Table table = loadTableDirect(conf, tablePath, tableName);
    FileIO io = table.io();
    Snapshot snapshot = table.currentSnapshot();
    if (snapshot == null) {
      System.out.println("OK " + tableName + " — no current snapshot (empty table)");
      return;
    }

    int missingManifests = 0;
    int checkedManifests = 0;
    for (ManifestFile manifestFile : snapshot.allManifests(io)) {
      checkedManifests++;
      if (!io.newInputFile(manifestFile.path()).exists()) {
        missingManifests++;
        System.out.println("MISSING MANIFEST " + tableName + " -> " + manifestFile.path());
      }
    }

    int missingDataFiles = 0;
    int checkedDataFiles = 0;
    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      for (FileScanTask task : tasks) {
        DataFile dataFile = task.file();
        checkedDataFiles++;
        String path = dataFile.path().toString();
        if (!io.newInputFile(path).exists()) {
          missingDataFiles++;
          System.out.println("MISSING DATA FILE " + tableName + " -> " + path);
        }
      }
    } catch (Exception e) {
      // planFiles() itself reads the manifest-list and every manifest — a missing/corrupt
      // manifest surfaces here as an exception, not silently as zero rows. Report it as a
      // closure failure rather than letting the caller mistake "threw" for "found no files".
      System.out.println("SCAN FAILED " + tableName + " (snapshot " + snapshot.snapshotId()
          + "): " + e.getClass().getSimpleName() + ": " + e.getMessage());
      System.exit(1);
    }

    System.out.println((missingManifests == 0 && missingDataFiles == 0 ? "OK " : "BROKEN ")
        + tableName + " — snapshot " + snapshot.snapshotId()
        + ": " + checkedManifests + " manifests checked (" + missingManifests + " missing), "
        + checkedDataFiles + " data files checked (" + missingDataFiles + " missing)");

    if (missingManifests > 0 || missingDataFiles > 0) {
      System.exit(1);
    }
  }

  /**
   * Loads an Iceberg table by scanning the metadata directory for the latest version file
   * directly, same as {@link CompactionRunner#loadTableDirect}. Duplicated rather than shared
   * because that method is private and this tool must stay strictly read-only, independent of
   * CompactionRunner's read-write table construction path.
   */
  private static Table loadTableDirect(Configuration conf, String tablePath, String tableName)
      throws Exception {
    FileSystem fs = FileSystem.get(new java.net.URI(tablePath), conf);
    Path metadataDir = new Path(tablePath + "/metadata");

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
          // skip non-numeric version file names
        }
      }
    }
    if (latestMetadataPath == null) {
      throw new IllegalStateException("No metadata files found in " + metadataDir);
    }

    HadoopFileIO fileIO = new HadoopFileIO(conf);
    StaticTableOperations ops = new StaticTableOperations(latestMetadataPath, fileIO);
    return new BaseTable(ops, tableName);
  }

  private static Configuration buildHadoopConf() {
    Configuration conf = new Configuration();
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
      conf.set("fs.s3a.change.detection.mode", "none");
      conf.set("fs.s3a.change.detection.version.required", "false");
      // R2 rejects AWS region names outright ("Must be one of: wnam, enam, ... auto") — it
      // ignores region for routing but the S3A/SDK client still requires a value it accepts.
      conf.set("fs.s3a.endpoint.region", "auto");
    }
    conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
    conf.set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
    return conf;
  }
}
