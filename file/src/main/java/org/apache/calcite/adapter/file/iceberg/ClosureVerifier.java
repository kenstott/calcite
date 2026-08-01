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
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;

/**
 * Read-only check that a table's CURRENT snapshot is actually complete on the given warehouse:
 * every manifest the manifest-list names, and every data file each manifest names, physically
 * exists. Loads via {@link IcebergDirectLoader}, which is safe to point at R2 (a missing key
 * there returns 403, not 404, which breaks {@code HadoopTableOperations.findVersion()}).
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

    Configuration conf = IcebergDirectLoader.buildHadoopConf();
    String hadoopWarehouse = warehouse.replace("s3://", "s3a://");
    String tablePath = hadoopWarehouse + "/" + tableName;

    Table table = IcebergDirectLoader.loadReadOnly(conf, tablePath, tableName);
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
}
