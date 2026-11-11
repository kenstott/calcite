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

import org.apache.calcite.adapter.file.util.LocalPaths;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadata.MetadataLogEntry;
import org.apache.iceberg.io.CloseableIterable;

import java.util.LinkedHashSet;
import java.util.Set;

/**
 * Resolves the union of files needed by EVERY snapshot a reader could still use, for the R2
 * cleanup dry run ({@code r2-closure-report.sh}). Unlike {@link ClosureResolver}, which pins the
 * current snapshot only, this keeps every retained snapshot's manifest list, manifests, data and
 * delete files, plus every metadata json named in the metadata log and the current one.
 *
 * <p>Output matches {@link ClosureResolver}: {@code VERSION <N>} then table-relative paths, one
 * per line. Read-only; it never writes or deletes anything.
 */
public final class RetainedClosureResolver {

  private RetainedClosureResolver() {}

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
      System.err.println("Usage: RetainedClosureResolver --warehouse <path> --table <name>");
      System.exit(1);
    }

    Configuration conf = IcebergDirectLoader.buildHadoopConf();
    String tablePath = warehouse.replace("s3://", "s3a://") + "/" + tableName;
    Table table = IcebergDirectLoader.loadReadOnly(conf, tablePath, tableName);
    int version = IcebergDirectLoader.currentVersion(conf, tablePath);

    Set<String> paths = new LinkedHashSet<>();
    paths.add("metadata/v" + version + ".metadata.json");

    TableMetadata metadata = ((HasTableOperations) table).operations().current();
    for (MetadataLogEntry entry : metadata.previousFiles()) {
      paths.add(relativize(tableName, entry.file()));
    }
    for (Snapshot snapshot : metadata.snapshots()) {
      String manifestList = snapshot.manifestListLocation();
      if (manifestList != null) {
        paths.add(relativize(tableName, manifestList));
      }
      for (ManifestFile manifest : snapshot.allManifests(table.io())) {
        paths.add(relativize(tableName, manifest.path()));
      }
      try (CloseableIterable<FileScanTask> tasks =
               table.newScan().useSnapshot(snapshot.snapshotId()).planFiles()) {
        for (FileScanTask task : tasks) {
          DataFile dataFile = task.file();
          paths.add(relativize(tableName, dataFile.path().toString()));
          for (DeleteFile deleteFile : task.deletes()) {
            paths.add(relativize(tableName, deleteFile.path().toString()));
          }
        }
      }
    }

    System.out.println("VERSION " + version);
    for (String path : paths) {
      System.out.println(path);
    }
  }

  private static String relativize(String tableName, CharSequence absolutePath) {
    String abs = absolutePath.toString();
    String marker = "/" + tableName + "/";
    int idx = LocalPaths.toSlashes(abs).lastIndexOf(marker);
    if (idx < 0) {
      throw new IllegalStateException(
          "File path '" + abs + "' does not contain table segment '" + marker + "'");
    }
    return abs.substring(idx + marker.length());
  }
}
