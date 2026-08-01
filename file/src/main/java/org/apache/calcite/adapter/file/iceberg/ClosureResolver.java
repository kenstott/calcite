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
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.CloseableIterable;

import java.util.LinkedHashSet;
import java.util.Set;

/**
 * Resolves exactly the files a table's CURRENT snapshot depends on, pinned to whichever version
 * is current at the single moment this reads {@code version-hint.text} — never re-reads it.
 *
 * <p>This is the read side of a version-pinned sync: a copy tool that (1) resolves this once,
 * (2) copy-only transfers exactly the emitted paths, then (3) writes the emitted VERSION as the
 * destination's {@code version-hint.text} is safe against concurrent ingestion with no liveness
 * check at all. Iceberg's commit model guarantees that once {@code version-hint.text} names
 * version N, every file N's manifest chain depends on was already fully and immutably written —
 * nothing a concurrent writer does afterward (new snapshots, compaction, more commits) can alter
 * what N depends on. The old approach (a point-in-time "is anything writing right now" check,
 * then a long non-atomic multi-pass copy) is what produced a torn {@code version-hint.text} on
 * R2 in the first place: two separate passes re-listed the source independently, so a snapshot
 * committed between passes 1 and 2 left the pointer referencing files the first pass never saw.
 * Pinning removes the race by construction instead of trying to detect and wait it out.
 *
 * <p>Emits, one per line, relative to the table root (i.e. with the {@code --warehouse}/
 * {@code --table} prefix stripped, so the output is directly usable as an rclone
 * {@code --files-from} list against {@code remote:bucket/schema/table}):
 * <pre>
 * VERSION &lt;N&gt;
 * metadata/vN.metadata.json
 * metadata/&lt;manifest-list&gt;.avro
 * metadata/&lt;manifest&gt;.avro
 * data/year=.../&lt;data file&gt;.parquet
 * ...
 * </pre>
 *
 * <p>Usage:
 * <pre>{@code
 * java -cp sih-govdata.jar org.apache.calcite.adapter.file.iceberg.ClosureResolver \
 *   --warehouse s3://bucket/sec --table filing_metadata
 * }</pre>
 */
public final class ClosureResolver {

  private ClosureResolver() {}

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
      System.err.println("Usage: ClosureResolver --warehouse <path> --table <name>");
      System.exit(1);
    }

    Configuration conf = IcebergDirectLoader.buildHadoopConf();
    String hadoopWarehouse = warehouse.replace("s3://", "s3a://");
    String tablePath = hadoopWarehouse + "/" + tableName;

    // One read of version-hint.text for the whole resolution — every path below comes from
    // the Table object this produces, so nothing here observes a later version.
    Table table = IcebergDirectLoader.loadReadOnly(conf, tablePath, tableName);
    int version = IcebergDirectLoader.currentVersion(conf, tablePath);

    Snapshot snapshot = table.currentSnapshot();
    Set<String> relativePaths = new LinkedHashSet<>();
    relativePaths.add("metadata/v" + version + ".metadata.json");

    if (snapshot != null) {
      String manifestList = snapshot.manifestListLocation();
      if (manifestList != null) {
        relativePaths.add(relativize(tableName, manifestList));
      }
      for (ManifestFile manifestFile : snapshot.allManifests(table.io())) {
        relativePaths.add(relativize(tableName, manifestFile.path()));
      }
      try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
        for (FileScanTask task : tasks) {
          DataFile dataFile = task.file();
          relativePaths.add(relativize(tableName, dataFile.path().toString()));
          for (DeleteFile deleteFile : task.deletes()) {
            relativePaths.add(relativize(tableName, deleteFile.path().toString()));
          }
        }
      }
    }

    System.out.println("VERSION " + version);
    for (String path : relativePaths) {
      System.out.println(path);
    }
  }

  /**
   * Returns everything after the LAST {@code /<tableName>/} segment of an absolute file path.
   * Splitting on the table-name segment (rather than comparing the full table path prefix)
   * sidesteps scheme differences (e.g. {@code s3a://} vs {@code s3://}) between the URI this
   * tool loaded the table from and whatever scheme Iceberg's FileIO recorded on individual
   * file entries — storage-provider-guard forbids branching on URI scheme, and this doesn't
   * need to: the table-name segment is scheme-independent.
   */
  private static String relativize(String tableName, CharSequence absolutePath) {
    String abs = absolutePath.toString();
    String marker = "/" + tableName + "/";
    int idx = abs.lastIndexOf(marker);
    if (idx < 0) {
      throw new IllegalStateException(
          "File path '" + abs + "' does not contain table segment '" + marker + "'");
    }
    return abs.substring(idx + marker.length());
  }
}
