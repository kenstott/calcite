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
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.CloseableIterable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;

/**
 * One-off repair tool: deletes every data file whose rows all match {@code column = value},
 * via Iceberg's metadata-only {@code deleteFromRowFilter} (a real delete, not a hand-edit of
 * warehouse files).
 *
 * <p>Exists for the case where a schema/dimension restructuring stops writing to an old
 * partition value entirely (e.g. renaming the values that feed a partition column), leaving
 * that value's prior data orphaned: no future run will ever touch it again via the normal
 * write path, so it sits stale until explicitly cleaned up. This only works when every row in
 * every affected file shares the same value for {@code column} — i.e. {@code column} is (or is
 * aligned with) the table's partition column, so files are never a mix of old/new values.
 * Iceberg throws {@code ValidationException} and commits nothing if that assumption doesn't
 * hold for some file, so a failed run here is always a no-op, never a partial corruption.
 *
 * <p>Usage:
 * <pre>{@code
 * java -cp sih-govdata.jar org.apache.calcite.adapter.file.iceberg.PartitionCleanupRunner \
 *   --warehouse s3://bucket/health \
 *   --table cdc_mortality \
 *   --column source_type \
 *   --value weekly \
 *   [--dry-run]
 * }</pre>
 */
public class PartitionCleanupRunner {

  private static final Logger LOGGER = LoggerFactory.getLogger(PartitionCleanupRunner.class);

  public static void main(String[] args) throws Exception {
    String warehouse = null;
    String tableName = null;
    String column = null;
    String value = null;
    boolean dryRun = false;

    for (int i = 0; i < args.length; i++) {
      switch (args[i]) {
      case "--warehouse":
        warehouse = args[++i];
        break;
      case "--table":
        tableName = args[++i];
        break;
      case "--column":
        column = args[++i];
        break;
      case "--value":
        value = args[++i];
        break;
      case "--dry-run":
        dryRun = true;
        break;
      default:
        System.err.println("Unknown argument: " + args[i]);
        System.exit(1);
      }
    }

    if (warehouse == null || tableName == null || column == null || value == null) {
      System.err.println("Usage: PartitionCleanupRunner --warehouse <path> --table <name> "
          + "--column <name> --value <value> [--dry-run]");
      System.exit(1);
    }

    Configuration conf = buildHadoopConf();
    String hadoopWarehouse = warehouse.replace("s3://", "s3a://");
    String tablePath = hadoopWarehouse + "/" + tableName;
    Table table = new HadoopTables(conf).load(tablePath);

    Expression filter = Expressions.equal(column, value);

    TableScan scan = table.newScan().filter(filter);
    Set<String> matchedFiles = new HashSet<>();
    long matchedRecords = 0;
    try (CloseableIterable<FileScanTask> tasks = scan.planFiles()) {
      for (FileScanTask task : tasks) {
        DataFile file = task.file();
        matchedFiles.add(file.path().toString());
        matchedRecords += file.recordCount();
      }
    }

    System.out.println("Table: " + tablePath);
    System.out.println("Filter: " + column + " = " + value);
    System.out.println("Matched files: " + matchedFiles.size());
    System.out.println("Matched records (file-level totals, pre-filter-within-file): "
        + matchedRecords);
    for (String f : matchedFiles) {
      System.out.println("  " + f);
    }

    if (dryRun) {
      System.out.println("DRY RUN — no changes committed.");
      return;
    }

    if (matchedFiles.isEmpty()) {
      System.out.println("Nothing to do.");
      return;
    }

    LOGGER.info("Deleting {} files where {} = {} from {}", matchedFiles.size(), column, value,
        tablePath);
    table.newDelete()
        .deleteFromRowFilter(filter)
        .commit();
    System.out.println("Committed. New snapshot: " + table.currentSnapshot().snapshotId());
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
    }
    conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
    conf.set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
    return conf;
  }

}
