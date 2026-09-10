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

import org.apache.calcite.adapter.file.iceberg.IcebergTableWriter;
import org.apache.calcite.adapter.file.iceberg.S3FileIOTables;

import org.apache.iceberg.Table;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Deletes the rows of one partition value from a set of Iceberg tables, in place.
 *
 * <p>Exists because the two ways to remove rows before this were both wrong for the job.
 * {@code data_purge.sh} drops a whole table, which for a table holding several year partitions
 * destroys the years you meant to keep and forces them to be re-derived from raw. The materializer's
 * {@code forceAccessions} path deletes precisely, but it also puts every named accession back into
 * {@code filterUnprocessed}'s work list, so removing rows implies re-extracting them.
 *
 * <p>This runner does neither: it deletes rows matching {@code --column=--value} through
 * {@link IcebergTableWriter#deleteRows(Map)} and stops. Re-materializing the partition afterwards
 * from staging that is already on disk is a separate, cheap step, and nothing is re-downloaded.
 *
 * <p>Defaults to a dry run — it reports what each table would lose and changes nothing. Pass
 * {@code --execute} to actually delete.
 *
 * <p>Usage:
 * <pre>
 *   IcebergPartitionDeleteRunner --warehouse s3://bucket/sec \
 *       --tables financial_line_items,filing_contexts --column year --value 2026 [--execute]
 * </pre>
 * S3 credentials come from {@code AWS_ACCESS_KEY_ID}, {@code AWS_SECRET_ACCESS_KEY} and
 * {@code AWS_ENDPOINT_OVERRIDE} — the launch-script-owned infra variables.
 */
public final class IcebergPartitionDeleteRunner {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(IcebergPartitionDeleteRunner.class);

  private IcebergPartitionDeleteRunner() {
  }

  public static void main(String[] args) {
    String warehouse = null;
    String tablesCsv = null;
    String column = null;
    String value = null;
    boolean execute = false;

    for (int i = 0; i < args.length; i++) {
      String a = args[i];
      if ("--warehouse".equals(a) && i + 1 < args.length) {
        warehouse = args[++i];
      } else if ("--tables".equals(a) && i + 1 < args.length) {
        tablesCsv = args[++i];
      } else if ("--column".equals(a) && i + 1 < args.length) {
        column = args[++i];
      } else if ("--value".equals(a) && i + 1 < args.length) {
        value = args[++i];
      } else if ("--execute".equals(a)) {
        execute = true;
      } else if ("--help".equals(a) || "-h".equals(a)) {
        usage();
        return;
      } else {
        System.err.println("Unknown argument: " + a);
        usage();
        System.exit(2);
      }
    }

    if (warehouse == null || tablesCsv == null || column == null || value == null) {
      usage();
      System.exit(2);
      return;
    }

    Map<String, String> s3Config = new HashMap<String, String>();
    s3Config.put("accessKeyId", System.getenv("AWS_ACCESS_KEY_ID"));
    s3Config.put("secretAccessKey", System.getenv("AWS_SECRET_ACCESS_KEY"));
    s3Config.put("endpoint", System.getenv("AWS_ENDPOINT_OVERRIDE"));

    String base = warehouse.endsWith("/")
        ? warehouse.substring(0, warehouse.length() - 1) : warehouse;

    List<String> tables = new ArrayList<String>();
    for (String t : tablesCsv.split(",")) {
      String trimmed = t.trim();
      if (!trimmed.isEmpty()) {
        tables.add(trimmed);
      }
    }

    System.out.println(execute
        ? "MODE: EXECUTE — rows will be deleted"
        : "MODE: DRY RUN — nothing will be changed (pass --execute to delete)");
    System.out.println("Warehouse: " + base);
    System.out.println("Filter:    " + column + " = " + value);
    System.out.println();

    int failures = 0;
    for (String tableName : tables) {
      String path = base + "/" + tableName;
      try {
        Table table = S3FileIOTables.loadWritable(path, s3Config);

        long matching = countMatching(table, column, value);
        System.out.printf("%-26s %,12d rows match%n", tableName, matching);

        if (execute && matching > 0) {
          Map<String, String> filter = new LinkedHashMap<String, String>();
          filter.put(column, value);
          new IcebergTableWriter(table, null).deleteRows(filter);
          System.out.printf("%-26s deleted%n", tableName);
        }
      } catch (Exception e) {
        failures++;
        System.out.printf("%-26s FAILED: %s%n", tableName, e.getMessage());
        LOGGER.warn("Delete failed for {}", tableName, e);
      }
    }

    System.out.println();
    System.out.println(failures == 0 ? "All tables processed." : failures + " table(s) failed.");
    if (failures > 0) {
      System.exit(1);
    }
  }

  /**
   * Counts rows matching the filter, so a dry run can state what would be lost rather than
   * describing it in the abstract. Uses the scan-level filter only, which is exact for a
   * partition column and an over-estimate for a non-partition one — the printed number is
   * therefore an upper bound on rows removed, never an under-count.
   */
  private static long countMatching(Table table, String column, String value) {
    long rows = 0;
    try (CloseableIterable<org.apache.iceberg.FileScanTask> tasks =
             table.newScan().filter(Expressions.equal(column, Integer.parseInt(value)))
                 .planFiles()) {
      for (org.apache.iceberg.FileScanTask task : tasks) {
        rows += task.file().recordCount();
      }
    } catch (NumberFormatException e) {
      return countMatchingString(table, column, value);
    } catch (Exception e) {
      LOGGER.warn("Could not count matching rows for {}: {}", column, e.getMessage());
      return -1;
    }
    return rows;
  }

  private static long countMatchingString(Table table, String column, String value) {
    long rows = 0;
    try (CloseableIterable<org.apache.iceberg.FileScanTask> tasks =
             table.newScan().filter(Expressions.equal(column, value)).planFiles()) {
      for (org.apache.iceberg.FileScanTask task : tasks) {
        rows += task.file().recordCount();
      }
    } catch (Exception e) {
      LOGGER.warn("Could not count matching rows for {}: {}", column, e.getMessage());
      return -1;
    }
    return rows;
  }

  private static void usage() {
    System.out.println("Usage: IcebergPartitionDeleteRunner --warehouse <path> --tables <t1,t2,...>");
    System.out.println("                                    --column <name> --value <v> [--execute]");
    System.out.println();
    System.out.println("Deletes rows matching column=value from each table, in place.");
    System.out.println("Defaults to a dry run; --execute performs the delete.");
    System.out.println("Does NOT re-extract anything — re-materialize separately if wanted.");
  }
}
