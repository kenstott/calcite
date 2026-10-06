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

import org.apache.calcite.adapter.file.iceberg.DedupeReport;
import org.apache.calcite.adapter.file.iceberg.IcebergTableWriter;
import org.apache.calcite.adapter.file.iceberg.S3FileIOTables;

import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

/**
 * Removes the surplus copies left when a whole accession (or other group) was ingested more than
 * once, in place, without re-fetching anything.
 *
 * <p>For each table and partition value it keeps one copy of every row of a group whose rows all
 * appear the same number of times (see {@link IcebergTableWriter#dedupeCopies}); every distinct
 * row, and therefore every logical key, survives, so anything that refers to a source row by key
 * (such as {@code ref.vectorized_chunks}) still resolves. Each table and partition is replaced in
 * one Iceberg snapshot, and the snapshot to roll back to is printed.
 *
 * <p>Defaults to a dry run that reports what would be removed. {@code --execute} needs an explicit
 * {@code --tables} and {@code --years}, so a run never defaults to the whole warehouse.
 *
 * <pre>
 *   IcebergDedupeRunner --warehouse s3://bucket/sec --schema sec \
 *       [--tables t1,t2] [--years 2018,2019] [--group-column accession_number] \
 *       [--partition-column year] [--execute]
 *   IcebergDedupeRunner --warehouse s3://bucket/sec --tables t --rollback-to &lt;snapshotId&gt;
 * </pre>
 * Primary keys come from the schema YAML's {@code constraints}. S3 credentials come from
 * {@code AWS_ACCESS_KEY_ID}, {@code AWS_SECRET_ACCESS_KEY} and {@code AWS_ENDPOINT_OVERRIDE}.
 */
public final class IcebergDedupeRunner {
  private static final Logger LOGGER = LoggerFactory.getLogger(IcebergDedupeRunner.class);

  private IcebergDedupeRunner() {
  }

  public static void main(String[] args) throws Exception {
    String warehouse = null;
    String schemaName = null;
    String tablesCsv = null;
    String yearsCsv = null;
    String groupColumn = "accession_number";
    String partitionColumn = "year";
    String rollbackTo = null;
    boolean execute = false;
    for (int i = 0; i < args.length; i++) {
      String a = args[i];
      if ("--warehouse".equals(a) && i + 1 < args.length) {
        warehouse = args[++i];
      } else if ("--schema".equals(a) && i + 1 < args.length) {
        schemaName = args[++i];
      } else if ("--tables".equals(a) && i + 1 < args.length) {
        tablesCsv = args[++i];
      } else if ("--years".equals(a) && i + 1 < args.length) {
        yearsCsv = args[++i];
      } else if ("--group-column".equals(a) && i + 1 < args.length) {
        groupColumn = args[++i];
      } else if ("--partition-column".equals(a) && i + 1 < args.length) {
        partitionColumn = args[++i];
      } else if ("--rollback-to".equals(a) && i + 1 < args.length) {
        rollbackTo = args[++i];
      } else if ("--execute".equals(a)) {
        execute = true;
      } else {
        System.err.println("Unknown argument: " + a);
        usage();
        System.exit(2);
      }
    }
    if (warehouse == null) {
      usage();
      System.exit(2);
    }
    Map<String, String> s3 = new HashMap<String, String>();
    s3.put("accessKeyId", System.getenv("AWS_ACCESS_KEY_ID"));
    s3.put("secretAccessKey", System.getenv("AWS_SECRET_ACCESS_KEY"));
    s3.put("endpoint", System.getenv("AWS_ENDPOINT_OVERRIDE"));
    String base = warehouse.endsWith("/") ? warehouse.substring(0, warehouse.length() - 1) : warehouse;

    if (rollbackTo != null) {
      rollback(base, s3, tablesCsv, rollbackTo);
      return;
    }
    if (schemaName == null) {
      System.err.println("--schema is required (primary keys come from its YAML constraints)");
      System.exit(2);
    }
    if (execute && (tablesCsv == null || yearsCsv == null)) {
      System.err.println("--execute needs an explicit --tables and --years");
      System.exit(2);
    }

    Map<String, List<String>> primaryKeys = loadPrimaryKeys(schemaName);
    List<String> tables = new ArrayList<String>();
    if (tablesCsv != null) {
      tables.addAll(split(tablesCsv));
    } else {
      tables.addAll(primaryKeys.keySet());
    }

    System.out.println(execute
        ? "MODE: EXECUTE — surplus copies will be removed"
        : "MODE: DRY RUN — nothing will be changed (pass --execute with --tables and --years)");
    System.out.println("Warehouse: " + base + "   group column: " + groupColumn);
    System.out.println();
    System.out.printf("%-24s %-6s %12s %12s %10s %12s  %s%n",
        "table", partitionColumn, "rows", "distinct", "groups*k", "to remove", "result");

    long totalToRemove = 0;
    long totalRemoved = 0;
    int failures = 0;
    for (String tableName : tables) {
      List<String> key = primaryKeys.get(tableName);
      if (key == null) {
        System.out.printf("%-24s no primary key in the %s schema YAML — skipped%n", tableName,
            schemaName);
        continue;
      }
      try {
        Table table = S3FileIOTables.loadWritable(base + "/" + tableName, s3);
        if (table.schema().findField(groupColumn) == null) {
          System.out.printf("%-24s has no column %s — skipped%n", tableName, groupColumn);
          continue;
        }
        List<String> years = yearsCsv != null ? split(yearsCsv)
            : partitionValues(table, partitionColumn);
        IcebergTableWriter writer = new IcebergTableWriter(table, null);
        for (String year : years) {
          table.refresh();
          DedupeReport r = writer.dedupeCopies(groupColumn, key,
              Expressions.equal(partitionColumn, Integer.parseInt(year)), execute);
          totalToRemove += r.rowsToRemove;
          totalRemoved += r.rowsRemoved;
          String result;
          if (r.rowsToRemove == 0) {
            result = "clean";
          } else if (!execute) {
            result = "would remove; copies=" + r.groupsByCopies;
          } else {
            result = r.verification + "; removed " + r.rowsRemoved
                + "; roll back to snapshot " + r.snapshotBefore;
          }
          System.out.printf("%-24s %-6s %,12d %,12d %,10d %,12d  %s%n", tableName, year, r.rows,
              r.distinctRows, r.groupsWithCopies(), r.rowsToRemove, result);
          if (execute && r.verification != null && !"OK".equals(r.verification)) {
            failures++;
          }
        }
      } catch (Exception e) {
        failures++;
        System.out.printf("%-24s FAILED: %s%n", tableName, e);
        LOGGER.warn("Dedupe failed for {}", tableName, e);
      }
    }
    System.out.println();
    System.out.printf("%,d row(s) %s%n", execute ? totalRemoved : totalToRemove,
        execute ? "removed" : "would be removed");
    if (failures > 0) {
      System.out.println(failures + " failure(s) or verification mismatch(es) — see above.");
      System.exit(1);
    }
  }

  private static void rollback(String base, Map<String, String> s3, String tablesCsv,
      String snapshotId) {
    if (tablesCsv == null || split(tablesCsv).size() != 1) {
      System.err.println("--rollback-to needs exactly one --tables name");
      System.exit(2);
    }
    String name = split(tablesCsv).get(0);
    Table table = S3FileIOTables.loadWritable(base + "/" + name, s3);
    long id = Long.parseLong(snapshotId);
    table.manageSnapshots().rollbackTo(id).commit();
    System.out.println(name + " rolled back to snapshot " + id);
  }

  /** The distinct values of {@code column} across the table's data files' partition tuples. */
  private static List<String> partitionValues(Table table, String column) throws Exception {
    PartitionSpec spec = table.spec();
    int idx = -1;
    for (int i = 0; i < spec.fields().size(); i++) {
      if (spec.fields().get(i).name().equals(column)) {
        idx = i;
      }
    }
    if (idx < 0) {
      throw new IllegalArgumentException(table.name() + " is not partitioned by " + column);
    }
    TreeSet<String> values = new TreeSet<String>();
    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      for (FileScanTask task : tasks) {
        Object v = task.file().partition().get(idx, Object.class);
        if (v != null) {
          values.add(String.valueOf(v));
        }
      }
    }
    return new ArrayList<String>(values);
  }

  /** {@code constraints.<table>.primaryKey} for every table in the schema's YAML resource. */
  @SuppressWarnings("unchecked")
  static Map<String, List<String>> loadPrimaryKeys(String schema) throws Exception {
    String resource = "/" + schema + "/" + schema + "-schema.yaml";
    LoaderOptions options = new LoaderOptions();
    options.setMaxAliasesForCollections(500);
    try (InputStream in = IcebergDedupeRunner.class.getResourceAsStream(resource)) {
      if (in == null) {
        throw new IllegalArgumentException("no schema YAML resource " + resource);
      }
      Map<String, Object> doc = new Yaml(options).load(in);
      Map<String, List<String>> out = new HashMap<String, List<String>>();
      Object constraints = doc.get("constraints");
      if (constraints instanceof Map) {
        for (Map.Entry<String, Object> e : ((Map<String, Object>) constraints).entrySet()) {
          if (e.getValue() instanceof Map) {
            Object pk = ((Map<String, Object>) e.getValue()).get("primaryKey");
            if (pk instanceof List) {
              out.put(e.getKey(), new ArrayList<String>((List<String>) pk));
            }
          }
        }
      }
      return out;
    }
  }

  private static List<String> split(String csv) {
    List<String> out = new ArrayList<String>();
    for (String s : Arrays.asList(csv.split(","))) {
      if (!s.trim().isEmpty()) {
        out.add(s.trim());
      }
    }
    return out;
  }

  private static void usage() {
    System.out.println("Usage: IcebergDedupeRunner --warehouse <s3://bucket/schema> --schema <name>");
    System.out.println("         [--tables t1,t2] [--years 2018,2019] [--group-column accession_number]");
    System.out.println("         [--partition-column year] [--execute]");
    System.out.println("       IcebergDedupeRunner --warehouse <...> --tables <t> --rollback-to <snapshotId>");
    System.out.println("Dry run by default; --execute needs --tables and --years.");
  }
}
