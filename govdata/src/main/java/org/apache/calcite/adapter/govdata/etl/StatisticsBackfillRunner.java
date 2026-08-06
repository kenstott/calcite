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

import org.apache.calcite.adapter.file.statistics.IcebergThetaStatistics;

import org.apache.iceberg.Table;
import org.apache.iceberg.hadoop.HadoopTables;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;

/**
 * Generates Iceberg column statistics for tables that have none.
 *
 * <p>The ETL publishes statistics as a side effect of writing, which leaves two kinds of table
 * uncovered: those that are genuinely static — reference and lookup tables that will never be
 * rewritten — and those that simply had nothing new on the day, which is most tables on most days
 * (three of four lands tables skipped on a live run). Both look identical from here: no statistics
 * for the current snapshot. Keying on that rather than trying to classify a table as "static"
 * avoids guessing at a property the schema does not actually declare — a table with no year
 * dimension may still be replaced wholesale every night.
 *
 * <p>Bootstraps only what is missing, so it is safe to re-run and cheap on a second pass. A table
 * that already carries per-partition sketches is left alone: its statistics are maintained
 * incrementally by the ETL and re-scanning would cost a full read to arrive at the same answer.
 *
 * <p>Usage:
 * <pre>
 * java -cp build/libs/sih-govdata.jar \
 *   org.apache.calcite.adapter.govdata.etl.StatisticsBackfillRunner \
 *   --warehouse s3a://govdata-parquet-v1 --schema ref,geo --dry-run
 * </pre>
 *
 * <p>Must run on JDK 21: writing sketches needs DataSketches, which refuses to initialise on a
 * newer JVM. Reading them does not, so this constraint stops here.
 */
public final class StatisticsBackfillRunner {

  private StatisticsBackfillRunner() {
  }

  public static void main(String[] args) throws Exception {
    String warehouse = null;
    List<String> schemas = new ArrayList<String>();
    List<String> tables = new ArrayList<String>();
    boolean dryRun = false;
    for (int i = 0; i < args.length; i++) {
      if ("--warehouse".equals(args[i]) && i + 1 < args.length) {
        warehouse = args[++i];
      } else if ("--schema".equals(args[i]) && i + 1 < args.length) {
        schemas.addAll(Arrays.asList(args[++i].split(",")));
      } else if ("--table".equals(args[i]) && i + 1 < args.length) {
        tables.addAll(Arrays.asList(args[++i].split(",")));
      } else if ("--dry-run".equals(args[i])) {
        dryRun = true;
      }
    }
    if (warehouse == null || schemas.isEmpty()) {
      System.err.println("Usage: --warehouse <s3a://bucket> --schema <a,b> "
          + "[--table <t1,t2>] [--dry-run]");
      System.exit(2);
    }

    org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
    String endpoint = System.getenv("AWS_ENDPOINT_OVERRIDE");
    if (endpoint != null && !endpoint.isEmpty()) {
      conf.set("fs.s3a.endpoint", endpoint);
      conf.set("fs.s3a.path.style.access", "true");
    }
    conf.set("fs.s3a.access.key", String.valueOf(System.getenv("AWS_ACCESS_KEY_ID")));
    conf.set("fs.s3a.secret.key", String.valueOf(System.getenv("AWS_SECRET_ACCESS_KEY")));
    HadoopTables hadoopTables = new HadoopTables(conf);

    int examined = 0;
    int bootstrapped = 0;
    int alreadyCovered = 0;
    int unreadable = 0;
    for (String schema : schemas) {
      for (String tableName : tablesIn(conf, warehouse, schema, tables)) {
        String location = warehouse + "/" + schema + "/" + tableName;
        examined++;
        Table table;
        try {
          table = hadoopTables.load(location);
        } catch (Exception e) {
          // Not every prefix under a schema is an Iceberg table.
          unreadable++;
          continue;
        }
        if (table.currentSnapshot() == null) {
          continue;   // empty table; nothing to describe
        }
        if (IcebergThetaStatistics.hasCarryForwardStatistics(table)) {
          alreadyCovered++;
          System.out.printf("  %-14s %-34s already covered%n", schema, tableName);
          continue;
        }
        if (dryRun) {
          System.out.printf("  %-14s %-34s WOULD bootstrap%n", schema, tableName);
          bootstrapped++;
          continue;
        }
        long started = System.currentTimeMillis();
        IcebergThetaStatistics.bootstrapFromScan(table,
            location + "/metadata/stats-backfill-" + UUID.randomUUID() + ".puffin");
        Table reloaded = hadoopTables.load(location);
        boolean ok = IcebergThetaStatistics.hasCarryForwardStatistics(reloaded);
        System.out.printf("  %-14s %-34s %s (%dms, %d columns)%n", schema, tableName,
            ok ? "bootstrapped" : "FAILED", System.currentTimeMillis() - started,
            IcebergThetaStatistics.readNdv(reloaded).size());
        if (ok) {
          bootstrapped++;
        }
      }
    }
    System.out.printf("%nexamined=%d bootstrapped=%d alreadyCovered=%d notAnIcebergTable=%d%n",
        examined, bootstrapped, alreadyCovered, unreadable);

    if (!dryRun) {
      backfillPrimaryKeyStatistics(schemas, tables);
    }
  }

  /**
   * Measures and records primary-key uniqueness for every table that declares a key.
   *
   * <p>Collecting it here is what keeps it off the verify path: verify falls back to measuring
   * on a miss, but that scan runs wherever verify runs — potentially across a WAN. Measured
   * here it runs once, next to the data, and every later verify reads a number.
   *
   * <p>Uses SQL rather than the Iceberg scan API above because counting distinct key tuples is
   * an aggregate the query engine already pushes down; reimplementing it over a table scan
   * would buffer the key set in this process.
   */
  private static void backfillPrimaryKeyStatistics(List<String> schemas, List<String> tables) {
    Map<String, List<String>> pkByTable =
        GovDataModelVerificationRunner.loadPrimaryKeys(new LinkedHashSet<>(schemas));
    if (pkByTable.isEmpty()) {
      return;
    }
    System.out.printf("%nPrimary-key uniqueness%n");
    int recorded = 0;
    int skipped = 0;
    for (String schema : schemas) {
      try {
        Class.forName("org.apache.calcite.adapter.govdata.GovDataDriver");
      } catch (ClassNotFoundException e) {
        throw new IllegalStateException("GovDataDriver not on the classpath", e);
      }
      try (Connection conn =
               DriverManager.getConnection("jdbc:govdata:source=" + schema)) {
        for (Map.Entry<String, List<String>> entry : pkByTable.entrySet()) {
          String key = entry.getKey();
          if (!key.startsWith(schema.toLowerCase(Locale.ROOT) + ".")) {
            continue;
          }
          String tableName = key.substring(key.indexOf('.') + 1);
          if (!tables.isEmpty() && !tables.contains(tableName)) {
            continue;
          }
          long started = System.currentTimeMillis();
          String outcome = GovDataModelVerificationRunner.recordPrimaryKeyStatistic(
              conn, schema, tableName, entry.getValue());
          System.out.printf("  %-14s %-34s %s (%dms)%n", schema, tableName, outcome,
              System.currentTimeMillis() - started);
          if (outcome.startsWith("recorded") || outcome.startsWith("already")) {
            recorded++;
          } else {
            skipped++;
          }
        }
      } catch (SQLException e) {
        // Reported, not swallowed: a schema that cannot be opened leaves its tables measuring
        // on the verify path, and the operator needs to know which ones.
        System.out.printf("  %-14s %-34s connect failed: %s%n", schema, "*", e.getMessage());
      }
    }
    System.out.printf("pkRecorded=%d pkUnrecorded=%d%n", recorded, skipped);
  }

  /** Table names under a schema prefix, or the explicit list when one was given. */
  private static List<String> tablesIn(org.apache.hadoop.conf.Configuration conf,
      String warehouse, String schema, List<String> explicit) throws Exception {
    if (!explicit.isEmpty()) {
      return explicit;
    }
    List<String> out = new ArrayList<String>();
    org.apache.hadoop.fs.Path base = new org.apache.hadoop.fs.Path(warehouse + "/" + schema);
    org.apache.hadoop.fs.FileSystem fs = base.getFileSystem(conf);
    for (org.apache.hadoop.fs.FileStatus st : fs.listStatus(base)) {
      if (st.isDirectory()) {
        out.add(st.getPath().getName());
      }
    }
    java.util.Collections.sort(out);
    return out;
  }
}
