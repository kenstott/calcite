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

import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

/**
 * Standalone verifier that exercises the GovData Calcite model end-to-end through
 * its internal DuckDB execution engine — the read path that the handcrafted DuckDB
 * DQ scripts ({@code *_dq.sql}) never touch.
 *
 * <p>It connects via {@code jdbc:calcite:model=<model.json>}, enumerates every table
 * the model actually exposes (so a table that fails to register surfaces as MISSING
 * when cross-checked against an expected list), runs a {@code SELECT * ... LIMIT n}
 * against each through Calcite -> DuckDB, and classifies the result as OK / EMPTY /
 * ERROR. Optional data-driven feature probes (geo spatial, semantic similarity) are
 * read from a probe file so their exact SQL can be iterated without a rebuild.
 *
 * <p>Usage:
 * <pre>
 * java -cp build/libs/sih-govdata.jar \
 *   org.apache.calcite.adapter.govdata.etl.GovDataModelVerificationRunner \
 *   --model /tmp/verify-model.json \
 *   --limit 1 \
 *   --expected /tmp/expected-tables.txt \
 *   --probes /tmp/feature-probes.txt \
 *   --schemas sec,geo,econ
 * </pre>
 *
 * <p>Exit codes: 0 = all exposed tables readable (no ERROR); 1 = one or more
 * ERROR/MISSING; 2 = could not connect / fatal.
 */
public final class GovDataModelVerificationRunner {

  private GovDataModelVerificationRunner() {
  }

  private static final class Config {
    String model;
    int limit = 1;
    String expected;
    String probes;
    Set<String> schemaFilter = new LinkedHashSet<String>();
  }

  private static final class TableResult {
    String schema;
    String table;
    String reach;  // OK, ERROR  (can the model reach/register the table at all)
    long count;    // row count from COUNT(*), -1 if reach failed
    String mat;    // OK, EMPTY, ERROR  (can a full row be materialized)
    int cols;
    String error;  // first error encountered (reach takes precedence)
  }

  public static void main(String[] args) throws Exception {
    Config cfg = parseArgs(args);
    if (cfg.model == null) {
      printUsage();
      System.exit(2);
      return;
    }

    Class.forName("org.apache.calcite.jdbc.Driver");
    Properties props = new Properties();
    props.setProperty("lex", "ORACLE");
    props.setProperty("unquotedCasing", "TO_LOWER");
    // Resolve identifiers/functions case-insensitively so unquoted (TO_LOWER) calls
    // match the registered UDF names (COSINE_SIMILARITY, ST_*), which Calcite stores
    // upper-cased.
    props.setProperty("caseSensitive", "false");
    props.setProperty("model", cfg.model);
    // Enable Calcite's spatial operator library so ST_* parse (execution still
    // pushes to DuckDB where applicable).
    props.setProperty("fun", "standard,spatial");

    long t0 = System.currentTimeMillis();
    System.out.println("Connecting to jdbc:calcite: model=" + cfg.model);

    Connection conn = null;
    try {
      conn = DriverManager.getConnection("jdbc:calcite:", props);
    } catch (Exception e) {
      System.err.println("FATAL: could not open Calcite connection: " + e.getMessage());
      e.printStackTrace();
      System.exit(2);
      return;
    }

    try {
      List<TableResult> results = probeTables(conn, cfg);
      List<String> missing = crossCheckExpected(cfg, results);
      int probeFailures = runProbes(conn, cfg);

      // ---- report ----
      int reachErr = 0;
      int matErr = 0;
      int dataOk = 0;
      int emptyOk = 0;
      System.out.println();
      System.out.println("================ PER-TABLE RESULTS ================");
      System.out.printf("%-15s %-32s %-6s %10s %-6s  %s%n",
          "SCHEMA", "TABLE", "REACH", "ROWS", "MAT", "DETAIL");
      for (int i = 0; i < results.size(); i++) {
        TableResult r = results.get(i);
        if (!"OK".equals(r.reach)) {
          reachErr++;
        } else if ("ERROR".equals(r.mat)) {
          matErr++;
        } else if (r.count > 0) {
          dataOk++;
        } else {
          emptyOk++;
        }
        System.out.printf("%-15s %-32s %-6s %10d %-6s  %s%n",
            r.schema, trunc(r.table, 32), r.reach, r.count, r.mat,
            r.error == null ? "" : trunc(r.error, 80));
      }

      System.out.println();
      System.out.println("================ SUMMARY ================");
      System.out.println("  exposed tables        : " + results.size());
      System.out.println("  REACH ok / err        : " + (results.size() - reachErr) + " / " + reachErr);
      System.out.println("  data present (rows>0) : " + dataOk);
      System.out.println("  reachable but empty   : " + emptyOk);
      System.out.println("  MATERIALIZE errors    : " + matErr);
      int err = reachErr + matErr;
      if (!missing.isEmpty()) {
        System.out.println("  MISSING (defined but not exposed): " + missing.size());
        for (int i = 0; i < missing.size(); i++) {
          System.out.println("      - " + missing.get(i));
        }
      }
      System.out.println("  feature-probe failures: " + probeFailures);
      System.out.println("  elapsed ms     : " + (System.currentTimeMillis() - t0));

      int exit = (err > 0 || !missing.isEmpty() || probeFailures > 0) ? 1 : 0;
      System.exit(exit);
    } finally {
      try {
        conn.close();
      } catch (Exception ignored) {
        // ignore
      }
    }
  }

  private static List<TableResult> probeTables(Connection conn, Config cfg) throws Exception {
    List<TableResult> results = new ArrayList<TableResult>();
    DatabaseMetaData md = conn.getMetaData();
    ResultSet tabs = md.getTables(null, null, "%", new String[] {"TABLE", "VIEW"});
    List<String[]> coords = new ArrayList<String[]>();
    while (tabs.next()) {
      String schema = tabs.getString("TABLE_SCHEM");
      String table = tabs.getString("TABLE_NAME");
      if (schema == null) {
        continue;
      }
      String slc = schema.toLowerCase();
      if (slc.equals("metadata") || slc.equals("information_schema")
          || slc.equals("system") || slc.equals("pg_catalog")) {
        continue;
      }
      if (!cfg.schemaFilter.isEmpty() && !cfg.schemaFilter.contains(slc)) {
        continue;
      }
      coords.add(new String[] {schema, table});
    }
    tabs.close();

    System.out.println("Enumerated " + coords.size() + " exposed tables. Probing (LIMIT "
        + cfg.limit + ") ...");
    for (int i = 0; i < coords.size(); i++) {
      String schema = coords.get(i)[0];
      String table = coords.get(i)[1];
      TableResult r = new TableResult();
      r.schema = schema;
      r.table = table;
      r.count = -1;

      // Phase 1: reachability + row count via COUNT(*) (no value materialization).
      Statement st = null;
      ResultSet rs = null;
      try {
        st = conn.createStatement();
        rs = st.executeQuery("SELECT COUNT(*) FROM \"" + schema + "\".\"" + table + "\"");
        if (rs.next()) {
          r.count = rs.getLong(1);
        }
        r.reach = "OK";
      } catch (Exception e) {
        r.reach = "ERROR";
        r.error = e.getMessage();
      } finally {
        closeQuietly(rs);
        closeQuietly(st);
      }

      // Phase 2: full-row materialization via SELECT * LIMIT n (forces every column
      // through the Calcite -> DuckDB value conversion path).
      if ("OK".equals(r.reach)) {
        st = null;
        rs = null;
        try {
          st = conn.createStatement();
          rs = st.executeQuery("SELECT * FROM \"" + schema + "\".\"" + table
              + "\" LIMIT " + cfg.limit);
          ResultSetMetaData rmd = rs.getMetaData();
          r.cols = rmd.getColumnCount();
          int n = 0;
          while (rs.next()) {
            for (int c = 1; c <= r.cols; c++) {
              rs.getObject(c);
            }
            n++;
          }
          r.mat = n > 0 ? "OK" : "EMPTY";
        } catch (Exception e) {
          r.mat = "ERROR";
          if (r.error == null) {
            r.error = e.getMessage();
          }
          if (System.getenv("VERIFY_STACK") != null) {
            System.err.println("STACK for " + schema + "." + table + ":");
            e.printStackTrace();
          }
        } finally {
          closeQuietly(rs);
          closeQuietly(st);
        }
      } else {
        r.mat = "-";
      }
      results.add(r);
    }
    return results;
  }

  private static List<String> crossCheckExpected(Config cfg, List<TableResult> results)
      throws Exception {
    List<String> missing = new ArrayList<String>();
    if (cfg.expected == null) {
      return missing;
    }
    Set<String> exposed = new LinkedHashSet<String>();
    for (int i = 0; i < results.size(); i++) {
      exposed.add((results.get(i).schema + "." + results.get(i).table).toLowerCase());
    }
    BufferedReader br = new BufferedReader(new FileReader(cfg.expected));
    try {
      String line;
      while ((line = br.readLine()) != null) {
        String t = line.trim().toLowerCase();
        if (t.isEmpty() || t.startsWith("#")) {
          continue;
        }
        if (!cfg.schemaFilter.isEmpty()) {
          int dot = t.indexOf('.');
          String sc = dot > 0 ? t.substring(0, dot) : t;
          if (!cfg.schemaFilter.contains(sc)) {
            continue;
          }
        }
        if (!exposed.contains(t)) {
          missing.add(t);
        }
      }
    } finally {
      br.close();
    }
    return missing;
  }

  private static int runProbes(Connection conn, Config cfg) throws Exception {
    if (cfg.probes == null) {
      return 0;
    }
    System.out.println();
    System.out.println("================ FEATURE PROBES ================");
    int failures = 0;
    BufferedReader br = new BufferedReader(new FileReader(cfg.probes));
    try {
      String line;
      while ((line = br.readLine()) != null) {
        String t = line.trim();
        if (t.isEmpty() || t.startsWith("#")) {
          continue;
        }
        int sep = t.indexOf("|||");
        String label = sep > 0 ? t.substring(0, sep).trim() : "probe";
        String sql = sep > 0 ? t.substring(sep + 3).trim() : t;
        Statement st = null;
        ResultSet rs = null;
        try {
          st = conn.createStatement();
          rs = st.executeQuery(sql);
          ResultSetMetaData rmd = rs.getMetaData();
          int cols = rmd.getColumnCount();
          StringBuilder first = new StringBuilder();
          int rows = 0;
          while (rs.next()) {
            if (rows == 0) {
              for (int c = 1; c <= cols && c <= 4; c++) {
                if (c > 1) {
                  first.append(", ");
                }
                String v = rs.getString(c);
                first.append(trunc(v == null ? "null" : v, 40));
              }
            }
            rows++;
          }
          System.out.println("  [OK]    " + label + " -> rows=" + rows + " cols=" + cols
              + " first=[" + first + "]");
        } catch (Exception e) {
          failures++;
          System.out.println("  [FAIL]  " + label + " -> " + trunc(e.getMessage(), 600));
        } finally {
          if (rs != null) {
            try {
              rs.close();
            } catch (Exception ignored) {
              // ignore
            }
          }
          if (st != null) {
            try {
              st.close();
            } catch (Exception ignored) {
              // ignore
            }
          }
        }
      }
    } finally {
      br.close();
    }
    return failures;
  }

  private static Config parseArgs(String[] args) {
    Config cfg = new Config();
    for (int i = 0; i < args.length; i++) {
      String a = args[i];
      if ("--model".equals(a)) {
        cfg.model = args[++i];
      } else if ("--limit".equals(a)) {
        cfg.limit = Integer.parseInt(args[++i]);
      } else if ("--expected".equals(a)) {
        cfg.expected = args[++i];
      } else if ("--probes".equals(a)) {
        cfg.probes = args[++i];
      } else if ("--schemas".equals(a)) {
        String[] parts = args[++i].split(",");
        for (int p = 0; p < parts.length; p++) {
          String s = parts[p].trim().toLowerCase();
          if (!s.isEmpty()) {
            cfg.schemaFilter.add(s);
          }
        }
      } else {
        System.err.println("Unknown argument: " + a);
        printUsage();
        System.exit(2);
      }
    }
    return cfg;
  }

  private static void closeQuietly(java.lang.AutoCloseable c) {
    if (c != null) {
      try {
        c.close();
      } catch (Exception ignored) {
        // ignore
      }
    }
  }

  private static String trunc(String s, int n) {
    if (s == null) {
      return "";
    }
    String oneLine = s.replace('\n', ' ').replace('\r', ' ');
    return oneLine.length() <= n ? oneLine : oneLine.substring(0, n) + "...";
  }

  private static void printUsage() {
    System.err.println("Usage: GovDataModelVerificationRunner --model <model.json> "
        + "[--limit N] [--expected <file>] [--probes <file>] [--schemas a,b,c]");
    System.err.println("  --model     Calcite model JSON (required)");
    System.err.println("  --limit     rows to fetch per table probe (default 1)");
    System.err.println("  --expected  file of 'schema.table' lines; reports defined-but-not-exposed");
    System.err.println("  --probes    file of 'label|||SQL' lines for feature probes (geo, semantic)");
    System.err.println("  --schemas   comma-separated schema allow-list (default: all)");
  }
}
