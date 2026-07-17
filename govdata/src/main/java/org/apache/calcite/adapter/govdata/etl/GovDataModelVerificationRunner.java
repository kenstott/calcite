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
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * Standalone verifier that exercises the GovData Calcite model end-to-end through
 * its internal DuckDB execution engine — the read path that the handcrafted DuckDB
 * DQ scripts ({@code *_dq.sql}) never touch.
 *
 * <p>It connects through {@link org.apache.calcite.adapter.govdata.GovDataDriver}
 * ({@code jdbc:govdata:source=<list>}), which builds the multi-schema
 * {@code autoDownload=false} introspection model internally from the schema YAMLs baked
 * into the jar — no hand-written model file. It then enumerates every table the
 * connection exposes via JDBC metadata, runs a {@code SELECT * ... LIMIT n} against each
 * through Calcite -> DuckDB, and classifies the result as OK / EMPTY / ERROR. An explicit
 * {@code --model} is still accepted as an alternative. Optional data-driven feature probes
 * (geo spatial, semantic similarity) are read from a probe file so their exact SQL can be
 * iterated without a rebuild.
 *
 * <p>Usage:
 * <pre>
 * java -cp build/libs/sih-govdata.jar \
 *   org.apache.calcite.adapter.govdata.etl.GovDataModelVerificationRunner \
 *   --source sec,geo,econ \
 *   --limit 1 \
 *   --probes /tmp/feature-probes.txt
 * </pre>
 *
 * <p>Exit codes: 0 = all exposed tables readable (no ERROR); 1 = one or more
 * ERROR/MISSING; 2 = could not connect / fatal.
 */
public final class GovDataModelVerificationRunner {

  private GovDataModelVerificationRunner() {
  }

  // dataSource -> bundled schema-definition YAML resource (classpath). Used to classify each
  // defined table as a base table, an intra-schema view, or an inter-schema view.
  private static final Map<String, String> SCHEMA_YAML = new LinkedHashMap<String, String>();
  // Canonical schema names, used to detect cross-schema references inside view SQL.
  private static final Set<String> KNOWN_SCHEMAS = new LinkedHashSet<String>();
  static {
    SCHEMA_YAML.put("sec", "/sec/sec-schema.yaml");
    SCHEMA_YAML.put("geo", "/geo/geo-schema.yaml");
    SCHEMA_YAML.put("econ", "/econ/econ-schema.yaml");
    SCHEMA_YAML.put("econ_reference", "/econ/econ-reference-schema.yaml");
    SCHEMA_YAML.put("census", "/census/census-schema.yaml");
    SCHEMA_YAML.put("crime", "/crime/crime-schema.yaml");
    SCHEMA_YAML.put("weather", "/weather/weather-schema.yaml");
    SCHEMA_YAML.put("ref", "/ref/ref-schema.yaml");
    SCHEMA_YAML.put("fec", "/fec/fec-schema.yaml");
    SCHEMA_YAML.put("fedregister", "/fedregister/fedregister-schema.yaml");
    SCHEMA_YAML.put("cyber_vuln", "/cyber/cyber-vuln-schema.yaml");
    SCHEMA_YAML.put("cyber_threat", "/cyber/cyber-threat-schema.yaml");
    SCHEMA_YAML.put("health", "/health/health-schema.yaml");
    SCHEMA_YAML.put("energy", "/energy/energy-schema.yaml");
    SCHEMA_YAML.put("edu", "/edu/edu-schema.yaml");
    SCHEMA_YAML.put("patents", "/patents/patents-schema.yaml");
    SCHEMA_YAML.put("lands", "/lands/lands-schema.yaml");
    SCHEMA_YAML.put("cftc", "/cftc/cftc-schema.yaml");
    SCHEMA_YAML.put("ag", "/ag/ag-schema.yaml");
    SCHEMA_YAML.put("disasters", "/disasters/disasters-schema.yaml");
    SCHEMA_YAML.put("housing", "/housing/housing-schema.yaml");
    SCHEMA_YAML.put("transport", "/transport/transport-schema.yaml");
    SCHEMA_YAML.put("environment", "/environment/environment-schema.yaml");
    SCHEMA_YAML.put("research", "/research/research-schema.yaml");
    KNOWN_SCHEMAS.addAll(SCHEMA_YAML.keySet());
  }

  private static final String BASE = "BASE";
  private static final String INTRA_VIEW = "INTRA_VIEW";
  private static final String INTER_VIEW = "INTER_VIEW";

  private static final class Config {
    String model;
    String source;  // comma-separated dataSource list; connects via jdbc:govdata:source=
    String primary; // primary schema (first in source) — only its tables are probed/reported;
                    // any other schemas are mounted only to resolve the primary's cross-schema views
    int limit = 1;
    String expected;
    String probes;
    Set<String> schemaFilter = new LinkedHashSet<String>();
    // Duplicate-row check: for each base table with a declared primaryKey, compare the count of
    // fully-keyed rows (all PK columns non-null) against the count of DISTINCT PK tuples. A
    // declared PK that is not unique is a defect (the signature of the Iceberg file-duplication
    // bug), so any table with >= dupThreshold duplicate keyed rows fails the run.
    boolean dupCheck = true;
    int dupThreshold = 1;   // minimum duplicate keyed-row count to flag/fail (>=1 → any duplication)
  }

  /** A table/view defined in a schema YAML, with its category and (for views) dependencies. */
  private static final class Defined {
    String schema;
    String table;
    String category;                                   // BASE, INTRA_VIEW, INTER_VIEW
    Set<String> deps = new LinkedHashSet<String>();    // other schemas referenced (INTER_VIEW)
  }

  /** Per-category status tally. */
  private static final class Counts {
    int data;
    int empty;
    int missing;
    int error;
    void tally(String status) {
      if ("DATA".equals(status)) {
        data++;
      } else if ("EMPTY".equals(status)) {
        empty++;
      } else if ("MISSING".equals(status)) {
        missing++;
      } else {
        error++;
      }
    }
    int total() {
      return data + empty + missing + error;
    }
    String line() {
      return total() + " total  (data " + data + " / empty " + empty
          + " / missing " + missing + " / error " + error + ")";
    }
  }

  private static final class TableResult {
    String schema;
    String table;
    String reach;  // OK, ERROR  (can the model reach/register the table at all)
    long count;    // row count from COUNT(*), -1 if reach failed
    String mat;    // OK, EMPTY, ERROR  (can a full row be materialized)
    int cols;
    String error;  // first error encountered (reach takes precedence)
    // Duplicate-row check (only for base tables with a declared primaryKey and count > 0):
    boolean pkChecked;   // the PK duplicate check actually ran for this table
    long keyedRows = -1; // rows where every PK column is non-null
    long distinctKeys = -1; // COUNT(DISTINCT pk-tuple) over those keyed rows
    long dupRows = -1;   // keyedRows - distinctKeys (>0 ⇒ the declared PK is not unique)
    String dupErr;       // dup-check SQL failed (reported as n/a, not a hard failure)
  }

  public static void main(String[] args) throws Exception {
    Config cfg = parseArgs(args);
    // The primary schema is the first in --source; only it is probed/reported. Any other schemas
    // in the list are mounted purely to resolve the primary's cross-schema (inter-schema) views.
    if (cfg.source != null) {
      String[] sp = cfg.source.split(",");
      for (int i = 0; i < sp.length; i++) {
        String s = sp[i].trim().toLowerCase();
        if (!s.isEmpty()) {
          cfg.primary = s;
          break;
        }
      }
    }
    if (cfg.source == null && cfg.model == null) {
      printUsage();
      System.exit(2);
      return;
    }

    Properties props = new Properties();
    props.setProperty("lex", "ORACLE");
    props.setProperty("unquotedCasing", "TO_LOWER");
    // Resolve identifiers/functions case-insensitively so unquoted (TO_LOWER) calls
    // match the registered UDF names (COSINE_SIMILARITY, ST_*), which Calcite stores
    // upper-cased.
    props.setProperty("caseSensitive", "false");
    // Enable Calcite's spatial operator library so ST_* parse (execution still
    // pushes to DuckDB where applicable).
    props.setProperty("fun", "standard,spatial");

    // Connection: prefer the GovDataDriver (jdbc:govdata:source=...), which builds the
    // multi-schema autoDownload=false introspection model internally from the schema
    // YAMLs baked into the jar — no hand-written model file. dataDirectory resolves from
    // GOVDATA_PARQUET_DIR. --model remains supported for verifying an explicit model.
    String jdbcUrl;
    if (cfg.source != null) {
      Class.forName("org.apache.calcite.adapter.govdata.GovDataDriver");
      jdbcUrl = "jdbc:govdata:source=" + cfg.source;
    } else {
      Class.forName("org.apache.calcite.jdbc.Driver");
      props.setProperty("model", cfg.model);
      jdbcUrl = "jdbc:calcite:";
    }

    long t0 = System.currentTimeMillis();
    System.out.println("Connecting to " + jdbcUrl
        + (cfg.model != null ? " model=" + cfg.model : ""));

    Connection conn = null;
    try {
      conn = DriverManager.getConnection(jdbcUrl, props);
    } catch (Exception e) {
      System.err.println("FATAL: could not open Calcite connection: " + e.getMessage());
      e.printStackTrace();
      System.exit(2);
      return;
    }

    try {
      List<TableResult> results = probeTables(conn, cfg);
      int probeFailures = runProbes(conn, cfg);

      // ---- categorized report ----
      // Classify every table defined in the bundled schema YAML(s) as a base table, an
      // intra-schema view, or an inter-schema view (a view whose SQL references another schema),
      // then cross-reference with what the connection actually exposed. Inter-schema views are
      // dropped when their dependency schema is not mounted, so they surface here as MISSING
      // rather than being silently absent.
      // Classify/report only the primary schema (secondary schemas were mounted just to resolve
      // its cross-schema views). In --model mode (no primary), fall back to all exposed schemas.
      Set<String> verifiedSchemas = new LinkedHashSet<String>();
      if (cfg.primary != null) {
        verifiedSchemas.add(cfg.primary);
      } else {
        for (int i = 0; i < results.size(); i++) {
          verifiedSchemas.add(results.get(i).schema.toLowerCase());
        }
      }
      Map<String, Defined> defined = classifyDefined(verifiedSchemas);
      Map<String, TableResult> exposed = new LinkedHashMap<String, TableResult>();
      for (int i = 0; i < results.size(); i++) {
        TableResult r = results.get(i);
        exposed.put((r.schema + "." + r.table).toLowerCase(), r);
      }

      Counts cBase = new Counts();
      Counts cIntra = new Counts();
      Counts cInter = new Counts();
      System.out.println();
      System.out.println("================ TABLE INVENTORY (by category) ================");
      printCategory("BASE TABLES", BASE, defined, exposed, cBase);
      printCategory("INTRA-SCHEMA VIEWS", INTRA_VIEW, defined, exposed, cIntra);
      printCategory("INTER-SCHEMA VIEWS (depend on other schemas)", INTER_VIEW, defined, exposed, cInter);

      // Exposed tables not present in any schema YAML (e.g. metadata/convenience tables).
      List<TableResult> unclassified = new ArrayList<TableResult>();
      Counts cOther = new Counts();
      for (int i = 0; i < results.size(); i++) {
        TableResult r = results.get(i);
        if (!defined.containsKey((r.schema + "." + r.table).toLowerCase())) {
          unclassified.add(r);
        }
      }
      if (!unclassified.isEmpty()) {
        System.out.println();
        System.out.println("-- UNCLASSIFIED (exposed, not in schema YAML) (" + unclassified.size() + ") --");
        System.out.printf("  %-15s %-34s %-8s %10s%n", "SCHEMA", "TABLE", "STATUS", "ROWS");
        for (int i = 0; i < unclassified.size(); i++) {
          TableResult r = unclassified.get(i);
          String st = statusOf(r);
          cOther.tally(st);
          System.out.printf("  %-15s %-34s %-8s %10d%n", r.schema, trunc(r.table, 34), st, r.count);
        }
      }

      // Tables whose declared primaryKey is not unique (>= dupThreshold duplicate keyed rows).
      List<TableResult> dupped = new ArrayList<TableResult>();
      for (int i = 0; i < results.size(); i++) {
        TableResult r = results.get(i);
        if (r.dupRows >= cfg.dupThreshold && r.dupRows > 0) {
          dupped.add(r);
        }
      }
      if (!dupped.isEmpty()) {
        System.out.println();
        System.out.println("-- DUPLICATE PRIMARY KEYS (" + dupped.size()
            + ") — declared PK not unique --");
        System.out.printf("  %-15s %-34s %12s %12s %8s%n",
            "SCHEMA", "TABLE", "KEYED_ROWS", "DISTINCT", "FACTOR");
        for (int i = 0; i < dupped.size(); i++) {
          TableResult r = dupped.get(i);
          System.out.printf("  %-15s %-34s %12d %12d %8s%n",
              r.schema, trunc(r.table, 34), r.keyedRows, r.distinctKeys, dupRatio(r));
        }
      }

      System.out.println();
      System.out.println("================ SUMMARY ================");
      System.out.println("  base tables          : " + cBase.line());
      System.out.println("  intra-schema views   : " + cIntra.line());
      System.out.println("  inter-schema views   : " + cInter.line()
          + (cInter.missing > 0
              ? "   (" + cInter.missing + " not exposed — dependency schema not mounted)" : ""));
      if (!unclassified.isEmpty()) {
        System.out.println("  unclassified         : " + cOther.line());
      }
      System.out.println("  feature-probe failures: " + probeFailures);
      System.out.println("  duplicate-PK tables  : " + dupped.size()
          + (cfg.dupCheck ? "" : "   (dup-check disabled)"));
      System.out.println("  elapsed ms           : " + (System.currentTimeMillis() - t0));

      // Inter-schema views missing because their dependency was not mounted are expected, not a
      // failure. Only errors, genuinely-missing base tables / intra-schema views, probe failures,
      // and non-unique declared primary keys fail the run.
      int errs = cBase.error + cIntra.error + cInter.error + cOther.error;
      int missingHard = cBase.missing + cIntra.missing;
      int exit = (errs > 0 || missingHard > 0 || probeFailures > 0 || !dupped.isEmpty()) ? 1 : 0;
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
      // Probe/report only the primary schema; secondary schemas are mounted just to resolve the
      // primary's cross-schema views.
      if (cfg.primary != null && !slc.equals(cfg.primary)) {
        continue;
      }
      coords.add(new String[] {schema, table});
    }
    tabs.close();

    // Primary-key columns per base table (schema.table → [pk cols]) from the bundled YAML,
    // used by the duplicate-row check below. Loaded once for the schema(s) being probed.
    Set<String> pkSchemas = new LinkedHashSet<String>();
    if (cfg.primary != null) {
      pkSchemas.add(cfg.primary);
    } else {
      pkSchemas.addAll(KNOWN_SCHEMAS);
    }
    Map<String, List<String>> pkByTable =
        cfg.dupCheck ? loadPrimaryKeys(pkSchemas) : new LinkedHashMap<String, List<String>>();

    System.out.println("Enumerated " + coords.size() + " exposed tables. Probing (LIMIT "
        + cfg.limit + ")" + (cfg.dupCheck ? ", PK dup-check on" : "") + " ...");
    // Live per-table progress: print each result as it completes (not just batched in the
    // final report) so slow R2 sweeps show progress and a timeout still yields partial data.
    System.out.printf("%-15s %-32s %-6s %10s %-6s %-11s %s%n",
        "SCHEMA", "TABLE", "REACH", "ROWS", "MAT", "DUP", "DETAIL");
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

      // Phase 3: duplicate-key check. Only for base tables that declare a primaryKey and hold
      // rows. Compare fully-keyed rows (all PK cols non-null — parquet keys can be null and a
      // null key is not a real duplicate) against DISTINCT PK tuples; a positive delta means the
      // declared PK is not unique. A dup-check SQL failure (e.g. an un-DISTINCT-able column type)
      // is recorded as n/a and never fails the run — only genuine duplication does.
      List<String> pk = pkByTable.get((schema + "." + table).toLowerCase());
      if (cfg.dupCheck && "OK".equals(r.reach) && r.count > 0 && pk != null && !pk.isEmpty()) {
        checkDuplicateKeys(conn, r, pk);
      }

      String detail = r.error != null ? trunc(r.error, 80)
          : (r.dupRows > 0 ? "DUPLICATE PK: " + r.dupRows + " dup rows over " + r.keyedRows
              + " keyed (" + dupRatio(r) + ")"
              : (r.dupErr != null ? "dup-check n/a: " + trunc(r.dupErr, 60) : ""));
      System.out.printf("%-15s %-32s %-6s %10d %-6s %-11s %s%n",
          r.schema, trunc(r.table, 32), r.reach, r.count, r.mat, dupCell(r), detail);
      results.add(r);
    }
    return results;
  }

  /**
   * Runs the PK duplicate check for one table: counts rows whose PK columns are all non-null,
   * and the number of DISTINCT PK tuples among them. Populates keyedRows / distinctKeys / dupRows
   * on the result, or dupErr if the SQL fails.
   */
  private static void checkDuplicateKeys(Connection conn, TableResult r, List<String> pk) {
    StringBuilder cols = new StringBuilder();
    StringBuilder notNull = new StringBuilder();
    for (int j = 0; j < pk.size(); j++) {
      if (j > 0) {
        cols.append(", ");
        notNull.append(" AND ");
      }
      String qc = "\"" + pk.get(j) + "\"";
      cols.append(qc);
      notNull.append(qc).append(" IS NOT NULL");
    }
    String from = "\"" + r.schema + "\".\"" + r.table + "\" WHERE " + notNull;
    // Two plain aggregates (not a scalar subquery) so each pushes cleanly to DuckDB and can't hit
    // a Calcite scalar-subquery planning edge case across the full table sweep.
    try {
      long keyed = scalarCount(conn, "SELECT COUNT(*) FROM " + from);
      long distinct = scalarCount(conn,
          "SELECT COUNT(*) FROM (SELECT DISTINCT " + cols + " FROM " + from + ")");
      r.keyedRows = keyed;
      r.distinctKeys = distinct;
      r.dupRows = keyed - distinct;
      r.pkChecked = true;
    } catch (Exception e) {
      r.dupErr = e.getMessage();
      if (System.getenv("VERIFY_STACK") != null) {
        System.err.println("DUP STACK for " + r.schema + "." + r.table + ":");
        e.printStackTrace();
      }
    }
  }

  /** Runs a single-column COUNT query and returns the long value (0 if no row). */
  private static long scalarCount(Connection conn, String sql) throws Exception {
    Statement st = null;
    ResultSet rs = null;
    try {
      st = conn.createStatement();
      rs = st.executeQuery(sql);
      return rs.next() ? rs.getLong(1) : 0L;
    } finally {
      closeQuietly(rs);
      closeQuietly(st);
    }
  }

  /** Duplication factor keyedRows/distinctKeys as a "N.NNx" string (empty if not computable). */
  private static String dupRatio(TableResult r) {
    if (r.distinctKeys <= 0) {
      return "";
    }
    return String.format("%.2fx", (double) r.keyedRows / (double) r.distinctKeys);
  }

  /** Compact DUP cell for the live table: "-" not checked, "ok", "n/a" (check errored), "DUP N.NNx". */
  private static String dupCell(TableResult r) {
    if (!r.pkChecked) {
      return r.dupErr != null ? "n/a" : "-";
    }
    return r.dupRows > 0 ? "DUP " + dupRatio(r) : "ok";
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
      } else if ("--source".equals(a)) {
        cfg.source = args[++i];
      } else if ("--limit".equals(a)) {
        cfg.limit = Integer.parseInt(args[++i]);
      } else if ("--no-dup".equals(a)) {
        cfg.dupCheck = false;
      } else if ("--dup-threshold".equals(a)) {
        cfg.dupThreshold = Integer.parseInt(args[++i]);
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

  /**
   * Loads the bundled schema YAML for each verified schema and classifies every defined
   * partitionedTable / view as a base table, an intra-schema view, or an inter-schema view (a
   * view whose SQL references another schema). Keyed by "schema.table" (lower-case). Schemas
   * whose YAML cannot be read are skipped (their tables fall into UNCLASSIFIED).
   */
  private static Map<String, Defined> classifyDefined(Set<String> schemas) {
    Map<String, Defined> defined = new LinkedHashMap<String, Defined>();
    for (String schema : schemas) {
      String resource = SCHEMA_YAML.get(schema);
      if (resource == null) {
        continue;
      }
      InputStream in = GovDataModelVerificationRunner.class.getResourceAsStream(resource);
      if (in == null) {
        System.out.println("  (no bundled YAML for schema '" + schema + "'; skipping classification)");
        continue;
      }
      try {
        // Match the schema factories' loader limit — these YAMLs use many anchors/aliases and
        // exceed SnakeYAML's default cap of 50 (otherwise large schemas like geo fail to parse).
        org.yaml.snakeyaml.LoaderOptions loaderOptions = new org.yaml.snakeyaml.LoaderOptions();
        loaderOptions.setMaxAliasesForCollections(500);
        Object root = new org.yaml.snakeyaml.Yaml(loaderOptions).load(in);
        if (!(root instanceof Map)) {
          continue;
        }
        Map<?, ?> top = (Map<?, ?>) root;
        Object pts = top.get("partitionedTables");
        if (pts instanceof List) {
          List<?> list = (List<?>) pts;
          for (int i = 0; i < list.size(); i++) {
            String name = tableName(list.get(i));
            if (name != null) {
              Defined d = new Defined();
              d.schema = schema;
              d.table = name;
              d.category = BASE;
              defined.put((schema + "." + name).toLowerCase(), d);
            }
          }
        }
        Object views = top.get("views");
        if (views instanceof List) {
          List<?> list = (List<?>) views;
          for (int i = 0; i < list.size(); i++) {
            if (!(list.get(i) instanceof Map)) {
              continue;
            }
            Map<?, ?> v = (Map<?, ?>) list.get(i);
            Object n = v.get("name");
            if (n == null) {
              continue;
            }
            String name = String.valueOf(n);
            String sql = v.get("sql") != null ? String.valueOf(v.get("sql")) : "";
            Set<String> deps = viewDependencies(sql, schema);
            Defined d = new Defined();
            d.schema = schema;
            d.table = name;
            d.category = deps.isEmpty() ? INTRA_VIEW : INTER_VIEW;
            d.deps = deps;
            defined.put((schema + "." + name).toLowerCase(), d);
          }
        }
      } catch (Exception e) {
        System.out.println("  (failed to parse YAML for '" + schema + "': " + e.getMessage() + ")");
      } finally {
        try {
          in.close();
        } catch (Exception ignored) {
          // ignore
        }
      }
    }
    return defined;
  }

  private static String tableName(Object o) {
    if (o instanceof Map) {
      Object n = ((Map<?, ?>) o).get("name");
      return n != null ? String.valueOf(n) : null;
    }
    return null;
  }

  /**
   * Loads the declared primaryKey column list for every base table in the given schemas from the
   * bundled YAML's top-level {@code constraints:} map. Keyed by "schema.table" (lower-case). Tables
   * without a primaryKey (and views, which are not in constraints) are simply absent, so the
   * duplicate-row check skips them.
   */
  private static Map<String, List<String>> loadPrimaryKeys(Set<String> schemas) {
    Map<String, List<String>> pks = new LinkedHashMap<String, List<String>>();
    for (String schema : schemas) {
      String resource = SCHEMA_YAML.get(schema);
      if (resource == null) {
        continue;
      }
      InputStream in = GovDataModelVerificationRunner.class.getResourceAsStream(resource);
      if (in == null) {
        continue;
      }
      try {
        org.yaml.snakeyaml.LoaderOptions loaderOptions = new org.yaml.snakeyaml.LoaderOptions();
        loaderOptions.setMaxAliasesForCollections(500);
        Object root = new org.yaml.snakeyaml.Yaml(loaderOptions).load(in);
        if (!(root instanceof Map)) {
          continue;
        }
        Object cons = ((Map<?, ?>) root).get("constraints");
        if (!(cons instanceof Map)) {
          continue;
        }
        for (Map.Entry<?, ?> e : ((Map<?, ?>) cons).entrySet()) {
          if (e.getKey() == null || !(e.getValue() instanceof Map)) {
            continue;
          }
          Object pk = ((Map<?, ?>) e.getValue()).get("primaryKey");
          if (!(pk instanceof List)) {
            continue;
          }
          List<String> cols = new ArrayList<String>();
          for (Object c : (List<?>) pk) {
            if (c != null) {
              cols.add(String.valueOf(c));
            }
          }
          if (!cols.isEmpty()) {
            pks.put((schema + "." + e.getKey()).toLowerCase(), cols);
          }
        }
      } catch (Exception e) {
        System.out.println("  (failed to parse constraints for '" + schema + "': " + e.getMessage() + ")");
      } finally {
        try {
          in.close();
        } catch (Exception ignored) {
          // ignore
        }
      }
    }
    return pks;
  }

  /**
   * Other schemas referenced by a view's SQL via a qualified reference (e.g. {@code geo.counties}
   * or {@code "geo"."counties"}). Matches a known schema name on a word boundary immediately
   * followed by an optional quote and a dot, which avoids matching unrelated identifiers.
   */
  private static Set<String> viewDependencies(String sql, String selfSchema) {
    Set<String> deps = new LinkedHashSet<String>();
    if (sql == null || sql.isEmpty()) {
      return deps;
    }
    String lower = sql.toLowerCase();
    for (String s : KNOWN_SCHEMAS) {
      if (s.equals(selfSchema)) {
        continue;
      }
      java.util.regex.Pattern p = java.util.regex.Pattern.compile(
          "(?<![a-z0-9_])" + java.util.regex.Pattern.quote(s) + "\"?\\s*\\.");
      if (p.matcher(lower).find()) {
        deps.add(s);
      }
    }
    return deps;
  }

  /** DATA (rows>0) / EMPTY (reachable, 0 rows) / ERROR (unreadable) / MISSING (not exposed). */
  private static String statusOf(TableResult r) {
    if (r == null) {
      return "MISSING";
    }
    if (!"OK".equals(r.reach) || "ERROR".equals(r.mat)) {
      return "ERROR";
    }
    return r.count > 0 ? "DATA" : "EMPTY";
  }

  private static void printCategory(String title, String category,
      Map<String, Defined> defined, Map<String, TableResult> exposed, Counts counts) {
    List<Defined> items = new ArrayList<Defined>();
    for (Map.Entry<String, Defined> e : defined.entrySet()) {
      if (category.equals(e.getValue().category)) {
        items.add(e.getValue());
      }
    }
    java.util.Collections.sort(items, new java.util.Comparator<Defined>() {
      public int compare(Defined a, Defined b) {
        int c = a.schema.compareTo(b.schema);
        return c != 0 ? c : a.table.compareTo(b.table);
      }
    });
    System.out.println();
    System.out.println("-- " + title + " (" + items.size() + ") --");
    if (items.isEmpty()) {
      return;
    }
    System.out.printf("  %-15s %-34s %-8s %10s  %s%n", "SCHEMA", "TABLE", "STATUS", "ROWS", "DETAIL");
    for (int i = 0; i < items.size(); i++) {
      Defined d = items.get(i);
      TableResult r = exposed.get((d.schema + "." + d.table).toLowerCase());
      String status = statusOf(r);
      counts.tally(status);
      String detail = "";
      if (!d.deps.isEmpty()) {
        detail = "needs: " + String.join(",", d.deps);
      }
      if (r != null && r.error != null && "ERROR".equals(status)) {
        detail = trunc(r.error, 60);
      } else if (r != null && r.dupRows > 0) {
        detail = "DUPLICATE PK: " + r.dupRows + " dup (" + dupRatio(r) + ")";
      }
      long rows = r != null ? r.count : -1;
      System.out.printf("  %-15s %-34s %-8s %10d  %s%n",
          d.schema, trunc(d.table, 34), status, rows, detail);
    }
  }

  private static void printUsage() {
    System.err.println("Usage: GovDataModelVerificationRunner (--source a,b,c | --model <model.json>) "
        + "[--limit N] [--expected <file>] [--probes <file>] [--schemas a,b,c]");
    System.err.println("  --source    comma-separated dataSource list; connects via "
        + "jdbc:govdata:source=... (model built internally; dir from GOVDATA_PARQUET_DIR)");
    System.err.println("  --model     Calcite model JSON (alternative to --source)");
    System.err.println("  --limit     rows to fetch per table probe (default 1)");
    System.err.println("  --no-dup    skip the primary-key duplicate-row check");
    System.err.println("  --dup-threshold N  min duplicate keyed-row count to flag/fail (default 1)");
    System.err.println("  --expected  file of 'schema.table' lines; reports defined-but-not-exposed");
    System.err.println("  --probes    file of 'label|||SQL' lines for feature probes (geo, semantic)");
    System.err.println("  --schemas   comma-separated schema allow-list (default: all)");
  }
}
