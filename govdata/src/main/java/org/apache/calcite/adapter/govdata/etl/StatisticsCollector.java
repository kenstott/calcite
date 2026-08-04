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

import org.apache.calcite.adapter.file.statistics.PGColumnStatisticsStore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Profiles a schema's tables and publishes per-column cardinalities to
 * {@link PGColumnStatisticsStore}.
 *
 * <p>Runs through the ordinary {@code jdbc:govdata:source=<schema>} connection rather than
 * reaching for Iceberg paths directly. That matters for two reasons: the aggregate pushes down
 * to DuckDB unchanged (the plan resolves to a single {@code JdbcAggregate} carrying
 * {@code COUNT(APPROXIMATE DISTINCT)}), and every table the model exposes is profilable by name
 * — no per-format path construction, and views are skipped for free because they are not base
 * tables.
 *
 * <p>All columns of a table are profiled in ONE query. DuckDB maintains every sketch
 * concurrently during a single scan, so the cost is a scan, not a scan per column: measured over
 * a 22.8M-row table, 10 columns in 0.63s; over a 4.5M-row table, 23 columns in 2.3s. Running
 * this after an ETL run — which already takes minutes to hours — is free in comparison, which is
 * why statistics are collected at write time instead of being estimated at query time.
 *
 * <p>Advisory throughout: a table that cannot be profiled is skipped with a warning and the
 * planner keeps its default estimates for it. Nothing here may fail an ETL run.
 */
public final class StatisticsCollector {

  private static final Logger LOGGER = LoggerFactory.getLogger(StatisticsCollector.class);

  private StatisticsCollector() {
  }

  /**
   * Profiles every base table in {@code schema} and publishes the results.
   *
   * @return number of tables successfully profiled
   */
  public static int collect(String schema, PGColumnStatisticsStore store) {
    if (store == null) {
      return 0;
    }
    int profiled = 0;
    Properties props = new Properties();
    props.setProperty("lex", "ORACLE");
    props.setProperty("unquotedCasing", "TO_LOWER");
    try {
      // The driver is not on the JDBC auto-registration path when this runs inside the ETL JVM,
      // so load it explicitly — same as GovDataModelVerificationRunner does.
      Class.forName("org.apache.calcite.adapter.govdata.GovDataDriver");
    } catch (ClassNotFoundException e) {
      LOGGER.warn("GovDataDriver not on the classpath — statistics not collected");
      return 0;
    }
    try (Connection conn =
             DriverManager.getConnection("jdbc:govdata:source=" + schema, props)) {
      Map<String, List<String>> tables = enumerateBaseTables(conn, schema);
      LOGGER.info("Collecting statistics for {} base tables in schema '{}'", tables.size(), schema);
      for (Map.Entry<String, List<String>> e : tables.entrySet()) {
        String table = e.getKey();
        List<String> cols = e.getValue();
        if (cols.isEmpty()) {
          continue;
        }
        try {
          String from = "\"" + schema + "\".\"" + table + "\"";
          long rows = rowCount(conn, from);
          if (rows <= 0) {
            // An empty table has nothing to describe; publishing zeros would tell the planner
            // the table is empty long after it stops being so.
            continue;
          }
          Map<String, Long> ndv = PGColumnStatisticsStore.computeNdv(conn, from, cols);
          if (ndv.isEmpty()) {
            continue;
          }
          store.put(schema, table, rows, ndv);
          profiled++;
        } catch (Exception perTable) {
          LOGGER.warn("Could not profile {}.{}: {}", schema, table, perTable.getMessage());
        }
      }
    } catch (Exception e) {
      LOGGER.warn("Statistics collection for schema '{}' did not run: {}", schema, e.getMessage());
    }
    LOGGER.info("Statistics collection complete for '{}': {} tables profiled", schema, profiled);
    return profiled;
  }

  /** Base tables only — views carry no independent cardinality worth storing. */
  private static Map<String, List<String>> enumerateBaseTables(Connection conn, String schema)
      throws Exception {
    Map<String, List<String>> out = new LinkedHashMap<String, List<String>>();
    DatabaseMetaData md = conn.getMetaData();
    try (ResultSet rs = md.getTables(null, schema, "%", new String[]{"TABLE"})) {
      while (rs.next()) {
        out.put(rs.getString("TABLE_NAME"), new ArrayList<String>());
      }
    }
    for (Map.Entry<String, List<String>> e : out.entrySet()) {
      try (ResultSet rs = md.getColumns(null, schema, e.getKey(), "%")) {
        while (rs.next()) {
          e.getValue().add(rs.getString("COLUMN_NAME"));
        }
      }
    }
    return out;
  }

  private static long rowCount(Connection conn, String from) throws Exception {
    try (Statement st = conn.createStatement();
         ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM " + from)) {
      return rs.next() ? rs.getLong(1) : 0L;
    }
  }
}
