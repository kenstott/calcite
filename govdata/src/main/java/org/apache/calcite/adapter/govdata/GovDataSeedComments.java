/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.adapter.govdata;

// storage-provider-guard:ignore-file - audited: opens the local staged seed catalog file only.

import com.fasterxml.jackson.databind.JsonNode;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Writes the schema YAMLs' table and column descriptions into a generated seed catalog as DuckDB
 * comments, so the seed carries the metadata and a reader of the DuckDB catalog (for example
 * {@code duckdb_columns().comment}) sees it. Run by {@code build-seed.sh} on the staged catalog
 * before it is checkpointed and bundled.
 *
 * <p>DuckDB cannot comment a schema, so schema-level descriptions are not written. A relation or
 * column the YAML describes but the catalog does not hold is counted and reported, not skipped
 * silently; a statement DuckDB rejects fails the run.
 */
public final class GovDataSeedComments {

  private GovDataSeedComments() {
  }

  /** Counts of what {@link #apply} wrote and what it found in the YAML but not the catalog. */
  public static final class Result {
    public int relationsCommented;
    public int columnsCommented;
    public int relationsNotInCatalog;
    public int columnsNotInCatalog;

    @Override public String toString() {
      return relationsCommented + " relation comments, " + columnsCommented + " column comments; "
          + relationsNotInCatalog + " described relations and " + columnsNotInCatalog
          + " described columns are not in the catalog";
    }
  }

  /** Usage: {@code GovDataSeedComments <catalog.duckdb>}. */
  public static void main(String[] args) throws SQLException {
    if (args.length != 1) {
      System.err.println("usage: GovDataSeedComments <catalog.duckdb>");
      System.exit(2);
    }
    try (Connection conn = DriverManager.getConnection("jdbc:duckdb:" + args[0])) {
      Result r = apply(conn);
      System.out.println("seed comments: " + r);
    }
  }

  /** Comments every relation and column of {@code conn}'s catalog that a bundled schema YAML describes. */
  public static Result apply(Connection conn) throws SQLException {
    // schema -> relation -> kind ("VIEW" or "TABLE")
    Map<String, Map<String, String>> kinds = new LinkedHashMap<String, Map<String, String>>();
    // schema.relation -> lower-cased column -> actual column name
    Map<String, Map<String, String>> columns = new HashMap<String, Map<String, String>>();
    try (Statement st = conn.createStatement()) {
      collectRelations(st, "SELECT schema_name, view_name FROM duckdb_views() WHERE NOT internal",
          "VIEW", kinds);
      collectRelations(st, "SELECT schema_name, table_name FROM duckdb_tables()", "TABLE", kinds);
      try (ResultSet rs = st.executeQuery(
          "SELECT schema_name, table_name, column_name FROM duckdb_columns() WHERE NOT internal")) {
        while (rs.next()) {
          String key = rs.getString(1) + "." + rs.getString(2);
          Map<String, String> cols = columns.get(key);
          if (cols == null) {
            cols = new HashMap<String, String>();
            columns.put(key, cols);
          }
          cols.put(rs.getString(3).toLowerCase(Locale.ROOT), rs.getString(3));
        }
      }
    }

    Result result = new Result();
    for (JsonNode sc : GovDataCatalog.build(new ArrayList<String>(kinds.keySet()))) {
      String schema = sc.get("schema").asText();
      Map<String, String> relations = kinds.get(schema);
      for (JsonNode tb : sc.path("tables")) {
        String table = tb.get("name").asText();
        String actual = find(relations, table);
        if (actual == null) {
          result.relationsNotInCatalog++;
          continue;
        }
        String kind = relations.get(actual);
        if (tb.hasNonNull("comment") && !tb.get("comment").asText().isEmpty()) {
          exec(conn, "COMMENT ON " + kind + " " + ident(schema) + "." + ident(actual) + " IS "
              + literal(tb.get("comment").asText()));
          result.relationsCommented++;
        }
        Map<String, String> cols = columns.get(schema + "." + actual);
        for (JsonNode c : tb.path("columns")) {
          String col = c.get("name").asText();
          String actualCol = cols == null ? null : cols.get(col.toLowerCase(Locale.ROOT));
          if (actualCol == null) {
            result.columnsNotInCatalog++;
            continue;
          }
          if (c.hasNonNull("comment") && !c.get("comment").asText().isEmpty()) {
            exec(conn, "COMMENT ON COLUMN " + ident(schema) + "." + ident(actual) + "."
                + ident(actualCol) + " IS " + literal(c.get("comment").asText()));
            result.columnsCommented++;
          }
        }
      }
    }
    return result;
  }

  private static void collectRelations(Statement st, String sql, String kind,
      Map<String, Map<String, String>> kinds) throws SQLException {
    try (ResultSet rs = st.executeQuery(sql)) {
      while (rs.next()) {
        Map<String, String> m = kinds.get(rs.getString(1));
        if (m == null) {
          m = new LinkedHashMap<String, String>();
          kinds.put(rs.getString(1), m);
        }
        m.put(rs.getString(2), kind);
      }
    }
  }

  private static String find(Map<String, String> relations, String name) {
    if (relations == null) {
      return null;
    }
    if (relations.containsKey(name)) {
      return name;
    }
    for (String r : relations.keySet()) {
      if (r.equalsIgnoreCase(name)) {
        return r;
      }
    }
    return null;
  }

  private static void exec(Connection conn, String sql) throws SQLException {
    try (Statement st = conn.createStatement()) {
      st.execute(sql);
    } catch (SQLException e) {
      throw new SQLException("seed comment failed: " + sql.substring(0, Math.min(120, sql.length()))
          + " -> " + e.getMessage(), e);
    }
  }

  private static String ident(String name) {
    return "\"" + name.replace("\"", "\"\"") + "\"";
  }

  private static String literal(String text) {
    return "'" + text.replace("'", "''") + "'";
  }
}
