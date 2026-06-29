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
package org.apache.calcite.adapter.file;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.Properties;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * FILE-025 / FILE-026 — exact directory-walk and schema-assembly goldens for the file adapter
 * (recode of the weaker contains()-based discovery tests).
 *
 * <p>These drive the real discovery path end-to-end: a {@link FileSchemaFactory}-backed schema is
 * built over a temp directory tree via a model, and the resulting catalog is inspected through the
 * JDBC {@code DatabaseMetaData} (mirrors {@code WalkingRediscoveryTest}). Nothing here calls a
 * private walker.
 *
 * <p>FILE-025 pins the discovered file-SET under recursion, a glob filter, and supported-type
 * include/exclude, format-agnostically across {@code .csv} and {@code .json}.
 *
 * <p>FILE-026 pins schema assembly: table names are derived from the relative path (separator
 * collapsed to {@code __} then sanitized), and a name collision across formats resolves to a
 * distinct, stable name.
 *
 * <p>NOTE on hive-style partitions: the default directory-scan path treats each
 * {@code year=2024/month=01/data.csv} leaf as its own table whose NAME embeds the
 * {@code year=}/{@code month=} path segments; it does NOT synthesize {@code year}/{@code month}
 * partition COLUMNS. Surfacing partition values as columns requires an explicit
 * {@code partitionedTables} config (PartitionedParquetTable), which is out of scope for the
 * default walk. FILE-026 therefore asserts the path-derived naming that the default path actually
 * produces, and only NOTEs the column behavior.
 */
@Tag("unit")
public class WalkingDiscoveryRequirementsTest {

  // ----------------------------------------------------------------- helpers ------------------

  private static void write(Path file, String content) throws Exception {
    Files.createDirectories(file.getParent());
    Files.write(file, content.getBytes(StandardCharsets.UTF_8));
  }

  private static void csv(Path file) throws Exception {
    write(file, "id,v\n1,x\n2,y\n");
  }

  private static void json(Path file) throws Exception {
    write(file, "[{\"id\":1,\"v\":\"x\"}]\n");
  }

  private static TreeSet<String> set(String... names) {
    TreeSet<String> s = new TreeSet<>();
    for (String n : names) {
      s.add(n);
    }
    return s;
  }

  /** Opens a fresh schema over {@code dir} and returns the file-derived table names (lowercased). */
  private static TreeSet<String> tables(Path dir, boolean recursive, String glob) throws Exception {
    Properties info = new Properties();
    info.put("model", "inline:" + model(dir, recursive, glob));
    info.put("lex", "ORACLE");
    info.put("unquotedCasing", "TO_LOWER");
    TreeSet<String> names = new TreeSet<>();
    try (Connection conn = DriverManager.getConnection("jdbc:calcite:", info);
         ResultSet rs = conn.getMetaData().getTables(null, null, "%", new String[] {"TABLE"})) {
      while (rs.next()) {
        String schema = rs.getString("TABLE_SCHEM");
        if (schema != null && schema.equalsIgnoreCase("s")) {
          names.add(rs.getString("TABLE_NAME").toLowerCase());
        }
      }
    }
    return names;
  }

  /** Returns the lowercased column names of one table in the freshly built schema. */
  private static TreeSet<String> columns(Path dir, String tableName) throws Exception {
    Properties info = new Properties();
    info.put("model", "inline:" + model(dir, true, null));
    info.put("lex", "ORACLE");
    info.put("unquotedCasing", "TO_LOWER");
    TreeSet<String> cols = new TreeSet<>();
    try (Connection conn = DriverManager.getConnection("jdbc:calcite:", info);
         ResultSet rs = conn.getMetaData().getColumns(null, null, tableName, "%")) {
      while (rs.next()) {
        String schema = rs.getString("TABLE_SCHEM");
        if (schema != null && schema.equalsIgnoreCase("s")) {
          cols.add(rs.getString("COLUMN_NAME").toLowerCase());
        }
      }
    }
    return cols;
  }

  private static String model(Path dir, boolean recursive, String glob) {
    StringBuilder operand = new StringBuilder();
    operand.append("      \"directory\": \"").append(dir.toString().replace("\\", "\\\\")).append("\",\n");
    operand.append("      \"ephemeralCache\": true,\n");
    operand.append("      \"executionEngine\": \"linq4j\",\n");
    // Pin casing to LOWER so the path-derived names are a simple, fully-predictable transform
    // (sanitize + toLowerCase). The default SMART_CASING re-segments multi-token names on word
    // boundaries, which is out of scope for these discovery/assembly goldens.
    operand.append("      \"tableNameCasing\": \"LOWER\",\n");
    operand.append("      \"columnNameCasing\": \"LOWER\",\n");
    operand.append("      \"recursive\": ").append(recursive);
    if (glob != null) {
      operand.append(",\n      \"glob\": \"").append(glob).append("\"");
    }
    operand.append("\n");
    return "{\n"
        + "  \"version\": \"1.0\",\n"
        + "  \"defaultSchema\": \"s\",\n"
        + "  \"schemas\": [{\n"
        + "    \"name\": \"s\",\n"
        + "    \"type\": \"custom\",\n"
        + "    \"factory\": \"org.apache.calcite.adapter.file.FileSchemaFactory\",\n"
        + "    \"operand\": {\n"
        + operand
        + "    }\n"
        + "  }]\n"
        + "}\n";
  }

  // ----------------------------------------------------------------- FILE-025 -----------------

  /**
   * (a) Recursion into subdirectories discovers the exact file-SET at every depth, mixing .csv and
   * .json; (b) a glob filter narrows it to exactly the matching subset; (c) non-recursive walk
   * excludes nested files while supported-type filtering excludes a .txt that is not a table source.
   */
  @Test @Tag("FILE-025") void walkDiscoversExactFileSet(@TempDir Path dir) throws Exception {
    // depth 0: a.csv, b.json   depth 1: sub/c.csv   depth 2: sub/deep/d.json
    // plus a non-table file that must never appear as a table.
    csv(dir.resolve("a.csv"));
    json(dir.resolve("b.json"));
    csv(dir.resolve("sub/c.csv"));
    json(dir.resolve("sub/deep/d.json"));
    write(dir.resolve("notes.txt"), "not a table source\n");

    // (a) recursion: every depth is discovered, format-agnostic. Nested names embed the path.
    assertEquals(set("a", "b", "sub__c", "sub__deep__d"), tables(dir, true, null),
        "recursive walk discovers the exact file-set across .csv and .json at all depths");

    // (c) non-recursive: only the top-level table sources; the .txt is excluded by type filtering.
    assertEquals(set("a", "b"), tables(dir, false, null),
        "non-recursive walk sees only depth-0 table sources (and never the .txt)");

    // (b) glob filter: restrict to .csv anywhere under the tree -> exactly the csv subset.
    assertEquals(set("a", "sub__c"), tables(dir, true, "**/*.csv"),
        "glob '**/*.csv' discovers exactly the csv files at any depth");
  }

  // ----------------------------------------------------------------- FILE-026 -----------------

  /**
   * Table names are derived from the relative path: a hive-style {@code sales/year=2024/month=01/
   * data.csv} leaf becomes a single table whose name embeds the path segments (separators -> '__',
   * '=' sanitized to '_'), and is queryable. The year/month path values are NOT surfaced as
   * columns by the default scan (see class NOTE).
   */
  @Test @Tag("FILE-026") void hiveStylePathDerivesNameNotPartitionColumns(@TempDir Path dir)
      throws Exception {
    csv(dir.resolve("sales/year=2024/month=01/data.csv"));

    // Path -> name: sales/year=2024/month=01/data
    //   separator -> "__"     : sales__year=2024__month=01__data
    //   sanitize '=' -> '_', collapse 3+ '_' -> "__"
    //   = sales__year_2024__month_01__data
    String expected = "sales__year_2024__month_01__data";
    assertEquals(set(expected), tables(dir, true, null),
        "hive-style path collapses into one path-derived, sanitized table name");

    // The columns are the CSV's own columns; year/month are part of the NAME, not synthesized cols.
    TreeSet<String> cols = columns(dir, expected);
    assertEquals(set("id", "v"), cols,
        "default scan exposes only the file's columns, not hive partition columns");
    assertFalse(cols.contains("year"), "default scan does NOT synthesize a 'year' partition column");
    assertFalse(cols.contains("month"), "default scan does NOT synthesize a 'month' partition column");

    // The path-derived table is actually queryable through the assembled schema.
    Properties info = new Properties();
    info.put("model", "inline:" + model(dir, true, null));
    info.put("lex", "ORACLE");
    info.put("unquotedCasing", "TO_LOWER");
    try (Connection conn = DriverManager.getConnection("jdbc:calcite:", info);
         ResultSet rs = conn.createStatement().executeQuery(
             "select count(*) as c from s." + expected)) {
      assertTrue(rs.next(), "count query returns a row");
      assertEquals(2L, rs.getLong("c"), "the path-derived table reads the file's two rows");
    }
  }

  /**
   * A name collision across two formats that would both map to base name {@code report} resolves
   * deterministically: the .csv registers as {@code report} and the .json is disambiguated to a
   * distinct, stable {@code report_json} (the CSV wins the bare base name; the format suffix
   * disambiguates the loser — fixed regardless of filesystem enumeration order).
   */
  @Test @Tag("FILE-026") void nameCollisionResolvesDeterministically(@TempDir Path dir)
      throws Exception {
    json(dir.resolve("report.json"));
    csv(dir.resolve("report.csv"));

    TreeSet<String> names = tables(dir, false, null);
    assertEquals(set("report", "report_json"), names,
        "colliding base name 'report' yields the csv as 'report' and the json disambiguated to 'report_json'");

    // Stability: rebuilding the same tree produces the identical, distinct mapping.
    assertEquals(names, tables(dir, false, null),
        "collision resolution is stable across rebuilds");
  }
}
