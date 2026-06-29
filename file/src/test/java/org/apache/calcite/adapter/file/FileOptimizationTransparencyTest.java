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
import org.junit.jupiter.api.parallel.Isolated;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * FILE-038 — transparency: every EXACT optimization preserves results. The same query run with the
 * statistics rules (filter pushdown / column pruning / join reorder) enabled must return a table
 * identical to the same query with them disabled. (HLL is excluded — it is an approximate
 * optimization, covered separately within its error bound.)
 *
 * <p>Toggles JVM-global gate properties, so the test is {@code @Isolated}.
 */
@Tag("unit")
@Isolated
public class FileOptimizationTransparencyTest {

  private static final String[] GATES = {
      "calcite.file.statistics.filter.enabled",
      "calcite.file.statistics.column.pruning.enabled",
      "calcite.file.statistics.join.reorder.enabled",
  };

  private static final String[] QUERIES = {
      "SELECT * FROM s.data ORDER BY id",
      "SELECT name FROM s.data WHERE id > 2 ORDER BY id",
      "SELECT COUNT(*) FROM s.data",
      "SELECT * FROM s.data WHERE id > 100000 ORDER BY id",  // always-false (min/max prunable)
      "SELECT * FROM s.data WHERE id >= 0 ORDER BY id",       // always-true
  };

  @Test @Tag("FILE-038") void exactOptimizationsPreserveResults(@TempDir Path root) throws Exception {
    Path src = Files.createDirectories(root.resolve("src"));
    StringBuilder csv = new StringBuilder("id,name,score\n");
    for (int i = 1; i <= 8; i++) {
      csv.append(i).append(",name").append(i).append(',').append(i * 1.5).append('\n');
    }
    Files.write(src.resolve("data.csv"), csv.toString().getBytes(StandardCharsets.UTF_8));
    Path cache = Files.createDirectories(root.resolve("cache"));

    for (String sql : QUERIES) {
      setGates(false);
      String off = render(query(src, cache, sql));
      setGates(true);
      try {
        String on = render(query(src, cache, sql));
        assertEquals(off, on, "an exact optimization changed results for: " + sql);
      } finally {
        setGates(false);
      }
    }
  }

  private static void setGates(boolean enabled) {
    for (String g : GATES) {
      System.setProperty(g, String.valueOf(enabled));
    }
  }

  private static List<String[]> query(Path src, Path cache, String sql) throws Exception {
    Properties info = new Properties();
    info.put("model", "inline:" + model(src, cache));
    info.put("lex", "ORACLE");
    info.put("unquotedCasing", "TO_LOWER");
    List<String[]> rows = new ArrayList<>();
    try (Connection conn = DriverManager.getConnection("jdbc:calcite:", info);
         ResultSet rs = conn.createStatement().executeQuery(sql)) {
      ResultSetMetaData md = rs.getMetaData();
      int cols = md.getColumnCount();
      while (rs.next()) {
        String[] row = new String[cols];
        for (int i = 1; i <= cols; i++) {
          Object v = rs.getObject(i);
          row[i - 1] = rs.wasNull() || v == null ? "␀" : v.toString();
        }
        rows.add(row);
      }
    }
    return rows;
  }

  private static String render(List<String[]> rows) {
    StringBuilder sb = new StringBuilder();
    for (String[] row : rows) {
      sb.append(String.join(" | ", row)).append('\n');
    }
    return sb.toString();
  }

  private static String model(Path src, Path cache) {
    return "{\n"
        + "  \"version\": \"1.0\",\n"
        + "  \"defaultSchema\": \"s\",\n"
        + "  \"schemas\": [{\n"
        + "    \"name\": \"s\",\n"
        + "    \"type\": \"custom\",\n"
        + "    \"factory\": \"org.apache.calcite.adapter.file.FileSchemaFactory\",\n"
        + "    \"operand\": {\n"
        + "      \"directory\": \"" + src.toString().replace("\\", "\\\\") + "\",\n"
        + "      \"baseDirectory\": \"" + cache.toString().replace("\\", "\\\\") + "\",\n"
        + "      \"ephemeralCache\": false,\n"
        + "      \"enableStatistics\": true,\n"
        + "      \"executionEngine\": \"parquet\"\n"
        + "    }\n"
        + "  }]\n"
        + "}\n";
  }
}
