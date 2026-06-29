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
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * FILE-009 / FILE-012 — engine invariance: the same source, through any execution engine, must
 * produce the identical table. Each engine ingests into its own cache; the captured value matrices
 * must match the linq4j baseline. A temporal-free fixture is used so the documented DuckDB
 * TIME/TIMESTAMP-as-JAVA_OBJECT divergence (an accepted exception) does not enter the comparison.
 */
@Tag("unit")
public class EngineInvarianceTest {

  private static final String[] ENGINES = {"linq4j", "parquet", "arrow", "duckdb"};

  @Test @Tag("FILE-009") void csvGoldenIsEngineInvariant(@TempDir Path root) throws Exception {
    Path src = Files.createDirectories(root.resolve("src"));
    Files.write(src.resolve("inv.csv"),
        ("id,name,score,active\n1,alice,9.5,true\n2,bob,8.0,false\n3,carol,7.25,true\n")
            .getBytes(StandardCharsets.UTF_8));
    assertInvariant(root, src, "SELECT * FROM s.inv ORDER BY id", true);
  }

  @Test @Tag("FILE-012") void jsonGoldenIsEngineInvariant(@TempDir Path root) throws Exception {
    Path src = Files.createDirectories(root.resolve("src"));
    Files.write(src.resolve("jinv.json"),
        ("[{\"id\":1,\"name\":\"alice\",\"score\":9.5},"
            + "{\"id\":2,\"name\":\"bob\",\"score\":8.0},"
            + "{\"id\":3,\"name\":\"carol\",\"score\":7.25}]")
            .getBytes(StandardCharsets.UTF_8));
    assertInvariant(root, src, "SELECT * FROM s.jinv ORDER BY id", false);
  }

  private static void assertInvariant(Path root, Path src, String sql, boolean csvInfer) throws Exception {
    List<String[]> baseline = null;
    String baselineEngine = null;
    for (String engine : ENGINES) {
      Path cache = Files.createDirectories(root.resolve("cache_" + engine));
      List<String[]> m = query(src, cache, engine, sql, csvInfer);
      if (baseline == null) {
        baseline = m;
        baselineEngine = engine;
      } else {
        assertEquals(render(baseline), render(m),
            engine + " must produce the same table as " + baselineEngine);
      }
    }
  }

  private static List<String[]> query(Path src, Path cache, String engine, String sql, boolean csvInfer)
      throws Exception {
    Properties info = new Properties();
    info.put("model", "inline:" + model(src, cache, engine, csvInfer));
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

  private static String model(Path src, Path cache, String engine, boolean csvInfer) {
    String infer = csvInfer
        ? ",\n      \"csvTypeInference\": { \"enabled\": true, \"samplingRate\": 1.0, "
            + "\"makeAllNullable\": true, \"inferDates\": true, \"inferTimes\": true, "
            + "\"inferTimestamps\": true }"
        : "";
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
        + "      \"executionEngine\": \"" + engine + "\"" + infer + "\n"
        + "    }\n"
        + "  }]\n"
        + "}\n";
  }

  private static String render(List<String[]> rows) {
    StringBuilder sb = new StringBuilder();
    for (String[] row : rows) {
      sb.append(String.join(" | ", row)).append('\n');
    }
    return sb.toString();
  }
}
