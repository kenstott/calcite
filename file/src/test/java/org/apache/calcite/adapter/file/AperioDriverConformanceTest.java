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

import org.apache.calcite.jdbc.CalciteConnection;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * FILE-190 — AperioDriver defaults {@code conformance.raggedUnionTypesToVarying=true} on the
 * delegated Calcite connection: a ragged VALUES row constructor widens to VARCHAR instead of
 * typing as CHAR(n) and blank-padding shorter literals, so an equi-join against a VARCHAR column
 * no longer silently drops every literal narrower than the widest. An explicit property or URL
 * parameter overrides the default.
 */
@Tag("integration")
@Tag("FILE-190")
public class AperioDriverConformanceTest {

  @TempDir File csvDir;
  @TempDir File storageDir;

  private String url(String extra) throws Exception {
    Files.write(new File(csvDir, "states.csv").toPath(),
        "state_name,pop\nCalifornia,39\nTexas,30\nNew York,19\nWashington,8\n"
            .getBytes(StandardCharsets.UTF_8));
    String src = csvDir.getAbsolutePath().replace('\\', '/');
    String storage = storageDir.getAbsolutePath().replace('\\', '/');
    return "jdbc:aperio:" + src + "?rw=on&schema=t&storage=" + storage + extra;
  }

  private static Map<String, Long> joinCounts(Connection conn) throws Exception {
    Map<String, Long> out = new LinkedHashMap<String, Long>();
    try (Statement st = conn.createStatement();
         ResultSet rs = st.executeQuery(
             "SELECT v.\"state_name\", COUNT(*) AS n "
                 + "FROM (VALUES ('California'), ('Texas'), ('New York'), ('Washington')) "
                 + "AS v(\"state_name\") "
                 + "JOIN \"t\".\"states\" t ON t.\"state_name\" = v.\"state_name\" "
                 + "GROUP BY v.\"state_name\" ORDER BY v.\"state_name\"")) {
      while (rs.next()) {
        out.put(rs.getString(1), rs.getLong(2));
      }
    }
    return out;
  }

  @Test void raggedValuesJoinMatchesEveryLiteralByDefault() throws Exception {
    try (Connection conn = DriverManager.getConnection(url(""))) {
      Map<String, Long> counts = joinCounts(conn);
      assertEquals(4, counts.size(),
          "all four literals match: no CHAR(n) blank-padding on the VALUES column");
      for (Map.Entry<String, Long> e : counts.entrySet()) {
        assertEquals(1L, e.getValue(), e.getKey());
      }
    }
  }

  @Test void conformanceDefaultsToRaggedUnionToVarying() throws Exception {
    try (Connection conn = DriverManager.getConnection(url(""))) {
      assertTrue(conn.unwrap(CalciteConnection.class).config().conformance()
              .shouldConvertRaggedUnionTypesToVarying(),
          "driver default layers raggedUnionTypesToVarying onto the base conformance");
    }
  }

  @Test void explicitOverrideKeepsCharPaddingSemantics() throws Exception {
    try (Connection conn = DriverManager.getConnection(
        url("&conformance.raggedUnionTypesToVarying=false"))) {
      assertFalse(conn.unwrap(CalciteConnection.class).config().conformance()
              .shouldConvertRaggedUnionTypesToVarying(),
          "an explicit false overrides the driver default");
      Map<String, Long> counts = joinCounts(conn);
      assertEquals(2, counts.size(),
          "CHAR(10) blank-padding is back: only the two 10-character literals "
              + "(tied for widest) survive the join");
      assertEquals(1L, counts.get("California"));
      assertEquals(1L, counts.get("Washington"));
    }
  }
}
