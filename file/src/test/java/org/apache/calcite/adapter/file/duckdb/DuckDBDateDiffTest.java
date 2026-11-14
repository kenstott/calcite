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
package org.apache.calcite.adapter.file.duckdb;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Day-difference between two date columns, un-aggregated and aggregated. */
@Tag("integration")
public class DuckDBDateDiffTest {

  @TempDir File tempDir;

  private Connection connect() throws Exception {
    try (PrintWriter w = new PrintWriter(new FileWriter(new File(tempDir, "decl.csv")))) {
      w.print("id,grp,begin_date,decl_date\n"
          + "1,a,2020-01-01,2020-01-11\n"
          + "2,a,2020-02-01,2020-02-21\n"
          + "3,b,2020-03-01,2020-03-04\n");
    }
    Map<String, Object> operand = new LinkedHashMap<>();
    operand.put("directory", tempDir.getAbsolutePath());
    operand.put("baseDirectory", tempDir.getAbsolutePath());
    operand.put("executionEngine", "duckdb");
    operand.put("ephemeralCache", true);
    Properties info = new Properties();
    info.put("lex", "ORACLE");
    info.put("unquotedCasing", "TO_LOWER");
    Connection conn = DriverManager.getConnection("jdbc:calcite:", info);
    org.apache.calcite.jdbc.CalciteConnection cc =
        conn.unwrap(org.apache.calcite.jdbc.CalciteConnection.class);
    cc.getRootSchema().add("files", org.apache.calcite.adapter.file.FileSchemaFactory.INSTANCE
        .create(cc.getRootSchema(), "files", operand));
    return DuckDBSqlRewriter.wrap(conn);
  }

  private String run(String sql) throws Exception {
    try (Connection c = connect(); Statement s = c.createStatement();
         ResultSet rs = s.executeQuery(sql)) {
      StringBuilder sb = new StringBuilder();
      while (rs.next()) {
        for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
          sb.append(rs.getString(i)).append('|');
        }
        sb.append(';');
      }
      return sb.toString();
    }
  }

  private static final String D = "CAST(decl_date AS DATE)";
  private static final String B = "CAST(begin_date AS DATE)";

  @Test public void timestampDiffUnaggregated() throws Exception {
    assertEquals("1|10|;2|20|;3|3|;",
        run("SELECT id, TIMESTAMPDIFF(DAY, " + B + ", " + D + ") FROM files.decl ORDER BY id"));
  }

  @Test public void timestampDiffAggregated() throws Exception {
    assertEquals("a|15|;b|3|;",
        run("SELECT grp, AVG(TIMESTAMPDIFF(DAY, " + B + ", " + D + ")) "
            + "FROM files.decl GROUP BY grp ORDER BY grp"));
  }

  @Test public void timestampDiffOverAggregatedSubquery() throws Exception {
    assertEquals("a|15|;b|3|;",
        run("SELECT grp, AVG(d) FROM (SELECT grp, id, TIMESTAMPDIFF(DAY, min(" + B
            + "), min(" + D + ")) d FROM files.decl GROUP BY grp, id) t "
            + "GROUP BY grp ORDER BY grp"));
  }

  @Test public void timestampDiffMonthAndSqlTsiUnit() throws Exception {
    assertEquals("0|;", run("SELECT TIMESTAMPDIFF(SQL_TSI_MONTH, " + B + ", " + D
        + ") FROM files.decl WHERE id = 1"));
  }

  @Test public void dateDiffAndDatediffFunctions() throws Exception {
    assertEquals("1|10|;2|20|;3|3|;",
        run("SELECT id, date_diff('day', " + B + ", " + D + ") FROM files.decl ORDER BY id"));
    assertEquals("1|10|;2|20|;3|3|;",
        run("SELECT id, datediff('day', " + B + ", " + D + ") FROM files.decl ORDER BY id"));
  }
}
