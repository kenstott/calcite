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
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * FILE-173 — ORACLE lexical + case-sensitivity, exact-assertion golden (recode of the weak
 * {@code OracleLexicalSettingsTest#testCaseSensitivityDemo}, which used {@code getInt(1)>0} and an
 * OR-tolerant not-found match). With {@code lex=ORACLE} + {@code unquotedCasing=TO_LOWER}: unquoted
 * identifiers fold to lower and resolve a lowercase-registered table; with UPPER table casing the
 * correctly-cased quoted name resolves while a wrong-case quoted name fails not-found;
 * {@code information_schema} access is case-insensitive. Hermetic: a committed-in-test CSV under a
 * {@code @TempDir}, {@code ephemeralCache:true}.
 */
@Tag("unit")
public class OracleLexicalRequirementsTest {

  /** DEPTS.csv — typed headers; with columnNameCasing=LOWER the columns are deptno/name. */
  private static void writeDepts(Path dir) throws Exception {
    Files.write(dir.resolve("DEPTS.csv"),
        ("DEPTNO:int,NAME:string\n10,Sales\n20,Marketing\n30,Accounts\n")
            .getBytes(StandardCharsets.UTF_8));
  }

  private static String model(Path dir, String schemaName, String tableCasing, String columnCasing) {
    return "{\n"
        + "  \"version\": \"1.0\",\n"
        + "  \"defaultSchema\": \"" + schemaName + "\",\n"
        + "  \"schemas\": [{\n"
        + "    \"name\": \"" + schemaName + "\",\n"
        + "    \"type\": \"custom\",\n"
        + "    \"factory\": \"org.apache.calcite.adapter.file.FileSchemaFactory\",\n"
        + "    \"operand\": {\n"
        + "      \"directory\": \"" + dir.toString().replace("\\", "\\\\") + "\",\n"
        + "      \"ephemeralCache\": true,\n"
        + "      \"executionEngine\": \"linq4j\",\n"
        + "      \"tableNameCasing\": \"" + tableCasing + "\",\n"
        + "      \"columnNameCasing\": \"" + columnCasing + "\"\n"
        + "    }\n"
        + "  }]\n"
        + "}\n";
  }

  private static Connection connect(String model) throws SQLException {
    Properties info = new Properties();
    info.put("model", "inline:" + model);
    info.put("lex", "ORACLE");
    info.put("unquotedCasing", "TO_LOWER");
    return DriverManager.getConnection("jdbc:calcite:", info);
  }

  private static long count(Connection c, String sql) throws SQLException {
    try (Statement st = c.createStatement(); ResultSet rs = st.executeQuery(sql)) {
      assertTrue(rs.next(), "count query returns a row: " + sql);
      return rs.getLong(1);
    }
  }

  @Test @Tag("FILE-173") void lowercaseIdentifiersResolveWithExactRows(@TempDir Path dir)
      throws Exception {
    writeDepts(dir);
    // LOWER casing -> schema "sales", table "depts", columns "deptno"/"name".
    try (Connection c = connect(model(dir, "sales", "LOWER", "LOWER"))) {
      // Unquoted folds to lower and resolves; quoted lowercase resolves too.
      assertEquals(3L, count(c, "select count(*) from sales.depts"), "unquoted resolves");
      assertEquals(3L, count(c, "select count(*) from \"sales\".\"depts\""), "quoted lowercase resolves");

      // Exact, ordered, full-value rows.
      try (Statement st = c.createStatement();
           ResultSet rs = st.executeQuery(
               "select \"deptno\", \"name\" from \"sales\".\"depts\" order by \"deptno\"")) {
        assertTrue(rs.next());
        assertEquals(10, rs.getInt("deptno"));
        assertEquals("Sales", rs.getString("name"));
        assertTrue(rs.next());
        assertEquals(20, rs.getInt("deptno"));
        assertEquals("Marketing", rs.getString("name"));
        assertTrue(rs.next());
        assertEquals(30, rs.getInt("deptno"));
        assertEquals("Accounts", rs.getString("name"));
        assertFalse(rs.next(), "exactly three rows");
      }
    }
  }

  @Test @Tag("FILE-173") void correctCaseQuotedResolvesAndWrongCaseFailsNotFound(@TempDir Path dir)
      throws Exception {
    writeDepts(dir);
    // UPPER casing -> schema "SALES", table "DEPTS".
    try (Connection c = connect(model(dir, "SALES", "UPPER", "UPPER"))) {
      // Correctly-cased quoted name resolves.
      assertEquals(3L, count(c, "select count(*) from \"SALES\".\"DEPTS\""), "correct-case quoted resolves");

      // A wrong-case quoted table name fails not-found (ORACLE lex is case-sensitive for quoted ids).
      SQLException ex = assertThrows(SQLException.class,
          () -> count(c, "select count(*) from \"SALES\".\"depts\""));
      assertTrue(ex.getMessage().contains("not found"),
          "wrong-case quoted name must fail not-found (got: " + ex.getMessage() + ")");
    }
  }

  @Test @Tag("FILE-173") void informationSchemaAccessIsCaseInsensitive(@TempDir Path dir)
      throws Exception {
    writeDepts(dir);
    try (Connection c = connect(model(dir, "sales", "LOWER", "LOWER"))) {
      // The information_schema TABLES view resolves identically regardless of identifier case.
      long upper = count(c, "select count(*) from information_schema.\"TABLES\"");
      long lower = count(c, "select count(*) from information_schema.\"tables\"");
      long mixed = count(c, "select count(*) from information_schema.\"Tables\"");
      assertEquals(upper, lower, "information_schema is case-insensitive (TABLES == tables)");
      assertEquals(upper, mixed, "information_schema is case-insensitive (TABLES == Tables)");
    }
  }
}
