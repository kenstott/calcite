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
package org.apache.calcite.adapter.askamerica;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Regression for the {@code query} MCP tool rejecting the Postgres/DuckDB-shell
 * {@code expr::type} infix cast shorthand with an opaque parser error (Calcite's own default
 * core parser has no grammar production for {@code ::} at all — {@code CAST(expr AS type)}
 * is the only spelling it accepts).
 *
 * <p>Exercises the exact {@code parserFactory} connection property {@link McpServer}'s own
 * schema-connection setup uses, against a bare in-memory {@code jdbc:calcite:} connection
 * (no schema, no govdata/R2 dependency) rather than {@code jdbc:govdata:} — this is testing the
 * parser-factory mechanism itself in a way that runs without live credentials, unlike
 * {@code McpServerIntegrationTest}'s subprocess-based end-to-end coverage of the same property
 * applied to a real government-data connection.
 */
@Tag("unit")
class InfixCastSupportTest {

  private static Connection openBareCalciteConnection() throws Exception {
    Properties props = new Properties();
    // SqlLibraryOperators.INFIX_CAST is declared under the postgresql function library — the
    // parser accepting "::" syntactically is not enough on its own; the operator it resolves
    // to must also be in the connection's active operator table (fun=) or validation rejects
    // it as "No match found for function signature ::" even though the parse succeeded.
    // Matches McpServer's own connection properties (fun=standard,postgresql,spatial,mssql,
    // bigquery), simplified here to just the library that matters for this operator.
    props.setProperty("fun", "standard,postgresql");
    props.setProperty("parserFactory",
        "org.apache.calcite.sql.parser.babel.SqlBabelParserImpl#FACTORY");
    return DriverManager.getConnection("jdbc:calcite:", props);
  }

  @Test void infixCastOnALiteralParsesAndEvaluates() throws Exception {
    try (Connection conn = openBareCalciteConnection();
         Statement st = conn.createStatement();
         ResultSet rs = st.executeQuery("SELECT (1)::double AS x")) {
      assertTrue(rs.next(), "query must return exactly one row");
      assertEquals(1.0d, rs.getDouble("x"), 0.0001);
    }
  }

  @Test void castAndInfixCastAgreeOnTheSameLiteral() throws Exception {
    try (Connection conn = openBareCalciteConnection();
         Statement st = conn.createStatement();
         ResultSet rs = st.executeQuery(
             "SELECT CAST(1 AS DOUBLE) AS via_cast, (1)::double AS via_infix")) {
      assertTrue(rs.next(), "query must return exactly one row");
      assertEquals(rs.getDouble("via_cast"), rs.getDouble("via_infix"), 0.0001,
          "CAST(...AS...) and ::type must produce the same value for the same literal");
    }
  }
}
