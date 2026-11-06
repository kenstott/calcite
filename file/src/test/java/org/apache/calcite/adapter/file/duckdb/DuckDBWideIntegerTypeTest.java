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
package org.apache.calcite.adapter.file.duckdb;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * A DuckDB column wider than BIGINT is read through Calcite as DECIMAL, not ANY.
 *
 * <p>DuckDB reports HUGEINT (what a SUM of BIGINT gives), UHUGEINT and UBIGINT as
 * {@link Types#OTHER}. Calcite typed them ANY, which reaches a JDBC client as JAVA_OBJECT; the
 * pgwire server's Arrow reader refuses that type, so a govdata view such as
 * {@code disasters_storm_impact_by_county} ({@code SUM(deaths_direct + deaths_indirect)}) failed
 * with "Error occurred while creating iterator" (Unmapped JDBC type: 2000).
 */
@Tag("unit")
class DuckDBWideIntegerTypeTest {

  @TempDir Path tempDir;

  @Test void aSumOfBigintAndAnUnsignedBigintAreDecimalAndKeepTheirValues() throws Exception {
    Path db = tempDir.resolve("wide.duckdb");
    try (Connection duck = DriverManager.getConnection("jdbc:duckdb:" + db);
         Statement s = duck.createStatement()) {
      s.execute("CREATE TABLE events (county VARCHAR, deaths BIGINT, ub UBIGINT)");
      s.execute("INSERT INTO events VALUES ('a', 1, 9), ('a', 2, NULL), ('b', NULL, 7)");
      s.execute("CREATE VIEW impact AS SELECT county, SUM(deaths) AS total_deaths, "
          + "MIN(ub) AS least FROM events GROUP BY county");
    }

    String model = "inline:{\"version\":\"1.0\",\"defaultSchema\":\"d\",\"schemas\":[{"
        + "\"name\":\"d\",\"type\":\"jdbc\",\"jdbcDriver\":\"org.duckdb.DuckDBDriver\","
        + "\"jdbcUrl\":\"jdbc:duckdb:" + db.toString().replace("\\", "/") + "\"}]}";
    Properties info = new Properties();
    info.setProperty("model", model);
    try (Connection calcite = DriverManager.getConnection("jdbc:calcite:", info);
         Statement s = calcite.createStatement();
         ResultSet rs = s.executeQuery("SELECT \"county\", \"total_deaths\", \"least\" "
             + "FROM \"d\".\"impact\" ORDER BY \"county\"")) {
      ResultSetMetaData md = rs.getMetaData();
      assertEquals(Types.DECIMAL, md.getColumnType(2), md.getColumnTypeName(2));
      assertEquals(0, md.getScale(2));
      assertEquals(Types.DECIMAL, md.getColumnType(3), md.getColumnTypeName(3));
      List<List<Object>> rows = new ArrayList<>();
      while (rs.next()) {
        BigDecimal deaths = rs.getBigDecimal(2);
        BigDecimal least = rs.getBigDecimal(3);
        rows.add(
            Arrays.asList(rs.getString(1),
                deaths == null ? null : deaths.toBigIntegerExact(),
                least == null ? null : least.toBigIntegerExact()));
      }
      assertEquals(
          Arrays.asList(
              Arrays.asList("a", BigInteger.valueOf(3), BigInteger.valueOf(9)),
              Arrays.asList("b", null, BigInteger.valueOf(7))),
          rows);
    }
  }
}
