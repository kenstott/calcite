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
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import javax.sql.DataSource;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * The duplicate-key count must see the rows as stored, on a connection where the key being tested
 * is declared to Calcite as unique.
 */
@Tag("unit")
public class DuplicateKeyCheckTest {

  private static final List<String> KEY = Arrays.asList("type", "date_key");

  @Test void duplicatesUnderADeclaredKeyAreCounted(@TempDir File dir) throws Exception {
    try (Connection conn = calendar(dir, "SELECT * FROM (VALUES ('day',1),('day',1),('day',1),"
        + "('day',2),('day',2),('day',2)) AS t(\"type\", date_key)")) {
      // The premise: planned through Calcite, the declared key makes the DISTINCT a no-op.
      try (Statement st = conn.createStatement();
           ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM (SELECT DISTINCT "
               + "\"type\", \"date_key\" FROM \"ref\".\"calendar\")")) {
        rs.next();
        assertEquals(6L, rs.getLong(1), "constraint not applied; this test proves nothing");
      }

      DataSource duckDb =
          GovDataModelVerificationRunner.duckDbDataSource(conn, "ref", "calendar");
      assertArrayEquals(new long[]{6L, 2L},
          GovDataModelVerificationRunner.countKeys(duckDb, "ref", "calendar", KEY));
    }
  }

  @Test void aSingleDuplicateAmongManyKeysIsCounted(@TempDir File dir) throws Exception {
    // 1,000 distinct keys, one of them stored twice, and one row with a null key column.
    try (Connection conn = calendar(dir, "SELECT 'day' AS \"type\", i AS date_key "
        + "FROM range(1000) AS r(i) UNION ALL SELECT 'day', 7 UNION ALL SELECT 'day', NULL")) {
      DataSource duckDb =
          GovDataModelVerificationRunner.duckDbDataSource(conn, "ref", "calendar");
      assertArrayEquals(new long[]{1001L, 1000L},
          GovDataModelVerificationRunner.countKeys(duckDb, "ref", "calendar", KEY));
      assertArrayEquals(new long[]{1001L, 1000L},
          GovDataModelVerificationRunner.countKeys(duckDb, "ref", "calendar",
              Arrays.asList("date_key")));
    }
  }

  @Test void aUniqueKeyCountsAsManyKeysAsRows(@TempDir File dir) throws Exception {
    try (Connection conn = calendar(dir,
        "SELECT 'day' AS \"type\", i AS date_key FROM range(1000) AS r(i)")) {
      DataSource duckDb =
          GovDataModelVerificationRunner.duckDbDataSource(conn, "ref", "calendar");
      assertArrayEquals(new long[]{1000L, 1000L},
          GovDataModelVerificationRunner.countKeys(duckDb, "ref", "calendar", KEY));
    }
  }

  @Test void aTableWithNoFullyKeyedRowIsUnverified(@TempDir File dir) throws Exception {
    try (Connection conn = calendar(dir, "SELECT 'day' AS \"type\", "
        + "CAST(NULL AS INTEGER) AS date_key FROM range(3) AS r(i)")) {
      DataSource duckDb =
          GovDataModelVerificationRunner.duckDbDataSource(conn, "ref", "calendar");
      long[] counts =
          GovDataModelVerificationRunner.countKeys(duckDb, "ref", "calendar", KEY);
      assertArrayEquals(new long[]{0L, 0L}, counts);
      assertEquals("no row has every key column non-null",
          GovDataModelVerificationRunner.unverifiedReason(true, counts[0], null));
    }
  }

  @Test void onlyACompletedCheckOverKeyedRowsIsVerified() {
    assertNull(GovDataModelVerificationRunner.unverifiedReason(true, 5L, null));
    assertNull(GovDataModelVerificationRunner.unverifiedReason(false, -1L, null));
    assertEquals("check failed: out of memory",
        GovDataModelVerificationRunner.unverifiedReason(false, -1L, "out of memory"));
  }

  /** A Calcite connection over a one-table schema whose declared key is (type, date_key). */
  private static Connection calendar(File dir, String rows) throws Exception {
    File data = new File(dir, "data");
    File tableDir = new File(data, "calendar");
    Files.createDirectories(tableDir.toPath());
    try (Connection duck = DriverManager.getConnection("jdbc:duckdb:");
         Statement st = duck.createStatement()) {
      st.execute("COPY (" + rows + ") TO '"
          + new File(tableDir, "part.parquet").getAbsolutePath() + "' (FORMAT PARQUET)");
    }
    File model = new File(dir, "model.json");
    Files.write(model.toPath(), ("{\"version\":\"1.0\",\"defaultSchema\":\"ref\",\"schemas\":[{"
        + "\"name\":\"ref\",\"type\":\"custom\","
        + "\"factory\":\"org.apache.calcite.adapter.file.FileSchemaFactory\",\"operand\":{"
        + "\"directory\":\"" + data.getAbsolutePath() + "\","
        + "\"baseDirectory\":\"" + new File(dir, "work").getAbsolutePath() + "\","
        + "\"executionEngine\":\"duckdb\",\"ephemeralCache\":true,"
        + "\"partitionedTables\":[{\"name\":\"calendar\",\"pattern\":\"calendar/*.parquet\"}],"
        + "\"tableConstraints\":{\"calendar\":{\"primaryKey\":[\"type\",\"date_key\"]}}"
        + "}}]}").getBytes(StandardCharsets.UTF_8));

    Properties props = new Properties();
    props.setProperty("lex", "ORACLE");
    props.setProperty("unquotedCasing", "TO_LOWER");
    props.setProperty("model", model.getAbsolutePath());
    return DriverManager.getConnection("jdbc:calcite:", props);
  }
}
