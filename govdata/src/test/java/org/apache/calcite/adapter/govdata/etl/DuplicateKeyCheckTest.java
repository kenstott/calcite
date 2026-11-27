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

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * The duplicate-key count must see the rows as stored, on a connection where the key being tested
 * is declared to Calcite as unique.
 */
@Tag("unit")
public class DuplicateKeyCheckTest {

  @Test void duplicatesUnderADeclaredKeyAreCounted(@TempDir File dir) throws Exception {
    File data = new File(dir, "data");
    File tableDir = new File(data, "calendar");
    Files.createDirectories(tableDir.toPath());
    try (Connection duck = DriverManager.getConnection("jdbc:duckdb:");
         Statement st = duck.createStatement()) {
      st.execute("COPY (SELECT * FROM (VALUES ('day',1),('day',1),('day',1),"
          + "('day',2),('day',2),('day',2)) AS t(\"type\", date_key)) TO '"
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

    List<String> key = Arrays.asList("type", "date_key");
    Properties props = new Properties();
    props.setProperty("lex", "ORACLE");
    props.setProperty("unquotedCasing", "TO_LOWER");
    props.setProperty("model", model.getAbsolutePath());
    try (Connection conn = DriverManager.getConnection("jdbc:calcite:", props)) {
      // The premise: planned through Calcite, the declared key makes the DISTINCT a no-op.
      try (Statement st = conn.createStatement();
           ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM (SELECT DISTINCT "
               + "\"type\", \"date_key\" FROM \"ref\".\"calendar\")")) {
        rs.next();
        assertEquals(6L, rs.getLong(1), "constraint not applied; this test proves nothing");
      }

      DataSource duckDb =
          GovDataModelVerificationRunner.duckDbDataSource(conn, "ref", "calendar");
      assertEquals(2L,
          GovDataModelVerificationRunner.scalarCount(duckDb,
              GovDataModelVerificationRunner.distinctKeysSql("ref", "calendar", key)));
    }
  }
}
