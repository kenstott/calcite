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
package org.apache.calcite.adapter.file.json;

import org.apache.calcite.adapter.file.BaseFileTest;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end (G2): a plain JSON file with ISO 8601 date/timestamp string columns must be
 * queryable through JDBC as real DATE/TIMESTAMP columns via {@code getDate}/{@code
 * getTimestamp}, not just typed correctly at the {@code JsonTable} row-type layer.
 */
@Tag("unit")
public class JsonDateInferenceJdbcTest extends BaseFileTest {
  @TempDir
  Path tempDir;

  @Test public void testIsoDateAndTimestampQueryableViaJdbc() throws Exception {
    File jsonFile = new File(tempDir.toFile(), "events.json");
    try (FileWriter writer = new FileWriter(jsonFile, StandardCharsets.UTF_8)) {
      writer.write("[{\"name\": \"launch\", \"day\": \"2024-01-15\", "
          + "\"logged_at\": \"2024-01-15T10:30:00\"},"
          + "{\"name\": \"review\", \"day\": \"2024-02-20\", "
          + "\"logged_at\": \"2024-02-20T14:00:00\"}]");
    }

    String model = buildTestModel("json_dates", tempDir.toFile().getAbsolutePath());
    Properties info = new Properties();
    info.put("model", "inline:" + model);
    applyEngineDefaults(info);

    try (Connection connection = DriverManager.getConnection("jdbc:calcite:", info);
         Statement statement = connection.createStatement()) {
      ResultSet rs =
          statement.executeQuery("SELECT * FROM json_dates.events ORDER BY name");

      assertTrue(rs.next());
      assertEquals(Date.valueOf("2024-01-15"), rs.getDate("day"),
          "ISO date string column must be queryable as a real SQL DATE");
      assertEquals(Timestamp.valueOf("2024-01-15 10:30:00"), rs.getTimestamp("logged_at"),
          "ISO local datetime string column must be queryable as a real SQL TIMESTAMP");

      assertTrue(rs.next());
      assertEquals(Date.valueOf("2024-02-20"), rs.getDate("day"));
      assertEquals(Timestamp.valueOf("2024-02-20 14:00:00"), rs.getTimestamp("logged_at"));
    }
  }
}
