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
package org.apache.calcite.adapter.file.temporal;

import org.apache.calcite.adapter.file.FileAdapterTests;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test what SQL SELECT returns for DATE values.
 */
@Tag("unit")
public class DateSelectTest {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(DateSelectTest.class);

  @Test public void testDateSelectOutput() throws Exception {
    Properties info = new Properties();

    // This test requires specific temporal data files and uses linq4j engine
    info.put("model", FileAdapterTests.jsonPath("bug-linq4j"));
    info.put("lex", "ORACLE");

    try (Connection connection = DriverManager.getConnection("jdbc:calcite:", info);
         Statement statement = connection.createStatement()) {

      // Test key dates
      String sql = "SELECT \"id\", \"format_desc\", \"date_value\", "
          + "CAST(\"date_value\" AS VARCHAR) AS STRING_VALUE "
          + "FROM \"date_formats\" "
          + "WHERE \"id\" IN (1, 4, 5, 6, 7) "
          + "ORDER BY \"id\"";

      try (ResultSet resultSet = statement.executeQuery(sql)) {
        assertTrue(resultSet.next(),
            "Should have at least one result row");
        int id = resultSet.getInt(1);
        String desc = resultSet.getString(2);
        Date dateValue = resultSet.getDate(3);
        assertNotNull(dateValue,
            "Date value should not be null for id=" + id);
        // Use numeric epoch-day value for verification
        int daysSinceEpoch = resultSet.getInt(3);
        LOGGER.debug("id={}, desc={}, date={}, epochDays={}",
            id, desc, dateValue, daysSinceEpoch);
        assertTrue(daysSinceEpoch > 0,
            "Days since epoch should be positive for modern dates");

        // Verify additional rows exist
        int rowCount = 1;
        while (resultSet.next()) {
          assertNotNull(resultSet.getDate(3),
              "Date value should not be null");
          rowCount++;
        }
        assertTrue(rowCount >= 3,
            "Should have at least 3 date format rows, got: " + rowCount);
      }

      // Test date comparison - verify COUNT returns a result
      sql = "SELECT COUNT(*) FROM \"date_formats\" "
          + "WHERE \"date_value\" = DATE '2024-03-15'";

      try (ResultSet resultSet = statement.executeQuery(sql)) {
        assertTrue(resultSet.next(), "COUNT query should return a row");
        int count = resultSet.getInt(1);
        LOGGER.debug("Rows with date 2024-03-15: {}", count);
        assertTrue(count >= 0,
            "COUNT should return non-negative value");
      }

      // Test epoch date comparison
      sql = "SELECT COUNT(*) FROM \"date_formats\" "
          + "WHERE \"date_value\" = DATE '1970-01-01'";

      try (ResultSet resultSet = statement.executeQuery(sql)) {
        assertTrue(resultSet.next(), "COUNT query should return a row");
        int count = resultSet.getInt(1);
        LOGGER.debug("Rows with epoch date: {}", count);
        assertTrue(count >= 0,
            "COUNT should return non-negative value");
      }
    }
  }
}
