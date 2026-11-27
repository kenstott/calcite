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
package org.apache.calcite.adapter.govdata;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A string literal may hold any Unicode character, not only those in Calcite's ISO-8859-1
 * default.
 */
@Tag("unit")
class UnicodeLiteralTest {

  private static final String TEXT =
      "Ábrego García — “q” 中";

  @Test void plainAndNationalLiteralsAreReturnedUnchanged() throws SQLException {
    try (Connection connection = DriverManager.getConnection("jdbc:calcite:");
         Statement statement = connection.createStatement();
         ResultSet rows =
             statement.executeQuery("VALUES ('" + TEXT + "', N'" + TEXT + "')")) {
      assertTrue(rows.next());
      assertEquals(TEXT, rows.getString(1));
      assertEquals(TEXT, rows.getString(2));
    }
  }
}
