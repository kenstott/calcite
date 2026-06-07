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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Tag("integration")
public class CompareTimestampsTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(CompareTimestampsTest.class);

    @Test public void testCompareTimestamps() throws Exception {
        // Register FileJdbcDriver
        Class.forName("org.apache.calcite.adapter.file.FileJdbcDriver");
        DriverManager.registerDriver(new FileJdbcDriver());

        Properties info = new Properties();
        info.put("model", FileAdapterTests.jsonPath("bug"));

        String sql = "select * from \"date\" where \"empno\" = 140";

        // Test with jdbc:calcite:
        LOGGER.debug("Testing with jdbc:calcite:");
        long calciteMillis = 0;
        try (Connection conn = DriverManager.getConnection("jdbc:calcite:", info);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            if (rs.next()) {
                Timestamp ts = rs.getTimestamp(4);
                calciteMillis = ts.getTime();
                LOGGER.debug("  Timestamp: {}", ts);
                LOGGER.debug("  Millis: {}", calciteMillis);
            }
        }

        // Test with second connection to verify consistency
        LOGGER.debug("Testing with second jdbc:calcite: connection:");
        long fileMillis = 0;
        try (Connection conn = DriverManager.getConnection("jdbc:calcite:", info);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            if (rs.next()) {
                Timestamp ts = rs.getTimestamp(4);
                fileMillis = ts.getTime();
                LOGGER.debug("  Timestamp: {}", ts);
                LOGGER.debug("  Millis: {}", fileMillis);
            }
        }

        LOGGER.debug("Difference: {} ms", fileMillis - calciteMillis);

        // Both connections should return the same timestamp value
        assertEquals(calciteMillis, fileMillis,
            "Separate connections should return the same timestamp value");
    }
}
