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
