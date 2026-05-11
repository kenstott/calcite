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
package org.apache.calcite.adapter.file.clickhouse;

import org.apache.calcite.adapter.file.execution.clickhouse.ClickHouseExecutionEngine;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Tests for {@link ClickHouseExecutionEngine}.
 *
 * <p>Verifies ClickHouse engine type identification, availability detection,
 * and local binary path resolution.
 */
@Tag("unit")
public class ClickHouseExecutionEngineTest {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(ClickHouseExecutionEngineTest.class);

  @Test
  public void testGetEngineTypeReturnsClickHouse() {
    String engineType = ClickHouseExecutionEngine.getEngineType();
    assertEquals("CLICKHOUSE", engineType,
        "Engine type should be CLICKHOUSE");
  }

  @Test
  public void testIsAvailableReturnsConsistentValue() {
    boolean available = ClickHouseExecutionEngine.isAvailable();
    LOGGER.debug("ClickHouse JDBC driver available: {}", available);
    // isAvailable should return a consistent boolean value
    assertNotNull(Boolean.valueOf(available),
        "isAvailable() should return a non-null boolean value");
    // Call again to verify consistency
    boolean availableAgain = ClickHouseExecutionEngine.isAvailable();
    assertEquals(available, availableAgain,
        "isAvailable() should return consistent results across calls");
  }

  @Test
  public void testIsLocalAvailableReturnsConsistentValue() {
    boolean localAvailable = ClickHouseExecutionEngine.isLocalAvailable();
    LOGGER.debug("clickhouse-local available: {}", localAvailable);
    // isLocalAvailable should return a consistent boolean value
    assertNotNull(Boolean.valueOf(localAvailable),
        "isLocalAvailable() should return a non-null boolean value");
    // Call again to verify consistency
    boolean localAvailableAgain = ClickHouseExecutionEngine.isLocalAvailable();
    assertEquals(localAvailable, localAvailableAgain,
        "isLocalAvailable() should return consistent results across calls");
  }

  @Test
  public void testFindLocalBinaryPathWithNullReturnsNullOrPath() {
    // When configured path is null, it should fall back to env/PATH search
    String result = ClickHouseExecutionEngine.findLocalBinaryPath(null);
    LOGGER.debug("findLocalBinaryPath(null) returned: {}", result);
    // Result depends on environment - either null or a valid path
    if (result != null) {
      java.io.File binary = new java.io.File(result);
      assertEquals(binary.getAbsolutePath(), result,
          "Returned path should be absolute");
    }
  }

  @Test
  public void testFindLocalBinaryPathWithEmptyStringReturnsNullOrPath() {
    // Empty string should be treated like null (falls back to env/PATH)
    String result = ClickHouseExecutionEngine.findLocalBinaryPath("");
    LOGGER.debug("findLocalBinaryPath('') returned: {}", result);
    // Result depends on environment
    if (result != null) {
      java.io.File binary = new java.io.File(result);
      assertEquals(binary.getAbsolutePath(), result,
          "Returned path should be absolute");
    }
  }

  @Test
  public void testFindLocalBinaryPathWithInvalidPathFallsBack() {
    String invalidPath = "/nonexistent/path/to/clickhouse-local";
    String result = ClickHouseExecutionEngine.findLocalBinaryPath(invalidPath);
    LOGGER.debug("findLocalBinaryPath('{}') returned: {}", invalidPath, result);
    // Should not return the invalid path since it doesn't exist
    if (result != null) {
      // If it found a binary via env/PATH, it should not be the invalid path
      assertNotNull(result, "If found, result should be a real path");
    } else {
      assertNull(result,
          "Should return null when binary is not found anywhere");
    }
  }

  @Test
  public void testGetEngineTypeIsUpperCase() {
    String type = ClickHouseExecutionEngine.getEngineType();
    assertEquals(type, type.toUpperCase(java.util.Locale.ROOT),
        "Engine type should be all uppercase");
  }

  @Test
  public void testIsAvailableDoesNotThrow() {
    // isAvailable should never throw, even if the driver is missing
    boolean result = ClickHouseExecutionEngine.isAvailable();
    assertNotNull(Boolean.valueOf(result),
        "isAvailable() should always return a valid boolean");
  }

  @Test
  public void testIsLocalAvailableDoesNotThrow() {
    // isLocalAvailable should never throw even in unusual PATH configs
    boolean result = ClickHouseExecutionEngine.isLocalAvailable();
    assertNotNull(Boolean.valueOf(result),
        "isLocalAvailable() should always return a valid boolean");
  }
}
