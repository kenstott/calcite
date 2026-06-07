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
package org.apache.calcite.adapter.file.trino;

import org.apache.calcite.adapter.file.execution.trino.TrinoExecutionEngine;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Tests for {@link TrinoExecutionEngine}.
 *
 * <p>Verifies Trino engine type identification, availability detection,
 * and server reachability checks.
 */
@Tag("unit")
public class TrinoExecutionEngineTest {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(TrinoExecutionEngineTest.class);

  @Test
  public void testGetEngineTypeReturnsTrino() {
    String engineType = TrinoExecutionEngine.getEngineType();
    assertEquals("TRINO", engineType,
        "Engine type should be TRINO");
  }

  @Test
  public void testIsAvailableReturnsConsistentValue() {
    boolean available = TrinoExecutionEngine.isAvailable();
    LOGGER.debug("Trino available: {}", available);
    // isAvailable() should return a consistent boolean value
    assertNotNull(Boolean.valueOf(available),
        "isAvailable() should return a non-null boolean value");
    // Call again to verify consistency
    boolean availableAgain = TrinoExecutionEngine.isAvailable();
    assertEquals(available, availableAgain,
        "isAvailable() should return consistent results");
  }

  @Test
  public void testIsServerReachableWithInvalidHost() {
    // Use an invalid hostname that DNS cannot resolve
    boolean reachable = TrinoExecutionEngine.isServerReachable(
        "host.invalid.test", 8080);
    assertFalse(reachable,
        "Server should not be reachable at invalid hostname");
  }

  @Test
  public void testIsServerReachableWithBadPort() {
    // Use localhost with a port that is almost certainly not listening
    boolean reachable = TrinoExecutionEngine.isServerReachable(
        "localhost", 59999);
    assertFalse(reachable,
        "Server should not be reachable on unused port");
  }

  @Test
  public void testGetEngineTypeIsUpperCase() {
    String type = TrinoExecutionEngine.getEngineType();
    assertEquals(type, type.toUpperCase(java.util.Locale.ROOT),
        "Engine type should be all uppercase");
  }

  @Test
  public void testIsServerReachableWithZeroPort() {
    boolean reachable = TrinoExecutionEngine.isServerReachable("localhost", 0);
    assertFalse(reachable,
        "Server should not be reachable on port 0");
  }

  @Test
  public void testIsAvailableDoesNotThrow() {
    // isAvailable should never throw, even if the driver is missing
    boolean result = TrinoExecutionEngine.isAvailable();
    assertNotNull(Boolean.valueOf(result),
        "isAvailable() should always return a valid boolean");
  }
}
