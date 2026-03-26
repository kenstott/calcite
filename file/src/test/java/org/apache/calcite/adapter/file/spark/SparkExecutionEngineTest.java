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
package org.apache.calcite.adapter.file.spark;

import org.apache.calcite.adapter.file.execution.spark.SparkExecutionEngine;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Tests for {@link SparkExecutionEngine}.
 *
 * <p>Verifies engine type, availability detection, and server reachability checks.
 */
@Tag("unit")
public class SparkExecutionEngineTest {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(SparkExecutionEngineTest.class);

  @Test
  public void testGetEngineTypeReturnsSpark() {
    String engineType = SparkExecutionEngine.getEngineType();
    assertEquals("SPARK", engineType,
        "Engine type should be SPARK");
  }

  @Test
  public void testIsAvailableReturnsConsistentValue() {
    boolean first = SparkExecutionEngine.isAvailable();
    boolean second = SparkExecutionEngine.isAvailable();
    assertEquals(first, second,
        "isAvailable() should return a consistent value across calls");
    LOGGER.debug("Spark/Hive JDBC driver available: {}", first);
  }

  @Test
  public void testIsAvailableReturnsBooleanWithoutThrowing() {
    // isAvailable should never throw, even if the driver is missing
    boolean result = SparkExecutionEngine.isAvailable();
    assertNotNull(Boolean.valueOf(result),
        "isAvailable() should return a non-null boolean value");
  }

  @Test
  public void testIsServerReachableWithBadHostReturnsFalse() {
    // A non-routable address should fail quickly
    boolean result = SparkExecutionEngine.isServerReachable("192.0.2.1", 10000);
    assertFalse(result,
        "Should not be reachable on a non-routable address");
  }

  @Test
  public void testIsServerReachableWithBadPortReturnsFalse() {
    // Port 1 is almost certainly not running a Thrift Server
    boolean result = SparkExecutionEngine.isServerReachable("localhost", 1);
    assertFalse(result,
        "Should not be reachable on port 1");
  }

  @Test
  public void testGetEngineTypeIsUpperCase() {
    String type = SparkExecutionEngine.getEngineType();
    assertEquals(type, type.toUpperCase(java.util.Locale.ROOT),
        "Engine type should be all uppercase");
  }

  @Test
  public void testIsServerReachableWithZeroPort() {
    boolean result = SparkExecutionEngine.isServerReachable("localhost", 0);
    assertFalse(result,
        "Should not be reachable on port 0");
  }
}
