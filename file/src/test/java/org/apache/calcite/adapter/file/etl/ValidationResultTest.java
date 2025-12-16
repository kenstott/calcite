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
package org.apache.calcite.adapter.file.etl;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for ValidationResult.
 */
@Tag("unit")
public class ValidationResultTest {

  @Test void testValidResult() {
    ValidationResult result = ValidationResult.valid();

    assertEquals(ValidationResult.Action.VALID, result.getAction());
    assertNull(result.getMessage());
    assertTrue(result.isValid());
    assertTrue(result.shouldContinue());
    assertTrue(result.shouldInclude());
  }

  @Test void testValidResultIsSingleton() {
    ValidationResult result1 = ValidationResult.valid();
    ValidationResult result2 = ValidationResult.valid();
    assertSame(result1, result2);
  }

  @Test void testDropResult() {
    ValidationResult result = ValidationResult.drop("Missing required field");

    assertEquals(ValidationResult.Action.DROP, result.getAction());
    assertEquals("Missing required field", result.getMessage());
    assertFalse(result.isValid());
    assertTrue(result.shouldContinue());
    assertFalse(result.shouldInclude());
  }

  @Test void testWarnResult() {
    ValidationResult result = ValidationResult.warn("Value out of expected range");

    assertEquals(ValidationResult.Action.WARN, result.getAction());
    assertEquals("Value out of expected range", result.getMessage());
    assertFalse(result.isValid());
    assertTrue(result.shouldContinue());
    assertTrue(result.shouldInclude());
  }

  @Test void testFailResult() {
    ValidationResult result = ValidationResult.fail("Critical validation error");

    assertEquals(ValidationResult.Action.FAIL, result.getAction());
    assertEquals("Critical validation error", result.getMessage());
    assertFalse(result.isValid());
    assertFalse(result.shouldContinue());
    assertFalse(result.shouldInclude());
  }

  @Test void testValidToString() {
    ValidationResult result = ValidationResult.valid();
    String toString = result.toString();
    assertTrue(toString.contains("VALID"));
  }

  @Test void testDropToString() {
    ValidationResult result = ValidationResult.drop("test message");
    String toString = result.toString();
    assertTrue(toString.contains("DROP"));
    assertTrue(toString.contains("test message"));
  }

  @Test void testWarnToString() {
    ValidationResult result = ValidationResult.warn("warning message");
    String toString = result.toString();
    assertTrue(toString.contains("WARN"));
    assertTrue(toString.contains("warning message"));
  }

  @Test void testFailToString() {
    ValidationResult result = ValidationResult.fail("error message");
    String toString = result.toString();
    assertTrue(toString.contains("FAIL"));
    assertTrue(toString.contains("error message"));
  }

  @Test void testNullMessage() {
    ValidationResult result = ValidationResult.drop(null);
    assertNull(result.getMessage());
    assertEquals(ValidationResult.Action.DROP, result.getAction());
  }

  @Test void testEmptyMessage() {
    ValidationResult result = ValidationResult.warn("");
    assertEquals("", result.getMessage());
    assertEquals(ValidationResult.Action.WARN, result.getAction());
  }
}
