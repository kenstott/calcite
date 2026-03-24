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
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link MaterializeResult}.
 */
@Tag("unit")
class MaterializeResultTest {

  @Test void testSuccess() {
    MaterializeResult result = MaterializeResult.success(1000, 5, 2500);

    assertEquals(MaterializeResult.Status.SUCCESS, result.getStatus());
    assertTrue(result.isSuccess());
    assertFalse(result.isSkipped());
    assertFalse(result.isError());
    assertEquals(1000, result.getRowCount());
    assertEquals(5, result.getFileCount());
    assertEquals(2500, result.getElapsedMillis());
    assertNull(result.getMessage());
  }

  @Test void testSkipped() {
    MaterializeResult result = MaterializeResult.skipped("Already up-to-date");

    assertEquals(MaterializeResult.Status.SKIPPED, result.getStatus());
    assertTrue(result.isSkipped());
    assertFalse(result.isSuccess());
    assertFalse(result.isError());
    assertEquals(0, result.getRowCount());
    assertEquals(0, result.getFileCount());
    assertEquals(0, result.getElapsedMillis());
    assertEquals("Already up-to-date", result.getMessage());
  }

  @Test void testError() {
    MaterializeResult result = MaterializeResult.error("Connection failed", 500);

    assertEquals(MaterializeResult.Status.ERROR, result.getStatus());
    assertTrue(result.isError());
    assertFalse(result.isSuccess());
    assertFalse(result.isSkipped());
    assertEquals(0, result.getRowCount());
    assertEquals(0, result.getFileCount());
    assertEquals(500, result.getElapsedMillis());
    assertEquals("Connection failed", result.getMessage());
  }

  @Test void testToStringSuccess() {
    MaterializeResult result = MaterializeResult.success(1000, 5, 2500);
    String str = result.toString();
    assertTrue(str.contains("SUCCESS"));
    assertTrue(str.contains("1000"));
    assertTrue(str.contains("2500ms"));
  }

  @Test void testToStringSkipped() {
    MaterializeResult result = MaterializeResult.skipped("up-to-date");
    String str = result.toString();
    assertTrue(str.contains("SKIPPED"));
    assertTrue(str.contains("up-to-date"));
  }

  @Test void testToStringError() {
    MaterializeResult result = MaterializeResult.error("failed", 100);
    String str = result.toString();
    assertTrue(str.contains("ERROR"));
    assertTrue(str.contains("failed"));
  }

  @Test void testToStringWithNegativeRowCount() {
    // When row count is -1 (unknown), it should not appear in toString
    MaterializeResult result = MaterializeResult.success(-1, -1, 100);
    String str = result.toString();
    // -1 should not appear for rows since the condition is rowCount >= 0
    assertFalse(str.contains("rows=-1"));
  }
}
