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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link SourceResult}.
 */
@Tag("unit")
class SourceResultTest {

  @Test void testSuccess() {
    SourceResult result = SourceResult.success(1000, 50000, 200, "http://api.example.com");
    assertEquals(SourceResult.Status.SUCCESS, result.getStatus());
    assertTrue(result.isSuccess());
    assertFalse(result.isError());
    assertEquals(1000, result.getRecordCount());
    assertEquals(50000, result.getBytesRead());
    assertEquals(200, result.getDurationMs());
    assertEquals("http://api.example.com", result.getSourceUrl());
    assertNull(result.getErrorMessage());
  }

  @Test void testSkipped() {
    SourceResult result = SourceResult.skipped("Already cached");
    assertEquals(SourceResult.Status.SKIPPED, result.getStatus());
    assertFalse(result.isSuccess());
    assertFalse(result.isError());
    assertEquals(0, result.getRecordCount());
    assertEquals(0, result.getBytesRead());
    assertEquals(0, result.getDurationMs());
    assertNull(result.getSourceUrl());
    assertEquals("Already cached", result.getErrorMessage());
  }

  @Test void testError() {
    SourceResult result = SourceResult.error("Connection timeout", 5000, "http://api.example.com");
    assertEquals(SourceResult.Status.ERROR, result.getStatus());
    assertFalse(result.isSuccess());
    assertTrue(result.isError());
    assertEquals(0, result.getRecordCount());
    assertEquals(0, result.getBytesRead());
    assertEquals(5000, result.getDurationMs());
    assertEquals("http://api.example.com", result.getSourceUrl());
    assertEquals("Connection timeout", result.getErrorMessage());
  }

  @Test void testToStringSuccess() {
    SourceResult result = SourceResult.success(100, 2048, 50, "http://example.com");
    String str = result.toString();
    assertNotNull(str);
    assertTrue(str.contains("SUCCESS"));
    assertTrue(str.contains("100"));
    assertTrue(str.contains("2048"));
    assertTrue(str.contains("example.com"));
  }

  @Test void testToStringError() {
    SourceResult result = SourceResult.error("timeout", 1000, "http://example.com");
    String str = result.toString();
    assertTrue(str.contains("ERROR"));
    assertTrue(str.contains("timeout"));
    assertTrue(str.contains("example.com"));
  }

  @Test void testToStringSkipped() {
    SourceResult result = SourceResult.skipped("cached");
    String str = result.toString();
    assertTrue(str.contains("SKIPPED"));
    assertTrue(str.contains("cached"));
  }

  @Test void testStatusEnumValues() {
    SourceResult.Status[] values = SourceResult.Status.values();
    assertEquals(3, values.length);
    assertNotNull(SourceResult.Status.valueOf("SUCCESS"));
    assertNotNull(SourceResult.Status.valueOf("SKIPPED"));
    assertNotNull(SourceResult.Status.valueOf("ERROR"));
  }
}
