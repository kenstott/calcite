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
package org.apache.calcite.adapter.file.etl;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link SchemaResult}.
 */
@Tag("unit")
class SchemaResultTest {

  @Test void testBuilderBasic() {
    SchemaResult result = SchemaResult.builder()
        .schemaName("test")
        .elapsedMs(1000)
        .build();
    assertEquals("test", result.getSchemaName());
    assertEquals(1000, result.getElapsedMs());
    assertEquals(0, result.getTotalRows());
    assertEquals(0, result.getSuccessfulTables());
    assertEquals(0, result.getFailedTables());
    assertEquals(0, result.getSkippedTables());
    assertEquals(0, result.getTotalTables());
    assertFalse(result.hasErrors());
    assertTrue(result.getErrors().isEmpty());
    assertTrue(result.getTableResults().isEmpty());
  }

  @Test void testAddTableResultSuccess() {
    EtlResult tableResult = EtlResult.success("t1", 100L, 1, 500L);

    SchemaResult result = SchemaResult.builder()
        .schemaName("test")
        .addTableResult("t1", tableResult)
        .elapsedMs(2000)
        .build();

    assertEquals(1, result.getSuccessfulTables());
    assertEquals(0, result.getFailedTables());
    assertEquals(0, result.getSkippedTables());
    assertEquals(1, result.getTotalTables());
    assertEquals(100, result.getTotalRows());
    assertNotNull(result.getTableResult("t1"));
    assertNull(result.getTableResult("nonexistent"));
  }

  @Test void testAddTableResultFailure() {
    EtlResult tableResult = EtlResult.failure("t1", "Connection refused", 100L);

    SchemaResult result = SchemaResult.builder()
        .schemaName("test")
        .addTableResult("t1", tableResult)
        .build();

    assertEquals(0, result.getSuccessfulTables());
    assertEquals(1, result.getFailedTables());
    assertTrue(result.hasErrors());
  }

  @Test void testAddTableResultSkipped() {
    EtlResult tableResult = EtlResult.skipped("t1", 100L);

    SchemaResult result = SchemaResult.builder()
        .schemaName("test")
        .addTableResult("t1", tableResult)
        .build();

    assertEquals(0, result.getSuccessfulTables());
    assertEquals(0, result.getFailedTables());
    assertEquals(1, result.getSkippedTables());
    assertFalse(result.hasErrors());
  }

  @Test void testMultipleTableResults() {
    SchemaResult result = SchemaResult.builder()
        .schemaName("econ")
        .addTableResult("gdp", EtlResult.success("gdp", 500L, 1, 1000L))
        .addTableResult("cpi", EtlResult.failure("cpi", "timeout", 100L))
        .addTableResult("jobs", EtlResult.skipped("jobs", 100L))
        .elapsedMs(5000)
        .build();

    assertEquals(1, result.getSuccessfulTables());
    assertEquals(1, result.getFailedTables());
    assertEquals(1, result.getSkippedTables());
    assertEquals(3, result.getTotalTables());
    assertEquals(500, result.getTotalRows());
    assertTrue(result.hasErrors());
  }

  @Test void testAddError() {
    SchemaResult result = SchemaResult.builder()
        .schemaName("test")
        .addError("Schema-level error")
        .build();

    assertEquals(1, result.getErrors().size());
    assertEquals("Schema-level error", result.getErrors().get(0));
  }

  @Test void testErrorsAreImmutable() {
    SchemaResult result = SchemaResult.builder()
        .schemaName("test")
        .build();
    try {
      result.getErrors().add("sneaky");
      assertTrue(false, "Expected UnsupportedOperationException");
    } catch (UnsupportedOperationException e) {
      // Expected
    }
  }

  @Test void testTableResultsAreImmutable() {
    SchemaResult result = SchemaResult.builder()
        .schemaName("test")
        .build();
    try {
      result.getTableResults().put("x", null);
      assertTrue(false, "Expected UnsupportedOperationException");
    } catch (UnsupportedOperationException e) {
      // Expected
    }
  }

  @Test void testToString() {
    SchemaResult result = SchemaResult.builder()
        .schemaName("econ")
        .addTableResult("gdp", EtlResult.success("gdp", 100L, 1, 500L))
        .elapsedMs(1000)
        .build();

    String str = result.toString();
    assertTrue(str.contains("econ"));
    assertTrue(str.contains("1"));
  }

  @Test void testConvenienceConstructor() {
    SchemaResult result = new SchemaResult("test", 3, 1, 2, 1000, 5000, "error msg");
    assertEquals("test", result.getSchemaName());
    assertEquals(3, result.getSuccessfulTables());
    assertEquals(1, result.getFailedTables());
    assertEquals(2, result.getSkippedTables());
    assertEquals(6, result.getTotalTables());
    assertEquals(1000, result.getTotalRows());
    assertEquals(5000, result.getElapsedMs());
    assertTrue(result.hasErrors());
    assertEquals(1, result.getErrors().size());
    assertTrue(result.getTableResults().isEmpty());
  }

  @Test void testConvenienceConstructorNullError() {
    SchemaResult result = new SchemaResult("test", 1, 0, 0, 50, 100, null);
    assertTrue(result.getErrors().isEmpty());
    assertFalse(result.hasErrors());
  }

  @Test void testHasErrorsWithFailedTablesButNoErrors() {
    SchemaResult result = SchemaResult.builder()
        .schemaName("test")
        .addTableResult("t1", EtlResult.failure("t1", "err", 100L))
        .build();
    assertTrue(result.hasErrors());
  }
}
