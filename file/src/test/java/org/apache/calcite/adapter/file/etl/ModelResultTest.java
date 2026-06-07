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

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link ModelResult}.
 */
@Tag("unit")
class ModelResultTest {

  @Test void testBasicModelResult() {
    SchemaResult sr = new SchemaResult("econ", 5, 0, 1, 1000, 5000, null);

    ModelResult result =
        new ModelResult("govdata", Collections.singletonList(sr), 1, 0, 6000);

    assertEquals("govdata", result.getModelName());
    assertEquals(1, result.getTotalSchemas());
    assertEquals(1, result.getSuccessfulSchemas());
    assertEquals(0, result.getFailedSchemas());
    assertEquals(6000, result.getElapsedMs());
    assertEquals(1000, result.getTotalRows());
    assertEquals(6, result.getTotalTables());
  }

  @Test void testMultipleSchemaResults() {
    SchemaResult sr1 = new SchemaResult("econ", 3, 0, 0, 500, 2000, null);
    SchemaResult sr2 = new SchemaResult("geo", 2, 1, 0, 300, 3000, "error");

    ModelResult result =
        new ModelResult("govdata", Arrays.asList(sr1, sr2), 1, 1, 5000);

    assertEquals(2, result.getTotalSchemas());
    assertEquals(1, result.getSuccessfulSchemas());
    assertEquals(1, result.getFailedSchemas());
    assertEquals(800, result.getTotalRows());
    assertEquals(6, result.getTotalTables());
  }

  @Test void testSchemaResultsAreImmutable() {
    SchemaResult sr = new SchemaResult("test", 1, 0, 0, 10, 100, null);
    ModelResult result =
        new ModelResult("model", Collections.singletonList(sr), 1, 0, 200);
    try {
      result.getSchemaResults().add(sr);
      assertTrue(false, "Expected UnsupportedOperationException");
    } catch (UnsupportedOperationException e) {
      // Expected
    }
  }

  @Test void testToString() {
    SchemaResult sr = new SchemaResult("econ", 2, 0, 0, 100, 1000, null);
    ModelResult result =
        new ModelResult("govdata", Collections.singletonList(sr), 1, 0, 2000);

    String str = result.toString();
    assertNotNull(str);
    assertTrue(str.contains("govdata"));
    assertTrue(str.contains("1"));
  }

  @Test void testEmptySchemaResults() {
    ModelResult result =
        new ModelResult("empty", Collections.<SchemaResult>emptyList(), 0, 0, 0);
    assertEquals(0, result.getTotalSchemas());
    assertEquals(0, result.getTotalRows());
    assertEquals(0, result.getTotalTables());
  }
}
