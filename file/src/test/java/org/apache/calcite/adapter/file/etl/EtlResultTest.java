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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link EtlResult}.
 */
@Tag("unit")
class EtlResultTest {

  @Test void testSuccessResult() {
    EtlResult result = EtlResult.success("test_pipeline", 1000, 5, 2500);

    assertEquals("test_pipeline", result.getPipelineName());
    assertEquals(1000, result.getTotalRows());
    assertEquals(5, result.getSuccessfulBatches());
    assertEquals(0, result.getFailedBatches());
    assertEquals(0, result.getSkippedBatches());
    assertEquals(5, result.getTotalBatches());
    assertEquals(2500, result.getElapsedMs());
    assertTrue(result.isSuccessful());
    assertTrue(result.isCompleteSuccess());
    assertFalse(result.isFailed());
    assertFalse(result.isSkipped());
    assertNull(result.getFailureMessage());
    assertTrue(result.getErrors().isEmpty());
  }

  @Test void testFailureResult() {
    EtlResult result = EtlResult.failure("test_pipeline", "Connection refused", 500);

    assertEquals("test_pipeline", result.getPipelineName());
    assertEquals(0, result.getTotalRows());
    assertTrue(result.isFailed());
    assertFalse(result.isSuccessful());
    assertFalse(result.isCompleteSuccess());
    assertEquals("Connection refused", result.getFailureMessage());
    assertEquals(500, result.getElapsedMs());
  }

  @Test void testSkippedResult() {
    EtlResult result = EtlResult.skipped("test_pipeline", 100);

    assertEquals("test_pipeline", result.getPipelineName());
    assertTrue(result.isSkipped());
    assertTrue(result.isSkippedEntirePipeline());
    assertTrue(result.isSuccessful());
    assertEquals(100, result.getElapsedMs());
  }

  @Test void testBuilderWithErrors() {
    EtlResult result = EtlResult.builder()
        .pipelineName("test")
        .totalRows(900)
        .successfulBatches(9)
        .failedBatches(1)
        .errors(Arrays.asList("Batch 3 failed: timeout"))
        .elapsedMs(5000)
        .build();

    assertFalse(result.isCompleteSuccess());
    assertTrue(result.isSuccessful());
    assertEquals(1, result.getErrors().size());
    assertEquals(1, result.getFailedBatches());
    assertEquals(10, result.getTotalBatches());
  }

  @Test void testBuilderWithSkippedBatches() {
    EtlResult result = EtlResult.builder()
        .pipelineName("test")
        .totalRows(500)
        .successfulBatches(5)
        .skippedBatches(3)
        .elapsedMs(1000)
        .build();

    assertEquals(3, result.getSkippedBatches());
    assertEquals(8, result.getTotalBatches());
  }

  @Test void testGetRowsPerSecond() {
    EtlResult result = EtlResult.success("test", 10000, 10, 2000);
    assertEquals(5000.0, result.getRowsPerSecond(), 0.01);
  }

  @Test void testGetRowsPerSecondZeroElapsed() {
    EtlResult result = EtlResult.success("test", 1000, 1, 0);
    assertEquals(0.0, result.getRowsPerSecond(), 0.01);
  }

  @Test void testTableLocation() {
    EtlResult result = EtlResult.builder()
        .pipelineName("test")
        .totalRows(100)
        .successfulBatches(1)
        .elapsedMs(1000)
        .tableLocation("s3://bucket/table")
        .materializeFormat(MaterializeConfig.Format.ICEBERG)
        .build();

    assertEquals("s3://bucket/table", result.getTableLocation());
    assertEquals(MaterializeConfig.Format.ICEBERG, result.getMaterializeFormat());
  }

  @Test void testToStringSuccess() {
    EtlResult result = EtlResult.success("test", 1000, 5, 2500);
    String str = result.toString();
    assertTrue(str.contains("test"));
    assertTrue(str.contains("1000"));
    assertTrue(str.contains("2500ms"));
  }

  @Test void testToStringFailure() {
    EtlResult result = EtlResult.failure("test", "Error occurred", 500);
    String str = result.toString();
    assertTrue(str.contains("FAILED"));
    assertTrue(str.contains("Error occurred"));
  }

  @Test void testToStringSkipped() {
    EtlResult result = EtlResult.skipped("test", 100);
    String str = result.toString();
    assertTrue(str.contains("SKIPPED"));
  }

  @Test void testToStringWithFailedBatches() {
    EtlResult result = EtlResult.builder()
        .pipelineName("test")
        .totalRows(500)
        .successfulBatches(4)
        .failedBatches(1)
        .elapsedMs(2000)
        .build();

    String str = result.toString();
    assertTrue(str.contains("1 failed"));
  }

  @Test void testToStringWithSkippedBatches() {
    EtlResult result = EtlResult.builder()
        .pipelineName("test")
        .totalRows(500)
        .successfulBatches(4)
        .skippedBatches(2)
        .elapsedMs(2000)
        .build();

    String str = result.toString();
    assertTrue(str.contains("2 skipped"));
  }

  @Test void testDefaultErrorsEmpty() {
    EtlResult result = EtlResult.success("test", 0, 0, 0);
    assertNotNullAndEmpty(result.getErrors());
  }

  private void assertNotNullAndEmpty(java.util.List<String> list) {
    assertTrue(list != null && list.isEmpty());
  }
}
