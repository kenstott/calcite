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
package org.apache.calcite.adapter.file.execution;

import org.apache.calcite.adapter.file.execution.parquet.ParquetConfig;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link ParquetConfig}.
 */
@Tag("unit")
public class ParquetConfigTest {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(ParquetConfigTest.class);

  @Test public void testConstructorSetsBatchSizeAndVectorized() {
    ParquetConfig config = new ParquetConfig(4096, true);
    assertEquals(4096, config.getBatchSize());
    assertTrue(config.isVectorizedReaderEnabled());
  }

  @Test public void testConstructorWithDisabledVectorized() {
    ParquetConfig config = new ParquetConfig(2048, false);
    assertEquals(2048, config.getBatchSize());
    assertFalse(config.isVectorizedReaderEnabled());
  }

  @Test public void testComputeOptimalBatchSizeReturnsPositiveValue() {
    int batchSize = ParquetConfig.computeOptimalBatchSize();
    assertTrue(batchSize > 0,
        "Optimal batch size should be positive");
    assertTrue(batchSize >= 1024,
        "Batch size should be at least 1024");
    assertTrue(batchSize <= 16384,
        "Batch size should be at most 16384");
    LOGGER.debug("Computed optimal batch size: {}", batchSize);
  }

  @Test public void testComputeOptimalBatchSizeIsConsistent() {
    int first = ParquetConfig.computeOptimalBatchSize();
    int second = ParquetConfig.computeOptimalBatchSize();
    // Batch sizes should be relatively close (runtime memory may vary slightly)
    assertTrue(Math.abs(first - second) < 1024,
        "Computed batch sizes should be consistent across calls");
  }

  @Test public void testDefaultConfigExists() {
    ParquetConfig defaultConfig = ParquetConfig.DEFAULT;
    assertNotNull(defaultConfig);
    assertTrue(defaultConfig.getBatchSize() >= 1024);
    assertFalse(defaultConfig.isVectorizedReaderEnabled(),
        "Default config should have vectorized reader disabled");
    LOGGER.debug("Default ParquetConfig: {}", defaultConfig);
  }

  @Test public void testFromSystemPropertiesWithDefaults() {
    // Clear any system properties that might affect the test
    String originalBatchSize = System.getProperty("calcite.file.parquet.batch.size");
    String originalVectorized = System.getProperty("parquet.enable.vectorized.reader");
    try {
      System.clearProperty("calcite.file.parquet.batch.size");
      System.clearProperty("parquet.enable.vectorized.reader");

      ParquetConfig config = ParquetConfig.fromSystemProperties();
      assertNotNull(config);
      assertTrue(config.getBatchSize() >= 1024);
      assertFalse(config.isVectorizedReaderEnabled());
    } finally {
      // Restore original values
      if (originalBatchSize != null) {
        System.setProperty("calcite.file.parquet.batch.size", originalBatchSize);
      }
      if (originalVectorized != null) {
        System.setProperty("parquet.enable.vectorized.reader", originalVectorized);
      }
    }
  }

  @Test public void testFromSystemPropertiesWithCustomBatchSize() {
    String original = System.getProperty("calcite.file.parquet.batch.size");
    try {
      System.setProperty("calcite.file.parquet.batch.size", "8192");

      ParquetConfig config = ParquetConfig.fromSystemProperties();
      assertEquals(8192, config.getBatchSize());
    } finally {
      if (original != null) {
        System.setProperty("calcite.file.parquet.batch.size", original);
      } else {
        System.clearProperty("calcite.file.parquet.batch.size");
      }
    }
  }

  @Test public void testFromSystemPropertiesWithVectorizedEnabled() {
    String original = System.getProperty("parquet.enable.vectorized.reader");
    try {
      System.setProperty("parquet.enable.vectorized.reader", "true");

      ParquetConfig config = ParquetConfig.fromSystemProperties();
      assertTrue(config.isVectorizedReaderEnabled());
    } finally {
      if (original != null) {
        System.setProperty("parquet.enable.vectorized.reader", original);
      } else {
        System.clearProperty("parquet.enable.vectorized.reader");
      }
    }
  }

  @Test public void testToStringContainsBatchSize() {
    ParquetConfig config = new ParquetConfig(4096, true);
    String str = config.toString();
    assertNotNull(str);
    assertTrue(str.contains("4096"),
        "toString should contain batch size");
    assertTrue(str.contains("true"),
        "toString should contain vectorized reader status");
    LOGGER.debug("ParquetConfig toString: {}", str);
  }

  @Test public void testSmallBatchSize() {
    ParquetConfig config = new ParquetConfig(1, false);
    assertEquals(1, config.getBatchSize());
  }

  @Test public void testLargeBatchSize() {
    ParquetConfig config = new ParquetConfig(1000000, false);
    assertEquals(1000000, config.getBatchSize());
  }
}
