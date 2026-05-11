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
 * Deep coverage tests for {@link ParquetConfig}.
 * Covers batch size computation, system properties, and configuration.
 */
@Tag("unit")
public class ParquetConfigDeepTest {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(ParquetConfigDeepTest.class);

  @Test
  public void testConstructorAndGetters() {
    ParquetConfig config = new ParquetConfig(4096, true);
    assertEquals(4096, config.getBatchSize());
    assertTrue(config.isVectorizedReaderEnabled());
  }

  @Test
  public void testConstructorWithVectorizedDisabled() {
    ParquetConfig config = new ParquetConfig(2048, false);
    assertEquals(2048, config.getBatchSize());
    assertFalse(config.isVectorizedReaderEnabled());
  }

  @Test
  public void testToString() {
    ParquetConfig config = new ParquetConfig(1024, true);
    String str = config.toString();
    assertNotNull(str);
    assertTrue(str.contains("1024"));
    assertTrue(str.contains("true"));
    assertTrue(str.contains("ParquetConfig"));
  }

  @Test
  public void testDefaultConfig() {
    ParquetConfig defaultConfig = ParquetConfig.DEFAULT;
    assertNotNull(defaultConfig);
    assertTrue(defaultConfig.getBatchSize() >= 1024);
    assertTrue(defaultConfig.getBatchSize() <= 16384);
    assertFalse(defaultConfig.isVectorizedReaderEnabled());
  }

  @Test
  public void testComputeOptimalBatchSize() {
    int batchSize = ParquetConfig.computeOptimalBatchSize();
    assertTrue(batchSize >= 1024, "Batch size should be at least 1024, got: " + batchSize);
    assertTrue(batchSize <= 16384, "Batch size should be at most 16384, got: " + batchSize);
  }

  @Test
  public void testFromSystemProperties() {
    // With default system properties
    ParquetConfig config = ParquetConfig.fromSystemProperties();
    assertNotNull(config);
    assertTrue(config.getBatchSize() >= 1024);
    assertTrue(config.getBatchSize() <= 16384);
  }

  @Test
  public void testFromSystemPropertiesWithCustomBatchSize() {
    String originalValue = System.getProperty("calcite.file.parquet.batch.size");
    try {
      System.setProperty("calcite.file.parquet.batch.size", "5000");
      ParquetConfig config = ParquetConfig.fromSystemProperties();
      assertEquals(5000, config.getBatchSize());
    } finally {
      if (originalValue != null) {
        System.setProperty("calcite.file.parquet.batch.size", originalValue);
      } else {
        System.clearProperty("calcite.file.parquet.batch.size");
      }
    }
  }

  @Test
  public void testFromSystemPropertiesWithVectorizedEnabled() {
    String originalValue = System.getProperty("parquet.enable.vectorized.reader");
    try {
      System.setProperty("parquet.enable.vectorized.reader", "true");
      ParquetConfig config = ParquetConfig.fromSystemProperties();
      assertTrue(config.isVectorizedReaderEnabled());
    } finally {
      if (originalValue != null) {
        System.setProperty("parquet.enable.vectorized.reader", originalValue);
      } else {
        System.clearProperty("parquet.enable.vectorized.reader");
      }
    }
  }

  @Test
  public void testMinBatchSize() {
    ParquetConfig config = new ParquetConfig(1, false);
    assertEquals(1, config.getBatchSize());
  }

  @Test
  public void testLargeBatchSize() {
    ParquetConfig config = new ParquetConfig(1000000, false);
    assertEquals(1000000, config.getBatchSize());
  }

  @Test
  public void testToStringWithVectorizedDisabled() {
    ParquetConfig config = new ParquetConfig(8192, false);
    String str = config.toString();
    assertTrue(str.contains("8192"));
    assertTrue(str.contains("false"));
  }
}
