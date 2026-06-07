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

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link MaterializeOptionsConfig}.
 */
@Tag("unit")
class MaterializeOptionsConfigTest {

  @Test void testDefaults() {
    MaterializeOptionsConfig config = MaterializeOptionsConfig.defaults();

    assertEquals(2, config.getThreads());
    assertEquals(100000, config.getRowGroupSize());
    assertEquals(10000, config.getBatchSize());
    assertEquals(MaterializeOptionsConfig.StagingMode.REMOTE, config.getStagingMode());
    assertFalse(config.isPreserveInsertionOrder());
    assertEquals(7, config.getEmptyResultTtlDays());
  }

  @Test void testBuilderCustomValues() {
    MaterializeOptionsConfig config = MaterializeOptionsConfig.builder()
        .threads(8)
        .rowGroupSize(200000)
        .batchSize(50000)
        .stagingMode(MaterializeOptionsConfig.StagingMode.LOCAL)
        .preserveInsertionOrder(true)
        .emptyResultTtlDays(14)
        .build();

    assertEquals(8, config.getThreads());
    assertEquals(200000, config.getRowGroupSize());
    assertEquals(50000, config.getBatchSize());
    assertEquals(MaterializeOptionsConfig.StagingMode.LOCAL, config.getStagingMode());
    assertTrue(config.isPreserveInsertionOrder());
    assertEquals(14, config.getEmptyResultTtlDays());
  }

  @Test void testBuilderZeroValuesUseDefaults() {
    MaterializeOptionsConfig config = MaterializeOptionsConfig.builder()
        .threads(0)
        .rowGroupSize(0)
        .batchSize(0)
        .emptyResultTtlDays(0)
        .build();

    assertEquals(2, config.getThreads());
    assertEquals(100000, config.getRowGroupSize());
    assertEquals(10000, config.getBatchSize());
    assertEquals(7, config.getEmptyResultTtlDays());
  }

  @Test void testEmptyResultTtlMillis() {
    MaterializeOptionsConfig config = MaterializeOptionsConfig.builder()
        .emptyResultTtlDays(7)
        .build();

    long expectedMillis = 7L * 24L * 60L * 60L * 1000L;
    assertEquals(expectedMillis, config.getEmptyResultTtlMillis());
  }

  @Test void testFromMap() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("threads", 4);
    map.put("rowGroupSize", 50000);
    map.put("batchSize", 25000);
    map.put("stagingMode", "local");
    map.put("preserveInsertionOrder", Boolean.TRUE);
    map.put("emptyResultTtlDays", 30);

    MaterializeOptionsConfig config = MaterializeOptionsConfig.fromMap(map);
    assertNotNull(config);
    assertEquals(4, config.getThreads());
    assertEquals(50000, config.getRowGroupSize());
    assertEquals(25000, config.getBatchSize());
    assertEquals(MaterializeOptionsConfig.StagingMode.LOCAL, config.getStagingMode());
    assertTrue(config.isPreserveInsertionOrder());
    assertEquals(30, config.getEmptyResultTtlDays());
  }

  @Test void testFromMapNull() {
    MaterializeOptionsConfig config = MaterializeOptionsConfig.fromMap(null);
    assertNotNull(config);
    // Should return defaults
    assertEquals(2, config.getThreads());
  }

  @Test void testFromMapEmpty() {
    MaterializeOptionsConfig config =
        MaterializeOptionsConfig.fromMap(new HashMap<String, Object>());
    assertNotNull(config);
    assertEquals(2, config.getThreads());
  }

  @Test void testFromMapInvalidStagingMode() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("stagingMode", "invalid_mode");

    MaterializeOptionsConfig config = MaterializeOptionsConfig.fromMap(map);
    assertNotNull(config);
    // Should use default REMOTE
    assertEquals(MaterializeOptionsConfig.StagingMode.REMOTE, config.getStagingMode());
  }

  @Test void testFromMapRemoteStagingMode() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("stagingMode", "REMOTE");

    MaterializeOptionsConfig config = MaterializeOptionsConfig.fromMap(map);
    assertEquals(MaterializeOptionsConfig.StagingMode.REMOTE, config.getStagingMode());
  }

  @Test void testStagingModeValues() {
    assertEquals(MaterializeOptionsConfig.StagingMode.LOCAL,
        MaterializeOptionsConfig.StagingMode.valueOf("LOCAL"));
    assertEquals(MaterializeOptionsConfig.StagingMode.REMOTE,
        MaterializeOptionsConfig.StagingMode.valueOf("REMOTE"));
  }
}
