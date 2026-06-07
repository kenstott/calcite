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
package org.apache.calcite.adapter.file.statistics;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Deep coverage tests for StatisticsConfig covering builder validation,
 * default values, constants, and toString.
 */
@Tag("unit")
public class StatisticsConfigDeepTest {

  // ===== Default config =====

  @Test void testDefaultConfig() {
    StatisticsConfig config = StatisticsConfig.DEFAULT;
    assertTrue(config.isHllEnabled());
    assertEquals(14, config.getHllPrecision());
    assertEquals(1000, config.getHllThreshold());
    assertTrue(config.getMaxCacheAge() > 0);
    assertTrue(config.isBackgroundGeneration());
    assertTrue(config.isAutoGenerateStatistics());
  }

  @Test void testNoHllConfig() {
    StatisticsConfig config = StatisticsConfig.NO_HLL;
    assertFalse(config.isHllEnabled());
  }

  // ===== Builder =====

  @Test void testBuilderDefaults() {
    StatisticsConfig config = new StatisticsConfig.Builder().build();
    assertTrue(config.isHllEnabled());
    assertEquals(14, config.getHllPrecision());
    assertEquals(1000, config.getHllThreshold());
    assertEquals(7 * 24 * 60 * 60 * 1000L, config.getMaxCacheAge());
    assertTrue(config.isBackgroundGeneration());
    assertTrue(config.isAutoGenerateStatistics());
  }

  @Test void testBuilderCustomValues() {
    StatisticsConfig config = new StatisticsConfig.Builder()
        .hllEnabled(false)
        .hllPrecision(10)
        .hllThreshold(500)
        .maxCacheAge(3600000)
        .backgroundGeneration(false)
        .autoGenerateStatistics(false)
        .build();

    assertFalse(config.isHllEnabled());
    assertEquals(10, config.getHllPrecision());
    assertEquals(500, config.getHllThreshold());
    assertEquals(3600000, config.getMaxCacheAge());
    assertFalse(config.isBackgroundGeneration());
    assertFalse(config.isAutoGenerateStatistics());
  }

  @Test void testBuilderHllPrecisionTooLow() {
    assertThrows(IllegalArgumentException.class,
        () -> new StatisticsConfig.Builder().hllPrecision(3));
  }

  @Test void testBuilderHllPrecisionTooHigh() {
    assertThrows(IllegalArgumentException.class,
        () -> new StatisticsConfig.Builder().hllPrecision(17));
  }

  @Test void testBuilderHllPrecisionBoundaryLow() {
    StatisticsConfig config = new StatisticsConfig.Builder().hllPrecision(4).build();
    assertEquals(4, config.getHllPrecision());
  }

  @Test void testBuilderHllPrecisionBoundaryHigh() {
    StatisticsConfig config = new StatisticsConfig.Builder().hllPrecision(16).build();
    assertEquals(16, config.getHllPrecision());
  }

  @Test void testBuilderHllThresholdNegative() {
    assertThrows(IllegalArgumentException.class,
        () -> new StatisticsConfig.Builder().hllThreshold(-1));
  }

  @Test void testBuilderHllThresholdZero() {
    StatisticsConfig config = new StatisticsConfig.Builder().hllThreshold(0).build();
    assertEquals(0, config.getHllThreshold());
  }

  @Test void testBuilderMaxCacheAgeNegative() {
    assertThrows(IllegalArgumentException.class,
        () -> new StatisticsConfig.Builder().maxCacheAge(-1));
  }

  @Test void testBuilderMaxCacheAgeZero() {
    StatisticsConfig config = new StatisticsConfig.Builder().maxCacheAge(0).build();
    assertEquals(0, config.getMaxCacheAge());
  }

  // ===== fromSystemProperties =====

  @Test void testFromSystemPropertiesDefaults() {
    // Save and clear the system property so other tests don't leak into this one
    String saved = System.getProperty("calcite.file.statistics.hll.enabled");
    try {
      System.clearProperty("calcite.file.statistics.hll.enabled");

      // Without setting any system properties, should use defaults from the property strings
      StatisticsConfig config = StatisticsConfig.fromSystemProperties();
      assertNotNull(config);
      // The method reads from system properties with defaults
      // Default hll.enabled is "false" in fromSystemProperties
      assertFalse(config.isHllEnabled());
      assertEquals(14, config.getHllPrecision());
    } finally {
      if (saved != null) {
        System.setProperty("calcite.file.statistics.hll.enabled", saved);
      }
    }
  }

  // ===== toString =====

  @Test void testToString() {
    StatisticsConfig config = new StatisticsConfig.Builder()
        .hllEnabled(true)
        .hllPrecision(12)
        .hllThreshold(2000)
        .maxCacheAge(86400000)
        .backgroundGeneration(false)
        .autoGenerateStatistics(true)
        .build();

    String str = config.toString();
    assertTrue(str.contains("hllEnabled=true"));
    assertTrue(str.contains("hllPrecision=12"));
    assertTrue(str.contains("hllThreshold=2000"));
    assertTrue(str.contains("maxCacheAge=86400000"));
    assertTrue(str.contains("backgroundGeneration=false"));
    assertTrue(str.contains("autoGenerateStatistics=true"));
  }
}
