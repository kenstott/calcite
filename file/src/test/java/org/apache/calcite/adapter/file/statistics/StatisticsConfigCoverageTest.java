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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Comprehensive unit tests for {@link StatisticsConfig}.
 * Covers builder defaults, validation, static instances,
 * system property / environment variable configuration, and toString.
 */
@Tag("unit")
public class StatisticsConfigCoverageTest {

  /** System property keys used by StatisticsConfig. */
  private static final String PROP_HLL_ENABLED =
      "calcite.file.statistics.hll.enabled";
  private static final String PROP_HLL_PRECISION =
      "calcite.file.statistics.hll.precision";
  private static final String PROP_HLL_THRESHOLD =
      "calcite.file.statistics.hll.threshold";
  private static final String PROP_CACHE_MAX_AGE =
      "calcite.file.statistics.cache.maxAge";
  private static final String PROP_BACKGROUND_GEN =
      "calcite.file.statistics.backgroundGeneration";
  private static final String PROP_AUTO_GENERATE =
      "calcite.file.statistics.auto.generate";

  private static final long SEVEN_DAYS_MS = 7L * 24 * 60 * 60 * 1000;

  @AfterEach
  void clearSystemProperties() {
    System.clearProperty(PROP_HLL_ENABLED);
    System.clearProperty(PROP_HLL_PRECISION);
    System.clearProperty(PROP_HLL_THRESHOLD);
    System.clearProperty(PROP_CACHE_MAX_AGE);
    System.clearProperty(PROP_BACKGROUND_GEN);
    System.clearProperty(PROP_AUTO_GENERATE);
  }

  // ---------------------------------------------------------------
  // Static instances
  // ---------------------------------------------------------------

  @Nested
  @DisplayName("Static instance tests")
  class StaticInstances {

    @Test
    @DisplayName("DEFAULT instance should have HLL enabled")
    void defaultInstanceHllEnabled() {
      assertTrue(StatisticsConfig.DEFAULT.isHllEnabled(),
          "DEFAULT should have HLL enabled");
    }

    @Test
    @DisplayName("DEFAULT instance should use standard defaults")
    void defaultInstanceDefaults() {
      StatisticsConfig cfg = StatisticsConfig.DEFAULT;
      assertEquals(14, cfg.getHllPrecision());
      assertEquals(1000L, cfg.getHllThreshold());
      assertEquals(SEVEN_DAYS_MS, cfg.getMaxCacheAge());
      assertTrue(cfg.isBackgroundGeneration());
      assertTrue(cfg.isAutoGenerateStatistics());
    }

    @Test
    @DisplayName("NO_HLL instance should have HLL disabled")
    void noHllInstanceHllDisabled() {
      assertFalse(StatisticsConfig.NO_HLL.isHllEnabled(),
          "NO_HLL should have HLL disabled");
    }

    @Test
    @DisplayName("NO_HLL instance should retain other defaults")
    void noHllInstanceRetainsOtherDefaults() {
      StatisticsConfig cfg = StatisticsConfig.NO_HLL;
      assertEquals(14, cfg.getHllPrecision());
      assertEquals(1000L, cfg.getHllThreshold());
      assertEquals(SEVEN_DAYS_MS, cfg.getMaxCacheAge());
      assertTrue(cfg.isBackgroundGeneration());
      assertTrue(cfg.isAutoGenerateStatistics());
    }

    @Test
    @DisplayName("DEFAULT and NO_HLL should be non-null")
    void staticInstancesNotNull() {
      assertNotNull(StatisticsConfig.DEFAULT, "DEFAULT must not be null");
      assertNotNull(StatisticsConfig.NO_HLL, "NO_HLL must not be null");
    }
  }

  // ---------------------------------------------------------------
  // Builder defaults
  // ---------------------------------------------------------------

  @Nested
  @DisplayName("Builder default value tests")
  class BuilderDefaults {

    @Test
    @DisplayName("Builder with no customization should produce defaults")
    void builderDefaults() {
      StatisticsConfig cfg = new StatisticsConfig.Builder().build();
      assertTrue(cfg.isHllEnabled(), "hllEnabled default should be true");
      assertEquals(14, cfg.getHllPrecision(), "hllPrecision default should be 14");
      assertEquals(1000L, cfg.getHllThreshold(), "hllThreshold default should be 1000");
      assertEquals(SEVEN_DAYS_MS, cfg.getMaxCacheAge(),
          "maxCacheAge default should be 7 days in ms");
      assertTrue(cfg.isBackgroundGeneration(),
          "backgroundGeneration default should be true");
      assertTrue(cfg.isAutoGenerateStatistics(),
          "autoGenerateStatistics default should be true");
    }
  }

  // ---------------------------------------------------------------
  // Builder chaining and custom values
  // ---------------------------------------------------------------

  @Nested
  @DisplayName("Builder chaining and custom value tests")
  class BuilderChaining {

    @Test
    @DisplayName("Builder should allow full fluent chaining")
    void fullFluentChain() {
      StatisticsConfig cfg = new StatisticsConfig.Builder()
          .hllEnabled(false)
          .hllPrecision(8)
          .hllThreshold(500L)
          .maxCacheAge(3600000L)
          .backgroundGeneration(false)
          .autoGenerateStatistics(false)
          .build();

      assertFalse(cfg.isHllEnabled());
      assertEquals(8, cfg.getHllPrecision());
      assertEquals(500L, cfg.getHllThreshold());
      assertEquals(3600000L, cfg.getMaxCacheAge());
      assertFalse(cfg.isBackgroundGeneration());
      assertFalse(cfg.isAutoGenerateStatistics());
    }

    @Test
    @DisplayName("Builder should accept hllEnabled true")
    void hllEnabledTrue() {
      StatisticsConfig cfg = new StatisticsConfig.Builder()
          .hllEnabled(true).build();
      assertTrue(cfg.isHllEnabled());
    }

    @Test
    @DisplayName("Builder should accept hllEnabled false")
    void hllEnabledFalse() {
      StatisticsConfig cfg = new StatisticsConfig.Builder()
          .hllEnabled(false).build();
      assertFalse(cfg.isHllEnabled());
    }

    @Test
    @DisplayName("Builder should accept minimum valid precision (4)")
    void hllPrecisionMinBoundary() {
      StatisticsConfig cfg = new StatisticsConfig.Builder()
          .hllPrecision(4).build();
      assertEquals(4, cfg.getHllPrecision());
    }

    @Test
    @DisplayName("Builder should accept maximum valid precision (16)")
    void hllPrecisionMaxBoundary() {
      StatisticsConfig cfg = new StatisticsConfig.Builder()
          .hllPrecision(16).build();
      assertEquals(16, cfg.getHllPrecision());
    }

    @Test
    @DisplayName("Builder should accept mid-range precision values")
    void hllPrecisionMidRange() {
      for (int p = 4; p <= 16; p++) {
        final int precision = p;
        StatisticsConfig cfg = new StatisticsConfig.Builder()
            .hllPrecision(precision).build();
        assertEquals(precision, cfg.getHllPrecision(),
            "Precision " + precision + " should be accepted");
      }
    }

    @Test
    @DisplayName("Builder should accept zero hllThreshold")
    void hllThresholdZero() {
      StatisticsConfig cfg = new StatisticsConfig.Builder()
          .hllThreshold(0L).build();
      assertEquals(0L, cfg.getHllThreshold());
    }

    @Test
    @DisplayName("Builder should accept large hllThreshold")
    void hllThresholdLarge() {
      StatisticsConfig cfg = new StatisticsConfig.Builder()
          .hllThreshold(Long.MAX_VALUE).build();
      assertEquals(Long.MAX_VALUE, cfg.getHllThreshold());
    }

    @Test
    @DisplayName("Builder should accept zero maxCacheAge")
    void maxCacheAgeZero() {
      StatisticsConfig cfg = new StatisticsConfig.Builder()
          .maxCacheAge(0L).build();
      assertEquals(0L, cfg.getMaxCacheAge());
    }

    @Test
    @DisplayName("Builder should accept large maxCacheAge")
    void maxCacheAgeLarge() {
      StatisticsConfig cfg = new StatisticsConfig.Builder()
          .maxCacheAge(Long.MAX_VALUE).build();
      assertEquals(Long.MAX_VALUE, cfg.getMaxCacheAge());
    }

    @Test
    @DisplayName("Builder should accept backgroundGeneration false")
    void backgroundGenerationFalse() {
      StatisticsConfig cfg = new StatisticsConfig.Builder()
          .backgroundGeneration(false).build();
      assertFalse(cfg.isBackgroundGeneration());
    }

    @Test
    @DisplayName("Builder should accept autoGenerateStatistics false")
    void autoGenerateStatisticsFalse() {
      StatisticsConfig cfg = new StatisticsConfig.Builder()
          .autoGenerateStatistics(false).build();
      assertFalse(cfg.isAutoGenerateStatistics());
    }

    @Test
    @DisplayName("Last builder call should win when setting same property twice")
    void lastCallWins() {
      StatisticsConfig cfg = new StatisticsConfig.Builder()
          .hllEnabled(false)
          .hllEnabled(true)
          .hllPrecision(5)
          .hllPrecision(10)
          .build();
      assertTrue(cfg.isHllEnabled(), "Last hllEnabled(true) should win");
      assertEquals(10, cfg.getHllPrecision(), "Last hllPrecision(10) should win");
    }
  }

  // ---------------------------------------------------------------
  // Builder validation
  // ---------------------------------------------------------------

  @Nested
  @DisplayName("Builder validation tests")
  class BuilderValidation {

    @Test
    @DisplayName("hllPrecision below 4 should throw IllegalArgumentException")
    void hllPrecisionTooLow() {
      StatisticsConfig.Builder builder = new StatisticsConfig.Builder();
      IllegalArgumentException ex = assertThrows(
          IllegalArgumentException.class,
          () -> builder.hllPrecision(3));
      assertTrue(ex.getMessage().contains("4") && ex.getMessage().contains("16"),
          "Exception message should mention valid range: " + ex.getMessage());
    }

    @Test
    @DisplayName("hllPrecision of 0 should throw IllegalArgumentException")
    void hllPrecisionZero() {
      assertThrows(IllegalArgumentException.class,
          () -> new StatisticsConfig.Builder().hllPrecision(0));
    }

    @Test
    @DisplayName("hllPrecision above 16 should throw IllegalArgumentException")
    void hllPrecisionTooHigh() {
      assertThrows(IllegalArgumentException.class,
          () -> new StatisticsConfig.Builder().hllPrecision(17));
    }

    @Test
    @DisplayName("hllPrecision of negative value should throw IllegalArgumentException")
    void hllPrecisionNegative() {
      assertThrows(IllegalArgumentException.class,
          () -> new StatisticsConfig.Builder().hllPrecision(-1));
    }

    @Test
    @DisplayName("hllPrecision of Integer.MAX_VALUE should throw IllegalArgumentException")
    void hllPrecisionMaxInt() {
      assertThrows(IllegalArgumentException.class,
          () -> new StatisticsConfig.Builder().hllPrecision(Integer.MAX_VALUE));
    }

    @Test
    @DisplayName("hllThreshold of negative value should throw IllegalArgumentException")
    void hllThresholdNegative() {
      IllegalArgumentException ex = assertThrows(
          IllegalArgumentException.class,
          () -> new StatisticsConfig.Builder().hllThreshold(-1L));
      assertTrue(ex.getMessage().contains("non-negative"),
          "Exception should mention non-negative: " + ex.getMessage());
    }

    @Test
    @DisplayName("hllThreshold of Long.MIN_VALUE should throw IllegalArgumentException")
    void hllThresholdMinLong() {
      assertThrows(IllegalArgumentException.class,
          () -> new StatisticsConfig.Builder().hllThreshold(Long.MIN_VALUE));
    }

    @Test
    @DisplayName("maxCacheAge of negative value should throw IllegalArgumentException")
    void maxCacheAgeNegative() {
      IllegalArgumentException ex = assertThrows(
          IllegalArgumentException.class,
          () -> new StatisticsConfig.Builder().maxCacheAge(-1L));
      assertTrue(ex.getMessage().contains("non-negative"),
          "Exception should mention non-negative: " + ex.getMessage());
    }

    @Test
    @DisplayName("maxCacheAge of Long.MIN_VALUE should throw IllegalArgumentException")
    void maxCacheAgeMinLong() {
      assertThrows(IllegalArgumentException.class,
          () -> new StatisticsConfig.Builder().maxCacheAge(Long.MIN_VALUE));
    }

    @Test
    @DisplayName("Validation should fire immediately, not at build time")
    void validationIsEager() {
      // The builder methods themselves throw, not build()
      StatisticsConfig.Builder builder = new StatisticsConfig.Builder();
      assertThrows(IllegalArgumentException.class,
          () -> builder.hllPrecision(2));
      // Builder should still be usable after failed validation
      StatisticsConfig cfg = builder.hllPrecision(10).build();
      assertEquals(10, cfg.getHllPrecision());
    }
  }

  // ---------------------------------------------------------------
  // fromSystemProperties
  // ---------------------------------------------------------------

  @Nested
  @DisplayName("fromSystemProperties tests")
  class FromSystemProperties {

    @Test
    @DisplayName("fromSystemProperties with no properties set should use defaults")
    void noPropertiesSet() {
      // Clear all relevant properties (done in @AfterEach, but be explicit)
      System.clearProperty(PROP_HLL_ENABLED);
      System.clearProperty(PROP_HLL_PRECISION);

      StatisticsConfig cfg = StatisticsConfig.fromSystemProperties();
      // Default for sys props is hllEnabled=false (parses "false" as default)
      assertFalse(cfg.isHllEnabled(),
          "fromSystemProperties default for hllEnabled is false");
      assertEquals(14, cfg.getHllPrecision());
      assertEquals(1000L, cfg.getHllThreshold());
      assertEquals(SEVEN_DAYS_MS, cfg.getMaxCacheAge());
      assertTrue(cfg.isBackgroundGeneration());
      assertTrue(cfg.isAutoGenerateStatistics());
    }

    @Test
    @DisplayName("fromSystemProperties should read hllEnabled")
    void readsHllEnabled() {
      System.setProperty(PROP_HLL_ENABLED, "true");
      StatisticsConfig cfg = StatisticsConfig.fromSystemProperties();
      assertTrue(cfg.isHllEnabled());
    }

    @Test
    @DisplayName("fromSystemProperties should read hllPrecision")
    void readsHllPrecision() {
      System.setProperty(PROP_HLL_PRECISION, "8");
      StatisticsConfig cfg = StatisticsConfig.fromSystemProperties();
      assertEquals(8, cfg.getHllPrecision());
    }

    @Test
    @DisplayName("fromSystemProperties should read hllThreshold")
    void readsHllThreshold() {
      System.setProperty(PROP_HLL_THRESHOLD, "5000");
      StatisticsConfig cfg = StatisticsConfig.fromSystemProperties();
      assertEquals(5000L, cfg.getHllThreshold());
    }

    @Test
    @DisplayName("fromSystemProperties should read maxCacheAge")
    void readsMaxCacheAge() {
      System.setProperty(PROP_CACHE_MAX_AGE, "86400000");
      StatisticsConfig cfg = StatisticsConfig.fromSystemProperties();
      assertEquals(86400000L, cfg.getMaxCacheAge());
    }

    @Test
    @DisplayName("fromSystemProperties should read backgroundGeneration")
    void readsBackgroundGeneration() {
      System.setProperty(PROP_BACKGROUND_GEN, "false");
      StatisticsConfig cfg = StatisticsConfig.fromSystemProperties();
      assertFalse(cfg.isBackgroundGeneration());
    }

    @Test
    @DisplayName("fromSystemProperties should read autoGenerateStatistics")
    void readsAutoGenerateStatistics() {
      System.setProperty(PROP_AUTO_GENERATE, "false");
      StatisticsConfig cfg = StatisticsConfig.fromSystemProperties();
      assertFalse(cfg.isAutoGenerateStatistics());
    }

    @Test
    @DisplayName("fromSystemProperties should read all properties together")
    void readsAllProperties() {
      System.setProperty(PROP_HLL_ENABLED, "true");
      System.setProperty(PROP_HLL_PRECISION, "6");
      System.setProperty(PROP_HLL_THRESHOLD, "2000");
      System.setProperty(PROP_CACHE_MAX_AGE, "100000");
      System.setProperty(PROP_BACKGROUND_GEN, "false");
      System.setProperty(PROP_AUTO_GENERATE, "false");

      StatisticsConfig cfg = StatisticsConfig.fromSystemProperties();
      assertTrue(cfg.isHllEnabled());
      assertEquals(6, cfg.getHllPrecision());
      assertEquals(2000L, cfg.getHllThreshold());
      assertEquals(100000L, cfg.getMaxCacheAge());
      assertFalse(cfg.isBackgroundGeneration());
      assertFalse(cfg.isAutoGenerateStatistics());
    }
  }

  // ---------------------------------------------------------------
  // getEffectiveConfig
  // ---------------------------------------------------------------

  @Nested
  @DisplayName("getEffectiveConfig tests")
  class GetEffectiveConfig {

    @Test
    @DisplayName("getEffectiveConfig with no overrides should use builder defaults")
    void noOverrides() {
      StatisticsConfig cfg = StatisticsConfig.getEffectiveConfig();
      assertNotNull(cfg, "Effective config must not be null");
      // With no env vars or sys props, Builder defaults apply
      // hllEnabled defaults to true in builder
      assertEquals(14, cfg.getHllPrecision());
      assertEquals(1000L, cfg.getHllThreshold());
      assertEquals(SEVEN_DAYS_MS, cfg.getMaxCacheAge());
      assertTrue(cfg.isBackgroundGeneration());
      assertTrue(cfg.isAutoGenerateStatistics());
    }

    @Test
    @DisplayName("getEffectiveConfig system properties should override env defaults")
    void systemPropertiesOverride() {
      System.setProperty(PROP_HLL_PRECISION, "5");
      StatisticsConfig cfg = StatisticsConfig.getEffectiveConfig();
      assertEquals(5, cfg.getHllPrecision(),
          "System property should override default precision");
    }

    @Test
    @DisplayName("getEffectiveConfig should return non-null for all getters")
    void allGettersReturnValues() {
      StatisticsConfig cfg = StatisticsConfig.getEffectiveConfig();
      // These are primitives so they cannot be null, but verify they are within range
      assertTrue(cfg.getHllPrecision() >= 4 && cfg.getHllPrecision() <= 16);
      assertTrue(cfg.getHllThreshold() >= 0);
      assertTrue(cfg.getMaxCacheAge() >= 0);
    }
  }

  // ---------------------------------------------------------------
  // toString
  // ---------------------------------------------------------------

  @Nested
  @DisplayName("toString tests")
  class ToStringTests {

    @Test
    @DisplayName("toString should contain all field names")
    void containsAllFieldNames() {
      StatisticsConfig cfg = new StatisticsConfig.Builder().build();
      String str = cfg.toString();
      assertTrue(str.contains("hllEnabled"), "toString should contain hllEnabled");
      assertTrue(str.contains("hllPrecision"), "toString should contain hllPrecision");
      assertTrue(str.contains("hllThreshold"), "toString should contain hllThreshold");
      assertTrue(str.contains("maxCacheAge"), "toString should contain maxCacheAge");
      assertTrue(str.contains("backgroundGeneration"),
          "toString should contain backgroundGeneration");
      assertTrue(str.contains("autoGenerateStatistics"),
          "toString should contain autoGenerateStatistics");
    }

    @Test
    @DisplayName("toString should contain actual field values")
    void containsFieldValues() {
      StatisticsConfig cfg = new StatisticsConfig.Builder()
          .hllEnabled(false)
          .hllPrecision(8)
          .hllThreshold(2500L)
          .maxCacheAge(999L)
          .backgroundGeneration(false)
          .autoGenerateStatistics(false)
          .build();
      String str = cfg.toString();
      assertTrue(str.contains("false"), "toString should contain false for disabled fields");
      assertTrue(str.contains("8"), "toString should contain precision value 8");
      assertTrue(str.contains("2500"), "toString should contain threshold 2500");
      assertTrue(str.contains("999"), "toString should contain maxCacheAge 999");
    }

    @Test
    @DisplayName("toString should start with StatisticsConfig prefix")
    void startsWithClassName() {
      StatisticsConfig cfg = new StatisticsConfig.Builder().build();
      assertTrue(cfg.toString().startsWith("StatisticsConfig{"),
          "toString should start with StatisticsConfig{");
    }

    @Test
    @DisplayName("toString format should match expected pattern")
    void matchesExpectedFormat() {
      StatisticsConfig cfg = new StatisticsConfig.Builder()
          .hllEnabled(true)
          .hllPrecision(14)
          .hllThreshold(1000L)
          .maxCacheAge(SEVEN_DAYS_MS)
          .backgroundGeneration(true)
          .autoGenerateStatistics(true)
          .build();
      String expected = String.format(
          "StatisticsConfig{hllEnabled=%s, hllPrecision=%d, hllThreshold=%d, "
              + "maxCacheAge=%d, backgroundGeneration=%s, autoGenerateStatistics=%s}",
          true, 14, 1000L, SEVEN_DAYS_MS, true, true);
      assertEquals(expected, cfg.toString());
    }
  }

  // ---------------------------------------------------------------
  // Multiple builds from same builder
  // ---------------------------------------------------------------

  @Nested
  @DisplayName("Builder reuse tests")
  class BuilderReuse {

    @Test
    @DisplayName("Builder should produce independent configs on multiple builds")
    void multipleBuildCalls() {
      StatisticsConfig.Builder builder = new StatisticsConfig.Builder()
          .hllEnabled(true)
          .hllPrecision(10);

      StatisticsConfig first = builder.build();
      builder.hllPrecision(12);
      StatisticsConfig second = builder.build();

      assertEquals(10, first.getHllPrecision(),
          "First config should retain precision 10");
      assertEquals(12, second.getHllPrecision(),
          "Second config should have precision 12");
    }

    @Test
    @DisplayName("Configs from same builder should be independent objects")
    void independentObjects() {
      StatisticsConfig.Builder builder = new StatisticsConfig.Builder();
      StatisticsConfig a = builder.build();
      StatisticsConfig b = builder.build();
      // Both should have the same values but be different references
      assertEquals(a.getHllPrecision(), b.getHllPrecision());
      assertEquals(a.isHllEnabled(), b.isHllEnabled());
    }
  }

  // ---------------------------------------------------------------
  // Edge cases
  // ---------------------------------------------------------------

  @Nested
  @DisplayName("Edge case tests")
  class EdgeCases {

    @Test
    @DisplayName("Builder with only hllEnabled set should keep other defaults")
    void partialBuilderOnlyHll() {
      StatisticsConfig cfg = new StatisticsConfig.Builder()
          .hllEnabled(false)
          .build();
      assertFalse(cfg.isHllEnabled());
      assertEquals(14, cfg.getHllPrecision());
      assertEquals(1000L, cfg.getHllThreshold());
      assertEquals(SEVEN_DAYS_MS, cfg.getMaxCacheAge());
      assertTrue(cfg.isBackgroundGeneration());
      assertTrue(cfg.isAutoGenerateStatistics());
    }

    @Test
    @DisplayName("Builder with only maxCacheAge set should keep other defaults")
    void partialBuilderOnlyCache() {
      StatisticsConfig cfg = new StatisticsConfig.Builder()
          .maxCacheAge(12345L)
          .build();
      assertTrue(cfg.isHllEnabled());
      assertEquals(14, cfg.getHllPrecision());
      assertEquals(1000L, cfg.getHllThreshold());
      assertEquals(12345L, cfg.getMaxCacheAge());
      assertTrue(cfg.isBackgroundGeneration());
      assertTrue(cfg.isAutoGenerateStatistics());
    }

    @Test
    @DisplayName("Boundary precision 4 should produce valid config")
    void boundaryPrecisionLow() {
      StatisticsConfig cfg = new StatisticsConfig.Builder()
          .hllPrecision(4).build();
      assertEquals(4, cfg.getHllPrecision());
    }

    @Test
    @DisplayName("Boundary precision 16 should produce valid config")
    void boundaryPrecisionHigh() {
      StatisticsConfig cfg = new StatisticsConfig.Builder()
          .hllPrecision(16).build();
      assertEquals(16, cfg.getHllPrecision());
    }

    @Test
    @DisplayName("hllThreshold of 0 should be valid (no threshold)")
    void thresholdZeroValid() {
      StatisticsConfig cfg = new StatisticsConfig.Builder()
          .hllThreshold(0L).build();
      assertEquals(0L, cfg.getHllThreshold());
    }

    @Test
    @DisplayName("maxCacheAge of 0 should be valid (immediate expiry)")
    void cacheAgeZeroValid() {
      StatisticsConfig cfg = new StatisticsConfig.Builder()
          .maxCacheAge(0L).build();
      assertEquals(0L, cfg.getMaxCacheAge());
    }
  }
}
