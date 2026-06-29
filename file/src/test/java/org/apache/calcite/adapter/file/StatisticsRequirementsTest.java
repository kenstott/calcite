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
package org.apache.calcite.adapter.file;

import org.apache.calcite.adapter.file.statistics.ColumnStatistics;
import org.apache.calcite.adapter.file.statistics.HyperLogLogSketch;
import org.apache.calcite.adapter.file.statistics.StatisticsConfig;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Exact-assertion recode of the weak file-adapter statistics tests (FILE-120, FILE-121,
 * FILE-040, FILE-126), pinning the real contracts of {@link HyperLogLogSketch},
 * {@link StatisticsConfig}, and {@link ColumnStatistics}. Hermetic: no JDBC, no files.
 */
@Tag("unit")
public class StatisticsRequirementsTest {

  // FILE-120 — HyperLogLogSketch core contract.

  @Test @Tag("FILE-120") void defaultPrecisionIs14With16384Buckets() {
    HyperLogLogSketch sketch = new HyperLogLogSketch();
    assertEquals(14, sketch.getPrecision(), "default precision is 14 bits");
    assertEquals(16384, sketch.getNumBuckets(), "2^14 == 16384 buckets");
  }

  @Test @Tag("FILE-120") void constructorRejectsPrecisionOutOfRange() {
    assertThrows(IllegalArgumentException.class, () -> new HyperLogLogSketch(3));
    assertThrows(IllegalArgumentException.class, () -> new HyperLogLogSketch(17));
    assertEquals(4, new HyperLogLogSketch(4).getPrecision());
    assertEquals(16, new HyperLogLogSketch(16).getPrecision());
  }

  @Test @Tag("FILE-120") void mergeRequiresEqualPrecision() {
    HyperLogLogSketch a = new HyperLogLogSketch(12);
    HyperLogLogSketch b = new HyperLogLogSketch(14);
    assertThrows(IllegalArgumentException.class, () -> a.merge(b));
  }

  @Test @Tag("FILE-120") void mergeTakesPerBucketMax() {
    int precision = 4; // 16 buckets
    byte[] left = new byte[16];
    byte[] right = new byte[16];
    left[0] = (byte) 5;
    left[1] = (byte) 2;
    right[0] = (byte) 3;
    right[1] = (byte) 7;
    HyperLogLogSketch a = new HyperLogLogSketch(precision, left);
    HyperLogLogSketch b = new HyperLogLogSketch(precision, right);

    a.merge(b);

    byte[] merged = a.getBuckets();
    assertEquals((byte) 5, merged[0], "bucket 0 keeps the larger left value");
    assertEquals((byte) 7, merged[1], "bucket 1 takes the larger right value");
  }

  @Test @Tag("FILE-120") void fromEstimateReturnsExactEstimate() {
    assertEquals(0L, HyperLogLogSketch.fromEstimate(0).getEstimate());
    assertEquals(1234567L, HyperLogLogSketch.fromEstimate(1234567).getEstimate());
  }

  @Test @Tag("FILE-120") void addSkipsNullValues() {
    HyperLogLogSketch sketch = new HyperLogLogSketch(12);
    sketch.add(null);
    assertEquals(0L, sketch.getEstimate(), "null is skipped, sketch stays empty");
  }

  // FILE-121 — StatisticsConfig two-layer behavior.

  @Test @Tag("FILE-121") void defaultConfigEnablesHll() {
    assertTrue(StatisticsConfig.DEFAULT.isHllEnabled(), "DEFAULT enables HLL");
  }

  @Test @Tag("FILE-121") void fromSystemPropertiesDefaultsWhenUnset() {
    StatisticsConfig config = StatisticsConfig.fromSystemProperties();
    if (System.getProperty("calcite.file.statistics.hll.enabled") == null) {
      assertEquals(false, config.isHllEnabled(), "fromSystemProperties defaults hll.enabled to false");
    }
    if (System.getProperty("calcite.file.statistics.hll.precision") == null) {
      assertEquals(14, config.getHllPrecision(), "defaults hll.precision to 14");
    }
    if (System.getProperty("calcite.file.statistics.hll.threshold") == null) {
      assertEquals(1000L, config.getHllThreshold(), "defaults hll.threshold to 1000");
    }
  }

  @Test @Tag("FILE-121") void builderHllPrecisionRejectsOutOfRange() {
    assertThrows(IllegalArgumentException.class, () -> new StatisticsConfig.Builder().hllPrecision(3));
    assertThrows(IllegalArgumentException.class, () -> new StatisticsConfig.Builder().hllPrecision(17));
    assertEquals(4, new StatisticsConfig.Builder().hllPrecision(4).build().getHllPrecision());
    assertEquals(16, new StatisticsConfig.Builder().hllPrecision(16).build().getHllPrecision());
  }

  // FILE-040 — accuracy + serialize/restore.

  @Test @Tag("FILE-040") void mergeOfDisjointSetsEstimatesCombinedCardinality() {
    int precision = 12;
    int perSet = 5000;
    HyperLogLogSketch a = new HyperLogLogSketch(precision);
    HyperLogLogSketch b = new HyperLogLogSketch(precision);
    for (int i = 0; i < perSet; i++) {
      a.add("a-" + i);
      b.add("b-" + i);
    }
    a.merge(b);

    long trueCount = perSet * 2L;
    long estimate = a.getEstimate();
    double error = Math.abs(estimate - trueCount) / (double) trueCount;
    assertTrue(error <= 0.10,
        "merged estimate " + estimate + " within 10% of " + trueCount + " (error=" + error + ")");
  }

  @Test @Tag("FILE-040") void serializeRestorePreservesState() {
    int precision = 12;
    HyperLogLogSketch original = new HyperLogLogSketch(precision);
    for (int i = 0; i < 2000; i++) {
      original.add("value-" + i);
    }
    HyperLogLogSketch restored = new HyperLogLogSketch(precision, original.getBuckets());
    assertEquals(original.getPrecision(), restored.getPrecision(), "precision preserved");
    assertEquals(original.getNumBuckets(), restored.getNumBuckets(), "bucket count preserved");
    assertEquals(original.getEstimate(), restored.getEstimate(), "estimate preserved exactly");
  }

  // FILE-126 — ColumnStatistics.getDistinctCount().

  @Test @Tag("FILE-126") void distinctCountUsesHllEstimateWhenSketchPresent() {
    HyperLogLogSketch sketch = HyperLogLogSketch.fromEstimate(54321);
    ColumnStatistics stats = new ColumnStatistics("c", null, null, 0L, 1_000_000L, sketch);
    assertEquals(54321L, stats.getDistinctCount(), "distinct count is the HLL estimate");
  }

  @Test @Tag("FILE-126") void distinctCountFallsBackToMinOfThousandAndTotal() {
    ColumnStatistics big = new ColumnStatistics("c", null, null, 0L, 1_000_000L, null);
    assertEquals(1000L, big.getDistinctCount(), "no sketch, large total -> min(1000, total) == 1000");
    ColumnStatistics small = new ColumnStatistics("c", null, null, 0L, 42L, null);
    assertEquals(42L, small.getDistinctCount(), "no sketch, small total -> total");
  }
}
