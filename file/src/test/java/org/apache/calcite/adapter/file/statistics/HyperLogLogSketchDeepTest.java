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

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Deep coverage tests for HyperLogLogSketch covering precision ranges,
 * merge, serialization, edge cases, and all add methods.
 */
@Tag("unit")
public class HyperLogLogSketchDeepTest {

  // ===== Construction =====

  @Test void testDefaultPrecision() {
    HyperLogLogSketch hll = new HyperLogLogSketch();
    assertEquals(14, hll.getPrecision());
    assertEquals(1 << 14, hll.getNumBuckets());
  }

  @Test void testMinPrecision() {
    HyperLogLogSketch hll = new HyperLogLogSketch(4);
    assertEquals(4, hll.getPrecision());
    assertEquals(16, hll.getNumBuckets());
  }

  @Test void testMaxPrecision() {
    HyperLogLogSketch hll = new HyperLogLogSketch(16);
    assertEquals(16, hll.getPrecision());
    assertEquals(65536, hll.getNumBuckets());
  }

  @Test void testPrecisionTooLow() {
    assertThrows(IllegalArgumentException.class, () -> new HyperLogLogSketch(3));
  }

  @Test void testPrecisionTooHigh() {
    assertThrows(IllegalArgumentException.class, () -> new HyperLogLogSketch(17));
  }

  @Test void testConstructFromBuckets() {
    byte[] originalBuckets = new byte[1 << 8]; // 256 buckets for precision 8
    originalBuckets[0] = 5;
    originalBuckets[1] = 3;
    HyperLogLogSketch hll = new HyperLogLogSketch(8, originalBuckets);
    assertEquals(8, hll.getPrecision());
    byte[] retrieved = hll.getBuckets();
    assertEquals(5, retrieved[0]);
    assertEquals(3, retrieved[1]);
    // Should be a copy, not the same reference
    assertNotSame(originalBuckets, retrieved);
  }

  @Test void testFromEstimate() {
    HyperLogLogSketch hll = HyperLogLogSketch.fromEstimate(12345);
    assertEquals(12345, hll.getEstimate());
    assertEquals(14, hll.getPrecision()); // default precision
  }

  @Test void testFromEstimateZero() {
    HyperLogLogSketch hll = HyperLogLogSketch.fromEstimate(0);
    assertEquals(0, hll.getEstimate());
  }

  // ===== Adding values =====

  @Test void testAddNull() {
    HyperLogLogSketch hll = new HyperLogLogSketch();
    long before = hll.getEstimate();
    hll.add(null);
    assertEquals(before, hll.getEstimate()); // Null should be skipped
  }

  @Test void testAddString() {
    HyperLogLogSketch hll = new HyperLogLogSketch();
    hll.addString("hello");
    hll.addString("world");
    assertTrue(hll.getEstimate() > 0);
  }

  @Test void testAddNumber() {
    HyperLogLogSketch hll = new HyperLogLogSketch();
    hll.addNumber(42);
    hll.addNumber(100);
    assertTrue(hll.getEstimate() > 0);
  }

  @Test void testAddMixedTypes() {
    HyperLogLogSketch hll = new HyperLogLogSketch();
    hll.add("string");
    hll.add(42);
    hll.add(3.14);
    hll.add(true); // Uses toString path
    assertTrue(hll.getEstimate() > 0);
  }

  @Test void testAddDuplicatesSameEstimate() {
    HyperLogLogSketch hll = new HyperLogLogSketch();
    for (int i = 0; i < 1000; i++) {
      hll.add("same_value");
    }
    // Estimate should be ~1 for all duplicates
    assertTrue(hll.getEstimate() <= 5);
  }

  @Test void testAddManyDistinctValues() {
    HyperLogLogSketch hll = new HyperLogLogSketch();
    int count = 10000;
    for (int i = 0; i < count; i++) {
      hll.add("value_" + i);
    }
    // Should be within ~20% of actual count for precision 14
    long estimate = hll.getEstimate();
    assertTrue(estimate > count * 0.8, "Estimate " + estimate + " too low for " + count);
    assertTrue(estimate < count * 1.2, "Estimate " + estimate + " too high for " + count);
  }

  // ===== Estimate ranges =====

  @Test void testEstimateEmptySketch() {
    HyperLogLogSketch hll = new HyperLogLogSketch();
    // Empty sketch - all buckets are zero, triggers small range correction
    long estimate = hll.getEstimate();
    assertEquals(0, estimate);
  }

  @Test void testEstimateSmallRange() {
    // Small range: rawEstimate <= 2.5 * numBuckets
    HyperLogLogSketch hll = new HyperLogLogSketch(4); // 16 buckets
    for (int i = 0; i < 10; i++) {
      hll.add("small_" + i);
    }
    assertTrue(hll.getEstimate() > 0);
  }

  @Test void testEstimateIntermediateRange() {
    // Intermediate range: need many distinct values
    HyperLogLogSketch hll = new HyperLogLogSketch(4); // 16 buckets
    for (int i = 0; i < 1000; i++) {
      hll.add("medium_" + i);
    }
    assertTrue(hll.getEstimate() > 0);
  }

  // ===== Merge =====

  @Test void testMerge() {
    HyperLogLogSketch hll1 = new HyperLogLogSketch(8);
    HyperLogLogSketch hll2 = new HyperLogLogSketch(8);

    for (int i = 0; i < 500; i++) {
      hll1.add("set1_" + i);
    }
    for (int i = 0; i < 500; i++) {
      hll2.add("set2_" + i);
    }

    long estimate1 = hll1.getEstimate();
    long estimate2 = hll2.getEstimate();

    hll1.merge(hll2);
    long merged = hll1.getEstimate();

    // Merged should be approximately the union
    assertTrue(merged >= Math.max(estimate1, estimate2));
  }

  @Test void testMergeDifferentPrecisionThrows() {
    HyperLogLogSketch hll1 = new HyperLogLogSketch(8);
    HyperLogLogSketch hll2 = new HyperLogLogSketch(10);
    assertThrows(IllegalArgumentException.class, () -> hll1.merge(hll2));
  }

  @Test void testMergeEmptyIntoPopulated() {
    HyperLogLogSketch hll1 = new HyperLogLogSketch(8);
    for (int i = 0; i < 100; i++) {
      hll1.add("val_" + i);
    }
    long beforeMerge = hll1.getEstimate();

    HyperLogLogSketch empty = new HyperLogLogSketch(8);
    hll1.merge(empty);

    // Should not change estimate
    assertEquals(beforeMerge, hll1.getEstimate());
  }

  // ===== Serialization =====

  @Test void testGetBucketsIsCopy() {
    HyperLogLogSketch hll = new HyperLogLogSketch(4);
    hll.add("value");
    byte[] buckets1 = hll.getBuckets();
    byte[] buckets2 = hll.getBuckets();
    assertNotSame(buckets1, buckets2);
    assertArrayEquals(buckets1, buckets2);
  }

  @Test void testRoundTripSerialize() {
    HyperLogLogSketch original = new HyperLogLogSketch(10);
    for (int i = 0; i < 500; i++) {
      original.add("item_" + i);
    }

    byte[] buckets = original.getBuckets();
    HyperLogLogSketch restored = new HyperLogLogSketch(10, buckets);

    assertEquals(original.getEstimate(), restored.getEstimate());
    assertEquals(original.getPrecision(), restored.getPrecision());
  }

  // ===== Memory usage =====

  @Test void testMemoryUsage() {
    HyperLogLogSketch hll = new HyperLogLogSketch(14);
    int memory = hll.getMemoryUsage();
    assertEquals((1 << 14) + 64, memory);
  }

  @Test void testMemoryUsageSmall() {
    HyperLogLogSketch hll = new HyperLogLogSketch(4);
    assertEquals(16 + 64, hll.getMemoryUsage());
  }

  // ===== Alpha values =====

  @Test void testAlphaForDifferentPrecisions() {
    // Precision 4 -> 16 buckets -> alpha = 0.673
    HyperLogLogSketch hll4 = new HyperLogLogSketch(4);
    // Precision 5 -> 32 buckets -> alpha = 0.697
    HyperLogLogSketch hll5 = new HyperLogLogSketch(5);
    // Precision 6 -> 64 buckets -> alpha = 0.709
    HyperLogLogSketch hll6 = new HyperLogLogSketch(6);
    // Precision 7 -> 128 buckets -> alpha = 0.7213 / (1 + 1.079/128)
    HyperLogLogSketch hll7 = new HyperLogLogSketch(7);

    // These should all produce different but valid estimates
    hll4.add("test");
    hll5.add("test");
    hll6.add("test");
    hll7.add("test");
    // Just verify no exceptions
    assertTrue(hll4.getEstimate() >= 0);
    assertTrue(hll5.getEstimate() >= 0);
    assertTrue(hll6.getEstimate() >= 0);
    assertTrue(hll7.getEstimate() >= 0);
  }

  // ===== toString =====

  @Test void testToString() {
    HyperLogLogSketch hll = new HyperLogLogSketch(8);
    String str = hll.toString();
    assertTrue(str.contains("precision=8"));
    assertTrue(str.contains("buckets=256"));
    assertTrue(str.contains("estimate="));
    assertTrue(str.contains("memory="));
  }

  @Test void testToStringFromEstimate() {
    HyperLogLogSketch hll = HyperLogLogSketch.fromEstimate(999);
    String str = hll.toString();
    assertTrue(str.contains("estimate=999"));
  }
}
