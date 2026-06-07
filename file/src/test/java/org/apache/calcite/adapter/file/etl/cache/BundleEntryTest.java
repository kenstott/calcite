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
package org.apache.calcite.adapter.file.etl.cache;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link BundleEntry}.
 */
@Tag("unit")
public class BundleEntryTest {

  @Test void testBundledEntryCreation() {
    BundleEntry entry = BundleEntry.bundled("run-20260310T1423.bin", 1024, 2048, 1710000000L);

    assertEquals("run-20260310T1423.bin", entry.getBundleFile());
    assertEquals(1024, entry.getOffset());
    assertEquals(2048, entry.getLength());
    assertEquals(1710000000L, entry.getTimestamp());
    assertTrue(entry.isBundled());
    assertFalse(entry.isIndividualObject());
  }

  @Test void testIndividualEntryCreation() {
    BundleEntry entry = BundleEntry.individual(5000, 1710000000L);

    assertNull(entry.getBundleFile());
    assertEquals(-1, entry.getOffset());
    assertEquals(5000, entry.getLength());
    assertEquals(1710000000L, entry.getTimestamp());
    assertFalse(entry.isBundled());
    assertTrue(entry.isIndividualObject());
  }

  @Test void testBundledEntryZeroOffset() {
    BundleEntry entry = BundleEntry.bundled("bundle.bin", 0, 100, 0);

    assertEquals(0, entry.getOffset());
    assertEquals(100, entry.getLength());
    assertEquals(0, entry.getTimestamp());
    assertTrue(entry.isBundled());
  }

  @Test void testIndividualEntryZeroLength() {
    BundleEntry entry = BundleEntry.individual(0, 0);

    assertEquals(0, entry.getLength());
    assertEquals(0, entry.getTimestamp());
    assertTrue(entry.isIndividualObject());
  }

  @Test void testBundledAndIndividualAreMutuallyExclusive() {
    BundleEntry bundled = BundleEntry.bundled("test.bin", 0, 100, 1000);
    BundleEntry individual = BundleEntry.individual(200, 2000);

    assertTrue(bundled.isBundled());
    assertFalse(bundled.isIndividualObject());

    assertFalse(individual.isBundled());
    assertTrue(individual.isIndividualObject());
  }

  @Test void testLargeOffsetAndLength() {
    long largeOffset = 10L * 1024 * 1024 * 1024; // 10 GB
    long largeLength = 5L * 1024 * 1024 * 1024;  // 5 GB

    BundleEntry entry = BundleEntry.bundled("large.bin", largeOffset, largeLength, Long.MAX_VALUE);

    assertEquals(largeOffset, entry.getOffset());
    assertEquals(largeLength, entry.getLength());
    assertEquals(Long.MAX_VALUE, entry.getTimestamp());
  }
}
