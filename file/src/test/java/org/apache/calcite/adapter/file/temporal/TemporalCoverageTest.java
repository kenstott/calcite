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
package org.apache.calcite.adapter.file.temporal;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.sql.Timestamp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Comprehensive unit tests for temporal wrapper classes:
 * {@link LocalTimestamp} and {@link UtcTimestamp}.
 */
@Tag("unit")
public class TemporalCoverageTest {

  // =========================================================================
  // Known epoch constants for test clarity
  // =========================================================================

  /** Unix epoch: 1970-01-01 00:00:00 UTC. */
  private static final long EPOCH_ZERO = 0L;

  /** 2024-01-01 00:00:00 UTC in epoch millis. */
  private static final long JAN_1_2024_UTC = 1704067200000L;

  /** 2024-06-15 12:30:45 UTC in epoch millis. */
  private static final long JUN_15_2024_UTC = 1718454645000L;

  /** 1999-12-31 23:59:59 UTC in epoch millis. */
  private static final long DEC_31_1999_UTC = 946684799000L;

  // =========================================================================
  // LocalTimestamp tests
  // =========================================================================

  @Test
  void testLocalTimestampConstructorStoresEpochMillis() {
    LocalTimestamp ts = new LocalTimestamp(JAN_1_2024_UTC);
    assertEquals(JAN_1_2024_UTC, ts.getOriginalUtcTime());
  }

  @Test
  void testLocalTimestampEpochZero() {
    LocalTimestamp ts = new LocalTimestamp(EPOCH_ZERO);
    assertEquals(EPOCH_ZERO, ts.getOriginalUtcTime());
    assertEquals("1970-01-01 00:00:00", ts.toString());
  }

  @Test
  void testLocalTimestampJan12024() {
    LocalTimestamp ts = new LocalTimestamp(JAN_1_2024_UTC);
    assertEquals(JAN_1_2024_UTC, ts.getOriginalUtcTime());
    assertEquals("2024-01-01 00:00:00", ts.toString());
  }

  @Test
  void testLocalTimestampToStringFormatsAsUtc() {
    LocalTimestamp ts = new LocalTimestamp(JUN_15_2024_UTC);
    assertEquals("2024-06-15 12:30:45", ts.toString());
  }

  @Test
  void testLocalTimestampToUtcStringMatchesToString() {
    LocalTimestamp ts = new LocalTimestamp(JAN_1_2024_UTC);
    assertEquals(ts.toString(), ts.toUTCString());
  }

  @Test
  void testLocalTimestampToUtcStringEpochZero() {
    LocalTimestamp ts = new LocalTimestamp(EPOCH_ZERO);
    assertEquals("1970-01-01 00:00:00", ts.toUTCString());
  }

  @Test
  void testLocalTimestampToUtcStringJun2024() {
    LocalTimestamp ts = new LocalTimestamp(JUN_15_2024_UTC);
    assertEquals("2024-06-15 12:30:45", ts.toUTCString());
  }

  @Test
  void testLocalTimestampGetOriginalUtcTimeConsistency() {
    // Verify that getOriginalUtcTime always returns the exact value passed to the constructor
    long[] testValues = {EPOCH_ZERO, JAN_1_2024_UTC, JUN_15_2024_UTC, DEC_31_1999_UTC};
    for (long millis : testValues) {
      LocalTimestamp ts = new LocalTimestamp(millis);
      assertEquals(millis, ts.getOriginalUtcTime(),
          "getOriginalUtcTime should return the constructor argument for millis=" + millis);
    }
  }

  @Test
  void testLocalTimestampDec311999() {
    LocalTimestamp ts = new LocalTimestamp(DEC_31_1999_UTC);
    assertEquals(DEC_31_1999_UTC, ts.getOriginalUtcTime());
    assertEquals("1999-12-31 23:59:59", ts.toString());
  }

  @Test
  void testLocalTimestampExtendsJavaSqlTimestamp() {
    LocalTimestamp ts = new LocalTimestamp(JAN_1_2024_UTC);
    assertTrue(ts instanceof Timestamp,
        "LocalTimestamp should extend java.sql.Timestamp");
  }

  @Test
  void testLocalTimestampGetTimeReturnsConstructorValue() {
    // The superclass getTime() should return the value passed to super(utcTime)
    LocalTimestamp ts = new LocalTimestamp(JAN_1_2024_UTC);
    assertEquals(JAN_1_2024_UTC, ts.getTime());
  }

  @Test
  void testLocalTimestampToStringAndToUtcStringAlwaysAgree() {
    // toString() and toUTCString() use the same logic, so they should always match
    long[] testValues = {EPOCH_ZERO, JAN_1_2024_UTC, JUN_15_2024_UTC, DEC_31_1999_UTC};
    for (long millis : testValues) {
      LocalTimestamp ts = new LocalTimestamp(millis);
      assertEquals(ts.toString(), ts.toUTCString(),
          "toString and toUTCString should match for millis=" + millis);
    }
  }

  // =========================================================================
  // UtcTimestamp tests
  // =========================================================================

  @Test
  void testUtcTimestampEpochZero() {
    UtcTimestamp ts = new UtcTimestamp(EPOCH_ZERO);
    assertEquals("1970-01-01 00:00:00", ts.toString());
  }

  @Test
  void testUtcTimestampJan12024() {
    UtcTimestamp ts = new UtcTimestamp(JAN_1_2024_UTC);
    assertEquals("2024-01-01 00:00:00", ts.toString());
  }

  @Test
  void testUtcTimestampToStringFormatsAsUtc() {
    UtcTimestamp ts = new UtcTimestamp(JUN_15_2024_UTC);
    assertEquals("2024-06-15 12:30:45", ts.toString());
  }

  @Test
  void testUtcTimestampDec311999() {
    UtcTimestamp ts = new UtcTimestamp(DEC_31_1999_UTC);
    assertEquals("1999-12-31 23:59:59", ts.toString());
  }

  @Test
  void testUtcTimestampExtendsJavaSqlTimestamp() {
    UtcTimestamp ts = new UtcTimestamp(JAN_1_2024_UTC);
    assertTrue(ts instanceof Timestamp,
        "UtcTimestamp should extend java.sql.Timestamp");
  }

  @Test
  void testUtcTimestampGetTimeReturnsConstructorValue() {
    UtcTimestamp ts = new UtcTimestamp(JAN_1_2024_UTC);
    assertEquals(JAN_1_2024_UTC, ts.getTime());
  }

  @Test
  void testUtcTimestampEqualsSameEpoch() {
    UtcTimestamp ts1 = new UtcTimestamp(JAN_1_2024_UTC);
    UtcTimestamp ts2 = new UtcTimestamp(JAN_1_2024_UTC);
    assertTrue(ts1.equals(ts2));
    assertTrue(ts2.equals(ts1));
  }

  @Test
  void testUtcTimestampEqualsDifferentEpoch() {
    UtcTimestamp ts1 = new UtcTimestamp(EPOCH_ZERO);
    UtcTimestamp ts2 = new UtcTimestamp(JAN_1_2024_UTC);
    assertFalse(ts1.equals(ts2));
    assertFalse(ts2.equals(ts1));
  }

  @Test
  void testUtcTimestampEqualsReturnsFalseForNonTimestamp() {
    UtcTimestamp ts = new UtcTimestamp(JAN_1_2024_UTC);
    assertFalse(ts.equals("not a timestamp"));
    assertFalse(ts.equals(Integer.valueOf(42)));
    assertFalse(ts.equals(null));
  }

  @Test
  void testUtcTimestampEqualsSelf() {
    UtcTimestamp ts = new UtcTimestamp(JAN_1_2024_UTC);
    assertTrue(ts.equals(ts));
  }

  @Test
  void testUtcTimestampEqualsJavaSqlTimestamp() {
    // UtcTimestamp should be equal to a plain Timestamp with the same epoch
    UtcTimestamp utc = new UtcTimestamp(JAN_1_2024_UTC);
    Timestamp plain = new Timestamp(JAN_1_2024_UTC);
    assertTrue(utc.equals(plain),
        "UtcTimestamp should equal a java.sql.Timestamp with the same epoch");
  }

  @Test
  void testUtcTimestampHashCodeConsistentWithEquals() {
    UtcTimestamp ts1 = new UtcTimestamp(JAN_1_2024_UTC);
    UtcTimestamp ts2 = new UtcTimestamp(JAN_1_2024_UTC);

    // If equals is true, hashCode must be the same
    assertEquals(ts1.hashCode(), ts2.hashCode());
  }

  @Test
  void testUtcTimestampHashCodeDifferentForDifferentValues() {
    UtcTimestamp ts1 = new UtcTimestamp(EPOCH_ZERO);
    UtcTimestamp ts2 = new UtcTimestamp(JAN_1_2024_UTC);

    // Not strictly required by the contract, but highly expected for different values
    assertNotEquals(ts1.hashCode(), ts2.hashCode(),
        "Different timestamps should ideally have different hash codes");
  }

  @Test
  void testUtcTimestampHashCodeStableAcrossInvocations() {
    UtcTimestamp ts = new UtcTimestamp(JAN_1_2024_UTC);
    int hash1 = ts.hashCode();
    int hash2 = ts.hashCode();
    assertEquals(hash1, hash2, "hashCode should be stable across invocations");
  }

  @Test
  void testUtcTimestampToStringConsistentAcrossInstances() {
    // Two instances with same epoch should produce identical toString
    UtcTimestamp ts1 = new UtcTimestamp(JUN_15_2024_UTC);
    UtcTimestamp ts2 = new UtcTimestamp(JUN_15_2024_UTC);
    assertEquals(ts1.toString(), ts2.toString());
  }
}
