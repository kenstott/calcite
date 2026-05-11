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
package org.apache.calcite.adapter.file.util;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Comprehensive unit tests for utility classes:
 * {@link NullEquivalents}, {@link ComparableArrayList}, and {@link ComparableLinkedHashMap}.
 */
@Tag("unit")
public class UtilCoverageTest {

  // =========================================================================
  // NullEquivalents tests
  // =========================================================================

  @Test
  void testNullEquivalentsDefaultSetIsAccessible() {
    Set<String> defaults = NullEquivalents.DEFAULT_NULL_EQUIVALENTS;
    assertNotNull(defaults);
    assertTrue(defaults.contains("NULL"));
    assertTrue(defaults.contains("NA"));
    assertTrue(defaults.contains("N/A"));
    assertTrue(defaults.contains("NONE"));
    assertTrue(defaults.contains("NIL"));
    assertTrue(defaults.contains(""));
    assertEquals(6, defaults.size());
  }

  @Test
  void testNullEquivalentsNullInputReturnsFalse() {
    // null input is not a "representation" of null, it IS null
    assertFalse(NullEquivalents.isNullRepresentation(null));
  }

  @Test
  void testNullEquivalentsEmptyStringReturnsTrue() {
    assertTrue(NullEquivalents.isNullRepresentation(""));
  }

  @Test
  void testNullEquivalentsBlankStringReturnsTrue() {
    // Blank strings (spaces only) get trimmed to empty, which is treated as null
    assertTrue(NullEquivalents.isNullRepresentation("   "));
    assertTrue(NullEquivalents.isNullRepresentation(" "));
    assertTrue(NullEquivalents.isNullRepresentation("\t"));
  }

  @Test
  void testNullEquivalentsDefaultMarkersCaseInsensitive() {
    // Lowercase
    assertTrue(NullEquivalents.isNullRepresentation("null"));
    assertTrue(NullEquivalents.isNullRepresentation("na"));
    assertTrue(NullEquivalents.isNullRepresentation("n/a"));
    assertTrue(NullEquivalents.isNullRepresentation("none"));
    assertTrue(NullEquivalents.isNullRepresentation("nil"));

    // Uppercase
    assertTrue(NullEquivalents.isNullRepresentation("NULL"));
    assertTrue(NullEquivalents.isNullRepresentation("NA"));
    assertTrue(NullEquivalents.isNullRepresentation("N/A"));
    assertTrue(NullEquivalents.isNullRepresentation("NONE"));
    assertTrue(NullEquivalents.isNullRepresentation("NIL"));

    // Mixed case
    assertTrue(NullEquivalents.isNullRepresentation("Null"));
    assertTrue(NullEquivalents.isNullRepresentation("Na"));
    assertTrue(NullEquivalents.isNullRepresentation("n/A"));
    assertTrue(NullEquivalents.isNullRepresentation("None"));
    assertTrue(NullEquivalents.isNullRepresentation("Nil"));
  }

  @Test
  void testNullEquivalentsTrimsWhitespace() {
    assertTrue(NullEquivalents.isNullRepresentation("  null  "));
    assertTrue(NullEquivalents.isNullRepresentation("  NA  "));
    assertTrue(NullEquivalents.isNullRepresentation(" N/A "));
    assertTrue(NullEquivalents.isNullRepresentation("\tNONE\t"));
    assertTrue(NullEquivalents.isNullRepresentation(" NIL "));
  }

  @Test
  void testNullEquivalentsNonNullValuesReturnFalse() {
    assertFalse(NullEquivalents.isNullRepresentation("hello"));
    assertFalse(NullEquivalents.isNullRepresentation("0"));
    assertFalse(NullEquivalents.isNullRepresentation("false"));
    assertFalse(NullEquivalents.isNullRepresentation("NaN"));
    assertFalse(NullEquivalents.isNullRepresentation("undefined"));
    assertFalse(NullEquivalents.isNullRepresentation("123"));
    assertFalse(NullEquivalents.isNullRepresentation("NULL_VALUE"));
    assertFalse(NullEquivalents.isNullRepresentation("NULLABLE"));
  }

  @Test
  void testNullEquivalentsCustomSetWorks() {
    Set<String> custom = new HashSet<>(Arrays.asList("MISSING", "UNKNOWN"));

    assertTrue(NullEquivalents.isNullRepresentation("missing", custom));
    assertTrue(NullEquivalents.isNullRepresentation("MISSING", custom));
    assertTrue(NullEquivalents.isNullRepresentation("unknown", custom));
    assertTrue(NullEquivalents.isNullRepresentation("Unknown", custom));

    // Default markers should NOT match with a custom set that excludes them
    assertFalse(NullEquivalents.isNullRepresentation("NULL", custom));
    assertFalse(NullEquivalents.isNullRepresentation("NA", custom));
    assertFalse(NullEquivalents.isNullRepresentation("NIL", custom));
  }

  @Test
  void testNullEquivalentsCustomSetNullInputReturnsFalse() {
    Set<String> custom = new HashSet<>(Collections.singletonList("MISSING"));
    assertFalse(NullEquivalents.isNullRepresentation(null, custom));
  }

  @Test
  void testNullEquivalentsCustomSetEmptyStringStillTrue() {
    // Even with custom set, blank/empty strings are treated as null representations
    // because the method checks trimmed.isEmpty() before checking the set
    Set<String> custom = new HashSet<>(Collections.singletonList("MISSING"));
    assertTrue(NullEquivalents.isNullRepresentation("", custom));
    assertTrue(NullEquivalents.isNullRepresentation("   ", custom));
  }

  @Test
  void testNullEquivalentsCustomEmptySet() {
    Set<String> empty = new HashSet<>();

    // Empty string still returns true (trimmed.isEmpty() check)
    assertTrue(NullEquivalents.isNullRepresentation("", empty));

    // But named markers should not match
    assertFalse(NullEquivalents.isNullRepresentation("NULL", empty));
    assertFalse(NullEquivalents.isNullRepresentation("NA", empty));
  }

  // =========================================================================
  // ComparableArrayList tests
  // =========================================================================

  @Test
  void testComparableArrayListEqualListsCompareToZero() {
    ComparableArrayList<String> list1 = new ComparableArrayList<>();
    list1.add("alpha");
    list1.add("beta");

    ComparableArrayList<String> list2 = new ComparableArrayList<>();
    list2.add("alpha");
    list2.add("beta");

    assertEquals(0, list1.compareTo(list2));
  }

  @Test
  void testComparableArrayListEmptyListsCompareToZero() {
    ComparableArrayList<String> list1 = new ComparableArrayList<>();
    ComparableArrayList<String> list2 = new ComparableArrayList<>();

    assertEquals(0, list1.compareTo(list2));
  }

  @Test
  void testComparableArrayListDifferentListsHaveConsistentOrdering() {
    ComparableArrayList<String> list1 = new ComparableArrayList<>();
    list1.add("alpha");

    ComparableArrayList<String> list2 = new ComparableArrayList<>();
    list2.add("beta");

    int comparison = list1.compareTo(list2);
    assertNotEquals(0, comparison);

    // Reverse comparison should have opposite sign
    int reverseComparison = list2.compareTo(list1);
    assertTrue(comparison < 0 && reverseComparison > 0
        || comparison > 0 && reverseComparison < 0,
        "Comparisons should have opposite signs");
  }

  @Test
  void testComparableArrayListWithIntegers() {
    ComparableArrayList<Integer> list1 = new ComparableArrayList<>();
    list1.add(1);
    list1.add(2);
    list1.add(3);

    ComparableArrayList<Integer> list2 = new ComparableArrayList<>();
    list2.add(1);
    list2.add(2);
    list2.add(3);

    assertEquals(0, list1.compareTo(list2));
  }

  @Test
  void testComparableArrayListDifferentSizes() {
    ComparableArrayList<String> shorter = new ComparableArrayList<>();
    shorter.add("alpha");

    ComparableArrayList<String> longer = new ComparableArrayList<>();
    longer.add("alpha");
    longer.add("beta");

    // Different sizes should not compare to zero
    assertNotEquals(0, shorter.compareTo(longer));
  }

  @Test
  void testComparableArrayListIsAnArrayList() {
    ComparableArrayList<String> list = new ComparableArrayList<>();
    list.add("one");
    list.add("two");
    list.add("three");

    assertEquals(3, list.size());
    assertEquals("one", list.get(0));
    assertEquals("two", list.get(1));
    assertEquals("three", list.get(2));
    assertTrue(list.contains("two"));
    assertFalse(list.isEmpty());
  }

  @Test
  void testComparableArrayListTransitiveOrdering() {
    // If a < b and b < c, then a < c
    ComparableArrayList<String> a = new ComparableArrayList<>();
    a.add("aaa");

    ComparableArrayList<String> b = new ComparableArrayList<>();
    b.add("bbb");

    ComparableArrayList<String> c = new ComparableArrayList<>();
    c.add("ccc");

    int ab = a.compareTo(b);
    int bc = b.compareTo(c);
    int ac = a.compareTo(c);

    if (ab < 0 && bc < 0) {
      assertTrue(ac < 0, "Transitivity: a < b and b < c implies a < c");
    }
  }

  // =========================================================================
  // ComparableLinkedHashMap tests
  // =========================================================================

  @Test
  void testComparableLinkedHashMapEqualMapsCompareToZero() {
    ComparableLinkedHashMap<String, String> map1 = new ComparableLinkedHashMap<>();
    map1.put("key1", "value1");
    map1.put("key2", "value2");

    ComparableLinkedHashMap<String, String> map2 = new ComparableLinkedHashMap<>();
    map2.put("key1", "value1");
    map2.put("key2", "value2");

    assertEquals(0, map1.compareTo(map2));
  }

  @Test
  void testComparableLinkedHashMapEmptyMapsCompareToZero() {
    ComparableLinkedHashMap<String, String> map1 = new ComparableLinkedHashMap<>();
    ComparableLinkedHashMap<String, String> map2 = new ComparableLinkedHashMap<>();

    assertEquals(0, map1.compareTo(map2));
  }

  @Test
  void testComparableLinkedHashMapDifferentValuesDoNotCompareToZero() {
    ComparableLinkedHashMap<String, String> map1 = new ComparableLinkedHashMap<>();
    map1.put("key", "alpha");

    ComparableLinkedHashMap<String, String> map2 = new ComparableLinkedHashMap<>();
    map2.put("key", "beta");

    int comparison = map1.compareTo(map2);
    assertNotEquals(0, comparison);

    // Reverse should have opposite sign
    int reverse = map2.compareTo(map1);
    assertTrue(comparison < 0 && reverse > 0
        || comparison > 0 && reverse < 0,
        "Comparisons should have opposite signs");
  }

  @Test
  void testComparableLinkedHashMapDifferentKeysDoNotCompareToZero() {
    ComparableLinkedHashMap<String, Integer> map1 = new ComparableLinkedHashMap<>();
    map1.put("aaa", 1);

    ComparableLinkedHashMap<String, Integer> map2 = new ComparableLinkedHashMap<>();
    map2.put("zzz", 1);

    assertNotEquals(0, map1.compareTo(map2));
  }

  @Test
  void testComparableLinkedHashMapDifferentSizes() {
    ComparableLinkedHashMap<String, String> smaller = new ComparableLinkedHashMap<>();
    smaller.put("key1", "val1");

    ComparableLinkedHashMap<String, String> larger = new ComparableLinkedHashMap<>();
    larger.put("key1", "val1");
    larger.put("key2", "val2");

    assertNotEquals(0, smaller.compareTo(larger));
  }

  @Test
  void testComparableLinkedHashMapPreservesInsertionOrder() {
    // LinkedHashMap preserves insertion order, which affects JSON serialization
    ComparableLinkedHashMap<String, String> map1 = new ComparableLinkedHashMap<>();
    map1.put("b", "2");
    map1.put("a", "1");

    ComparableLinkedHashMap<String, String> map2 = new ComparableLinkedHashMap<>();
    map2.put("a", "1");
    map2.put("b", "2");

    // Same content but different insertion order may produce different JSON
    // (LinkedHashMap serializes in insertion order)
    // map1 serializes as {"b":"2","a":"1"}, map2 as {"a":"1","b":"2"}
    int comparison = map1.compareTo(map2);
    assertNotEquals(0, comparison,
        "Different insertion order should produce different JSON serialization");
  }

  @Test
  void testComparableLinkedHashMapIsALinkedHashMap() {
    ComparableLinkedHashMap<String, Integer> map = new ComparableLinkedHashMap<>();
    map.put("one", 1);
    map.put("two", 2);
    map.put("three", 3);

    assertEquals(3, map.size());
    assertEquals(Integer.valueOf(1), map.get("one"));
    assertEquals(Integer.valueOf(2), map.get("two"));
    assertEquals(Integer.valueOf(3), map.get("three"));
    assertTrue(map.containsKey("two"));
    assertFalse(map.isEmpty());
  }

  @Test
  void testComparableLinkedHashMapWithIntegerKeys() {
    ComparableLinkedHashMap<Integer, String> map1 = new ComparableLinkedHashMap<>();
    map1.put(1, "one");
    map1.put(2, "two");

    ComparableLinkedHashMap<Integer, String> map2 = new ComparableLinkedHashMap<>();
    map2.put(1, "one");
    map2.put(2, "two");

    assertEquals(0, map1.compareTo(map2));
  }

  @Test
  void testComparableLinkedHashMapSameContentSameOrderEquals() {
    ComparableLinkedHashMap<String, String> map1 = new ComparableLinkedHashMap<>();
    map1.put("x", "10");
    map1.put("y", "20");
    map1.put("z", "30");

    ComparableLinkedHashMap<String, String> map2 = new ComparableLinkedHashMap<>();
    map2.put("x", "10");
    map2.put("y", "20");
    map2.put("z", "30");

    assertEquals(0, map1.compareTo(map2));
  }
}
