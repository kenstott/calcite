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
package org.apache.calcite.adapter.file.partition;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for scoping processed keys to a year.
 *
 * <p>A key of several components is stored as {@code accession=X__year=2021} and comes back with
 * both parts. A key of one component is stored as its bare value, so it comes back as
 * {@code {source_key=2021}} — the year is there, under a different name. Testing only the first
 * shape hides the second, and a year-partitioned table keyed on year alone then matches nothing:
 * every period reads as unprocessed and is done again on every run.
 */
@Tag("unit")
class ProcessedKeyYearScopingTest {

  /** Minimal tracker returning a fixed set, exercising only the year-scoping default. */
  private static IncrementalTracker trackerReturning(final Set<Map<String, String>> keys) {
    return (IncrementalTracker) java.lang.reflect.Proxy.newProxyInstance(
        IncrementalTracker.class.getClassLoader(),
        new Class<?>[] {IncrementalTracker.class},
        (proxy, method, args) -> {
          if ("getProcessedKeyValues".equals(method.getName()) && method.getParameterCount() == 1) {
            return keys;
          }
          if (method.isDefault()) {
            return java.lang.invoke.MethodHandles
                .privateLookupIn(IncrementalTracker.class, java.lang.invoke.MethodHandles.lookup())
                .unreflectSpecial(method, IncrementalTracker.class)
                .bindTo(proxy).invokeWithArguments(args);
          }
          return null;
        });
  }

  private static Map<String, String> key(String... pairs) {
    Map<String, String> m = new LinkedHashMap<>();
    for (int i = 0; i < pairs.length; i += 2) {
      m.put(pairs[i], pairs[i + 1]);
    }
    return m;
  }

  private static Set<Map<String, String>> setOf(List<Map<String, String>> maps) {
    return new HashSet<>(maps);
  }

  @Test void testCompositeKeysAreScopedByTheirYearComponent() {
    IncrementalTracker tracker = trackerReturning(setOf(Arrays.asList(
        key("accession", "0000320193-21-000001", "year", "2021"),
        key("accession", "0000320193-22-000002", "year", "2022"))));

    Set<Map<String, String>> scoped = tracker.getProcessedKeyValues("filing_metadata", "2021");

    assertEquals(1, scoped.size());
    assertEquals("0000320193-21-000001", scoped.iterator().next().get("accession"));
  }

  /**
   * A single-component key is stored bare, so its year arrives under source_key.
   *
   * <p>Excluding it reports the period as unprocessed, and the pipeline redoes work it has already
   * completed — silently, since nothing distinguishes "not done" from "recorded under a name the
   * filter did not look at".
   */
  @Test void testSingleComponentKeyIsScopedByItsSoleValue() {
    IncrementalTracker tracker = trackerReturning(setOf(Arrays.asList(
        key("source_key", "2021"),
        key("source_key", "2022"))));

    Set<Map<String, String>> scoped = tracker.getProcessedKeyValues("annual_table", "2021");

    assertEquals(1, scoped.size(), "the sole value is the year for a year-only partition");
    assertEquals("2021", scoped.iterator().next().get("source_key"));
  }

  @Test void testNullYearReturnsEverything() {
    IncrementalTracker tracker = trackerReturning(setOf(Arrays.asList(
        key("accession", "a", "year", "2021"),
        key("source_key", "2022"))));

    assertEquals(2, tracker.getProcessedKeyValues("t", null).size());
  }

  /**
   * A multi-component key without a year must not be scoped by one of its values.
   *
   * <p>Reading the sole value is only meaningful when there is exactly one. Applying it more
   * widely would match a key on some unrelated component — a cik that happens to read as a year.
   */
  @Test void testMultiComponentKeyWithoutAYearNeverMatches() {
    IncrementalTracker tracker = trackerReturning(setOf(Arrays.asList(
        key("cik", "2021", "accession", "x"))));

    assertTrue(tracker.getProcessedKeyValues("t", "2021").isEmpty(),
        "a key with no year component belongs to no year");
  }

  @Test void testYearsAreMatchedExactly() {
    IncrementalTracker tracker = trackerReturning(setOf(Arrays.asList(
        key("accession", "a", "year", "20211"),
        key("source_key", "20210"))));

    assertTrue(tracker.getProcessedKeyValues("t", "2021").isEmpty(),
        "2021 must not match 20211 or 20210");
  }
}
