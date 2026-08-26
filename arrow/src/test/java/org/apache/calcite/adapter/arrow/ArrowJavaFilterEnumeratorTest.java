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
package org.apache.calcite.adapter.arrow;

import org.apache.calcite.util.ImmutableIntList;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.SeekableReadChannel;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileInputStream;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests {@link ArrowJavaFilterEnumerator} directly, independent of {@link GandivaAvailability}
 * and the planner — this is the path a real deployment actually takes, since Gandiva's native
 * JIT does not start on the platform this adapter is normally deployed to (see the
 * {@code feat(arrow): make Gandiva an optional accelerator} commit), while every other test in
 * this module exercises the Gandiva-accelerated path whenever Gandiva happens to work on the
 * machine running the test — true on this platform (Apple Silicon), but not the one that
 * motivated making Gandiva optional in the first place. Not gated by {@link ArrowExtension}: it
 * must run everywhere, precisely because it is the fallback for everywhere Gandiva does not.
 *
 * <p>Each test cross-checks {@link ArrowJavaFilterEnumerator}'s output against a reference
 * computed by scanning every row with {@link ArrowScanEnumerator} (unfiltered) and applying the
 * same condition directly in test code — an equivalence check, not a table of expected literals,
 * so it does not depend on knowing {@link ArrowDataTest}'s exact generated values.
 */
class ArrowJavaFilterEnumeratorTest {
  private static final ImmutableIntList ALL_FIELDS = ImmutableIntList.of(0, 1, 2, 3);

  @Test void equalPushesDownAndMatchesUnfilteredScan(@TempDir Path tempDir) throws Exception {
    File file = writeTestData(tempDir);
    Schema schema = readSchema(file);

    List<Object[]> expected = new ArrayList<>();
    for (Object[] row : scanAll(file)) {
      if (((Number) row[0]).intValue() == 25) {
        expected.add(row);
      }
    }
    assertFalse(expected.isEmpty(), "test data should contain at least one intField=25 row");

    List<Object[]> actual =
        drain(new ArrowJavaFilterEnumerator(openReader(file), ALL_FIELDS, schema,
            Arrays.asList("intField equal 25 integer")));
    assertRowsEqual(expected, actual);
  }

  @Test void greaterThanPushesDownAndMatchesUnfilteredScan(@TempDir Path tempDir) throws Exception {
    File file = writeTestData(tempDir);
    Schema schema = readSchema(file);

    List<Object[]> expected = new ArrayList<>();
    for (Object[] row : scanAll(file)) {
      if (((Number) row[0]).intValue() > 40) {
        expected.add(row);
      }
    }
    assertFalse(expected.isEmpty());

    List<Object[]> actual =
        drain(new ArrowJavaFilterEnumerator(openReader(file), ALL_FIELDS, schema,
            Arrays.asList("intField greater_than 40 integer")));
    assertRowsEqual(expected, actual);
  }

  @Test void lessThanOrEqualToPushesDownAndMatchesUnfilteredScan(@TempDir Path tempDir)
      throws Exception {
    File file = writeTestData(tempDir);
    Schema schema = readSchema(file);

    List<Object[]> expected = new ArrayList<>();
    for (Object[] row : scanAll(file)) {
      if (((Number) row[0]).intValue() <= 3) {
        expected.add(row);
      }
    }
    assertFalse(expected.isEmpty());

    List<Object[]> actual =
        drain(new ArrowJavaFilterEnumerator(openReader(file), ALL_FIELDS, schema,
            Arrays.asList("intField less_than_or_equal_to 3 integer")));
    assertRowsEqual(expected, actual);
  }

  @Test void notEqualPushesDownAndMatchesUnfilteredScan(@TempDir Path tempDir) throws Exception {
    File file = writeTestData(tempDir);
    Schema schema = readSchema(file);

    List<Object[]> expected = new ArrayList<>();
    for (Object[] row : scanAll(file)) {
      if (((Number) row[0]).intValue() != 10) {
        expected.add(row);
      }
    }

    List<Object[]> actual =
        drain(new ArrowJavaFilterEnumerator(openReader(file), ALL_FIELDS, schema,
            Arrays.asList("intField not_equal 10 integer")));
    assertRowsEqual(expected, actual);
  }

  @Test void twoConjunctsAreBothApplied(@TempDir Path tempDir) throws Exception {
    File file = writeTestData(tempDir);
    Schema schema = readSchema(file);

    List<Object[]> expected = new ArrayList<>();
    for (Object[] row : scanAll(file)) {
      int intField = ((Number) row[0]).intValue();
      if (intField > 10 && intField < 20) {
        expected.add(row);
      }
    }
    assertFalse(expected.isEmpty());

    List<Object[]> actual =
        drain(new ArrowJavaFilterEnumerator(openReader(file), ALL_FIELDS, schema,
            Arrays.asList("intField greater_than 10 integer", "intField less_than 20 integer")));
    assertRowsEqual(expected, actual);
  }

  @Test void filterThatMatchesNothingReturnsNoRows(@TempDir Path tempDir) throws Exception {
    File file = writeTestData(tempDir);
    Schema schema = readSchema(file);

    List<Object[]> actual =
        drain(new ArrowJavaFilterEnumerator(openReader(file), ALL_FIELDS, schema,
            Arrays.asList("intField equal 99999 integer")));
    assertTrue(actual.isEmpty());
  }

  @Test void filterThatMatchesEverythingReturnsFullScan(@TempDir Path tempDir) throws Exception {
    File file = writeTestData(tempDir);
    Schema schema = readSchema(file);

    List<Object[]> expected = scanAll(file);
    List<Object[]> actual =
        drain(new ArrowJavaFilterEnumerator(openReader(file), ALL_FIELDS, schema,
            Arrays.asList("intField greater_than_or_equal_to 0 integer")));
    assertRowsEqual(expected, actual);
  }

  @Test void stringEqualityPushesDown(@TempDir Path tempDir) throws Exception {
    File file = writeTestData(tempDir);
    Schema schema = readSchema(file);

    List<Object[]> expected = new ArrayList<>();
    for (Object[] row : scanAll(file)) {
      if ("15".equals(String.valueOf(row[1]))) {
        expected.add(row);
      }
    }
    assertFalse(expected.isEmpty());

    List<Object[]> actual =
        drain(new ArrowJavaFilterEnumerator(openReader(file), ALL_FIELDS, schema,
            Arrays.asList("stringField equal '15' string")));
    assertRowsEqual(expected, actual);
  }

  // -- helpers -----------------------------------------------------------

  private static File writeTestData(Path tempDir) throws Exception {
    File file = tempDir.resolve("javafilter-test.arrow").toFile();
    new ArrowDataTest().writeArrowData(file);
    return file;
  }

  private static ArrowFileReader openReader(File file) throws Exception {
    FileInputStream fis = new FileInputStream(file);
    return new ArrowFileReader(new SeekableReadChannel(fis.getChannel()),
        new RootAllocator(Long.MAX_VALUE));
  }

  private static Schema readSchema(File file) throws Exception {
    try (ArrowFileReader reader = openReader(file)) {
      return reader.getVectorSchemaRoot().getSchema();
    }
  }

  /** Every row, every field, via the same no-pushdown path a condition-free query takes. */
  private static List<Object[]> scanAll(File file) throws Exception {
    return drain(new ArrowScanEnumerator(openReader(file), ALL_FIELDS));
  }

  private static List<Object[]> drain(AbstractArrowEnumerator enumerator) {
    List<Object[]> rows = new ArrayList<>();
    try {
      while (enumerator.moveNext()) {
        Object current = enumerator.current();
        rows.add(current instanceof Object[] ? (Object[]) current : new Object[]{current});
      }
    } finally {
      enumerator.close();
    }
    return rows;
  }

  private static void assertRowsEqual(List<Object[]> expected, List<Object[]> actual) {
    assertEquals(expected.size(), actual.size(),
        () -> "row count mismatch: expected " + expected.size() + ", got " + actual.size());
    for (int i = 0; i < expected.size(); i++) {
      assertEquals(Arrays.toString(expected.get(i)), Arrays.toString(actual.get(i)),
          "row " + i + " mismatch");
    }
  }
}
