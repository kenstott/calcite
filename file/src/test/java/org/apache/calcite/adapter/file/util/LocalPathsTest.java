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
package org.apache.calcite.adapter.file.util;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link LocalPaths}.
 *
 * <p>Each rule is stated for both platforms through the overloads that take the platform
 * as an argument, so the Windows expectations are exercised wherever the tests run.
 */
@Tag("unit")
class LocalPathsTest {
  private static final boolean WINDOWS = true;
  private static final boolean UNIX = false;

  @Test void aUriIsToldFromALocalPath() {
    assertTrue(LocalPaths.isUri("s3://bucket/key"));
    assertTrue(LocalPaths.isUri("s3a://bucket/key"));
    assertTrue(LocalPaths.isUri("https://host/a/b.csv"));
    assertTrue(LocalPaths.isUri("hdfs://nn:8020/a"));
    assertTrue(LocalPaths.isUri("file:///C:/data"));
    assertTrue(LocalPaths.isUri("file:/data"));
    assertFalse(LocalPaths.isUri("/data/sales"));
    assertFalse(LocalPaths.isUri("data/sales"));
    // A drive letter is not a scheme
    assertFalse(LocalPaths.isUri("C:\\data\\sales"));
    assertFalse(LocalPaths.isUri("C:/data/sales"));
    assertFalse(LocalPaths.isUri("D:\\a\\calcite/file/sales"));
  }

  @Test void normalizeGivesAWindowsLocalPathOneSeparator() {
    assertEquals("D:\\a\\calcite\\file\\sales",
        LocalPaths.normalize("D:\\a\\calcite/file/sales", WINDOWS));
    assertEquals("C:\\data\\sales", LocalPaths.normalize("C:/data/sales", WINDOWS));
    assertEquals("data\\sales\\*.csv", LocalPaths.normalize("data/sales/*.csv", WINDOWS));
  }

  @Test void normalizeLeavesUrisAndUnixPathsAlone() {
    assertEquals("s3://bucket/a/b", LocalPaths.normalize("s3://bucket/a/b", WINDOWS));
    assertEquals("https://host/a/b", LocalPaths.normalize("https://host/a/b", WINDOWS));
    assertEquals("file:///C:/data", LocalPaths.normalize("file:///C:/data", WINDOWS));
    assertEquals("/data/sales", LocalPaths.normalize("/data/sales", UNIX));
    // Off Windows a backslash is a character of a file name
    assertEquals("/data/odd\\name", LocalPaths.normalize("/data/odd\\name", UNIX));
    assertEquals("s3://bucket/a/b", LocalPaths.normalize("s3://bucket/a/b", UNIX));
  }

  @Test void toSlashesKeepsEveryCharacterInPlace() {
    assertEquals("C:/wh/t/data/year=2020/f.parquet",
        LocalPaths.toSlashes("C:\\wh\\t\\data\\year=2020\\f.parquet", WINDOWS));
    assertEquals("C:/wh/t/data/f.parquet",
        LocalPaths.toSlashes("C:\\wh/t\\data/f.parquet", WINDOWS));
    assertEquals("s3://b/odd\\key", LocalPaths.toSlashes("s3://b/odd\\key", WINDOWS));
    assertEquals("/wh/odd\\name", LocalPaths.toSlashes("/wh/odd\\name", UNIX));
  }

  @Test void joinUsesTheSeparatorOfThePath() {
    assertEquals("C:\\data\\sales\\emps.csv",
        LocalPaths.join("C:\\data\\sales", "emps.csv", WINDOWS));
    assertEquals("C:\\data\\sales\\emps.csv",
        LocalPaths.join("C:\\data\\sales\\", "emps.csv", WINDOWS));
    assertEquals("D:\\a\\calcite\\file\\sales\\year=*\\*.parquet",
        LocalPaths.join("D:\\a\\calcite/file/sales", "year=*/*.parquet", WINDOWS));
    assertEquals("C:\\data\\sales", LocalPaths.join("C:/data/", "sales", WINDOWS));

    assertEquals("s3://bucket/wh/orders", LocalPaths.join("s3://bucket/wh", "orders", WINDOWS));
    assertEquals("s3://bucket/wh/orders", LocalPaths.join("s3://bucket/wh/", "orders", WINDOWS));
    assertEquals("s3://bucket/wh/year=*/*.parquet",
        LocalPaths.join("s3://bucket/wh", "year=*/*.parquet", WINDOWS));

    assertEquals("/data/sales/emps.csv", LocalPaths.join("/data/sales", "emps.csv", UNIX));
    assertEquals("/data/sales/emps.csv", LocalPaths.join("/data/sales/", "emps.csv", UNIX));
    assertEquals("s3://bucket/wh/orders", LocalPaths.join("s3://bucket/wh", "orders", UNIX));
  }

  @Test void separatorsAtTheEnds() {
    assertTrue(LocalPaths.endsWithSeparator("C:\\data\\", WINDOWS));
    assertTrue(LocalPaths.endsWithSeparator("C:\\data/", WINDOWS));
    assertFalse(LocalPaths.endsWithSeparator("C:\\data", WINDOWS));
    assertFalse(LocalPaths.endsWithSeparator("s3://b/odd\\", WINDOWS));
    assertTrue(LocalPaths.endsWithSeparator("/data/", UNIX));
    assertFalse(LocalPaths.endsWithSeparator("/data\\", UNIX));
    assertFalse(LocalPaths.endsWithSeparator("", UNIX));

    assertTrue(LocalPaths.startsWithSeparator("\\child.csv", WINDOWS));
    assertTrue(LocalPaths.startsWithSeparator("/child.csv", WINDOWS));
    assertFalse(LocalPaths.startsWithSeparator("\\child.csv", UNIX));
    assertFalse(LocalPaths.startsWithSeparator("", WINDOWS));
  }

  @Test void anAbsoluteLocalPath() {
    assertTrue(LocalPaths.isAbsolute("C:\\data", WINDOWS));
    assertTrue(LocalPaths.isAbsolute("d:/data", WINDOWS));
    assertTrue(LocalPaths.isAbsolute("\\\\server\\share", WINDOWS));
    assertTrue(LocalPaths.isAbsolute("/data", WINDOWS));
    assertFalse(LocalPaths.isAbsolute("data\\sales", WINDOWS));
    assertTrue(LocalPaths.isAbsolute("/data", UNIX));
    assertFalse(LocalPaths.isAbsolute("data/sales", UNIX));
    // Off Windows "C:" is the start of a relative file name
    assertFalse(LocalPaths.isAbsolute("C:\\data", UNIX));
  }

  @Test void theLastSegmentAndWhatPrecedesIt() {
    assertEquals("emps.csv", LocalPaths.fileName("C:\\data\\sales\\emps.csv", WINDOWS));
    assertEquals("emps.csv", LocalPaths.fileName("C:\\data\\sales/emps.csv", WINDOWS));
    assertEquals("*.csv", LocalPaths.fileName("C:\\Temp\\junit123/*.csv", WINDOWS));
    assertEquals("emps.csv", LocalPaths.fileName("emps.csv", WINDOWS));
    assertEquals("", LocalPaths.fileName("C:\\data\\", WINDOWS));
    assertEquals("key.parquet", LocalPaths.fileName("s3://bucket/a/key.parquet", WINDOWS));
    assertEquals("odd\\key", LocalPaths.fileName("s3://bucket/a/odd\\key", WINDOWS));
    assertEquals("emps.csv", LocalPaths.fileName("/data/sales/emps.csv", UNIX));
    assertEquals("odd\\name.csv", LocalPaths.fileName("/data/odd\\name.csv", UNIX));
    assertEquals("", LocalPaths.fileName("", UNIX));

    assertEquals("C:\\Temp\\junit123", LocalPaths.parent("C:\\Temp\\junit123/*.csv", WINDOWS));
    assertEquals("C:\\data\\sales", LocalPaths.parent("C:\\data\\sales\\emps.csv", WINDOWS));
    assertEquals("/data/sales", LocalPaths.parent("/data/sales/emps.csv", UNIX));
    assertEquals("", LocalPaths.parent("/emps.csv", UNIX));
    assertNull(LocalPaths.parent("emps.csv", UNIX));
    assertNull(LocalPaths.parent("emps.csv", WINDOWS));

    assertEquals(16, LocalPaths.lastSeparator("C:\\Temp\\junit123/*.csv", WINDOWS));
    assertEquals(7, LocalPaths.lastSeparator("C:\\Temp\\x.csv", WINDOWS));
    assertEquals(-1, LocalPaths.lastSeparator("C:\\Temp\\x.csv", UNIX));
    assertEquals(2, LocalPaths.firstSeparator("C:\\Temp/x.csv", WINDOWS));
    assertEquals(7, LocalPaths.firstSeparator("C:\\Temp/x.csv", UNIX));
    assertEquals(-1, LocalPaths.firstSeparator("x.csv", WINDOWS));
  }

  @Test void aDirectoryBecomesAFileNamePrefix() {
    assertEquals("a_b", LocalPaths.directoryPrefix("a\\b\\book.xlsx", WINDOWS));
    assertEquals("a_b", LocalPaths.directoryPrefix("a/b\\book.xlsx", WINDOWS));
    assertEquals("a_b", LocalPaths.directoryPrefix("a/b/book.xlsx", WINDOWS));
    assertNull(LocalPaths.directoryPrefix("book.xlsx", WINDOWS));
    assertEquals("a_b", LocalPaths.directoryPrefix("a/b/book.xlsx", UNIX));
    assertNull(LocalPaths.directoryPrefix("book.xlsx", UNIX));
    assertNull(LocalPaths.directoryPrefix("odd\\book.xlsx", UNIX));
  }

  @Test void relativizeOnWindowsMatchesEitherSeparatorAndAnyCase() {
    // The schema directory of the Windows runner against a listed file
    assertEquals("emps.csv",
        LocalPaths.relativize("D:\\a\\calcite\\calcite\\file\\build/resources/test/sales",
            "D:\\a\\calcite\\calcite\\file\\build\\resources\\test\\sales\\emps.csv", WINDOWS));
    assertEquals("sub\\emps.csv",
        LocalPaths.relativize("C:/data/sales", "C:\\data\\sales\\sub\\emps.csv", WINDOWS));
    assertEquals("sub/emps.csv",
        LocalPaths.relativize("C:\\data\\sales\\", "C:\\data\\sales/sub/emps.csv", WINDOWS));
    assertEquals("emps.csv",
        LocalPaths.relativize("d:\\Data\\Sales", "D:\\data\\sales\\emps.csv", WINDOWS));
    assertEquals("", LocalPaths.relativize("C:\\data\\sales", "C:/data/sales", WINDOWS));
    assertNull(LocalPaths.relativize("C:\\data\\sales", "C:\\data\\sales2\\emps.csv", WINDOWS));
    assertNull(LocalPaths.relativize("C:\\data\\sales", "C:\\data\\emps.csv", WINDOWS));
    assertNull(LocalPaths.relativize("C:\\data\\sales", "C:\\data", WINDOWS));
  }

  @Test void relativizeKeepsUrisExact() {
    assertEquals("year=2020/f.parquet",
        LocalPaths.relativize("s3://bucket/wh", "s3://bucket/wh/year=2020/f.parquet", WINDOWS));
    // Case and backslash are significant in a URI on every platform
    assertNull(LocalPaths.relativize("s3://bucket/WH", "s3://bucket/wh/f.parquet", WINDOWS));
    assertNull(LocalPaths.relativize("s3://bucket/wh", "s3://bucket/wh\\f.parquet", WINDOWS));
    assertNull(LocalPaths.relativize("s3://bucket/wh", "C:\\bucket\\wh\\f.parquet", WINDOWS));
    assertEquals("f.parquet",
        LocalPaths.relativize("s3://bucket/wh/", "s3://bucket/wh/f.parquet", UNIX));
  }

  @Test void relativizeOffWindowsIsTheSlashOperation() {
    assertEquals("emps.csv", LocalPaths.relativize("/data/sales", "/data/sales/emps.csv", UNIX));
    assertEquals("emps.csv", LocalPaths.relativize("/data/sales/", "/data/sales/emps.csv", UNIX));
    assertEquals("sub/emps.csv",
        LocalPaths.relativize("/data/sales", "/data/sales/sub/emps.csv", UNIX));
    assertEquals("", LocalPaths.relativize("/data/sales", "/data/sales", UNIX));
    assertEquals("data/emps.csv", LocalPaths.relativize("/", "/data/emps.csv", UNIX));
    assertEquals("data/emps.csv", LocalPaths.relativize("", "data/emps.csv", UNIX));
    assertNull(LocalPaths.relativize("/data/sales", "/data/sales2/emps.csv", UNIX));
    assertNull(LocalPaths.relativize("/data/Sales", "/data/sales/emps.csv", UNIX));
    assertNull(LocalPaths.relativize("/data/sales", "/data/sales\\emps.csv", UNIX));
    assertNull(LocalPaths.relativize("/", "data/emps.csv", UNIX));
  }
}
