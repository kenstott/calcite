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

import org.apache.calcite.util.Source;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link DirectFileSource}.
 */
@Tag("unit")
class DirectFileSourceTest {

  @TempDir
  Path tempDir;

  @Test void testFileReturnsOriginalFile() throws IOException {
    File f = tempDir.resolve("test.txt").toFile();
    Files.write(f.toPath(), "hello".getBytes(StandardCharsets.UTF_8));
    DirectFileSource source = new DirectFileSource(f);
    assertEquals(f, source.file());
  }

  @Test void testFileOptReturnsPresent() throws IOException {
    File f = tempDir.resolve("test.txt").toFile();
    Files.write(f.toPath(), "hello".getBytes(StandardCharsets.UTF_8));
    DirectFileSource source = new DirectFileSource(f);
    Optional<File> opt = source.fileOpt();
    assertTrue(opt.isPresent());
    assertEquals(f, opt.get());
  }

  @Test void testPathReturnsFilePath() throws IOException {
    File f = tempDir.resolve("data.csv").toFile();
    Files.write(f.toPath(), "a,b".getBytes(StandardCharsets.UTF_8));
    DirectFileSource source = new DirectFileSource(f);
    assertEquals(f.getPath(), source.path());
  }

  @Test void testUrlReturnsFileUrl() throws IOException {
    File f = tempDir.resolve("data.csv").toFile();
    Files.write(f.toPath(), "a,b".getBytes(StandardCharsets.UTF_8));
    DirectFileSource source = new DirectFileSource(f);
    URL url = source.url();
    assertNotNull(url);
    assertTrue(url.toString().startsWith("file:"));
  }

  @Test void testProtocol() {
    File f = new File("/tmp/test.csv");
    DirectFileSource source = new DirectFileSource(f);
    assertEquals("file", source.protocol());
  }

  @Test void testReader() throws IOException {
    File f = tempDir.resolve("test.txt").toFile();
    String content = "hello world";
    Files.write(f.toPath(), content.getBytes(StandardCharsets.UTF_8));
    DirectFileSource source = new DirectFileSource(f);

    try (Reader reader = source.reader()) {
      char[] buffer = new char[100];
      int read = reader.read(buffer);
      assertEquals(content, new String(buffer, 0, read));
    }
  }

  @Test void testOpenStream() throws IOException {
    File f = tempDir.resolve("test.txt").toFile();
    byte[] data = "binary data".getBytes(StandardCharsets.UTF_8);
    Files.write(f.toPath(), data);
    DirectFileSource source = new DirectFileSource(f);

    try (InputStream is = source.openStream()) {
      byte[] buffer = new byte[100];
      int read = is.read(buffer);
      assertEquals("binary data", new String(buffer, 0, read, StandardCharsets.UTF_8));
    }
  }

  @Test void testAppendWithFileSource() throws IOException {
    File parentDir = tempDir.resolve("parent").toFile();
    parentDir.mkdirs();
    File childFile = new File("child.csv");

    DirectFileSource parent = new DirectFileSource(parentDir);
    DirectFileSource child = new DirectFileSource(childFile);
    Source appended = parent.append(child);

    assertNotNull(appended);
    assertTrue(appended.path().contains("child.csv"));
  }

  @Test void testRelativePathSubset() throws IOException {
    File baseFile = new File("/base/path");
    File fullFile = new File("/base/path/sub/file.csv");

    DirectFileSource base = new DirectFileSource(baseFile);
    DirectFileSource full = new DirectFileSource(fullFile);
    Source relative = full.relative(base);
    assertNotNull(relative);
  }

  @Test void testRelativePathNotSubset() {
    File otherFile = new File("/other/path/file.csv");
    File baseFile = new File("/base/path");

    DirectFileSource other = new DirectFileSource(otherFile);
    DirectFileSource base = new DirectFileSource(baseFile);
    Source relative = other.relative(base);
    // Should return self when not relative
    assertNotNull(relative);
    assertEquals(other.path(), relative.path());
  }

  @Test void testTrimSuffix() {
    File f = new File("/path/to/file.csv.gz");
    DirectFileSource source = new DirectFileSource(f);
    Source trimmed = source.trim(".gz");
    assertNotNull(trimmed);
    assertTrue(trimmed.path().endsWith(".csv"));
  }

  @Test void testTrimSuffixNotPresent() {
    File f = new File("/path/to/file.csv");
    DirectFileSource source = new DirectFileSource(f);
    Source trimmed = source.trim(".gz");
    assertNotNull(trimmed);
    assertEquals(source.path(), trimmed.path());
  }

  @Test void testTrimSuffixNull() {
    File f = new File("/path/to/file.csv");
    DirectFileSource source = new DirectFileSource(f);
    Source trimmed = source.trim(null);
    assertNotNull(trimmed);
    assertEquals(source.path(), trimmed.path());
  }

  @Test void testTrimOrNullSuffix() {
    File f = new File("/path/to/file.csv.gz");
    DirectFileSource source = new DirectFileSource(f);
    Source trimmed = source.trimOrNull(".gz");
    assertNotNull(trimmed);
    assertTrue(trimmed.path().endsWith(".csv"));
  }

  @Test void testTrimOrNullSuffixNotPresent() {
    File f = new File("/path/to/file.csv");
    DirectFileSource source = new DirectFileSource(f);
    Source trimmed = source.trimOrNull(".gz");
    assertNull(trimmed);
  }

  @Test void testTrimOrNullSuffixNull() {
    File f = new File("/path/to/file.csv");
    DirectFileSource source = new DirectFileSource(f);
    Source trimmed = source.trimOrNull(null);
    assertNull(trimmed);
  }

  @Test void testMultipleReadersAreFresh() throws IOException {
    File f = tempDir.resolve("test.txt").toFile();
    Files.write(f.toPath(), "content".getBytes(StandardCharsets.UTF_8));
    DirectFileSource source = new DirectFileSource(f);

    // Both readers should work independently
    try (Reader r1 = source.reader()) {
      try (Reader r2 = source.reader()) {
        assertNotNull(r1);
        assertNotNull(r2);
      }
    }
  }
}
