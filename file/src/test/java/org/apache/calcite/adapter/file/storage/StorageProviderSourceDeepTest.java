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
package org.apache.calcite.adapter.file.storage;

import org.apache.calcite.util.Source;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Deep coverage tests for {@link StorageProviderSource} covering
 * path manipulation, append, relative, trim, protocol, and file methods.
 */
@Tag("unit")
class StorageProviderSourceDeepTest {

  @TempDir
  File tempDir;

  private StorageProviderSource createSource(String path) {
    StorageProvider.FileEntry entry =
        new StorageProvider.FileEntry(path, extractName(path), false, 100, 1000L);
    return new StorageProviderSource(entry, new LocalFileStorageProvider());
  }

  private String extractName(String path) {
    int lastSlash = path.lastIndexOf('/');
    return lastSlash >= 0 ? path.substring(lastSlash + 1) : path;
  }

  @Test void testPath() {
    StorageProviderSource source = createSource("/data/files/test.csv");
    assertEquals("/data/files/test.csv", source.path());
  }

  @Test void testProtocol() {
    StorageProviderSource source = createSource("/data/test.csv");
    assertEquals("local", source.protocol());
  }

  @Test void testUrlReturnsNull() {
    StorageProviderSource source = createSource("/data/test.csv");
    assertNull(source.url());
  }

  @Test void testOpenStream() throws IOException {
    File testFile = new File(tempDir, "stream.txt");
    Files.write(testFile.toPath(), "hello world".getBytes(StandardCharsets.UTF_8));

    StorageProviderSource source = createSource(testFile.getAbsolutePath());
    try (InputStream is = source.openStream()) {
      assertNotNull(is);
      byte[] content = new byte[11];
      int read = is.read(content);
      assertEquals(11, read);
      assertEquals("hello world", new String(content, StandardCharsets.UTF_8));
    }
  }

  @Test void testReader() throws IOException {
    File testFile = new File(tempDir, "reader.txt");
    Files.write(testFile.toPath(), "hello reader".getBytes(StandardCharsets.UTF_8));

    StorageProviderSource source = createSource(testFile.getAbsolutePath());
    try (Reader reader = source.reader()) {
      assertNotNull(reader);
      char[] buf = new char[12];
      int read = reader.read(buf);
      assertEquals(12, read);
      assertEquals("hello reader", new String(buf));
    }
  }

  @Test void testAppend() {
    StorageProviderSource base = createSource("/data/files/");
    Source child = org.apache.calcite.util.Sources.of(new File("/child.csv"));

    Source appended = base.append(child);
    assertNotNull(appended);
    assertTrue(appended.path().contains("child.csv"));
  }

  @Test void testRelativeSubpath() {
    StorageProviderSource source = createSource("/data/files/test.csv");
    Source basePath = org.apache.calcite.util.Sources.of(new File("/data/files/"));

    Source relative = source.relative(basePath);
    assertNotNull(relative);
    // Relative path should not start with /data/files/
    assertEquals("test.csv", relative.path());
  }

  @Test void testRelativeNotSubpath() {
    StorageProviderSource source = createSource("/other/path/test.csv");
    Source basePath = org.apache.calcite.util.Sources.of(new File("/data/files/"));

    Source relative = source.relative(basePath);
    assertNotNull(relative);
    // Should return self since not a subpath
    assertSame(source, relative);
  }

  @Test void testTrimSuffix() {
    StorageProviderSource source = createSource("/data/test.csv.gz");
    Source trimmed = source.trim(".gz");
    assertNotNull(trimmed);
    assertEquals("/data/test.csv", trimmed.path());
  }

  @Test void testTrimSuffixNoMatch() {
    StorageProviderSource source = createSource("/data/test.csv");
    Source trimmed = source.trim(".gz");
    assertSame(source, trimmed);
  }

  @Test void testTrimOrNullSuffix() {
    StorageProviderSource source = createSource("/data/test.csv.gz");
    Source trimmed = source.trimOrNull(".gz");
    assertNotNull(trimmed);
    assertEquals("/data/test.csv", trimmed.path());
  }

  @Test void testTrimOrNullNoMatch() {
    StorageProviderSource source = createSource("/data/test.csv");
    Source trimmed = source.trimOrNull(".gz");
    assertNull(trimmed);
  }

  @Test void testGetStorageProvider() {
    StorageProviderSource source = createSource("/data/test.csv");
    assertNotNull(source.getStorageProvider());
    assertEquals("local", source.getStorageProvider().getStorageType());
  }

  @Test void testGetFileEntry() {
    StorageProviderSource source = createSource("/data/test.csv");
    assertNotNull(source.getFileEntry());
    assertEquals("test.csv", source.getFileEntry().getName());
    assertEquals("/data/test.csv", source.getFileEntry().getPath());
  }

  @Test void testFileOpt() {
    StorageProviderSource source = createSource("/data/test.csv");
    assertTrue(source.fileOpt().isPresent());
  }

  @Test void testFile() {
    StorageProviderSource source = createSource("/data/test.csv");
    File file = source.file();
    assertNotNull(file);
  }

  @Test void testRelativeWithLeadingSlashMismatch() {
    // Test the path normalization in relative()
    StorageProviderSource source = createSource("data/files/test.csv");
    Source basePath = org.apache.calcite.util.Sources.of(new File("/data/files/"));

    Source relative = source.relative(basePath);
    assertNotNull(relative);
  }

  @Test void testAppendWithLeadingSlash() {
    StorageProviderSource base = createSource("/data/files");
    StorageProvider.FileEntry childEntry =
        new StorageProvider.FileEntry("/sub/child.csv", "child.csv", false, 50, 500L);
    StorageProviderSource child = new StorageProviderSource(childEntry, new LocalFileStorageProvider());

    Source appended = base.append(child);
    assertNotNull(appended);
    assertTrue(appended.path().contains("child.csv"));
  }
}
