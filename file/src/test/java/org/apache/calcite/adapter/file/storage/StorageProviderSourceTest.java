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

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link StorageProviderSource}.
 */
@Tag("unit")
public class StorageProviderSourceTest {

  @TempDir
  Path tempDir;

  @Test void testPath() {
    StorageProvider.FileEntry entry =
        new StorageProvider.FileEntry("data/file.csv", "file.csv",
            false, 1024, 0);
    InMemoryProvider provider = new InMemoryProvider();

    StorageProviderSource source = new StorageProviderSource(entry, provider);

    assertEquals("data/file.csv", source.path());
  }

  @Test void testProtocol() {
    StorageProvider.FileEntry entry =
        new StorageProvider.FileEntry("data/file.csv", "file.csv",
            false, 1024, 0);
    InMemoryProvider provider = new InMemoryProvider();

    StorageProviderSource source = new StorageProviderSource(entry, provider);

    assertEquals("memory", source.protocol());
  }

  @Test void testUrlReturnsNull() {
    StorageProvider.FileEntry entry =
        new StorageProvider.FileEntry("data/file.csv", "file.csv",
            false, 1024, 0);
    InMemoryProvider provider = new InMemoryProvider();

    StorageProviderSource source = new StorageProviderSource(entry, provider);

    assertNull(source.url());
  }

  @Test void testOpenStream() throws IOException {
    InMemoryProvider provider = new InMemoryProvider();
    provider.addFile("data/file.csv", "csv,content");

    StorageProvider.FileEntry entry =
        new StorageProvider.FileEntry("data/file.csv", "file.csv",
            false, 11, 0);

    StorageProviderSource source = new StorageProviderSource(entry, provider);

    try (InputStream is = source.openStream()) {
      byte[] buffer = new byte[1024];
      int bytesRead = is.read(buffer);
      assertEquals("csv,content", new String(buffer, 0, bytesRead, StandardCharsets.UTF_8));
    }
  }

  @Test void testReader() throws IOException {
    InMemoryProvider provider = new InMemoryProvider();
    provider.addFile("data/file.csv", "header\nrow1");

    StorageProvider.FileEntry entry =
        new StorageProvider.FileEntry("data/file.csv", "file.csv",
            false, 12, 0);

    StorageProviderSource source = new StorageProviderSource(entry, provider);

    try (Reader reader = source.reader()) {
      char[] buffer = new char[1024];
      int charsRead = reader.read(buffer);
      assertEquals("header\nrow1", new String(buffer, 0, charsRead));
    }
  }

  @Test void testAppend() {
    StorageProvider.FileEntry parentEntry =
        new StorageProvider.FileEntry("base/dir", "dir", true, 0, 0);
    InMemoryProvider provider = new InMemoryProvider();

    StorageProviderSource parent = new StorageProviderSource(parentEntry, provider);

    Source childSource = new SimpleSource("child/file.csv");
    Source appended = parent.append(childSource);

    assertNotNull(appended);
    assertEquals("base/dir/child/file.csv", appended.path());
  }

  @Test void testAppendRemovesLeadingSlash() {
    StorageProvider.FileEntry parentEntry =
        new StorageProvider.FileEntry("base", "base", true, 0, 0);
    InMemoryProvider provider = new InMemoryProvider();

    StorageProviderSource parent = new StorageProviderSource(parentEntry, provider);

    Source childSource = new SimpleSource("/leading-slash.csv");
    Source appended = parent.append(childSource);

    assertEquals("base/leading-slash.csv", appended.path());
  }

  @Test void testRelative() {
    StorageProvider.FileEntry entry =
        new StorageProvider.FileEntry("base/dir/sub/file.csv", "file.csv",
            false, 100, 0);
    InMemoryProvider provider = new InMemoryProvider();

    StorageProviderSource source = new StorageProviderSource(entry, provider);

    Source baseSource = new SimpleSource("base/dir");
    Source relative = source.relative(baseSource);

    assertEquals("sub/file.csv", relative.path());
  }

  @Test void testRelativeNonMatchingBase() {
    StorageProvider.FileEntry entry =
        new StorageProvider.FileEntry("other/path/file.csv", "file.csv",
            false, 100, 0);
    InMemoryProvider provider = new InMemoryProvider();

    StorageProviderSource source = new StorageProviderSource(entry, provider);

    Source baseSource = new SimpleSource("base/dir");
    Source relative = source.relative(baseSource);

    // Should return self when base doesn't match
    assertEquals("other/path/file.csv", relative.path());
  }

  @Test void testTrim() {
    StorageProvider.FileEntry entry =
        new StorageProvider.FileEntry("data/file.csv.gz", "file.csv.gz",
            false, 100, 0);
    InMemoryProvider provider = new InMemoryProvider();

    StorageProviderSource source = new StorageProviderSource(entry, provider);

    Source trimmed = source.trim(".gz");

    assertEquals("data/file.csv", trimmed.path());
  }

  @Test void testTrimNonMatchingSuffix() {
    StorageProvider.FileEntry entry =
        new StorageProvider.FileEntry("data/file.csv", "file.csv",
            false, 100, 0);
    InMemoryProvider provider = new InMemoryProvider();

    StorageProviderSource source = new StorageProviderSource(entry, provider);

    Source trimmed = source.trim(".gz");

    // Should return self when suffix doesn't match
    assertEquals("data/file.csv", trimmed.path());
  }

  @Test void testTrimOrNullWithMatch() {
    StorageProvider.FileEntry entry =
        new StorageProvider.FileEntry("data/file.csv.gz", "file.csv.gz",
            false, 100, 0);
    InMemoryProvider provider = new InMemoryProvider();

    StorageProviderSource source = new StorageProviderSource(entry, provider);

    Source trimmed = source.trimOrNull(".gz");

    assertNotNull(trimmed);
    assertEquals("data/file.csv", trimmed.path());
  }

  @Test void testTrimOrNullWithoutMatch() {
    StorageProvider.FileEntry entry =
        new StorageProvider.FileEntry("data/file.csv", "file.csv",
            false, 100, 0);
    InMemoryProvider provider = new InMemoryProvider();

    StorageProviderSource source = new StorageProviderSource(entry, provider);

    Source trimmed = source.trimOrNull(".gz");

    assertNull(trimmed);
  }

  @Test void testGetStorageProvider() {
    StorageProvider.FileEntry entry =
        new StorageProvider.FileEntry("data/file.csv", "file.csv",
            false, 100, 0);
    InMemoryProvider provider = new InMemoryProvider();

    StorageProviderSource source = new StorageProviderSource(entry, provider);

    assertEquals(provider, source.getStorageProvider());
  }

  @Test void testGetFileEntry() {
    StorageProvider.FileEntry entry =
        new StorageProvider.FileEntry("data/file.csv", "file.csv",
            false, 100, 1234);
    InMemoryProvider provider = new InMemoryProvider();

    StorageProviderSource source = new StorageProviderSource(entry, provider);

    assertEquals(entry, source.getFileEntry());
    assertEquals("file.csv", source.getFileEntry().getName());
    assertEquals(100, source.getFileEntry().getSize());
  }

  @Test void testFileOptReturnsPresent() {
    StorageProvider.FileEntry entry =
        new StorageProvider.FileEntry("data/file.csv", "file.csv",
            false, 100, 0);
    InMemoryProvider provider = new InMemoryProvider();

    StorageProviderSource source = new StorageProviderSource(entry, provider);

    Optional<File> fileOpt = source.fileOpt();
    assertTrue(fileOpt.isPresent());
  }

  @Test void testFileReturnsStorageProviderFile() {
    StorageProvider.FileEntry entry =
        new StorageProvider.FileEntry("data/file.csv", "file.csv",
            false, 100, 0);
    InMemoryProvider provider = new InMemoryProvider();

    StorageProviderSource source = new StorageProviderSource(entry, provider);

    File file = source.file();
    assertNotNull(file);
    assertTrue(file instanceof StorageProviderFile);
  }

  /**
   * Simple Source implementation for testing append/relative.
   */
  private static class SimpleSource implements Source {
    private final String path;

    SimpleSource(String path) {
      this.path = path;
    }

    @Override public java.net.URL url() {
      return null;
    }

    @Override public String path() {
      return path;
    }

    @Override public InputStream openStream() {
      return new ByteArrayInputStream(new byte[0]);
    }

    @Override public Reader reader() {
      return new java.io.StringReader("");
    }

    @Override public String protocol() {
      return "simple";
    }

    @Override public Source append(Source child) {
      return new SimpleSource(path + "/" + child.path());
    }

    @Override public Source relative(Source source) {
      return this;
    }

    @Override public Source trim(String suffix) {
      return this;
    }

    @Override public Source trimOrNull(String suffix) {
      return null;
    }

    @Override public Optional<File> fileOpt() {
      return Optional.empty();
    }

    @Override public File file() {
      return new File(path);
    }
  }

  /**
   * In-memory StorageProvider for testing.
   */
  private static class InMemoryProvider implements StorageProvider {
    private final java.util.Map<String, String> files =
        new java.util.HashMap<String, String>();

    void addFile(String path, String content) {
      files.put(path, content);
    }

    @Override
    public List<StorageProvider.FileEntry> listFiles(String path, boolean recursive) {
      return new ArrayList<StorageProvider.FileEntry>();
    }

    @Override
    public StorageProvider.FileMetadata getMetadata(String path) throws IOException {
      return new StorageProvider.FileMetadata(path, 0, 0, "text/csv", null);
    }

    @Override
    public InputStream openInputStream(String path) throws IOException {
      String content = files.get(path);
      if (content == null) {
        throw new IOException("File not found: " + path);
      }
      return new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public Reader openReader(String path) throws IOException {
      String content = files.get(path);
      if (content == null) {
        throw new IOException("File not found: " + path);
      }
      return new java.io.StringReader(content);
    }

    @Override
    public boolean exists(String path) {
      return files.containsKey(path);
    }

    @Override
    public boolean isDirectory(String path) {
      return false;
    }

    @Override
    public String getStorageType() {
      return "memory";
    }

    @Override
    public String resolvePath(String basePath, String relativePath) {
      return basePath + "/" + relativePath;
    }
  }
}
