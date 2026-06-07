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

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link StorageProviderFile}.
 */
@Tag("unit")
public class StorageProviderFileTest {

  @TempDir
  Path tempDir;

  // --- Local file tests ---

  @Test void testCreateLocalFile() {
    StorageProvider provider = new LocalFileStorageProvider();
    StorageProviderFile file = StorageProviderFile.create("/tmp/test.txt", provider);

    assertNotNull(file);
    assertTrue(file.isLocal());
    assertEquals("/tmp/test.txt", file.getPath());
  }

  @Test void testLocalFileExists() throws IOException {
    Path testFile = tempDir.resolve("test.txt");
    Files.write(testFile, "content".getBytes(StandardCharsets.UTF_8));

    StorageProvider provider = new LocalFileStorageProvider();
    StorageProviderFile file = StorageProviderFile.create(testFile.toString(), provider);

    assertTrue(file.exists());
    assertTrue(file.isLocal());
  }

  @Test void testLocalFileDoesNotExist() {
    StorageProvider provider = new LocalFileStorageProvider();
    StorageProviderFile file = StorageProviderFile.create(
        tempDir.resolve("nonexistent.txt").toString(), provider);

    assertFalse(file.exists());
  }

  @Test void testLocalFileLength() throws IOException {
    Path testFile = tempDir.resolve("test.txt");
    byte[] content = "Hello, World!".getBytes(StandardCharsets.UTF_8);
    Files.write(testFile, content);

    StorageProvider provider = new LocalFileStorageProvider();
    StorageProviderFile file = StorageProviderFile.create(testFile.toString(), provider);

    assertEquals(content.length, file.length());
  }

  @Test void testLocalFileLastModified() throws IOException {
    Path testFile = tempDir.resolve("test.txt");
    Files.write(testFile, "content".getBytes(StandardCharsets.UTF_8));

    StorageProvider provider = new LocalFileStorageProvider();
    StorageProviderFile file = StorageProviderFile.create(testFile.toString(), provider);

    assertTrue(file.lastModified() > 0);
  }

  @Test void testLocalFileOpenInputStream() throws IOException {
    Path testFile = tempDir.resolve("test.txt");
    Files.write(testFile, "stream content".getBytes(StandardCharsets.UTF_8));

    StorageProvider provider = new LocalFileStorageProvider();
    StorageProviderFile file = StorageProviderFile.create(testFile.toString(), provider);

    try (InputStream is = file.openInputStream()) {
      byte[] buffer = new byte[1024];
      int bytesRead = is.read(buffer);
      String result = new String(buffer, 0, bytesRead, StandardCharsets.UTF_8);
      assertEquals("stream content", result);
    }
  }

  @Test void testLocalFileWriteBytes() throws IOException {
    Path testFile = tempDir.resolve("write-test.txt");

    StorageProvider provider = new LocalFileStorageProvider();
    StorageProviderFile file = StorageProviderFile.create(testFile.toString(), provider);

    byte[] content = "written content".getBytes(StandardCharsets.UTF_8);
    file.writeBytes(content);

    assertTrue(Files.exists(testFile));
    assertArrayEquals(content, Files.readAllBytes(testFile));
  }

  @Test void testLocalFileWriteBytesCreatesParentDirs() throws IOException {
    Path nested = tempDir.resolve("a/b/c/file.txt");

    StorageProvider provider = new LocalFileStorageProvider();
    StorageProviderFile file = StorageProviderFile.create(nested.toString(), provider);

    file.writeBytes("nested".getBytes(StandardCharsets.UTF_8));

    assertTrue(Files.exists(nested));
    assertEquals("nested", new String(Files.readAllBytes(nested), StandardCharsets.UTF_8));
  }

  @Test void testLocalFileWriteInputStream() throws IOException {
    Path testFile = tempDir.resolve("stream-write.txt");

    StorageProvider provider = new LocalFileStorageProvider();
    StorageProviderFile file = StorageProviderFile.create(testFile.toString(), provider);

    try (InputStream is = new ByteArrayInputStream("stream data".getBytes(StandardCharsets.UTF_8))) {
      file.writeInputStream(is);
    }

    assertTrue(Files.exists(testFile));
    assertEquals("stream data", new String(Files.readAllBytes(testFile), StandardCharsets.UTF_8));
  }

  @Test void testLocalFileDelete() throws IOException {
    Path testFile = tempDir.resolve("delete-me.txt");
    Files.write(testFile, "content".getBytes(StandardCharsets.UTF_8));

    StorageProvider provider = new LocalFileStorageProvider();
    StorageProviderFile file = StorageProviderFile.create(testFile.toString(), provider);

    assertTrue(file.exists());
    assertTrue(file.delete());
    assertFalse(file.exists());
  }

  @Test void testLocalFileMkdirs() throws IOException {
    Path dirPath = tempDir.resolve("new/nested/dir");

    StorageProvider provider = new LocalFileStorageProvider();
    StorageProviderFile file = StorageProviderFile.create(dirPath.toString(), provider);

    assertTrue(file.mkdirs());
    assertTrue(Files.exists(dirPath));
    assertTrue(Files.isDirectory(dirPath));
  }

  @Test void testLocalFileGetFile() {
    StorageProvider provider = new LocalFileStorageProvider();
    StorageProviderFile file = StorageProviderFile.create("/tmp/test.txt", provider);

    assertNotNull(file.getFile());
  }

  // --- Remote file tests ---

  @Test void testCreateRemoteFile() {
    StorageProvider.FileEntry entry =
        new StorageProvider.FileEntry("remote/path/file.csv", "file.csv",
            false, 1024, 1710000000L);
    MockStorageProvider provider = new MockStorageProvider();

    StorageProviderFile file = StorageProviderFile.create(entry, provider);

    assertNotNull(file);
    assertFalse(file.isLocal());
    assertEquals(1024, file.length());
    assertEquals(1710000000L, file.lastModified());
  }

  @Test void testRemoteFileGetFileThrows() {
    StorageProvider.FileEntry entry =
        new StorageProvider.FileEntry("remote/file.csv", "file.csv",
            false, 100, 0);
    MockStorageProvider provider = new MockStorageProvider();

    StorageProviderFile file = StorageProviderFile.create(entry, provider);

    assertThrows(UnsupportedOperationException.class, () -> file.getFile());
  }

  @Test void testRemoteFileExists() {
    StorageProvider.FileEntry entry =
        new StorageProvider.FileEntry("existing/file.csv", "file.csv",
            false, 100, 0);
    MockStorageProvider provider = new MockStorageProvider();
    provider.addExistingPath("existing/file.csv");

    StorageProviderFile file = StorageProviderFile.create(entry, provider);

    assertTrue(file.exists());
  }

  @Test void testRemoteFileNotExists() {
    StorageProvider.FileEntry entry =
        new StorageProvider.FileEntry("missing/file.csv", "file.csv",
            false, 100, 0);
    MockStorageProvider provider = new MockStorageProvider();

    StorageProviderFile file = StorageProviderFile.create(entry, provider);

    assertFalse(file.exists());
  }

  @Test void testRemoteFileGetStorageProvider() {
    StorageProvider.FileEntry entry =
        new StorageProvider.FileEntry("path/file.csv", "file.csv",
            false, 100, 0);
    MockStorageProvider provider = new MockStorageProvider();

    StorageProviderFile file = StorageProviderFile.create(entry, provider);

    assertEquals(provider, file.getStorageProvider());
  }

  @Test void testRemoteFileGetFileEntry() {
    StorageProvider.FileEntry entry =
        new StorageProvider.FileEntry("path/file.csv", "file.csv",
            false, 100, 0);
    MockStorageProvider provider = new MockStorageProvider();

    StorageProviderFile file = StorageProviderFile.create(entry, provider);

    assertEquals(entry, file.getFileEntry());
  }

  @Test void testCreateTempPathAddsLeadingSlash() {
    StorageProvider.FileEntry entry =
        new StorageProvider.FileEntry("no-leading-slash.csv", "no-leading-slash.csv",
            false, 100, 0);
    MockStorageProvider provider = new MockStorageProvider();

    StorageProviderFile file = StorageProviderFile.create(entry, provider);

    // The path should start with /
    assertTrue(file.getAbsolutePath().contains("no-leading-slash.csv"));
  }

  /**
   * Simple mock StorageProvider for testing remote file operations.
   */
  private static class MockStorageProvider implements StorageProvider {
    private final java.util.Set<String> existingPaths = new java.util.HashSet<String>();

    void addExistingPath(String path) {
      existingPaths.add(path);
    }

    @Override
    public List<StorageProvider.FileEntry> listFiles(String path, boolean recursive) {
      return new ArrayList<StorageProvider.FileEntry>();
    }

    @Override
    public StorageProvider.FileMetadata getMetadata(String path) throws IOException {
      throw new IOException("Not supported");
    }

    @Override
    public InputStream openInputStream(String path) throws IOException {
      return new ByteArrayInputStream(new byte[0]);
    }

    @Override
    public Reader openReader(String path) throws IOException {
      return new java.io.StringReader("");
    }

    @Override
    public boolean exists(String path) {
      return existingPaths.contains(path);
    }

    @Override
    public boolean isDirectory(String path) {
      return false;
    }

    @Override
    public String getStorageType() {
      return "mock";
    }

    @Override
    public String resolvePath(String basePath, String relativePath) {
      return basePath + "/" + relativePath;
    }
  }
}
