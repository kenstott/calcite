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

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link LocalFileStorageProvider}.
 * Covers edge cases and methods not covered by the existing StorageProviderTest.
 */
@Tag("unit")
public class LocalFileStorageProviderTest {

  @TempDir
  Path tempDir;

  private final LocalFileStorageProvider provider = new LocalFileStorageProvider();

  // --- getStorageType ---

  @Test void testGetStorageType() {
    assertEquals("local", provider.getStorageType());
  }

  // --- readRange ---

  @Test void testReadRangeFullFile() throws IOException {
    Path file = tempDir.resolve("range.txt");
    Files.write(file, "Hello, World!".getBytes(StandardCharsets.UTF_8));

    byte[] result = provider.readRange(file.toString(), 0, 13);
    assertEquals("Hello, World!", new String(result, StandardCharsets.UTF_8));
  }

  @Test void testReadRangePartial() throws IOException {
    Path file = tempDir.resolve("range.txt");
    Files.write(file, "Hello, World!".getBytes(StandardCharsets.UTF_8));

    byte[] result = provider.readRange(file.toString(), 7, 5);
    assertEquals("World", new String(result, StandardCharsets.UTF_8));
  }

  @Test void testReadRangeFromBeginning() throws IOException {
    Path file = tempDir.resolve("range.txt");
    Files.write(file, "ABCDEFGHIJ".getBytes(StandardCharsets.UTF_8));

    byte[] result = provider.readRange(file.toString(), 0, 5);
    assertEquals("ABCDE", new String(result, StandardCharsets.UTF_8));
  }

  @Test void testReadRangeBeyondFileLength() throws IOException {
    Path file = tempDir.resolve("range.txt");
    Files.write(file, "short".getBytes(StandardCharsets.UTF_8));

    // Request more than available - should return what's available
    byte[] result = provider.readRange(file.toString(), 0, 100);
    assertEquals("short", new String(result, StandardCharsets.UTF_8));
  }

  // --- listFiles ---

  @Test void testListFilesNonExistentDirectory() {
    assertThrows(IOException.class,
        () -> provider.listFiles(tempDir.resolve("nope").toString(), false));
  }

  @Test void testListFilesRecursive() throws IOException {
    Path subDir = tempDir.resolve("sub");
    Files.createDirectories(subDir);
    Files.write(tempDir.resolve("top.csv"), "top".getBytes(StandardCharsets.UTF_8));
    Files.write(subDir.resolve("nested.csv"), "nested".getBytes(StandardCharsets.UTF_8));

    List<StorageProvider.FileEntry> entries = provider.listFiles(tempDir.toString(), true);

    // Should find top.csv, sub directory, and nested.csv
    boolean foundTop = false;
    boolean foundNested = false;
    boolean foundSubDir = false;
    for (StorageProvider.FileEntry entry : entries) {
      if (entry.getName().equals("top.csv")) {
        foundTop = true;
      }
      if (entry.getName().equals("nested.csv")) {
        foundNested = true;
      }
      if (entry.getName().equals("sub") && entry.isDirectory()) {
        foundSubDir = true;
      }
    }
    assertTrue(foundTop);
    assertTrue(foundNested);
    assertTrue(foundSubDir);
  }

  @Test void testListFilesNonRecursive() throws IOException {
    Path subDir = tempDir.resolve("sub");
    Files.createDirectories(subDir);
    Files.write(tempDir.resolve("top.csv"), "top".getBytes(StandardCharsets.UTF_8));
    Files.write(subDir.resolve("nested.csv"), "nested".getBytes(StandardCharsets.UTF_8));

    List<StorageProvider.FileEntry> entries = provider.listFiles(tempDir.toString(), false);

    boolean foundNested = false;
    for (StorageProvider.FileEntry entry : entries) {
      if (entry.getName().equals("nested.csv")) {
        foundNested = true;
      }
    }
    // Non-recursive should not include nested files
    assertFalse(foundNested);
  }

  // --- exists with file:// URLs ---

  @Test void testExistsWithFileUrl() throws IOException {
    Path file = tempDir.resolve("test.txt");
    Files.write(file, "content".getBytes(StandardCharsets.UTF_8));

    String fileUrl = file.toUri().toString();
    assertTrue(provider.exists(fileUrl));
  }

  @Test void testExistsWithInvalidFileUrl() {
    assertFalse(provider.exists("file://invalid[path"));
  }

  // --- isDirectory with file:// URLs ---

  @Test void testIsDirectoryWithFileUrl() {
    String dirUrl = tempDir.toUri().toString();
    assertTrue(provider.isDirectory(dirUrl));
  }

  @Test void testIsDirectoryWithInvalidFileUrl() {
    assertFalse(provider.isDirectory("file://invalid[path"));
  }

  // --- openInputStream with file:// URLs ---

  @Test void testOpenInputStreamWithFileUrl() throws IOException {
    Path file = tempDir.resolve("url-test.txt");
    Files.write(file, "url content".getBytes(StandardCharsets.UTF_8));

    String fileUrl = file.toUri().toString();
    try (InputStream is = provider.openInputStream(fileUrl)) {
      byte[] buffer = new byte[1024];
      int bytesRead = is.read(buffer);
      assertEquals("url content", new String(buffer, 0, bytesRead, StandardCharsets.UTF_8));
    }
  }

  @Test void testOpenInputStreamNonExistent() {
    assertThrows(IOException.class,
        () -> provider.openInputStream(tempDir.resolve("nope.txt").toString()));
  }

  // --- openReader with file:// URLs ---

  @Test void testOpenReaderWithFileUrl() throws IOException {
    Path file = tempDir.resolve("reader-test.txt");
    Files.write(file, "reader content".getBytes(StandardCharsets.UTF_8));

    String fileUrl = file.toUri().toString();
    try (Reader reader = provider.openReader(fileUrl)) {
      char[] buffer = new char[1024];
      int charsRead = reader.read(buffer);
      assertEquals("reader content", new String(buffer, 0, charsRead));
    }
  }

  @Test void testOpenReaderNonExistent() {
    assertThrows(IOException.class,
        () -> provider.openReader(tempDir.resolve("nope.txt").toString()));
  }

  // --- getMetadata ---

  @Test void testGetMetadataNonExistent() {
    assertThrows(IOException.class,
        () -> provider.getMetadata(tempDir.resolve("nope.txt").toString()));
  }

  @Test void testGetMetadataNoEtag() throws IOException {
    Path file = tempDir.resolve("meta.csv");
    Files.write(file, "data".getBytes(StandardCharsets.UTF_8));

    StorageProvider.FileMetadata metadata = provider.getMetadata(file.toString());

    assertNull(metadata.getEtag());
    assertTrue(metadata.getSize() > 0);
    assertTrue(metadata.getLastModified() > 0);
  }

  // --- resolvePath edge cases ---

  @Test void testResolvePathEmptyRelative() {
    String result = provider.resolvePath("/base/dir", "");
    assertEquals("/base/dir", result);
  }

  @Test void testResolvePathNullRelative() {
    String result = provider.resolvePath("/base/dir", null);
    assertEquals("/base/dir", result);
  }

  @Test void testResolvePathNormalizes() {
    String result = provider.resolvePath("/base/dir", "sub/../other.txt");
    assertEquals("/base/dir/other.txt", result);
  }

  // --- writeFile with file:// URLs ---

  @Test void testWriteFileWithFileUrl() throws IOException {
    Path file = tempDir.resolve("url-write.txt");
    String fileUrl = file.toUri().toString();

    provider.writeFile(fileUrl, "written via url".getBytes(StandardCharsets.UTF_8));

    assertTrue(Files.exists(file));
    assertEquals("written via url", new String(Files.readAllBytes(file), StandardCharsets.UTF_8));
  }

  // --- cleanupMacosMetadata ---

  @Test void testCleanupMacosMetadata() throws IOException {
    Path dir = tempDir.resolve("cleanup-test");
    Files.createDirectories(dir);

    // Create metadata files
    Files.write(dir.resolve("._file.csv"), "resource fork".getBytes(StandardCharsets.UTF_8));
    Files.write(dir.resolve(".DS_Store"), "ds store".getBytes(StandardCharsets.UTF_8));
    Files.write(dir.resolve("data.csv.crc"), "checksum".getBytes(StandardCharsets.UTF_8));
    Files.write(dir.resolve("backup.csv~"), "backup".getBytes(StandardCharsets.UTF_8));
    Files.write(dir.resolve("temp.csv.tmp"), "temp".getBytes(StandardCharsets.UTF_8));

    // Create a real data file
    Files.write(dir.resolve("data.csv"), "real data".getBytes(StandardCharsets.UTF_8));

    provider.cleanupMacosMetadata(dir.toString());

    // Metadata files should be deleted
    assertFalse(Files.exists(dir.resolve("._file.csv")));
    assertFalse(Files.exists(dir.resolve(".DS_Store")));
    assertFalse(Files.exists(dir.resolve("data.csv.crc")));
    assertFalse(Files.exists(dir.resolve("backup.csv~")));
    assertFalse(Files.exists(dir.resolve("temp.csv.tmp")));

    // Real data file should remain
    assertTrue(Files.exists(dir.resolve("data.csv")));
  }

  @Test void testCleanupMacosMetadataRecursive() throws IOException {
    Path dir = tempDir.resolve("cleanup-recursive");
    Path subDir = dir.resolve("sub");
    Files.createDirectories(subDir);

    Files.write(dir.resolve("._top.csv"), "top resource".getBytes(StandardCharsets.UTF_8));
    Files.write(subDir.resolve("._nested.csv"), "nested resource".getBytes(StandardCharsets.UTF_8));
    Files.write(subDir.resolve("real.csv"), "data".getBytes(StandardCharsets.UTF_8));

    provider.cleanupMacosMetadata(dir.toString());

    assertFalse(Files.exists(dir.resolve("._top.csv")));
    assertFalse(Files.exists(subDir.resolve("._nested.csv")));
    assertTrue(Files.exists(subDir.resolve("real.csv")));
  }

  @Test void testCleanupMacosMetadataNonExistentDir() throws IOException {
    // Should not throw for non-existent directory
    provider.cleanupMacosMetadata(tempDir.resolve("nope").toString());
  }

  // --- copyFile edge cases ---

  @Test void testCopyFileNonExistentSource() {
    assertThrows(IOException.class,
        () -> provider.copyFile(
            tempDir.resolve("nope.txt").toString(),
            tempDir.resolve("dest.txt").toString()));
  }

  @Test void testCopyFileCreatesParentDirs() throws IOException {
    Path source = tempDir.resolve("source.txt");
    Files.write(source, "source content".getBytes(StandardCharsets.UTF_8));

    Path dest = tempDir.resolve("a/b/c/dest.txt");

    provider.copyFile(source.toString(), dest.toString());

    assertTrue(Files.exists(dest));
    assertEquals("source content", new String(Files.readAllBytes(dest), StandardCharsets.UTF_8));
  }

  // --- normalizePath (static method on StorageProvider interface) ---

  @Test void testNormalizePathNull() {
    assertNull(StorageProvider.normalizePath(null));
  }

  @Test void testNormalizePathS3aSingleSlash() {
    assertEquals("s3a://bucket/key", StorageProvider.normalizePath("s3a:/bucket/key"));
  }

  @Test void testNormalizePathS3aDoubleSlash() {
    assertEquals("s3a://bucket/key", StorageProvider.normalizePath("s3a://bucket/key"));
  }

  @Test void testNormalizePathS3SingleSlash() {
    assertEquals("s3://bucket/key", StorageProvider.normalizePath("s3:/bucket/key"));
  }

  @Test void testNormalizePathHdfsSingleSlash() {
    assertEquals("hdfs://namenode:9000/path", StorageProvider.normalizePath("hdfs:/namenode:9000/path"));
  }

  @Test void testNormalizePathLocalUnchanged() {
    assertEquals("/local/path/file.csv", StorageProvider.normalizePath("/local/path/file.csv"));
  }

  // --- FileEntry ---

  @Test void testFileEntryGetters() {
    StorageProvider.FileEntry entry =
        new StorageProvider.FileEntry("/path/file.csv", "file.csv",
            false, 1024, 1710000000L);

    assertEquals("/path/file.csv", entry.getPath());
    assertEquals("file.csv", entry.getName());
    assertFalse(entry.isDirectory());
    assertEquals(1024, entry.getSize());
    assertEquals(1710000000L, entry.getLastModified());
  }

  @Test void testFileEntryDirectory() {
    StorageProvider.FileEntry entry =
        new StorageProvider.FileEntry("/path/dir", "dir",
            true, 0, 1710000000L);

    assertTrue(entry.isDirectory());
    assertEquals(0, entry.getSize());
  }

  // --- FileMetadata ---

  @Test void testFileMetadataGetters() {
    StorageProvider.FileMetadata metadata =
        new StorageProvider.FileMetadata("/path/file.csv", 2048, 1710000000L,
            "text/csv", "etag-abc");

    assertEquals("/path/file.csv", metadata.getPath());
    assertEquals(2048, metadata.getSize());
    assertEquals(1710000000L, metadata.getLastModified());
    assertEquals("text/csv", metadata.getContentType());
    assertEquals("etag-abc", metadata.getEtag());
  }

  @Test void testFileMetadataWithNullEtag() {
    StorageProvider.FileMetadata metadata =
        new StorageProvider.FileMetadata("/file.csv", 100, 0, "text/csv", null);

    assertNull(metadata.getEtag());
  }
}
