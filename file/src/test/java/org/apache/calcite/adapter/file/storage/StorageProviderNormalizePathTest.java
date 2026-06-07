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

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link StorageProvider#normalizePath}, {@link StorageProvider#hasChanged},
 * and inner classes {@link StorageProvider.FileEntry} and {@link StorageProvider.FileMetadata}.
 */
@Tag("unit")
class StorageProviderNormalizePathTest {

  @TempDir
  File tempDir;

  // --- normalizePath tests ---

  @Test void testNormalizePathNull() {
    assertNull(StorageProvider.normalizePath(null));
  }

  @Test void testNormalizePathRegularPath() {
    assertEquals("/tmp/file.parquet", StorageProvider.normalizePath("/tmp/file.parquet"));
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

  @Test void testNormalizePathS3DoubleSlash() {
    assertEquals("s3://bucket/key", StorageProvider.normalizePath("s3://bucket/key"));
  }

  @Test void testNormalizePathHdfsSingleSlash() {
    assertEquals("hdfs://namenode/path", StorageProvider.normalizePath("hdfs:/namenode/path"));
  }

  @Test void testNormalizePathHdfsDoubleSlash() {
    assertEquals("hdfs://namenode/path", StorageProvider.normalizePath("hdfs://namenode/path"));
  }

  @Test void testNormalizePathLocalPath() {
    assertEquals("/local/path/file.parquet",
        StorageProvider.normalizePath("/local/path/file.parquet"));
  }

  // --- FileEntry tests ---

  @Test void testFileEntryCreation() {
    StorageProvider.FileEntry entry =
        new StorageProvider.FileEntry("/path/file.csv", "file.csv", false, 1024, 1000L);
    assertEquals("/path/file.csv", entry.getPath());
    assertEquals("file.csv", entry.getName());
    assertFalse(entry.isDirectory());
    assertEquals(1024, entry.getSize());
    assertEquals(1000L, entry.getLastModified());
  }

  @Test void testFileEntryDirectory() {
    StorageProvider.FileEntry entry =
        new StorageProvider.FileEntry("/path/dir", "dir", true, 0, 2000L);
    assertTrue(entry.isDirectory());
    assertEquals(0, entry.getSize());
  }

  // --- FileMetadata tests ---

  @Test void testFileMetadataCreation() {
    StorageProvider.FileMetadata metadata =
        new StorageProvider.FileMetadata("/path/file.csv", 2048, 3000L,
            "text/csv", "abc123");
    assertEquals("/path/file.csv", metadata.getPath());
    assertEquals(2048, metadata.getSize());
    assertEquals(3000L, metadata.getLastModified());
    assertEquals("text/csv", metadata.getContentType());
    assertEquals("abc123", metadata.getEtag());
  }

  @Test void testFileMetadataWithNullEtag() {
    StorageProvider.FileMetadata metadata =
        new StorageProvider.FileMetadata("/path/file.csv", 1024, 1000L,
            "application/octet-stream", null);
    assertNull(metadata.getEtag());
  }

  // --- hasChanged tests ---

  @Test void testHasChangedWithNullCachedMetadata() throws IOException {
    LocalFileStorageProvider provider = new LocalFileStorageProvider();
    File testFile = new File(tempDir, "test.txt");
    Files.write(testFile.toPath(), "data".getBytes());

    assertTrue(provider.hasChanged(testFile.getAbsolutePath(), null));
  }

  @Test void testHasChangedSameMetadata() throws IOException {
    LocalFileStorageProvider provider = new LocalFileStorageProvider();
    File testFile = new File(tempDir, "same.txt");
    Files.write(testFile.toPath(), "data".getBytes());

    StorageProvider.FileMetadata cached = provider.getMetadata(testFile.getAbsolutePath());

    // Same file, same metadata - should not have changed
    assertFalse(provider.hasChanged(testFile.getAbsolutePath(), cached));
  }

  @Test void testHasChangedDifferentSize() throws IOException {
    LocalFileStorageProvider provider = new LocalFileStorageProvider();
    File testFile = new File(tempDir, "size.txt");
    Files.write(testFile.toPath(), "data".getBytes());

    // Create cached metadata with different size
    StorageProvider.FileMetadata cached =
        new StorageProvider.FileMetadata(testFile.getAbsolutePath(), 999999, 0L,
            "application/octet-stream", null);

    assertTrue(provider.hasChanged(testFile.getAbsolutePath(), cached));
  }

  @Test void testHasChangedWithMatchingEtags() throws IOException {
    LocalFileStorageProvider provider = new LocalFileStorageProvider();
    File testFile = new File(tempDir, "etag.txt");
    Files.write(testFile.toPath(), "data".getBytes());

    StorageProvider.FileMetadata currentMeta = provider.getMetadata(testFile.getAbsolutePath());

    // Create cached metadata with same size, same etag
    StorageProvider.FileMetadata cached =
        new StorageProvider.FileMetadata(testFile.getAbsolutePath(),
            currentMeta.getSize(), currentMeta.getLastModified(),
            "application/octet-stream", "etag123");

    // Both have the same size; but current has null etag, cached has non-null
    // Falls through to timestamp comparison
    assertFalse(provider.hasChanged(testFile.getAbsolutePath(), cached));
  }

  @Test void testHasChangedNonExistentFileAssumeChanged() throws IOException {
    LocalFileStorageProvider provider = new LocalFileStorageProvider();
    StorageProvider.FileMetadata cached =
        new StorageProvider.FileMetadata("/nonexistent/file", 100, 1000L,
            "text/plain", null);

    // Should return true (assume changed) when file doesn't exist
    assertTrue(provider.hasChanged("/nonexistent/file", cached));
  }

  // --- Default method tests on StorageProvider interface ---

  @Test void testDefaultReadRangeThrows() {
    StorageProvider provider = new MinimalStorageProvider();
    assertThrows(UnsupportedOperationException.class,
        () -> provider.readRange("/path", 0, 100));
  }

  @Test void testDefaultWriteFileThrows() {
    StorageProvider provider = new MinimalStorageProvider();
    assertThrows(UnsupportedOperationException.class,
        () -> provider.writeFile("/path", new byte[0]));
  }

  @Test void testDefaultWriteFileStreamThrows() {
    StorageProvider provider = new MinimalStorageProvider();
    assertThrows(UnsupportedOperationException.class,
        () -> provider.writeFile("/path", (java.io.InputStream) null));
  }

  @Test void testDefaultCreateDirectoriesThrows() {
    StorageProvider provider = new MinimalStorageProvider();
    assertThrows(UnsupportedOperationException.class,
        () -> provider.createDirectories("/path"));
  }

  @Test void testDefaultDeleteThrows() {
    StorageProvider provider = new MinimalStorageProvider();
    assertThrows(UnsupportedOperationException.class,
        () -> provider.delete("/path"));
  }

  @Test void testDefaultCopyFileThrows() {
    StorageProvider provider = new MinimalStorageProvider();
    assertThrows(UnsupportedOperationException.class,
        () -> provider.copyFile("/source", "/dest"));
  }

  @Test void testDefaultGetS3ConfigReturnsNull() {
    StorageProvider provider = new MinimalStorageProvider();
    assertNull(provider.getS3Config());
  }

  @Test void testDefaultEnsureLifecycleRuleNoOp() throws IOException {
    StorageProvider provider = new MinimalStorageProvider();
    // Should not throw
    provider.ensureLifecycleRule("prefix", 30);
  }

  @Test void testDefaultCleanupMacosMetadataNoOp() throws IOException {
    StorageProvider provider = new MinimalStorageProvider();
    // Should not throw
    provider.cleanupMacosMetadata("/tmp");
  }

  @Test void testDefaultDeleteBatchFallback() throws IOException {
    // LocalFileStorageProvider uses the default deleteBatch which calls delete()
    LocalFileStorageProvider provider = new LocalFileStorageProvider();
    File f1 = new File(tempDir, "del1.txt");
    File f2 = new File(tempDir, "del2.txt");
    Files.write(f1.toPath(), "a".getBytes());
    Files.write(f2.toPath(), "b".getBytes());

    java.util.List<String> paths = java.util.Arrays.asList(
        f1.getAbsolutePath(), f2.getAbsolutePath());
    int deleted = provider.deleteBatch(paths);
    assertEquals(2, deleted);
    assertFalse(f1.exists());
    assertFalse(f2.exists());
  }

  @Test void testDeleteBatchWithNonExistentFiles() throws IOException {
    LocalFileStorageProvider provider = new LocalFileStorageProvider();
    java.util.List<String> paths = java.util.Arrays.asList(
        new File(tempDir, "nope1.txt").getAbsolutePath(),
        new File(tempDir, "nope2.txt").getAbsolutePath());
    int deleted = provider.deleteBatch(paths);
    assertEquals(0, deleted);
  }

  /**
   * Minimal StorageProvider implementation for testing default methods.
   */
  private static class MinimalStorageProvider implements StorageProvider {
    @Override public java.util.List<FileEntry> listFiles(String path, boolean recursive) {
      return java.util.Collections.emptyList();
    }

    @Override public FileMetadata getMetadata(String path) {
      return new FileMetadata(path, 0, 0, null, null);
    }

    @Override public java.io.InputStream openInputStream(String path) {
      return new java.io.ByteArrayInputStream(new byte[0]);
    }

    @Override public java.io.Reader openReader(String path) {
      return new java.io.StringReader("");
    }

    @Override public boolean exists(String path) {
      return false;
    }

    @Override public boolean isDirectory(String path) {
      return false;
    }

    @Override public String getStorageType() {
      return "minimal";
    }

    @Override public String resolvePath(String basePath, String relativePath) {
      return basePath + "/" + relativePath;
    }
  }
}
