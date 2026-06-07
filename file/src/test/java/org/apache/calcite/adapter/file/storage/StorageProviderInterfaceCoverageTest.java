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

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Coverage tests for the {@link StorageProvider} interface default methods
 * and inner classes ({@link StorageProvider.FileEntry} and {@link StorageProvider.FileMetadata}).
 */
@Tag("unit")
public class StorageProviderInterfaceCoverageTest {

  // ---------------------------------------------------------------
  // FileEntry tests
  // ---------------------------------------------------------------

  @Nested
  @DisplayName("FileEntry")
  class FileEntryTests {

    @Test
    @DisplayName("constructor and getters return correct values")
    void testConstructorAndGetters() {
      StorageProvider.FileEntry entry =
          new StorageProvider.FileEntry("/path/to/file.csv", "file.csv", false, 1024L, 1700000000L);
      assertEquals("/path/to/file.csv", entry.getPath());
      assertEquals("file.csv", entry.getName());
      assertFalse(entry.isDirectory());
      assertEquals(1024L, entry.getSize());
      assertEquals(1700000000L, entry.getLastModified());
    }

    @Test
    @DisplayName("directory entry returns isDirectory=true")
    void testDirectoryEntry() {
      StorageProvider.FileEntry entry =
          new StorageProvider.FileEntry("/path/to/dir", "dir", true, 0L, 1700000000L);
      assertTrue(entry.isDirectory());
      assertEquals(0L, entry.getSize());
    }

    @Test
    @DisplayName("entry with zero size and zero timestamp")
    void testZeroValues() {
      StorageProvider.FileEntry entry =
          new StorageProvider.FileEntry("", "", false, 0L, 0L);
      assertEquals("", entry.getPath());
      assertEquals("", entry.getName());
      assertEquals(0L, entry.getSize());
      assertEquals(0L, entry.getLastModified());
    }
  }

  // ---------------------------------------------------------------
  // FileMetadata tests
  // ---------------------------------------------------------------

  @Nested
  @DisplayName("FileMetadata")
  class FileMetadataTests {

    @Test
    @DisplayName("constructor and getters return correct values")
    void testConstructorAndGetters() {
      StorageProvider.FileMetadata meta =
          new StorageProvider.FileMetadata("/path/file.csv", 2048L, 1700000000L,
              "text/csv", "abc123");
      assertEquals("/path/file.csv", meta.getPath());
      assertEquals(2048L, meta.getSize());
      assertEquals(1700000000L, meta.getLastModified());
      assertEquals("text/csv", meta.getContentType());
      assertEquals("abc123", meta.getEtag());
    }

    @Test
    @DisplayName("null contentType and etag are handled")
    void testNullContentTypeAndEtag() {
      StorageProvider.FileMetadata meta =
          new StorageProvider.FileMetadata("/path/file.bin", 0L, 0L, null, null);
      assertNull(meta.getContentType());
      assertNull(meta.getEtag());
    }
  }

  // ---------------------------------------------------------------
  // Default method tests (using a minimal stub implementation)
  // ---------------------------------------------------------------

  /**
   * Minimal implementation of StorageProvider that only implements required abstract methods.
   * Used to test all default methods in the interface.
   */
  static class StubStorageProvider implements StorageProvider {
    private final String storageType;
    private boolean existsResult = true;
    private StorageProvider.FileMetadata metadataResult;

    StubStorageProvider(String storageType) {
      this.storageType = storageType;
    }

    void setExistsResult(boolean result) {
      this.existsResult = result;
    }

    void setMetadataResult(StorageProvider.FileMetadata result) {
      this.metadataResult = result;
    }

    @Override public List<StorageProvider.FileEntry> listFiles(String path, boolean recursive) {
      return new ArrayList<>();
    }

    @Override public StorageProvider.FileMetadata getMetadata(String path) throws IOException {
      if (metadataResult != null) {
        return metadataResult;
      }
      throw new IOException("Not found");
    }

    @Override public InputStream openInputStream(String path) {
      return new ByteArrayInputStream("test content".getBytes(StandardCharsets.UTF_8));
    }

    @Override public Reader openReader(String path) {
      return new java.io.InputStreamReader(openInputStream(path), StandardCharsets.UTF_8);
    }

    @Override public boolean exists(String path) {
      return existsResult;
    }

    @Override public boolean isDirectory(String path) {
      return false;
    }

    @Override public String getStorageType() {
      return storageType;
    }

    @Override public String resolvePath(String basePath, String relativePath) {
      return basePath + "/" + relativePath;
    }
  }

  @Nested
  @DisplayName("Default methods")
  class DefaultMethodTests {

    @Test
    @DisplayName("readRange throws UnsupportedOperationException by default")
    void testReadRangeDefault() {
      StubStorageProvider provider = new StubStorageProvider("test");
      UnsupportedOperationException ex = assertThrows(UnsupportedOperationException.class,
          () -> provider.readRange("/path", 0, 100));
      assertTrue(ex.getMessage().contains("test"));
      assertTrue(ex.getMessage().contains("Range reads"));
    }

    @Test
    @DisplayName("writeFile(byte[]) throws UnsupportedOperationException by default")
    void testWriteFileBytesDefault() {
      StubStorageProvider provider = new StubStorageProvider("mytype");
      UnsupportedOperationException ex = assertThrows(UnsupportedOperationException.class,
          () -> provider.writeFile("/path", new byte[]{1, 2, 3}));
      assertTrue(ex.getMessage().contains("mytype"));
    }

    @Test
    @DisplayName("writeFile(InputStream) throws UnsupportedOperationException by default")
    void testWriteFileStreamDefault() {
      StubStorageProvider provider = new StubStorageProvider("mytype");
      InputStream is = new ByteArrayInputStream(new byte[]{1, 2, 3});
      UnsupportedOperationException ex = assertThrows(UnsupportedOperationException.class,
          () -> provider.writeFile("/path", is));
      assertTrue(ex.getMessage().contains("mytype"));
    }

    @Test
    @DisplayName("createDirectories throws UnsupportedOperationException by default")
    void testCreateDirectoriesDefault() {
      StubStorageProvider provider = new StubStorageProvider("mytype");
      UnsupportedOperationException ex = assertThrows(UnsupportedOperationException.class,
          () -> provider.createDirectories("/path/new"));
      assertTrue(ex.getMessage().contains("mytype"));
    }

    @Test
    @DisplayName("delete throws UnsupportedOperationException by default")
    void testDeleteDefault() {
      StubStorageProvider provider = new StubStorageProvider("mytype");
      UnsupportedOperationException ex = assertThrows(UnsupportedOperationException.class,
          () -> provider.delete("/path"));
      assertTrue(ex.getMessage().contains("mytype"));
    }

    @Test
    @DisplayName("copyFile throws UnsupportedOperationException by default")
    void testCopyFileDefault() {
      StubStorageProvider provider = new StubStorageProvider("mytype");
      UnsupportedOperationException ex = assertThrows(UnsupportedOperationException.class,
          () -> provider.copyFile("/src", "/dst"));
      assertTrue(ex.getMessage().contains("mytype"));
    }

    @Test
    @DisplayName("ensureLifecycleRule is a no-op by default")
    void testEnsureLifecycleRuleDefault() throws IOException {
      StubStorageProvider provider = new StubStorageProvider("test");
      // Should not throw - it's a no-op
      provider.ensureLifecycleRule("prefix/", 30);
    }

    @Test
    @DisplayName("cleanupMacosMetadata is a no-op by default")
    void testCleanupMacosMetadataDefault() throws IOException {
      StubStorageProvider provider = new StubStorageProvider("test");
      // Should not throw - it's a no-op
      provider.cleanupMacosMetadata("/some/path");
    }

    @Test
    @DisplayName("getS3Config returns null by default")
    void testGetS3ConfigDefault() {
      StubStorageProvider provider = new StubStorageProvider("test");
      assertNull(provider.getS3Config());
    }

    @Test
    @DisplayName("hasChanged returns true when cachedMetadata is null")
    void testHasChangedNullCache() throws IOException {
      StubStorageProvider provider = new StubStorageProvider("test");
      assertTrue(provider.hasChanged("/path", null));
    }

    @Test
    @DisplayName("hasChanged returns true when sizes differ")
    void testHasChangedDifferentSize() throws IOException {
      StubStorageProvider provider = new StubStorageProvider("test");
      StorageProvider.FileMetadata currentMeta =
          new StorageProvider.FileMetadata("/path", 100L, 1000L, null, null);
      StorageProvider.FileMetadata cachedMeta =
          new StorageProvider.FileMetadata("/path", 200L, 1000L, null, null);
      provider.setMetadataResult(currentMeta);
      assertTrue(provider.hasChanged("/path", cachedMeta));
    }

    @Test
    @DisplayName("hasChanged returns false when etags match")
    void testHasChangedEtagMatch() throws IOException {
      StubStorageProvider provider = new StubStorageProvider("test");
      StorageProvider.FileMetadata currentMeta =
          new StorageProvider.FileMetadata("/path", 100L, 1000L, null, "etag123");
      StorageProvider.FileMetadata cachedMeta =
          new StorageProvider.FileMetadata("/path", 100L, 1000L, null, "etag123");
      provider.setMetadataResult(currentMeta);
      assertFalse(provider.hasChanged("/path", cachedMeta));
    }

    @Test
    @DisplayName("hasChanged returns true when etags differ")
    void testHasChangedEtagDiffer() throws IOException {
      StubStorageProvider provider = new StubStorageProvider("test");
      StorageProvider.FileMetadata currentMeta =
          new StorageProvider.FileMetadata("/path", 100L, 1000L, null, "etag123");
      StorageProvider.FileMetadata cachedMeta =
          new StorageProvider.FileMetadata("/path", 100L, 1000L, null, "etag456");
      provider.setMetadataResult(currentMeta);
      assertTrue(provider.hasChanged("/path", cachedMeta));
    }

    @Test
    @DisplayName("hasChanged returns false when lastModified within 1 second (no etag)")
    void testHasChangedLastModifiedClose() throws IOException {
      StubStorageProvider provider = new StubStorageProvider("test");
      StorageProvider.FileMetadata currentMeta =
          new StorageProvider.FileMetadata("/path", 100L, 1000500L, null, null);
      StorageProvider.FileMetadata cachedMeta =
          new StorageProvider.FileMetadata("/path", 100L, 1000000L, null, null);
      provider.setMetadataResult(currentMeta);
      assertFalse(provider.hasChanged("/path", cachedMeta));
    }

    @Test
    @DisplayName("hasChanged returns true when lastModified differs by more than 1 second")
    void testHasChangedLastModifiedFar() throws IOException {
      StubStorageProvider provider = new StubStorageProvider("test");
      StorageProvider.FileMetadata currentMeta =
          new StorageProvider.FileMetadata("/path", 100L, 5000L, null, null);
      StorageProvider.FileMetadata cachedMeta =
          new StorageProvider.FileMetadata("/path", 100L, 1000L, null, null);
      provider.setMetadataResult(currentMeta);
      assertTrue(provider.hasChanged("/path", cachedMeta));
    }

    @Test
    @DisplayName("hasChanged returns true when getMetadata throws IOException")
    void testHasChangedMetadataThrows() throws IOException {
      StubStorageProvider provider = new StubStorageProvider("test");
      // metadataResult is null, so getMetadata throws IOException
      StorageProvider.FileMetadata cachedMeta =
          new StorageProvider.FileMetadata("/path", 100L, 1000L, null, null);
      assertTrue(provider.hasChanged("/path", cachedMeta));
    }
  }

  // ---------------------------------------------------------------
  // Static normalizePath tests
  // ---------------------------------------------------------------

  @Nested
  @DisplayName("normalizePath")
  class NormalizePathTests {

    @Test
    @DisplayName("null input returns null")
    void testNull() {
      assertNull(StorageProvider.normalizePath(null));
    }

    @Test
    @DisplayName("s3a:/ (single slash) is fixed to s3a://")
    void testS3aSingleSlash() {
      assertEquals("s3a://bucket/key", StorageProvider.normalizePath("s3a:/bucket/key"));
    }

    @Test
    @DisplayName("s3a:// (double slash) is left unchanged")
    void testS3aDoubleSlash() {
      assertEquals("s3a://bucket/key", StorageProvider.normalizePath("s3a://bucket/key"));
    }

    @Test
    @DisplayName("s3:/ (single slash) is fixed to s3://")
    void testS3SingleSlash() {
      assertEquals("s3://bucket/key", StorageProvider.normalizePath("s3:/bucket/key"));
    }

    @Test
    @DisplayName("s3:// (double slash) is left unchanged")
    void testS3DoubleSlash() {
      assertEquals("s3://bucket/key", StorageProvider.normalizePath("s3://bucket/key"));
    }

    @Test
    @DisplayName("hdfs:/ (single slash) is fixed to hdfs://")
    void testHdfsSingleSlash() {
      assertEquals("hdfs://namenode/path", StorageProvider.normalizePath("hdfs:/namenode/path"));
    }

    @Test
    @DisplayName("hdfs:// (double slash) is left unchanged")
    void testHdfsDoubleSlash() {
      assertEquals("hdfs://namenode/path", StorageProvider.normalizePath("hdfs://namenode/path"));
    }

    @Test
    @DisplayName("regular path is left unchanged")
    void testRegularPath() {
      assertEquals("/local/path/file.csv", StorageProvider.normalizePath("/local/path/file.csv"));
    }

    @Test
    @DisplayName("empty string is left unchanged")
    void testEmptyString() {
      assertEquals("", StorageProvider.normalizePath(""));
    }
  }

  // ---------------------------------------------------------------
  // deleteBatch default method
  // ---------------------------------------------------------------

  @Nested
  @DisplayName("deleteBatch default")
  class DeleteBatchTests {

    @Test
    @DisplayName("deleteBatch calls delete for each path")
    void testDeleteBatchDelegates() throws IOException {
      List<String> deletedPaths = new ArrayList<>();
      StubStorageProvider provider = new StubStorageProvider("test") {
        @Override public boolean delete(String path) {
          deletedPaths.add(path);
          return true;
        }
      };

      List<String> paths = Arrays.asList("/a.csv", "/b.csv", "/c.csv");
      int count = provider.deleteBatch(paths);
      assertEquals(3, count);
      assertEquals(paths, deletedPaths);
    }

    @Test
    @DisplayName("deleteBatch counts only successful deletes")
    void testDeleteBatchPartialSuccess() throws IOException {
      StubStorageProvider provider = new StubStorageProvider("test") {
        @Override public boolean delete(String path) {
          // Only /a.csv gets deleted successfully
          return path.equals("/a.csv");
        }
      };

      List<String> paths = Arrays.asList("/a.csv", "/b.csv");
      int count = provider.deleteBatch(paths);
      assertEquals(1, count);
    }
  }
}
