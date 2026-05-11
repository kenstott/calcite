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
package org.apache.calcite.adapter.file.storage;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Deep coverage tests for StorageProvider interface default methods.
 */
@Tag("unit")
public class StorageProviderDeepCoverageTest {

  /**
   * Minimal StorageProvider implementation for testing default methods.
   */
  private static class TestStorageProvider implements StorageProvider {
    private FileMetadata metadataToReturn;
    private boolean throwOnGetMetadata = false;

    @Override public List<FileEntry> listFiles(String path, boolean recursive) {
      return new ArrayList<>();
    }

    @Override public FileMetadata getMetadata(String path) throws IOException {
      if (throwOnGetMetadata) {
        throw new IOException("Test metadata error");
      }
      return metadataToReturn;
    }

    @Override public InputStream openInputStream(String path) throws IOException {
      return new java.io.ByteArrayInputStream(new byte[0]);
    }

    @Override public Reader openReader(String path) throws IOException {
      return new java.io.InputStreamReader(openInputStream(path));
    }

    @Override public boolean exists(String path) {
      return true;
    }

    @Override public boolean isDirectory(String path) {
      return false;
    }

    @Override public String getStorageType() {
      return "test";
    }

    @Override public String resolvePath(String basePath, String relativePath) {
      return basePath + "/" + relativePath;
    }

    void setMetadataToReturn(FileMetadata metadata) {
      this.metadataToReturn = metadata;
    }

    void setThrowOnGetMetadata(boolean shouldThrow) {
      this.throwOnGetMetadata = shouldThrow;
    }
  }

  // --- readRange default ---

  @Test
  void testReadRangeDefaultThrows() {
    TestStorageProvider provider = new TestStorageProvider();
    UnsupportedOperationException ex = assertThrows(
        UnsupportedOperationException.class,
        () -> provider.readRange("/file.txt", 0, 100));
    assertTrue(ex.getMessage().contains("test"));
    assertTrue(ex.getMessage().contains("Range reads"));
  }

  // --- writeFile (byte[]) default ---

  @Test
  void testWriteFileByteArrayDefaultThrows() {
    TestStorageProvider provider = new TestStorageProvider();
    UnsupportedOperationException ex = assertThrows(
        UnsupportedOperationException.class,
        () -> provider.writeFile("/file.txt", new byte[0]));
    assertTrue(ex.getMessage().contains("test"));
  }

  // --- writeFile (InputStream) default ---

  @Test
  void testWriteFileInputStreamDefaultThrows() {
    TestStorageProvider provider = new TestStorageProvider();
    UnsupportedOperationException ex = assertThrows(
        UnsupportedOperationException.class,
        () -> provider.writeFile("/file.txt",
            new java.io.ByteArrayInputStream(new byte[0])));
    assertTrue(ex.getMessage().contains("test"));
  }

  // --- createDirectories default ---

  @Test
  void testCreateDirectoriesDefaultThrows() {
    TestStorageProvider provider = new TestStorageProvider();
    UnsupportedOperationException ex = assertThrows(
        UnsupportedOperationException.class,
        () -> provider.createDirectories("/dir"));
    assertTrue(ex.getMessage().contains("test"));
    assertTrue(ex.getMessage().contains("Directory creation"));
  }

  // --- delete default ---

  @Test
  void testDeleteDefaultThrows() {
    TestStorageProvider provider = new TestStorageProvider();
    UnsupportedOperationException ex = assertThrows(
        UnsupportedOperationException.class,
        () -> provider.delete("/file.txt"));
    assertTrue(ex.getMessage().contains("test"));
    assertTrue(ex.getMessage().contains("Delete"));
  }

  // --- copyFile default ---

  @Test
  void testCopyFileDefaultThrows() {
    TestStorageProvider provider = new TestStorageProvider();
    UnsupportedOperationException ex = assertThrows(
        UnsupportedOperationException.class,
        () -> provider.copyFile("/source.txt", "/dest.txt"));
    assertTrue(ex.getMessage().contains("test"));
    assertTrue(ex.getMessage().contains("Copy"));
  }

  // --- deleteBatch default ---

  @Test
  void testDeleteBatchDefaultDelegatesToDelete() throws IOException {
    final List<String> deletedPaths = new ArrayList<>();
    StorageProvider provider = new TestStorageProvider() {
      @Override public boolean delete(String path) {
        deletedPaths.add(path);
        return true;
      }
    };

    List<String> paths = new ArrayList<>();
    paths.add("/file1.txt");
    paths.add("/file2.txt");
    paths.add("/file3.txt");

    int deleted = provider.deleteBatch(paths);
    assertEquals(3, deleted);
    assertEquals(3, deletedPaths.size());
  }

  @Test
  void testDeleteBatchPartialFailure() throws IOException {
    StorageProvider provider = new TestStorageProvider() {
      @Override public boolean delete(String path) {
        return path.contains("1"); // Only deletes paths containing "1"
      }
    };

    List<String> paths = new ArrayList<>();
    paths.add("/file1.txt");
    paths.add("/file2.txt");
    paths.add("/file3.txt");

    int deleted = provider.deleteBatch(paths);
    assertEquals(1, deleted);
  }

  // --- ensureLifecycleRule default ---

  @Test
  void testEnsureLifecycleRuleDefaultIsNoOp() throws IOException {
    TestStorageProvider provider = new TestStorageProvider();
    // Should not throw
    provider.ensureLifecycleRule("prefix/", 7);
  }

  // --- cleanupMacosMetadata default ---

  @Test
  void testCleanupMacosMetadataDefaultIsNoOp() throws IOException {
    TestStorageProvider provider = new TestStorageProvider();
    // Should not throw
    provider.cleanupMacosMetadata("/some/dir");
  }

  // --- getS3Config default ---

  @Test
  void testGetS3ConfigDefaultReturnsNull() {
    TestStorageProvider provider = new TestStorageProvider();
    assertNull(provider.getS3Config());
  }

  // --- normalizePath static method ---

  @Test
  void testNormalizePathNull() {
    assertNull(StorageProvider.normalizePath(null));
  }

  @Test
  void testNormalizePathS3aSingleSlash() {
    assertEquals("s3a://bucket/path", StorageProvider.normalizePath("s3a:/bucket/path"));
  }

  @Test
  void testNormalizePathS3aDoubleSlash() {
    assertEquals("s3a://bucket/path", StorageProvider.normalizePath("s3a://bucket/path"));
  }

  @Test
  void testNormalizePathS3SingleSlash() {
    assertEquals("s3://bucket/path", StorageProvider.normalizePath("s3:/bucket/path"));
  }

  @Test
  void testNormalizePathS3DoubleSlash() {
    assertEquals("s3://bucket/path", StorageProvider.normalizePath("s3://bucket/path"));
  }

  @Test
  void testNormalizePathHdfsSingleSlash() {
    assertEquals("hdfs://namenode/path", StorageProvider.normalizePath("hdfs:/namenode/path"));
  }

  @Test
  void testNormalizePathHdfsDoubleSlash() {
    assertEquals("hdfs://namenode/path", StorageProvider.normalizePath("hdfs://namenode/path"));
  }

  @Test
  void testNormalizePathRegularPath() {
    assertEquals("/local/path/file.txt", StorageProvider.normalizePath("/local/path/file.txt"));
  }

  // --- hasChanged default ---

  @Test
  void testHasChangedWithNullCachedMetadata() throws IOException {
    TestStorageProvider provider = new TestStorageProvider();
    assertTrue(provider.hasChanged("/file.txt", null));
  }

  @Test
  void testHasChangedWhenSizeDiffers() throws IOException {
    TestStorageProvider provider = new TestStorageProvider();
    StorageProvider.FileMetadata cached =
        new StorageProvider.FileMetadata("/file.txt", 100, System.currentTimeMillis(),
            "text/plain", null);
    StorageProvider.FileMetadata current =
        new StorageProvider.FileMetadata("/file.txt", 200, System.currentTimeMillis(),
            "text/plain", null);
    provider.setMetadataToReturn(current);

    assertTrue(provider.hasChanged("/file.txt", cached));
  }

  @Test
  void testHasChangedWhenEtagsDiffer() throws IOException {
    TestStorageProvider provider = new TestStorageProvider();
    long now = System.currentTimeMillis();
    StorageProvider.FileMetadata cached =
        new StorageProvider.FileMetadata("/file.txt", 100, now, "text/plain", "etag1");
    StorageProvider.FileMetadata current =
        new StorageProvider.FileMetadata("/file.txt", 100, now, "text/plain", "etag2");
    provider.setMetadataToReturn(current);

    assertTrue(provider.hasChanged("/file.txt", cached));
  }

  @Test
  void testHasChangedWhenEtagsMatch() throws IOException {
    TestStorageProvider provider = new TestStorageProvider();
    long now = System.currentTimeMillis();
    StorageProvider.FileMetadata cached =
        new StorageProvider.FileMetadata("/file.txt", 100, now, "text/plain", "etag1");
    StorageProvider.FileMetadata current =
        new StorageProvider.FileMetadata("/file.txt", 100, now, "text/plain", "etag1");
    provider.setMetadataToReturn(current);

    assertFalse(provider.hasChanged("/file.txt", cached));
  }

  @Test
  void testHasChangedWhenTimeDiffSmall() throws IOException {
    TestStorageProvider provider = new TestStorageProvider();
    long now = System.currentTimeMillis();
    StorageProvider.FileMetadata cached =
        new StorageProvider.FileMetadata("/file.txt", 100, now, "text/plain", null);
    StorageProvider.FileMetadata current =
        new StorageProvider.FileMetadata("/file.txt", 100, now + 500, "text/plain", null);
    provider.setMetadataToReturn(current);

    // Less than 1 second difference, should be treated as unchanged
    assertFalse(provider.hasChanged("/file.txt", cached));
  }

  @Test
  void testHasChangedWhenTimeDiffLarge() throws IOException {
    TestStorageProvider provider = new TestStorageProvider();
    long now = System.currentTimeMillis();
    StorageProvider.FileMetadata cached =
        new StorageProvider.FileMetadata("/file.txt", 100, now, "text/plain", null);
    StorageProvider.FileMetadata current =
        new StorageProvider.FileMetadata("/file.txt", 100, now + 2000, "text/plain", null);
    provider.setMetadataToReturn(current);

    // More than 1 second difference
    assertTrue(provider.hasChanged("/file.txt", cached));
  }

  @Test
  void testHasChangedWhenGetMetadataFails() throws IOException {
    TestStorageProvider provider = new TestStorageProvider();
    provider.setThrowOnGetMetadata(true);
    StorageProvider.FileMetadata cached =
        new StorageProvider.FileMetadata("/file.txt", 100, System.currentTimeMillis(),
            "text/plain", null);

    // When getMetadata throws, assume changed
    assertTrue(provider.hasChanged("/file.txt", cached));
  }

  @Test
  void testHasChangedWithOneNullEtag() throws IOException {
    TestStorageProvider provider = new TestStorageProvider();
    long now = System.currentTimeMillis();
    // Cached has etag, current does not (or vice versa) - should fallback to time comparison
    StorageProvider.FileMetadata cached =
        new StorageProvider.FileMetadata("/file.txt", 100, now, "text/plain", "etag1");
    StorageProvider.FileMetadata current =
        new StorageProvider.FileMetadata("/file.txt", 100, now, "text/plain", null);
    provider.setMetadataToReturn(current);

    // ETags not both present, falls through to time comparison. Same time = not changed
    assertFalse(provider.hasChanged("/file.txt", cached));
  }

  // --- FileEntry ---

  @Test
  void testFileEntryAccessors() {
    StorageProvider.FileEntry entry =
        new StorageProvider.FileEntry("/path/file.txt", "file.txt", false, 1024, 12345L);
    assertEquals("/path/file.txt", entry.getPath());
    assertEquals("file.txt", entry.getName());
    assertFalse(entry.isDirectory());
    assertEquals(1024, entry.getSize());
    assertEquals(12345L, entry.getLastModified());
  }

  @Test
  void testFileEntryDirectory() {
    StorageProvider.FileEntry entry =
        new StorageProvider.FileEntry("/path/dir", "dir", true, 0, 54321L);
    assertEquals("/path/dir", entry.getPath());
    assertEquals("dir", entry.getName());
    assertTrue(entry.isDirectory());
    assertEquals(0, entry.getSize());
    assertEquals(54321L, entry.getLastModified());
  }

  // --- FileMetadata ---

  @Test
  void testFileMetadataAccessors() {
    StorageProvider.FileMetadata metadata =
        new StorageProvider.FileMetadata("/test/file.csv", 2048, 99999L,
            "text/csv", "abc123");
    assertEquals("/test/file.csv", metadata.getPath());
    assertEquals(2048, metadata.getSize());
    assertEquals(99999L, metadata.getLastModified());
    assertEquals("text/csv", metadata.getContentType());
    assertEquals("abc123", metadata.getEtag());
  }

  @Test
  void testFileMetadataWithNullEtag() {
    StorageProvider.FileMetadata metadata =
        new StorageProvider.FileMetadata("/test/file.csv", 512, 88888L,
            "application/json", null);
    assertNull(metadata.getEtag());
    assertNotNull(metadata.getContentType());
  }
}
