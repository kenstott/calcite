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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for the {@link StorageProvider} interface default methods,
 * {@link StorageProvider.FileEntry}, {@link StorageProvider.FileMetadata},
 * the {@code normalizePath} static method, and the {@link StorageProviderFactory}
 * factory methods. Covers the base class/interface logic without requiring
 * infrastructure dependencies.
 */
@Tag("unit")
public class StorageProviderBaseCoverageTest {

  /**
   * Minimal implementation of StorageProvider for testing default methods.
   */
  private static class TestableStorageProvider implements StorageProvider {
    private final Map<String, FileMetadata> metadataMap = new HashMap<>();
    private boolean throwOnGetMetadata = false;

    @Override public List<FileEntry> listFiles(String path, boolean recursive) {
      return new ArrayList<>();
    }

    @Override public FileMetadata getMetadata(String path) throws IOException {
      if (throwOnGetMetadata) {
        throw new IOException("Simulated metadata error");
      }
      FileMetadata metadata = metadataMap.get(path);
      if (metadata == null) {
        throw new IOException("File not found: " + path);
      }
      return metadata;
    }

    @Override public InputStream openInputStream(String path) {
      return new ByteArrayInputStream("test data".getBytes(StandardCharsets.UTF_8));
    }

    @Override public Reader openReader(String path) {
      return new java.io.InputStreamReader(openInputStream(path), StandardCharsets.UTF_8);
    }

    @Override public boolean exists(String path) {
      return metadataMap.containsKey(path);
    }

    @Override public boolean isDirectory(String path) {
      return false;
    }

    @Override public String getStorageType() {
      return "testable";
    }

    @Override public String resolvePath(String basePath, String relativePath) {
      if (basePath.endsWith("/")) {
        return basePath + relativePath;
      }
      return basePath + "/" + relativePath;
    }

    void addMetadata(String path, FileMetadata metadata) {
      metadataMap.put(path, metadata);
    }

    void setThrowOnGetMetadata(boolean throwOnGetMetadata) {
      this.throwOnGetMetadata = throwOnGetMetadata;
    }
  }

  // --- FileEntry tests ---

  @Test
  void testFileEntryConstructorAndGetters() {
    StorageProvider.FileEntry entry =
        new StorageProvider.FileEntry("path/to/file.csv", "file.csv",
            false, 12345L, 1700000000000L);

    assertEquals("path/to/file.csv", entry.getPath());
    assertEquals("file.csv", entry.getName());
    assertFalse(entry.isDirectory());
    assertEquals(12345L, entry.getSize());
    assertEquals(1700000000000L, entry.getLastModified());
  }

  @Test
  void testFileEntryAsDirectory() {
    StorageProvider.FileEntry entry =
        new StorageProvider.FileEntry("path/to/dir", "dir",
            true, 0L, 1700000000000L);

    assertEquals("path/to/dir", entry.getPath());
    assertEquals("dir", entry.getName());
    assertTrue(entry.isDirectory());
    assertEquals(0L, entry.getSize());
  }

  @Test
  void testFileEntryWithZeroSize() {
    StorageProvider.FileEntry entry =
        new StorageProvider.FileEntry("empty.txt", "empty.txt",
            false, 0L, 0L);

    assertEquals("empty.txt", entry.getPath());
    assertEquals(0L, entry.getSize());
    assertEquals(0L, entry.getLastModified());
  }

  // --- FileMetadata tests ---

  @Test
  void testFileMetadataConstructorAndGetters() {
    StorageProvider.FileMetadata metadata =
        new StorageProvider.FileMetadata("/test/file.csv", 54321L,
            1700000000000L, "text/csv", "etag-abc123");

    assertEquals("/test/file.csv", metadata.getPath());
    assertEquals(54321L, metadata.getSize());
    assertEquals(1700000000000L, metadata.getLastModified());
    assertEquals("text/csv", metadata.getContentType());
    assertEquals("etag-abc123", metadata.getEtag());
  }

  @Test
  void testFileMetadataWithNullEtag() {
    StorageProvider.FileMetadata metadata =
        new StorageProvider.FileMetadata("/test/file.csv", 100L,
            1700000000000L, "application/octet-stream", null);

    assertNull(metadata.getEtag());
    assertEquals("application/octet-stream", metadata.getContentType());
  }

  @Test
  void testFileMetadataWithZeroValues() {
    StorageProvider.FileMetadata metadata =
        new StorageProvider.FileMetadata("file.txt", 0L, 0L,
            "text/plain", null);

    assertEquals(0L, metadata.getSize());
    assertEquals(0L, metadata.getLastModified());
  }

  // --- normalizePath static method ---

  @Test
  void testNormalizePathNull() {
    assertNull(StorageProvider.normalizePath(null));
  }

  @Test
  void testNormalizePathRegularPath() {
    assertEquals("/local/path/file.txt",
        StorageProvider.normalizePath("/local/path/file.txt"));
  }

  @Test
  void testNormalizePathS3aWithSingleSlash() {
    assertEquals("s3a://bucket/key/file.parquet",
        StorageProvider.normalizePath("s3a:/bucket/key/file.parquet"));
  }

  @Test
  void testNormalizePathS3aAlreadyCorrect() {
    assertEquals("s3a://bucket/key/file.parquet",
        StorageProvider.normalizePath("s3a://bucket/key/file.parquet"));
  }

  @Test
  void testNormalizePathS3WithSingleSlash() {
    assertEquals("s3://bucket/key/file.parquet",
        StorageProvider.normalizePath("s3:/bucket/key/file.parquet"));
  }

  @Test
  void testNormalizePathS3AlreadyCorrect() {
    assertEquals("s3://bucket/key/file.parquet",
        StorageProvider.normalizePath("s3://bucket/key/file.parquet"));
  }

  @Test
  void testNormalizePathHdfsWithSingleSlash() {
    assertEquals("hdfs://namenode/data/file.parquet",
        StorageProvider.normalizePath("hdfs:/namenode/data/file.parquet"));
  }

  @Test
  void testNormalizePathHdfsAlreadyCorrect() {
    assertEquals("hdfs://namenode/data/file.parquet",
        StorageProvider.normalizePath("hdfs://namenode/data/file.parquet"));
  }

  @Test
  void testNormalizePathEmptyString() {
    assertEquals("", StorageProvider.normalizePath(""));
  }

  @Test
  void testNormalizePathFtpNotAffected() {
    assertEquals("ftp://server/path",
        StorageProvider.normalizePath("ftp://server/path"));
  }

  @Test
  void testNormalizePathHttpNotAffected() {
    assertEquals("http://example.com/file.csv",
        StorageProvider.normalizePath("http://example.com/file.csv"));
  }

  // --- Default method: readRange ---

  @Test
  void testReadRangeThrowsUnsupported() {
    TestableStorageProvider provider = new TestableStorageProvider();
    UnsupportedOperationException ex = assertThrows(
        UnsupportedOperationException.class,
        () -> provider.readRange("/file.csv", 0, 100));
    assertTrue(ex.getMessage().contains("testable"));
  }

  // --- Default method: writeFile (byte[]) ---

  @Test
  void testWriteFileBytesThrowsUnsupported() {
    TestableStorageProvider provider = new TestableStorageProvider();
    UnsupportedOperationException ex = assertThrows(
        UnsupportedOperationException.class,
        () -> provider.writeFile("/file.csv", new byte[0]));
    assertTrue(ex.getMessage().contains("testable"));
  }

  // --- Default method: writeFile (InputStream) ---

  @Test
  void testWriteFileStreamThrowsUnsupported() {
    TestableStorageProvider provider = new TestableStorageProvider();
    UnsupportedOperationException ex = assertThrows(
        UnsupportedOperationException.class,
        () -> provider.writeFile("/file.csv",
            new ByteArrayInputStream(new byte[0])));
    assertTrue(ex.getMessage().contains("testable"));
  }

  // --- Default method: createDirectories ---

  @Test
  void testCreateDirectoriesThrowsUnsupported() {
    TestableStorageProvider provider = new TestableStorageProvider();
    UnsupportedOperationException ex = assertThrows(
        UnsupportedOperationException.class,
        () -> provider.createDirectories("/dir/path"));
    assertTrue(ex.getMessage().contains("testable"));
  }

  // --- Default method: delete ---

  @Test
  void testDeleteThrowsUnsupported() {
    TestableStorageProvider provider = new TestableStorageProvider();
    UnsupportedOperationException ex = assertThrows(
        UnsupportedOperationException.class,
        () -> provider.delete("/file.csv"));
    assertTrue(ex.getMessage().contains("testable"));
  }

  // --- Default method: deleteBatch ---

  @Test
  void testDeleteBatchDelegatesToDelete() {
    // deleteBatch calls delete() for each path; since delete throws, this should too
    TestableStorageProvider provider = new TestableStorageProvider();
    assertThrows(UnsupportedOperationException.class,
        () -> provider.deleteBatch(Arrays.asList("/a.csv", "/b.csv")));
  }

  // --- Default method: ensureLifecycleRule ---

  @Test
  void testEnsureLifecycleRuleNoOp() throws IOException {
    TestableStorageProvider provider = new TestableStorageProvider();
    // Should not throw - it's a no-op for non-S3 providers
    provider.ensureLifecycleRule("prefix/", 30);
  }

  // --- Default method: copyFile ---

  @Test
  void testCopyFileThrowsUnsupported() {
    TestableStorageProvider provider = new TestableStorageProvider();
    UnsupportedOperationException ex = assertThrows(
        UnsupportedOperationException.class,
        () -> provider.copyFile("/source.csv", "/dest.csv"));
    assertTrue(ex.getMessage().contains("testable"));
  }

  // --- Default method: hasChanged ---

  @Test
  void testHasChangedWithNullMetadata() throws IOException {
    TestableStorageProvider provider = new TestableStorageProvider();
    assertTrue(provider.hasChanged("/any/path.csv", null));
  }

  @Test
  void testHasChangedSameMetadata() throws IOException {
    TestableStorageProvider provider = new TestableStorageProvider();
    long now = System.currentTimeMillis();
    StorageProvider.FileMetadata metadata =
        new StorageProvider.FileMetadata("/test/file.csv", 100L, now,
            "text/csv", null);
    provider.addMetadata("/test/file.csv", metadata);

    assertFalse(provider.hasChanged("/test/file.csv", metadata));
  }

  @Test
  void testHasChangedDifferentSize() throws IOException {
    TestableStorageProvider provider = new TestableStorageProvider();
    long now = System.currentTimeMillis();
    StorageProvider.FileMetadata current =
        new StorageProvider.FileMetadata("/test/file.csv", 200L, now,
            "text/csv", null);
    provider.addMetadata("/test/file.csv", current);

    StorageProvider.FileMetadata cached =
        new StorageProvider.FileMetadata("/test/file.csv", 100L, now,
            "text/csv", null);

    assertTrue(provider.hasChanged("/test/file.csv", cached));
  }

  @Test
  void testHasChangedDifferentEtag() throws IOException {
    TestableStorageProvider provider = new TestableStorageProvider();
    long now = System.currentTimeMillis();
    StorageProvider.FileMetadata current =
        new StorageProvider.FileMetadata("/test/file.csv", 100L, now,
            "text/csv", "etag-new");
    provider.addMetadata("/test/file.csv", current);

    StorageProvider.FileMetadata cached =
        new StorageProvider.FileMetadata("/test/file.csv", 100L, now,
            "text/csv", "etag-old");

    assertTrue(provider.hasChanged("/test/file.csv", cached));
  }

  @Test
  void testHasChangedSameEtag() throws IOException {
    TestableStorageProvider provider = new TestableStorageProvider();
    long now = System.currentTimeMillis();
    StorageProvider.FileMetadata current =
        new StorageProvider.FileMetadata("/test/file.csv", 100L, now,
            "text/csv", "etag-same");
    provider.addMetadata("/test/file.csv", current);

    StorageProvider.FileMetadata cached =
        new StorageProvider.FileMetadata("/test/file.csv", 100L, now,
            "text/csv", "etag-same");

    assertFalse(provider.hasChanged("/test/file.csv", cached));
  }

  @Test
  void testHasChangedDifferentLastModifiedBeyondThreshold() throws IOException {
    TestableStorageProvider provider = new TestableStorageProvider();
    long now = System.currentTimeMillis();
    StorageProvider.FileMetadata current =
        new StorageProvider.FileMetadata("/test/file.csv", 100L, now,
            "text/csv", null);
    provider.addMetadata("/test/file.csv", current);

    StorageProvider.FileMetadata cached =
        new StorageProvider.FileMetadata("/test/file.csv", 100L,
            now - 5000, "text/csv", null);

    assertTrue(provider.hasChanged("/test/file.csv", cached));
  }

  @Test
  void testHasChangedSameLastModifiedWithinThreshold() throws IOException {
    TestableStorageProvider provider = new TestableStorageProvider();
    long now = System.currentTimeMillis();
    StorageProvider.FileMetadata current =
        new StorageProvider.FileMetadata("/test/file.csv", 100L, now,
            "text/csv", null);
    provider.addMetadata("/test/file.csv", current);

    // Within 1000ms threshold
    StorageProvider.FileMetadata cached =
        new StorageProvider.FileMetadata("/test/file.csv", 100L,
            now - 500, "text/csv", null);

    assertFalse(provider.hasChanged("/test/file.csv", cached));
  }

  @Test
  void testHasChangedReturnsTrue_WhenGetMetadataFails() throws IOException {
    TestableStorageProvider provider = new TestableStorageProvider();
    provider.setThrowOnGetMetadata(true);

    StorageProvider.FileMetadata cached =
        new StorageProvider.FileMetadata("/test/file.csv", 100L,
            System.currentTimeMillis(), "text/csv", null);

    // When getMetadata fails, hasChanged should return true (assume changed)
    assertTrue(provider.hasChanged("/test/file.csv", cached));
  }

  @Test
  void testHasChangedWithOneEtagNull() throws IOException {
    TestableStorageProvider provider = new TestableStorageProvider();
    long now = System.currentTimeMillis();

    // Current has etag, cached does not
    StorageProvider.FileMetadata current =
        new StorageProvider.FileMetadata("/test/file.csv", 100L, now,
            "text/csv", "etag-abc");
    provider.addMetadata("/test/file.csv", current);

    StorageProvider.FileMetadata cached =
        new StorageProvider.FileMetadata("/test/file.csv", 100L, now,
            "text/csv", null);

    // When one etag is null, it falls through to timestamp comparison
    assertFalse(provider.hasChanged("/test/file.csv", cached));
  }

  // --- Default method: getS3Config ---

  @Test
  void testGetS3ConfigReturnsNull() {
    TestableStorageProvider provider = new TestableStorageProvider();
    assertNull(provider.getS3Config());
  }

  // --- Default method: cleanupMacosMetadata ---

  @Test
  void testCleanupMacosMetadataNoOp() throws IOException {
    TestableStorageProvider provider = new TestableStorageProvider();
    // Should not throw - it's a no-op by default
    provider.cleanupMacosMetadata("/some/dir");
  }

  // --- StorageProviderFactory tests ---

  @Test
  void testCreateFromUrlNull() {
    StorageProvider p = StorageProviderFactory.createFromUrl(null);
    assertEquals("local", p.getStorageType());
  }

  @Test
  void testCreateFromUrlEmpty() {
    StorageProvider p = StorageProviderFactory.createFromUrl("");
    assertEquals("local", p.getStorageType());
  }

  @Test
  void testCreateFromUrlNoScheme() {
    StorageProvider p = StorageProviderFactory.createFromUrl("/local/path/file.csv");
    assertEquals("local", p.getStorageType());
  }

  @Test
  void testCreateFromUrlFile() {
    StorageProviderFactory.clearCache();
    StorageProvider p = StorageProviderFactory.createFromUrl("file:///path/file.csv");
    assertEquals("local", p.getStorageType());
  }

  @Test
  void testCreateFromUrlHttp() {
    StorageProviderFactory.clearCache();
    StorageProvider p = StorageProviderFactory.createFromUrl("http://example.com/file.csv");
    assertEquals("http", p.getStorageType());
  }

  @Test
  void testCreateFromUrlHttps() {
    StorageProviderFactory.clearCache();
    StorageProvider p = StorageProviderFactory.createFromUrl("https://example.com/file.csv");
    assertEquals("http", p.getStorageType());
  }

  @Test
  void testCreateFromUrlFtp() {
    StorageProviderFactory.clearCache();
    StorageProvider p = StorageProviderFactory.createFromUrl("ftp://server/path");
    assertEquals("ftp", p.getStorageType());
  }

  @Test
  void testCreateFromUrlFtps() {
    StorageProviderFactory.clearCache();
    StorageProvider p = StorageProviderFactory.createFromUrl("ftps://server/path");
    assertEquals("ftp", p.getStorageType());
  }

  @Test
  void testCreateFromUrlSftp() {
    StorageProviderFactory.clearCache();
    StorageProvider p = StorageProviderFactory.createFromUrl("sftp://user@server/path");
    assertEquals("sftp", p.getStorageType());
  }

  @Test
  void testCreateFromUrlS3RequiresCredentials() {
    assertThrows(IllegalArgumentException.class,
        () -> StorageProviderFactory.createFromUrl("s3://bucket/key"));
  }

  @Test
  void testCreateFromUrlUnsupportedScheme() {
    assertThrows(IllegalArgumentException.class,
        () -> StorageProviderFactory.createFromUrl("gopher://host/path"));
  }

  @Test
  void testCreateFromTypeLocal() {
    StorageProviderFactory.clearCache();
    StorageProvider p = StorageProviderFactory.createFromType("local", null);
    assertEquals("local", p.getStorageType());
  }

  @Test
  void testCreateFromTypeFile() {
    StorageProviderFactory.clearCache();
    StorageProvider p = StorageProviderFactory.createFromType("file", null);
    assertEquals("local", p.getStorageType());
  }

  @Test
  void testCreateFromTypeNullDefaultsToLocal() {
    StorageProviderFactory.clearCache();
    StorageProvider p = StorageProviderFactory.createFromType(null, null);
    assertEquals("local", p.getStorageType());
  }

  @Test
  void testCreateFromTypeEmptyDefaultsToLocal() {
    StorageProviderFactory.clearCache();
    StorageProvider p = StorageProviderFactory.createFromType("", null);
    assertEquals("local", p.getStorageType());
  }

  @Test
  void testCreateFromTypeHttp() {
    StorageProviderFactory.clearCache();
    StorageProvider p = StorageProviderFactory.createFromType("http", null);
    assertEquals("http", p.getStorageType());
  }

  @Test
  void testCreateFromTypeHttps() {
    StorageProviderFactory.clearCache();
    StorageProvider p = StorageProviderFactory.createFromType("https", null);
    assertEquals("http", p.getStorageType());
  }

  @Test
  void testCreateFromTypeFtp() {
    StorageProviderFactory.clearCache();
    StorageProvider p = StorageProviderFactory.createFromType("ftp", null);
    assertEquals("ftp", p.getStorageType());
  }

  @Test
  void testCreateFromTypeFtps() {
    StorageProviderFactory.clearCache();
    StorageProvider p = StorageProviderFactory.createFromType("ftps", null);
    assertEquals("ftp", p.getStorageType());
  }

  @Test
  void testCreateFromTypeSftp() {
    StorageProviderFactory.clearCache();
    StorageProvider p = StorageProviderFactory.createFromType("sftp", null);
    assertEquals("sftp", p.getStorageType());
  }

  @Test
  void testCreateFromTypeSftpWithConfig() {
    StorageProviderFactory.clearCache();
    Map<String, Object> config = new HashMap<>();
    config.put("username", "testuser");
    config.put("password", "testpass");
    config.put("privateKeyPath", "/path/to/key");
    config.put("strictHostKeyChecking", true);
    StorageProvider p = StorageProviderFactory.createFromType("sftp", config);
    assertEquals("sftp", p.getStorageType());
  }

  @Test
  void testCreateFromTypeSharepointWithStaticToken() {
    StorageProviderFactory.clearCache();
    Map<String, Object> config = new HashMap<>();
    config.put("siteUrl", "https://example.sharepoint.com");
    config.put("accessToken", "test-token");
    StorageProvider p = StorageProviderFactory.createFromType("sharepoint", config);
    assertEquals("sharepoint-rest", p.getStorageType());
  }

  @Test
  void testCreateFromTypeSharepointWithClientCredentials() {
    StorageProviderFactory.clearCache();
    Map<String, Object> config = new HashMap<>();
    config.put("siteUrl", "https://example.sharepoint.com");
    config.put("tenantId", "tenant-123");
    config.put("clientId", "client-456");
    config.put("clientSecret", "secret-789");
    StorageProvider p = StorageProviderFactory.createFromType("sharepoint", config);
    assertEquals("sharepoint-rest", p.getStorageType());
  }

  @Test
  void testCreateFromTypeSharepointWithGraphApi() {
    StorageProviderFactory.clearCache();
    Map<String, Object> config = new HashMap<>();
    config.put("siteUrl", "https://example.sharepoint.com");
    config.put("tenantId", "tenant-123");
    config.put("clientId", "client-456");
    config.put("clientSecret", "secret-789");
    config.put("useGraphApi", true);
    StorageProvider p = StorageProviderFactory.createFromType("sharepoint", config);
    assertEquals("microsoft-graph", p.getStorageType());
  }

  @Test
  void testCreateFromTypeSharepointMissingSiteUrl() {
    assertThrows(IllegalArgumentException.class,
        () -> StorageProviderFactory.createFromType("sharepoint", null));
  }

  @Test
  void testCreateFromTypeSharepointNoAuth() {
    Map<String, Object> config = new HashMap<>();
    config.put("siteUrl", "https://example.sharepoint.com");
    assertThrows(IllegalArgumentException.class,
        () -> StorageProviderFactory.createFromType("sharepoint", config));
  }

  @Test
  void testCreateFromTypeS3RequiresCredentials() {
    assertThrows(IllegalArgumentException.class,
        () -> StorageProviderFactory.createFromType("s3", null));
  }

  @Test
  void testCreateFromTypeS3EmptyConfig() {
    assertThrows(IllegalArgumentException.class,
        () -> StorageProviderFactory.createFromType("s3", new HashMap<>()));
  }

  @Test
  void testCreateFromTypeUnsupported() {
    assertThrows(IllegalArgumentException.class,
        () -> StorageProviderFactory.createFromType("gopher", null));
  }

  @Test
  void testClearCache() {
    // Just ensure clearCache does not throw
    StorageProviderFactory.clearCache();
    // Create a provider to populate cache
    StorageProviderFactory.createFromType("local", null);
    // Clear again
    StorageProviderFactory.clearCache();
  }

  // --- SharePoint factory with refresh token ---

  @Test
  void testCreateFromTypeSharepointWithRefreshToken() {
    StorageProviderFactory.clearCache();
    Map<String, Object> config = new HashMap<>();
    config.put("siteUrl", "https://example.sharepoint.com");
    config.put("tenantId", "tenant-123");
    config.put("clientId", "client-456");
    config.put("refreshToken", "refresh-token-789");
    StorageProvider p = StorageProviderFactory.createFromType("sharepoint", config);
    assertEquals("sharepoint-rest", p.getStorageType());
  }

  @Test
  void testCreateFromTypeSharepointRefreshTokenMissingTenantId() {
    Map<String, Object> config = new HashMap<>();
    config.put("siteUrl", "https://example.sharepoint.com");
    config.put("refreshToken", "refresh-token-789");
    assertThrows(IllegalArgumentException.class,
        () -> StorageProviderFactory.createFromType("sharepoint", config));
  }

  @Test
  void testCreateFromTypeSharepointClientCredentialsMissingTenantId() {
    Map<String, Object> config = new HashMap<>();
    config.put("siteUrl", "https://example.sharepoint.com");
    config.put("clientId", "client-456");
    config.put("clientSecret", "secret-789");
    assertThrows(IllegalArgumentException.class,
        () -> StorageProviderFactory.createFromType("sharepoint", config));
  }

  // --- SharePoint factory with graph API and different token types ---

  @Test
  void testCreateFromTypeSharepointGraphWithRefreshToken() {
    StorageProviderFactory.clearCache();
    Map<String, Object> config = new HashMap<>();
    config.put("siteUrl", "https://example.sharepoint.com");
    config.put("tenantId", "tenant-123");
    config.put("clientId", "client-456");
    config.put("refreshToken", "refresh-token-789");
    config.put("useGraphApi", true);
    StorageProvider p = StorageProviderFactory.createFromType("sharepoint", config);
    assertEquals("microsoft-graph", p.getStorageType());
  }

  @Test
  void testCreateFromTypeSharepointGraphWithStaticToken() {
    StorageProviderFactory.clearCache();
    Map<String, Object> config = new HashMap<>();
    config.put("siteUrl", "https://example.sharepoint.com");
    config.put("accessToken", "static-token");
    config.put("useGraphApi", true);
    StorageProvider p = StorageProviderFactory.createFromType("sharepoint", config);
    assertEquals("microsoft-graph", p.getStorageType());
  }

  // --- HTTP config integration ---

  @Test
  void testCreateFromTypeHttpWithConfig() {
    StorageProviderFactory.clearCache();
    Map<String, Object> config = new HashMap<>();
    config.put("method", "POST");
    config.put("body", "{\"query\": \"SELECT 1\"}");
    StorageProvider p = StorageProviderFactory.createFromType("http", config);
    assertEquals("http", p.getStorageType());
  }

  @Test
  void testCreateFromTypeHttpWithHeaders() {
    StorageProviderFactory.clearCache();
    Map<String, Object> config = new HashMap<>();
    Map<String, String> headers = new HashMap<>();
    headers.put("Authorization", "Bearer token123");
    config.put("headers", headers);
    StorageProvider p = StorageProviderFactory.createFromType("http", config);
    assertEquals("http", p.getStorageType());
  }
}
