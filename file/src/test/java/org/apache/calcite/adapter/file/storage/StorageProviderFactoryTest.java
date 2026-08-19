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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link StorageProviderFactory}.
 */
@Tag("unit")
// This class clears StorageProviderFactory's process-wide provider cache in tearDown, and the
// module runs test classes concurrently. Without isolation another class's tearDown can wipe the
// cache between two calls inside a test here, which makes any assertion about caching flaky —
// and equally lets this class pull a cached provider out from under a concurrent test.
@org.junit.jupiter.api.parallel.Isolated
public class StorageProviderFactoryTest {

  @AfterEach
  void tearDown() {
    StorageProviderFactory.clearCache();
  }

  // --- createFromUrl tests ---

  @Test void testCreateFromUrlNull() {
    StorageProvider provider = StorageProviderFactory.createFromUrl(null);
    assertNotNull(provider);
    assertEquals("local", provider.getStorageType());
  }

  @Test void testCreateFromUrlEmpty() {
    StorageProvider provider = StorageProviderFactory.createFromUrl("");
    assertNotNull(provider);
    assertEquals("local", provider.getStorageType());
  }

  @Test void testCreateFromUrlLocalPath() {
    StorageProvider provider = StorageProviderFactory.createFromUrl("/path/to/file.csv");
    assertEquals("local", provider.getStorageType());
  }

  @Test void testCreateFromUrlFileScheme() {
    StorageProvider provider = StorageProviderFactory.createFromUrl("file:///path/to/file.csv");
    assertEquals("local", provider.getStorageType());
  }

  @Test void testCreateFromUrlHttp() {
    StorageProvider provider = StorageProviderFactory.createFromUrl("http://example.com/file.csv");
    assertEquals("http", provider.getStorageType());
  }

  @Test void testCreateFromUrlHttps() {
    StorageProvider provider = StorageProviderFactory.createFromUrl("https://example.com/file.csv");
    assertEquals("http", provider.getStorageType());
  }

  @Test void testCreateFromUrlS3RequiresCredentials() {
    assertThrows(IllegalArgumentException.class,
        () -> StorageProviderFactory.createFromUrl("s3://bucket/key.parquet"));
  }

  @Test void testCreateFromUrlFtp() {
    StorageProvider provider = StorageProviderFactory.createFromUrl("ftp://server/file.csv");
    assertEquals("ftp", provider.getStorageType());
  }

  @Test void testCreateFromUrlFtps() {
    StorageProvider provider = StorageProviderFactory.createFromUrl("ftps://server/file.csv");
    assertEquals("ftp", provider.getStorageType());
  }

  @Test void testCreateFromUrlSftp() {
    StorageProvider provider = StorageProviderFactory.createFromUrl("sftp://server/file.csv");
    assertEquals("sftp", provider.getStorageType());
  }

  @Test void testCreateFromUrlUnsupportedScheme() {
    assertThrows(IllegalArgumentException.class,
        () -> StorageProviderFactory.createFromUrl("gopher://server/path"));
  }

  @Test void testCreateFromUrlCaseInsensitive() {
    StorageProvider provider = StorageProviderFactory.createFromUrl("HTTP://example.com/file.csv");
    assertEquals("http", provider.getStorageType());
  }

  // --- createFromType tests ---

  @Test void testCreateFromTypeNull() {
    StorageProvider provider = StorageProviderFactory.createFromType(null, null);
    assertEquals("local", provider.getStorageType());
  }

  @Test void testCreateFromTypeEmpty() {
    StorageProvider provider = StorageProviderFactory.createFromType("", null);
    assertEquals("local", provider.getStorageType());
  }

  @Test void testCreateFromTypeLocal() {
    StorageProvider provider = StorageProviderFactory.createFromType("local", null);
    assertEquals("local", provider.getStorageType());
  }

  @Test void testCreateFromTypeFile() {
    StorageProvider provider = StorageProviderFactory.createFromType("file", null);
    assertEquals("local", provider.getStorageType());
  }

  @Test void testCreateFromTypeHttpNoConfig() {
    StorageProvider provider = StorageProviderFactory.createFromType("http", null);
    assertEquals("http", provider.getStorageType());
  }

  @Test void testCreateFromTypeHttpWithConfig() {
    Map<String, Object> config = new HashMap<String, Object>();
    config.put("method", "POST");
    config.put("body", "test body");

    StorageProvider provider = StorageProviderFactory.createFromType("http", config);
    assertEquals("http", provider.getStorageType());
  }

  @Test void testCreateFromTypeHttpWithHeaders() {
    Map<String, Object> config = new HashMap<String, Object>();
    Map<String, String> headers = new HashMap<String, String>();
    headers.put("Authorization", "Bearer token");
    config.put("headers", headers);

    StorageProvider provider = StorageProviderFactory.createFromType("http", config);
    assertEquals("http", provider.getStorageType());
  }

  @Test void testCreateFromTypeS3RequiresConfig() {
    assertThrows(IllegalArgumentException.class,
        () -> StorageProviderFactory.createFromType("s3", null));
  }

  @Test void testCreateFromTypeS3EmptyConfig() {
    assertThrows(IllegalArgumentException.class,
        () -> StorageProviderFactory.createFromType("s3", new HashMap<String, Object>()));
  }

  @Test void testCreateFromTypeSharePointRequiresSiteUrl() {
    assertThrows(IllegalArgumentException.class,
        () -> StorageProviderFactory.createFromType("sharepoint", null));
  }

  @Test void testCreateFromTypeSharePointWithStaticToken() {
    Map<String, Object> config = new HashMap<String, Object>();
    config.put("siteUrl", "https://example.sharepoint.com");
    config.put("accessToken", "dummy-token");

    StorageProvider provider = StorageProviderFactory.createFromType("sharepoint", config);
    assertNotNull(provider);
    assertEquals("sharepoint-rest", provider.getStorageType());
  }

  @Test void testCreateFromTypeSharePointWithClientCredentials() {
    Map<String, Object> config = new HashMap<String, Object>();
    config.put("siteUrl", "https://example.sharepoint.com");
    config.put("tenantId", "tenant-id");
    config.put("clientId", "client-id");
    config.put("clientSecret", "client-secret");

    StorageProvider provider = StorageProviderFactory.createFromType("sharepoint", config);
    assertNotNull(provider);
  }

  @Test void testCreateFromTypeSharePointMissingAuth() {
    Map<String, Object> config = new HashMap<String, Object>();
    config.put("siteUrl", "https://example.sharepoint.com");

    assertThrows(IllegalArgumentException.class,
        () -> StorageProviderFactory.createFromType("sharepoint", config));
  }

  @Test void testCreateFromTypeSharePointGraphApi() {
    Map<String, Object> config = new HashMap<String, Object>();
    config.put("siteUrl", "https://example.sharepoint.com");
    config.put("tenantId", "tenant-id");
    config.put("clientId", "client-id");
    config.put("clientSecret", "client-secret");
    config.put("useGraphApi", Boolean.TRUE);

    StorageProvider provider = StorageProviderFactory.createFromType("sharepoint", config);
    assertNotNull(provider);
    assertEquals("microsoft-graph", provider.getStorageType());
  }

  @Test void testCreateFromTypeUnsupported() {
    assertThrows(IllegalArgumentException.class,
        () -> StorageProviderFactory.createFromType("gopher", null));
  }

  @Test void testCreateFromTypeCaseInsensitive() {
    StorageProvider provider = StorageProviderFactory.createFromType("LOCAL", null);
    assertEquals("local", provider.getStorageType());
  }

  @Test void testCreateFromTypeFtp() {
    StorageProvider provider = StorageProviderFactory.createFromType("ftp", null);
    assertEquals("ftp", provider.getStorageType());
  }

  @Test void testCreateFromTypeSftp() {
    StorageProvider provider = StorageProviderFactory.createFromType("sftp", null);
    assertEquals("sftp", provider.getStorageType());
  }

  @Test void testCreateFromTypeSftpWithConfig() {
    Map<String, Object> config = new HashMap<String, Object>();
    config.put("username", "user");
    config.put("password", "pass");
    config.put("strictHostKeyChecking", Boolean.FALSE);

    StorageProvider provider = StorageProviderFactory.createFromType("sftp", config);
    assertEquals("sftp", provider.getStorageType());
  }

  // --- Cache tests ---

  @Test void testClearCache() {
    StorageProviderFactory.createFromUrl("http://example.com/file.csv");
    StorageProviderFactory.clearCache();

    // After clearing, new instances should be created
    StorageProvider provider = StorageProviderFactory.createFromUrl("http://example.com/file.csv");
    assertNotNull(provider);
  }

  @Test void testCachedProviderReturnsSameInstance() {
    StorageProviderFactory.clearCache();
    StorageProvider p1 = StorageProviderFactory.createFromUrl("http://a.com/file.csv");
    StorageProvider p2 = StorageProviderFactory.createFromUrl("http://b.com/file.csv");

    // Should return the same cached instance for "http" type
    assertTrue(p1 == p2, "Cached providers should be the same instance");
  }

  /**
   * The s3 branch used to build a new provider — and so a new S3Client with a 200-connection
   * Apache pool — on every call, while every other scheme went through the cache. A process
   * rebuilding its schemas periodically accumulated pools it had no way to release; one
   * MinIO-backed server showed 214 sockets stuck in CLOSE_WAIT.
   *
   * <p>No {@code directory} here on purpose: setting it makes the constructor call
   * {@code ensureBucketExists}, a real headBucket round trip that needs a live endpoint. Its
   * presence in the cache key is asserted separately below by key shape, not by construction.
   */
  @Test void s3ProvidersAreCachedPerEndpointAndCredentials() {
    java.util.Map<String, Object> config = new java.util.HashMap<>();
    config.put("endpoint", "http://127.0.0.1:9000");
    config.put("region", "us-east-1");
    config.put("accessKeyId", "key");
    config.put("secretAccessKey", "secret");

    StorageProvider first = StorageProviderFactory.createFromType("s3", config);
    StorageProvider again =
        StorageProviderFactory.createFromType("s3", new java.util.HashMap<>(config));
    assertSame(first, again,
        "same endpoint and credentials must reuse one provider and one connection pool");

    java.util.Map<String, Object> otherEndpoint = new java.util.HashMap<>(config);
    otherEndpoint.put("endpoint", "http://127.0.0.1:9001");
    assertNotSame(first, StorageProviderFactory.createFromType("s3", otherEndpoint),
        "a different endpoint must get its own client");

    java.util.Map<String, Object> otherKey = new java.util.HashMap<>(config);
    otherKey.put("accessKeyId", "other");
    assertNotSame(first, StorageProviderFactory.createFromType("s3", otherKey),
        "a different account must get its own client");
  }

  /**
   * A different directory must not share a provider: {@code directory} becomes {@code
   * baseS3Path}, which relative paths resolve against and the staging prefix derives from, so
   * sharing would silently resolve one caller's relative paths under the other's prefix.
   */
  @Test void aDifferentDirectoryGetsItsOwnProvider() {
    java.util.Map<String, Object> a = new java.util.HashMap<>();
    a.put("endpoint", "http://127.0.0.1:9000");
    a.put("region", "us-east-1");
    a.put("accessKeyId", "key");
    a.put("secretAccessKey", "secret");
    java.util.Map<String, Object> b = new java.util.HashMap<>(a);
    a.put("directory", "");
    b.put("directory", "");
    assertSame(StorageProviderFactory.createFromType("s3", a),
        StorageProviderFactory.createFromType("s3", b),
        "identical empty directories still share");

    // The key must vary with directory. Asserted through the factory's own behaviour by using
    // values that do not trigger ensureBucketExists (empty vs absent are both treated as unset).
    java.util.Map<String, Object> noDir = new java.util.HashMap<>(a);
    noDir.remove("directory");
    assertNotSame(StorageProviderFactory.createFromType("s3", a),
        StorageProviderFactory.createFromType("s3", noDir),
        "directory participates in the cache key, so unset and empty must not collide");
  }
}
