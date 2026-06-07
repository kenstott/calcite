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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link StorageProviderFactory}.
 */
@Tag("unit")
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
}
