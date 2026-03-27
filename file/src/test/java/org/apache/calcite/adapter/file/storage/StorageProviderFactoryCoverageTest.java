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

import org.apache.calcite.adapter.file.iceberg.IcebergStorageProvider;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Coverage tests for {@link StorageProviderFactory}.
 * Tests all URL scheme and storage type combinations,
 * caching behavior, and error handling.
 */
@Tag("unit")
public class StorageProviderFactoryCoverageTest {

  @BeforeEach
  void setUp() {
    StorageProviderFactory.clearCache();
  }

  // ---------------------------------------------------------------
  // createFromUrl tests
  // ---------------------------------------------------------------

  @Nested
  @DisplayName("createFromUrl")
  class CreateFromUrl {

    @Test
    @DisplayName("null URL returns LocalFileStorageProvider")
    void testNullUrl() {
      StorageProvider provider = StorageProviderFactory.createFromUrl(null);
      assertNotNull(provider);
      assertTrue(provider instanceof LocalFileStorageProvider);
    }

    @Test
    @DisplayName("empty URL returns LocalFileStorageProvider")
    void testEmptyUrl() {
      StorageProvider provider = StorageProviderFactory.createFromUrl("");
      assertNotNull(provider);
      assertTrue(provider instanceof LocalFileStorageProvider);
    }

    @Test
    @DisplayName("URL without scheme returns cached LocalFileStorageProvider")
    void testNoScheme() {
      StorageProvider provider = StorageProviderFactory.createFromUrl("/some/local/path");
      assertNotNull(provider);
      assertTrue(provider instanceof LocalFileStorageProvider);
    }

    @Test
    @DisplayName("file:// URL returns cached LocalFileStorageProvider")
    void testFileScheme() {
      StorageProvider provider = StorageProviderFactory.createFromUrl("file:///tmp/test.csv");
      assertNotNull(provider);
      assertTrue(provider instanceof LocalFileStorageProvider);
    }

    @Test
    @DisplayName("http:// URL returns cached HttpStorageProvider")
    void testHttpScheme() {
      StorageProvider provider =
          StorageProviderFactory.createFromUrl("http://example.com/data.csv");
      assertNotNull(provider);
      assertTrue(provider instanceof HttpStorageProvider);
    }

    @Test
    @DisplayName("https:// URL returns cached HttpStorageProvider")
    void testHttpsScheme() {
      StorageProvider provider =
          StorageProviderFactory.createFromUrl("https://example.com/data.csv");
      assertNotNull(provider);
      assertTrue(provider instanceof HttpStorageProvider);
    }

    @Test
    @DisplayName("s3:// URL throws IllegalArgumentException")
    void testS3Scheme() {
      IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
          () -> StorageProviderFactory.createFromUrl("s3://bucket/key"));
      assertTrue(ex.getMessage().contains("S3 storage requires explicit credentials"));
    }

    @Test
    @DisplayName("hdfs:// URL returns cached HDFSStorageProvider")
    void testHdfsScheme() {
      StorageProvider provider =
          StorageProviderFactory.createFromUrl("hdfs://namenode/path/file.csv");
      assertNotNull(provider);
      assertEquals("hdfs", provider.getStorageType());
    }

    @Test
    @DisplayName("ftp:// URL returns cached FtpStorageProvider")
    void testFtpScheme() {
      StorageProvider provider =
          StorageProviderFactory.createFromUrl("ftp://ftp.example.com/data.csv");
      assertNotNull(provider);
      assertTrue(provider instanceof FtpStorageProvider);
    }

    @Test
    @DisplayName("ftps:// URL returns cached FtpStorageProvider (same as ftp)")
    void testFtpsScheme() {
      StorageProvider provider =
          StorageProviderFactory.createFromUrl("ftps://ftp.example.com/data.csv");
      assertNotNull(provider);
      assertTrue(provider instanceof FtpStorageProvider);
    }

    @Test
    @DisplayName("sftp:// URL returns cached SftpStorageProvider")
    void testSftpScheme() {
      StorageProvider provider =
          StorageProviderFactory.createFromUrl("sftp://host/path/data.csv");
      assertNotNull(provider);
      assertTrue(provider instanceof SftpStorageProvider);
    }

    @Test
    @DisplayName("iceberg:// URL returns IcebergStorageProvider (not cached)")
    void testIcebergScheme() {
      StorageProvider provider =
          StorageProviderFactory.createFromUrl("iceberg://catalog/namespace/table");
      assertNotNull(provider);
      assertTrue(provider instanceof IcebergStorageProvider);
    }

    @Test
    @DisplayName("unsupported scheme throws IllegalArgumentException")
    void testUnsupportedScheme() {
      IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
          () -> StorageProviderFactory.createFromUrl("gopher://example.com/path"));
      assertTrue(ex.getMessage().contains("Unsupported URL scheme: gopher"));
    }

    @Test
    @DisplayName("scheme matching is case-insensitive")
    void testSchemeIsCaseInsensitive() {
      StorageProvider provider =
          StorageProviderFactory.createFromUrl("HTTP://example.com/data.csv");
      assertNotNull(provider);
      assertTrue(provider instanceof HttpStorageProvider);
    }
  }

  // ---------------------------------------------------------------
  // createFromType tests
  // ---------------------------------------------------------------

  @Nested
  @DisplayName("createFromType")
  class CreateFromType {

    @Test
    @DisplayName("null storageType defaults to local")
    void testNullType() {
      StorageProvider provider = StorageProviderFactory.createFromType(null, null);
      assertNotNull(provider);
      assertTrue(provider instanceof LocalFileStorageProvider);
    }

    @Test
    @DisplayName("empty storageType defaults to local")
    void testEmptyType() {
      StorageProvider provider = StorageProviderFactory.createFromType("", null);
      assertNotNull(provider);
      assertTrue(provider instanceof LocalFileStorageProvider);
    }

    @Test
    @DisplayName("type 'local' returns LocalFileStorageProvider")
    void testLocalType() {
      StorageProvider provider = StorageProviderFactory.createFromType("local", null);
      assertNotNull(provider);
      assertTrue(provider instanceof LocalFileStorageProvider);
    }

    @Test
    @DisplayName("type 'file' returns LocalFileStorageProvider")
    void testFileType() {
      StorageProvider provider = StorageProviderFactory.createFromType("file", null);
      assertNotNull(provider);
      assertTrue(provider instanceof LocalFileStorageProvider);
    }

    @Test
    @DisplayName("type 'http' without config returns cached HttpStorageProvider")
    void testHttpTypeNoConfig() {
      StorageProvider provider = StorageProviderFactory.createFromType("http", null);
      assertNotNull(provider);
      assertTrue(provider instanceof HttpStorageProvider);
    }

    @Test
    @DisplayName("type 'http' with method config returns configured HttpStorageProvider")
    void testHttpTypeWithMethodConfig() {
      Map<String, Object> config = new HashMap<>();
      config.put("method", "POST");
      StorageProvider provider = StorageProviderFactory.createFromType("http", config);
      assertNotNull(provider);
      assertEquals("http", provider.getStorageType());
    }

    @Test
    @DisplayName("type 'http' with headers config returns configured HttpStorageProvider")
    void testHttpTypeWithHeadersConfig() {
      Map<String, Object> config = new HashMap<>();
      config.put("headers", new HashMap<>());
      StorageProvider provider = StorageProviderFactory.createFromType("http", config);
      assertNotNull(provider);
      assertEquals("http", provider.getStorageType());
    }

    @Test
    @DisplayName("type 'https' returns HttpStorageProvider")
    void testHttpsType() {
      StorageProvider provider = StorageProviderFactory.createFromType("https", null);
      assertNotNull(provider);
      assertTrue(provider instanceof HttpStorageProvider);
    }

    @Test
    @DisplayName("type 's3' without config throws IllegalArgumentException")
    void testS3TypeNoConfig() {
      assertThrows(IllegalArgumentException.class,
          () -> StorageProviderFactory.createFromType("s3", null));
    }

    @Test
    @DisplayName("type 's3' with empty config throws IllegalArgumentException")
    void testS3TypeEmptyConfig() {
      assertThrows(IllegalArgumentException.class,
          () -> StorageProviderFactory.createFromType("s3", new HashMap<>()));
    }

    @Test
    @DisplayName("type 's3' with credentials config returns S3StorageProvider")
    void testS3TypeWithConfig() {
      Map<String, Object> config = new HashMap<>();
      config.put("accessKeyId", "test-key");
      config.put("secretAccessKey", "test-secret");
      config.put("region", "us-east-1");
      StorageProvider provider = StorageProviderFactory.createFromType("s3", config);
      assertNotNull(provider);
      assertTrue(provider instanceof S3StorageProvider);
    }

    @Test
    @DisplayName("type 'hdfs' without config returns cached HDFSStorageProvider")
    void testHdfsTypeNoConfig() {
      StorageProvider provider = StorageProviderFactory.createFromType("hdfs", null);
      assertNotNull(provider);
      assertEquals("hdfs", provider.getStorageType());
    }

    @Test
    @DisplayName("type 'ftp' returns cached FtpStorageProvider")
    void testFtpType() {
      StorageProvider provider = StorageProviderFactory.createFromType("ftp", null);
      assertNotNull(provider);
      assertTrue(provider instanceof FtpStorageProvider);
    }

    @Test
    @DisplayName("type 'ftps' returns cached FtpStorageProvider")
    void testFtpsType() {
      StorageProvider provider = StorageProviderFactory.createFromType("ftps", null);
      assertNotNull(provider);
      assertTrue(provider instanceof FtpStorageProvider);
    }

    @Test
    @DisplayName("type 'sftp' without config returns cached SftpStorageProvider")
    void testSftpTypeNoConfig() {
      StorageProvider provider = StorageProviderFactory.createFromType("sftp", null);
      assertNotNull(provider);
      assertTrue(provider instanceof SftpStorageProvider);
    }

    @Test
    @DisplayName("type 'sftp' with credentials returns configured SftpStorageProvider")
    void testSftpTypeWithConfig() {
      Map<String, Object> config = new HashMap<>();
      config.put("username", "user");
      config.put("password", "pass");
      config.put("privateKeyPath", "/home/user/.ssh/id_rsa");
      config.put("strictHostKeyChecking", Boolean.TRUE);
      StorageProvider provider = StorageProviderFactory.createFromType("sftp", config);
      assertNotNull(provider);
      assertTrue(provider instanceof SftpStorageProvider);
    }

    @Test
    @DisplayName("type 'sftp' with config missing strictHostKey uses default false")
    void testSftpTypeConfigMissingStrictHostKey() {
      Map<String, Object> config = new HashMap<>();
      config.put("username", "user");
      config.put("password", "pass");
      StorageProvider provider = StorageProviderFactory.createFromType("sftp", config);
      assertNotNull(provider);
      assertTrue(provider instanceof SftpStorageProvider);
    }

    @Test
    @DisplayName("type 'iceberg' returns IcebergStorageProvider")
    void testIcebergType() {
      StorageProvider provider = StorageProviderFactory.createFromType("iceberg", null);
      assertNotNull(provider);
      assertTrue(provider instanceof IcebergStorageProvider);
    }

    @Test
    @DisplayName("type 'iceberg' with config returns IcebergStorageProvider")
    void testIcebergTypeWithConfig() {
      Map<String, Object> config = new HashMap<>();
      config.put("warehouse", "s3://bucket/warehouse");
      StorageProvider provider = StorageProviderFactory.createFromType("iceberg", config);
      assertNotNull(provider);
      assertTrue(provider instanceof IcebergStorageProvider);
    }

    @Test
    @DisplayName("unsupported storageType throws IllegalArgumentException")
    void testUnsupportedType() {
      IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
          () -> StorageProviderFactory.createFromType("nosql", null));
      assertTrue(ex.getMessage().contains("Unsupported storage type: nosql"));
    }

    @Test
    @DisplayName("type matching is case-insensitive")
    void testTypeIsCaseInsensitive() {
      StorageProvider provider = StorageProviderFactory.createFromType("LOCAL", null);
      assertNotNull(provider);
      assertTrue(provider instanceof LocalFileStorageProvider);
    }
  }

  // ---------------------------------------------------------------
  // SharePoint authentication paths via createFromType
  // ---------------------------------------------------------------

  @Nested
  @DisplayName("createFromType sharepoint")
  class SharePointType {

    @Test
    @DisplayName("sharepoint without siteUrl throws IllegalArgumentException")
    void testSharepointMissingSiteUrl() {
      Map<String, Object> config = new HashMap<>();
      config.put("accessToken", "token123");
      IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
          () -> StorageProviderFactory.createFromType("sharepoint", config));
      assertTrue(ex.getMessage().contains("siteUrl"));
    }

    @Test
    @DisplayName("sharepoint with null config throws IllegalArgumentException")
    void testSharepointNullConfig() {
      assertThrows(IllegalArgumentException.class,
          () -> StorageProviderFactory.createFromType("sharepoint", null));
    }

    @Test
    @DisplayName("sharepoint with accessToken returns SharePointRestStorageProvider")
    void testSharepointAccessToken() {
      Map<String, Object> config = new HashMap<>();
      config.put("siteUrl", "https://tenant.sharepoint.com/sites/test");
      config.put("accessToken", "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9...");
      StorageProvider provider = StorageProviderFactory.createFromType("sharepoint", config);
      assertNotNull(provider);
      assertTrue(provider instanceof SharePointRestStorageProvider);
    }

    @Test
    @DisplayName("sharepoint with clientId+clientSecret returns SharePointRestStorageProvider")
    void testSharepointClientCredentials() {
      Map<String, Object> config = new HashMap<>();
      config.put("siteUrl", "https://tenant.sharepoint.com/sites/test");
      config.put("tenantId", "tenant-id-123");
      config.put("clientId", "client-id-456");
      config.put("clientSecret", "secret-789");
      StorageProvider provider = StorageProviderFactory.createFromType("sharepoint", config);
      assertNotNull(provider);
      assertTrue(provider instanceof SharePointRestStorageProvider);
    }

    @Test
    @DisplayName("sharepoint with clientId+clientSecret missing tenantId throws")
    void testSharepointClientCredentialsMissingTenant() {
      Map<String, Object> config = new HashMap<>();
      config.put("siteUrl", "https://tenant.sharepoint.com/sites/test");
      config.put("clientId", "client-id-456");
      config.put("clientSecret", "secret-789");
      IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
          () -> StorageProviderFactory.createFromType("sharepoint", config));
      assertTrue(ex.getMessage().contains("tenantId"));
    }

    @Test
    @DisplayName("sharepoint with refreshToken returns SharePointRestStorageProvider")
    void testSharepointRefreshToken() {
      Map<String, Object> config = new HashMap<>();
      config.put("siteUrl", "https://tenant.sharepoint.com/sites/test");
      config.put("tenantId", "tenant-id-123");
      config.put("clientId", "client-id-456");
      config.put("refreshToken", "refresh-token-value");
      StorageProvider provider = StorageProviderFactory.createFromType("sharepoint", config);
      assertNotNull(provider);
      assertTrue(provider instanceof SharePointRestStorageProvider);
    }

    @Test
    @DisplayName("sharepoint with refreshToken missing tenantId throws")
    void testSharepointRefreshTokenMissingTenant() {
      Map<String, Object> config = new HashMap<>();
      config.put("siteUrl", "https://tenant.sharepoint.com/sites/test");
      config.put("refreshToken", "refresh-token-value");
      IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
          () -> StorageProviderFactory.createFromType("sharepoint", config));
      assertTrue(ex.getMessage().contains("tenantId"));
    }

    @Test
    @DisplayName("sharepoint with refreshToken missing clientId throws")
    void testSharepointRefreshTokenMissingClientId() {
      Map<String, Object> config = new HashMap<>();
      config.put("siteUrl", "https://tenant.sharepoint.com/sites/test");
      config.put("tenantId", "tenant-id-123");
      config.put("refreshToken", "refresh-token-value");
      IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
          () -> StorageProviderFactory.createFromType("sharepoint", config));
      assertTrue(ex.getMessage().contains("clientId"));
    }

    @Test
    @DisplayName("sharepoint with certificatePath+certificatePassword missing tenantId throws")
    void testSharepointCertMissingTenant() {
      Map<String, Object> config = new HashMap<>();
      config.put("siteUrl", "https://tenant.sharepoint.com/sites/test");
      config.put("clientId", "client-id-456");
      config.put("certificatePath", "/path/to/cert.pfx");
      config.put("certificatePassword", "certpass");
      IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
          () -> StorageProviderFactory.createFromType("sharepoint", config));
      assertTrue(ex.getMessage().contains("tenantId"));
    }

    @Test
    @DisplayName("sharepoint with certificatePath+certificatePassword missing clientId throws")
    void testSharepointCertMissingClientId() {
      Map<String, Object> config = new HashMap<>();
      config.put("siteUrl", "https://tenant.sharepoint.com/sites/test");
      config.put("tenantId", "tenant-id-123");
      config.put("certificatePath", "/path/to/cert.pfx");
      config.put("certificatePassword", "certpass");
      IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
          () -> StorageProviderFactory.createFromType("sharepoint", config));
      assertTrue(ex.getMessage().contains("clientId"));
    }

    @Test
    @DisplayName("sharepoint with no auth method throws IllegalArgumentException")
    void testSharepointNoAuthMethod() {
      Map<String, Object> config = new HashMap<>();
      config.put("siteUrl", "https://tenant.sharepoint.com/sites/test");
      IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
          () -> StorageProviderFactory.createFromType("sharepoint", config));
      assertTrue(ex.getMessage().contains("accessToken"));
    }

    @Test
    @DisplayName("sharepoint with useGraphApi returns MicrosoftGraphStorageProvider (clientSecret)")
    void testSharepointGraphApiClientSecret() {
      Map<String, Object> config = new HashMap<>();
      config.put("siteUrl", "https://tenant.sharepoint.com/sites/test");
      config.put("tenantId", "tenant-id-123");
      config.put("clientId", "client-id-456");
      config.put("clientSecret", "secret-789");
      config.put("useGraphApi", Boolean.TRUE);
      StorageProvider provider = StorageProviderFactory.createFromType("sharepoint", config);
      assertNotNull(provider);
      assertTrue(provider instanceof MicrosoftGraphStorageProvider);
    }

    @Test
    @DisplayName("sharepoint with useGraphApi returns MicrosoftGraphStorageProvider (refreshToken)")
    void testSharepointGraphApiRefreshToken() {
      Map<String, Object> config = new HashMap<>();
      config.put("siteUrl", "https://tenant.sharepoint.com/sites/test");
      config.put("tenantId", "tenant-id-123");
      config.put("clientId", "client-id-456");
      config.put("refreshToken", "refresh-token-value");
      config.put("useGraphApi", Boolean.TRUE);
      StorageProvider provider = StorageProviderFactory.createFromType("sharepoint", config);
      assertNotNull(provider);
      assertTrue(provider instanceof MicrosoftGraphStorageProvider);
    }

    @Test
    @DisplayName("sharepoint with useGraphApi returns MicrosoftGraphStorageProvider (staticToken)")
    void testSharepointGraphApiStaticToken() {
      Map<String, Object> config = new HashMap<>();
      config.put("siteUrl", "https://tenant.sharepoint.com/sites/test");
      config.put("accessToken", "static-token");
      config.put("useGraphApi", Boolean.TRUE);
      StorageProvider provider = StorageProviderFactory.createFromType("sharepoint", config);
      assertNotNull(provider);
      assertTrue(provider instanceof MicrosoftGraphStorageProvider);
    }

    @Test
    @DisplayName("sharepoint with useLegacyAuth returns SharePointRestStorageProvider (no realm)")
    void testSharepointLegacyAuthNoRealm() {
      Map<String, Object> config = new HashMap<>();
      config.put("siteUrl", "https://tenant.sharepoint.com/sites/test");
      config.put("tenantId", "tenant-id-123");
      config.put("clientId", "client-id-456");
      config.put("clientSecret", "secret-789");
      config.put("useLegacyAuth", Boolean.TRUE);
      StorageProvider provider = StorageProviderFactory.createFromType("sharepoint", config);
      assertNotNull(provider);
      assertTrue(provider instanceof SharePointRestStorageProvider);
    }

    @Test
    @DisplayName("sharepoint with useLegacyAuth and realm returns SharePointRestStorageProvider")
    void testSharepointLegacyAuthWithRealm() {
      Map<String, Object> config = new HashMap<>();
      config.put("siteUrl", "https://tenant.sharepoint.com/sites/test");
      config.put("tenantId", "tenant-id-123");
      config.put("clientId", "client-id-456");
      config.put("clientSecret", "secret-789");
      config.put("useLegacyAuth", Boolean.TRUE);
      config.put("realm", "custom-realm-id");
      StorageProvider provider = StorageProviderFactory.createFromType("sharepoint", config);
      assertNotNull(provider);
      assertTrue(provider instanceof SharePointRestStorageProvider);
    }

    @Test
    @DisplayName("sharepoint with certificatePath+certificatePassword wraps in RuntimeException on bad cert")
    void testSharepointCertBadPath() {
      Map<String, Object> config = new HashMap<>();
      config.put("siteUrl", "https://tenant.sharepoint.com/sites/test");
      config.put("tenantId", "tenant-id-123");
      config.put("clientId", "client-id-456");
      config.put("certificatePath", "/nonexistent/cert.pfx");
      config.put("certificatePassword", "password");
      RuntimeException ex = assertThrows(RuntimeException.class,
          () -> StorageProviderFactory.createFromType("sharepoint", config));
      assertTrue(ex.getMessage().contains("certificate"));
    }
  }

  // ---------------------------------------------------------------
  // Caching behavior
  // ---------------------------------------------------------------

  @Nested
  @DisplayName("Provider caching")
  class Caching {

    @Test
    @DisplayName("cached providers return the same instance for same key")
    void testCachingReturnsSameInstance() {
      StorageProvider first = StorageProviderFactory.createFromUrl("file:///a.csv");
      StorageProvider second = StorageProviderFactory.createFromUrl("file:///b.csv");
      assertSame(first, second, "Cached providers with same key should be the same instance");
    }

    @Test
    @DisplayName("clearCache resets all cached providers")
    void testClearCache() {
      StorageProvider before = StorageProviderFactory.createFromUrl("file:///a.csv");
      StorageProviderFactory.clearCache();
      StorageProvider after = StorageProviderFactory.createFromUrl("file:///b.csv");
      assertNotNull(before);
      assertNotNull(after);
    }

    @Test
    @DisplayName("http and ftp use distinct cache keys")
    void testDifferentCacheKeys() {
      StorageProvider http = StorageProviderFactory.createFromUrl("http://example.com/a.csv");
      StorageProvider ftp = StorageProviderFactory.createFromUrl("ftp://ftp.example.com/a.csv");
      assertTrue(http instanceof HttpStorageProvider);
      assertTrue(ftp instanceof FtpStorageProvider);
    }

    @Test
    @DisplayName("iceberg URLs do not cache (each call creates new instance)")
    void testIcebergNotCached() {
      StorageProvider first =
          StorageProviderFactory.createFromUrl("iceberg://catalog/ns/table1");
      StorageProvider second =
          StorageProviderFactory.createFromUrl("iceberg://catalog/ns/table2");
      assertTrue(first instanceof IcebergStorageProvider);
      assertTrue(second instanceof IcebergStorageProvider);
    }
  }
}
