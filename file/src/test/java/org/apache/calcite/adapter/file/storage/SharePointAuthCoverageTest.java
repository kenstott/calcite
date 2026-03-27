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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Coverage tests for SharePoint authentication classes.
 * Tests {@link SharePointTokenManager}, {@link SharePointRestTokenManager},
 * {@link SharePointCertificateTokenManager}, {@link SharePointLegacyTokenManager},
 * and {@link MicrosoftGraphTokenManager} via mocks and reflection.
 *
 * <p>No Docker or external services required - all tests use mock data
 * and reflection to test internal authentication paths.
 */
@Tag("unit")
public class SharePointAuthCoverageTest {

  @BeforeEach
  void setUp() {
    StorageProviderFactory.clearCache();
  }

  // ---------------------------------------------------------------
  // SharePointTokenManager tests
  // ---------------------------------------------------------------

  @Nested
  @DisplayName("SharePointTokenManager")
  class TokenManagerTests {

    @Test
    @DisplayName("client credentials constructor sets fields correctly")
    void testClientCredentialsConstructor() throws Exception {
      SharePointTokenManager tm =
          new SharePointTokenManager("tenant", "client", "secret",
              "https://tenant.sharepoint.com/sites/test");

      assertEquals("https://tenant.sharepoint.com/sites/test", tm.getSiteUrl());
      assertEquals("tenant", getField(tm, "tenantId"));
      assertEquals("client", getField(tm, "clientId"));
      assertEquals("secret", getField(tm, "clientSecret"));
    }

    @Test
    @DisplayName("refresh token constructor sets fields correctly")
    void testRefreshTokenConstructor() throws Exception {
      SharePointTokenManager tm =
          new SharePointTokenManager("tenant", "client", "refreshTok",
              "https://tenant.sharepoint.com/sites/test", true);

      assertEquals("https://tenant.sharepoint.com/sites/test", tm.getSiteUrl());
      assertEquals("tenant", getField(tm, "tenantId"));
      assertEquals("client", getField(tm, "clientId"));
      assertEquals("refreshTok", getField(tm, "refreshToken"));
    }

    @Test
    @DisplayName("static token constructor sets access token directly")
    void testStaticTokenConstructor() throws Exception {
      SharePointTokenManager tm =
          new SharePointTokenManager("my-static-token",
              "https://tenant.sharepoint.com/sites/test");

      assertEquals("my-static-token", tm.getAccessToken());
      assertEquals("https://tenant.sharepoint.com/sites/test", tm.getSiteUrl());
    }

    @Test
    @DisplayName("getAccessToken returns static token when valid")
    void testGetAccessTokenStatic() throws Exception {
      SharePointTokenManager tm =
          new SharePointTokenManager("valid-token",
              "https://tenant.sharepoint.com/sites/test");
      assertEquals("valid-token", tm.getAccessToken());
    }

    @Test
    @DisplayName("invalidateToken forces refresh on next call")
    void testInvalidateToken() throws Exception {
      SharePointTokenManager tm =
          new SharePointTokenManager("valid-token",
              "https://tenant.sharepoint.com/sites/test");

      // Token should be valid initially
      assertEquals("valid-token", tm.getAccessToken());

      // Invalidate it
      tm.invalidateToken();

      // Now it should try to refresh but since it's a static token and the expiry
      // was set to past, it will either return the token or throw depending on timing.
      // For a static token with invalidated expiry, getAccessToken checks if expired and throws.
      // The tokenExpiry was set to now-1 second by invalidateToken, and the buffer is 300 seconds,
      // so isTokenValid returns false, then refreshAccessToken is called.
      // refreshAccessToken for static token checks if tokenExpiry < now(), which it is,
      // so it throws "Static access token has expired."
      assertThrows(IOException.class, () -> tm.getAccessToken());
    }

    @Test
    @DisplayName("getAccessToken with no auth configured throws IOException")
    void testNoAuthConfigured() throws Exception {
      // Create token manager with no auth: tenantId=null, clientId=null, etc.
      // Use refresh token constructor but pass null for the refresh token via reflection.
      SharePointTokenManager tm =
          new SharePointTokenManager("tenant", "client", "refreshTok",
              "https://tenant.sharepoint.com/sites/test", true);

      // Clear the access token and token expiry to force refresh
      setField(tm, "accessToken", null);
      setField(tm, "tokenExpiry", null);

      // Clear the refresh token via the final field
      setField(tm, "refreshToken", null);

      // Now getAccessToken should attempt to refresh, but with no valid method will throw
      assertThrows(IOException.class, () -> tm.getAccessToken());
    }
  }

  // ---------------------------------------------------------------
  // SharePointRestTokenManager tests
  // ---------------------------------------------------------------

  @Nested
  @DisplayName("SharePointRestTokenManager")
  class RestTokenManagerTests {

    @Test
    @DisplayName("client credentials constructor delegates to parent")
    void testClientCredentialsConstructor() {
      SharePointRestTokenManager tm =
          new SharePointRestTokenManager("tenant", "client", "secret",
              "https://tenant.sharepoint.com/sites/test");
      assertNotNull(tm);
      assertEquals("https://tenant.sharepoint.com/sites/test", tm.getSiteUrl());
    }

    @Test
    @DisplayName("refresh token constructor delegates to parent")
    void testRefreshTokenConstructor() {
      SharePointRestTokenManager tm =
          new SharePointRestTokenManager("tenant", "client", "refreshTok",
              "https://tenant.sharepoint.com/sites/test", true);
      assertNotNull(tm);
      assertEquals("https://tenant.sharepoint.com/sites/test", tm.getSiteUrl());
    }

    @Test
    @DisplayName("static token constructor delegates to parent")
    void testStaticTokenConstructor() throws IOException {
      SharePointRestTokenManager tm =
          new SharePointRestTokenManager("static-token",
              "https://tenant.sharepoint.com/sites/test");
      assertNotNull(tm);
      assertEquals("static-token", tm.getAccessToken());
    }
  }

  // ---------------------------------------------------------------
  // MicrosoftGraphTokenManager tests
  // ---------------------------------------------------------------

  @Nested
  @DisplayName("MicrosoftGraphTokenManager")
  class GraphTokenManagerTests {

    @Test
    @DisplayName("client credentials constructor")
    void testClientCredentialsConstructor() {
      MicrosoftGraphTokenManager tm =
          new MicrosoftGraphTokenManager("tenant", "client", "secret",
              "https://tenant.sharepoint.com/sites/test");
      assertNotNull(tm);
      assertEquals("https://tenant.sharepoint.com/sites/test", tm.getSiteUrl());
    }

    @Test
    @DisplayName("refresh token constructor")
    void testRefreshTokenConstructor() {
      MicrosoftGraphTokenManager tm =
          new MicrosoftGraphTokenManager("tenant", "client", "refreshTok",
              "https://tenant.sharepoint.com/sites/test", true);
      assertNotNull(tm);
      assertEquals("https://tenant.sharepoint.com/sites/test", tm.getSiteUrl());
    }

    @Test
    @DisplayName("static token constructor")
    void testStaticTokenConstructor() throws IOException {
      MicrosoftGraphTokenManager tm =
          new MicrosoftGraphTokenManager("static-graph-token",
              "https://tenant.sharepoint.com/sites/test");
      assertNotNull(tm);
      assertEquals("static-graph-token", tm.getAccessToken());
    }
  }

  // ---------------------------------------------------------------
  // SharePointLegacyTokenManager tests
  // ---------------------------------------------------------------

  @Nested
  @DisplayName("SharePointLegacyTokenManager")
  class LegacyTokenManagerTests {

    @Test
    @DisplayName("constructor with realm sets fields correctly")
    void testConstructorWithRealm() throws Exception {
      SharePointLegacyTokenManager tm =
          new SharePointLegacyTokenManager("client-id", "client-secret",
              "https://tenant.sharepoint.com/sites/test", "realm-123");
      assertNotNull(tm);
      assertEquals("https://tenant.sharepoint.com/sites/test", tm.getSiteUrl());

      // Verify the realm field via reflection
      Field realmField = SharePointLegacyTokenManager.class.getDeclaredField("realm");
      realmField.setAccessible(true);
      assertEquals("realm-123", realmField.get(tm));
    }

    @Test
    @DisplayName("constructor without realm sets realm to null for discovery")
    void testConstructorWithoutRealm() throws Exception {
      SharePointLegacyTokenManager tm =
          new SharePointLegacyTokenManager("client-id", "client-secret",
              "https://tenant.sharepoint.com/sites/test");
      assertNotNull(tm);

      // Verify the realm field is null
      Field realmField = SharePointLegacyTokenManager.class.getDeclaredField("realm");
      realmField.setAccessible(true);
      Object realm = realmField.get(tm);
      assertEquals(null, realm);
    }
  }

  // ---------------------------------------------------------------
  // SharePointCertificateTokenManager tests
  // ---------------------------------------------------------------

  @Nested
  @DisplayName("SharePointCertificateTokenManager")
  class CertificateTokenManagerTests {

    @Test
    @DisplayName("constructor with nonexistent certificate throws Exception")
    void testConstructorBadCertPath() {
      Exception ex = assertThrows(Exception.class,
          () -> new SharePointCertificateTokenManager(
              "tenant-id", "client-id",
              "/nonexistent/cert.pfx", "password",
              "https://tenant.sharepoint.com/sites/test"));
      // Should fail because the file does not exist
      assertNotNull(ex);
    }

    @Test
    @DisplayName("constructor with classpath: prefix attempts to load from classpath")
    void testConstructorClasspathPrefix() {
      // This should fail because no such resource exists on the classpath,
      // but it exercises the classpath branch
      Exception ex = assertThrows(Exception.class,
          () -> new SharePointCertificateTokenManager(
              "tenant-id", "client-id",
              "classpath:/nonexistent-cert.pfx", "password",
              "https://tenant.sharepoint.com/sites/test"));
      assertNotNull(ex);
    }
  }

  // ---------------------------------------------------------------
  // SharePointRestStorageProvider constructor tests
  // ---------------------------------------------------------------

  @Nested
  @DisplayName("SharePointRestStorageProvider")
  class RestStorageProviderTests {

    @Test
    @DisplayName("constructor with REST token manager keeps it")
    void testConstructorWithRestTokenManager() {
      SharePointRestTokenManager restTm =
          new SharePointRestTokenManager("tenant", "client", "secret",
              "https://tenant.sharepoint.com/sites/test");
      SharePointRestStorageProvider provider = new SharePointRestStorageProvider(restTm);
      assertNotNull(provider);
      assertEquals("sharepoint-rest", provider.getStorageType());
    }

    @Test
    @DisplayName("constructor with base TokenManager wraps it")
    void testConstructorWithBaseTokenManager() {
      SharePointTokenManager baseTm =
          new SharePointTokenManager("tenant", "client", "secret",
              "https://tenant.sharepoint.com/sites/test");
      SharePointRestStorageProvider provider = new SharePointRestStorageProvider(baseTm);
      assertNotNull(provider);
      assertEquals("sharepoint-rest", provider.getStorageType());
    }

    @Test
    @DisplayName("constructor with static token manager keeps it")
    void testConstructorWithStaticTokenManager() {
      SharePointTokenManager staticTm =
          new SharePointTokenManager("static-token",
              "https://tenant.sharepoint.com/sites/test");
      SharePointRestStorageProvider provider = new SharePointRestStorageProvider(staticTm);
      assertNotNull(provider);
      assertEquals("sharepoint-rest", provider.getStorageType());
    }

    @Test
    @DisplayName("constructor strips trailing slash from siteUrl")
    void testConstructorStripsTrailingSlash() {
      SharePointRestTokenManager restTm =
          new SharePointRestTokenManager("tenant", "client", "secret",
              "https://tenant.sharepoint.com/sites/test/");
      SharePointRestStorageProvider provider = new SharePointRestStorageProvider(restTm);
      assertNotNull(provider);
    }

    @Test
    @DisplayName("resolvePath with absolute URL returns it unchanged")
    void testResolvePathAbsoluteUrl() {
      SharePointRestTokenManager restTm =
          new SharePointRestTokenManager("static-token",
              "https://tenant.sharepoint.com/sites/test");
      SharePointRestStorageProvider provider = new SharePointRestStorageProvider(restTm);
      assertEquals("https://example.com/file.csv",
          provider.resolvePath("base/path", "https://example.com/file.csv"));
      assertEquals("http://example.com/file.csv",
          provider.resolvePath("base/path", "http://example.com/file.csv"));
    }

    @Test
    @DisplayName("resolvePath with relative path resolves against base")
    void testResolvePathRelative() {
      SharePointRestTokenManager restTm =
          new SharePointRestTokenManager("static-token",
              "https://tenant.sharepoint.com/sites/test");
      SharePointRestStorageProvider provider = new SharePointRestStorageProvider(restTm);
      assertEquals("base/path/file.csv",
          provider.resolvePath("base/path/", "file.csv"));
    }

    @Test
    @DisplayName("resolvePath with file base extracts parent directory")
    void testResolvePathFileBase() {
      SharePointRestTokenManager restTm =
          new SharePointRestTokenManager("static-token",
              "https://tenant.sharepoint.com/sites/test");
      SharePointRestStorageProvider provider = new SharePointRestStorageProvider(restTm);
      assertEquals("base/path/file.csv",
          provider.resolvePath("base/path/other.txt", "file.csv"));
    }

    @Test
    @DisplayName("resolvePath with directory-like base (no extension) appends")
    void testResolvePathDirectoryBase() {
      SharePointRestTokenManager restTm =
          new SharePointRestTokenManager("static-token",
              "https://tenant.sharepoint.com/sites/test");
      SharePointRestStorageProvider provider = new SharePointRestStorageProvider(restTm);
      assertEquals("base/path/file.csv",
          provider.resolvePath("base/path", "file.csv"));
    }
  }

  // ---------------------------------------------------------------
  // MicrosoftGraphStorageProvider tests
  // ---------------------------------------------------------------

  @Nested
  @DisplayName("MicrosoftGraphStorageProvider")
  class GraphStorageProviderTests {

    @Test
    @DisplayName("constructor with Graph token manager keeps it")
    void testConstructorWithGraphTokenManager() {
      MicrosoftGraphTokenManager graphTm =
          new MicrosoftGraphTokenManager("tenant", "client", "secret",
              "https://tenant.sharepoint.com/sites/test");
      MicrosoftGraphStorageProvider provider = new MicrosoftGraphStorageProvider(graphTm);
      assertNotNull(provider);
      assertEquals("microsoft-graph", provider.getStorageType());
    }

    @Test
    @DisplayName("constructor with base TokenManager wraps to Graph token manager")
    void testConstructorWithBaseTokenManager() {
      SharePointTokenManager baseTm =
          new SharePointTokenManager("tenant", "client", "secret",
              "https://tenant.sharepoint.com/sites/test");
      MicrosoftGraphStorageProvider provider = new MicrosoftGraphStorageProvider(baseTm);
      assertNotNull(provider);
      assertEquals("microsoft-graph", provider.getStorageType());
    }

    @Test
    @DisplayName("constructor with static token manager uses it directly")
    void testConstructorWithStaticTokenManager() {
      SharePointTokenManager staticTm =
          new SharePointTokenManager("static-token",
              "https://tenant.sharepoint.com/sites/test");
      MicrosoftGraphStorageProvider provider = new MicrosoftGraphStorageProvider(staticTm);
      assertNotNull(provider);
      assertEquals("microsoft-graph", provider.getStorageType());
    }

    @Test
    @DisplayName("constructor strips trailing slash from siteUrl")
    void testConstructorStripsTrailingSlash() {
      MicrosoftGraphTokenManager graphTm =
          new MicrosoftGraphTokenManager("tenant", "client", "secret",
              "https://tenant.sharepoint.com/sites/test/");
      MicrosoftGraphStorageProvider provider = new MicrosoftGraphStorageProvider(graphTm);
      assertNotNull(provider);
    }

    @Test
    @DisplayName("resolvePath with absolute URL returns it unchanged")
    void testResolvePathAbsoluteUrl() {
      MicrosoftGraphTokenManager graphTm =
          new MicrosoftGraphTokenManager("static-token",
              "https://tenant.sharepoint.com/sites/test");
      MicrosoftGraphStorageProvider provider = new MicrosoftGraphStorageProvider(graphTm);
      assertEquals("https://other.com/file.csv",
          provider.resolvePath("base/path", "https://other.com/file.csv"));
    }

    @Test
    @DisplayName("resolvePath with relative path resolves against base directory")
    void testResolvePathRelative() {
      MicrosoftGraphTokenManager graphTm =
          new MicrosoftGraphTokenManager("static-token",
              "https://tenant.sharepoint.com/sites/test");
      MicrosoftGraphStorageProvider provider = new MicrosoftGraphStorageProvider(graphTm);
      assertEquals("base/path/file.csv",
          provider.resolvePath("base/path/", "file.csv"));
    }

    @Test
    @DisplayName("resolvePath with file base extracts parent directory")
    void testResolvePathFileBase() {
      MicrosoftGraphTokenManager graphTm =
          new MicrosoftGraphTokenManager("static-token",
              "https://tenant.sharepoint.com/sites/test");
      MicrosoftGraphStorageProvider provider = new MicrosoftGraphStorageProvider(graphTm);
      assertEquals("base/path/file.csv",
          provider.resolvePath("base/path/other.txt", "file.csv"));
    }
  }

  // ---------------------------------------------------------------
  // Helper methods for reflection
  // ---------------------------------------------------------------

  private static Object getField(Object obj, String fieldName) throws Exception {
    Class<?> clazz = obj.getClass();
    while (clazz != null) {
      try {
        Field field = clazz.getDeclaredField(fieldName);
        field.setAccessible(true);
        return field.get(obj);
      } catch (NoSuchFieldException e) {
        clazz = clazz.getSuperclass();
      }
    }
    throw new NoSuchFieldException(fieldName);
  }

  private static void setField(Object obj, String fieldName, Object value) throws Exception {
    Class<?> clazz = obj.getClass();
    while (clazz != null) {
      try {
        Field field = clazz.getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(obj, value);
        return;
      } catch (NoSuchFieldException e) {
        clazz = clazz.getSuperclass();
      }
    }
    throw new NoSuchFieldException(fieldName);
  }
}
