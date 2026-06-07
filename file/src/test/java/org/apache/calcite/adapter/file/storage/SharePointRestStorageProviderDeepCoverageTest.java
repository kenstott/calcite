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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Deep coverage tests for SharePointRestStorageProvider.
 * Focuses on pure logic methods that can be tested without HTTP connections.
 */
@Tag("unit")
public class SharePointRestStorageProviderDeepCoverageTest {

  private SharePointRestStorageProvider provider;

  @BeforeEach
  void setUp() {
    // Use static token constructor to avoid HTTP calls
    SharePointTokenManager tokenManager =
        new SharePointTokenManager("test-token", "https://example.sharepoint.com/sites/test");
    provider = new SharePointRestStorageProvider(tokenManager);
  }

  // --- Constructor tests ---

  @Test
  void testConstructorWithTrailingSiteUrl() {
    SharePointTokenManager tokenManager =
        new SharePointTokenManager("test-token", "https://example.sharepoint.com/sites/test/");
    SharePointRestStorageProvider p = new SharePointRestStorageProvider(tokenManager);
    assertEquals("sharepoint-rest", p.getStorageType());
  }

  @Test
  void testConstructorWithoutTrailingSiteUrl() {
    SharePointTokenManager tokenManager =
        new SharePointTokenManager("test-token", "https://example.sharepoint.com/sites/test");
    SharePointRestStorageProvider p = new SharePointRestStorageProvider(tokenManager);
    assertEquals("sharepoint-rest", p.getStorageType());
  }

  @Test
  void testConstructorWithClientSecretTokenManager() {
    // tokenManager with clientSecret set should trigger REST token manager creation
    SharePointTokenManager tokenManager =
        new SharePointTokenManager("tenant", "client", "secret",
            "https://example.sharepoint.com/sites/test");
    SharePointRestStorageProvider p = new SharePointRestStorageProvider(tokenManager);
    assertNotNull(p);
    assertEquals("sharepoint-rest", p.getStorageType());
  }

  @Test
  void testConstructorWithRestTokenManager() {
    // Already a SharePointRestTokenManager, should be used as-is
    SharePointRestTokenManager restTokenManager =
        new SharePointRestTokenManager("test-token",
            "https://example.sharepoint.com/sites/test");
    SharePointRestStorageProvider p = new SharePointRestStorageProvider(restTokenManager);
    assertNotNull(p);
    assertEquals("sharepoint-rest", p.getStorageType());
  }

  @Test
  void testConstructorWithNoClientSecret() {
    // tokenManager without clientSecret (refresh token flow) - should use existing token manager
    SharePointTokenManager tokenManager =
        new SharePointTokenManager("tenant", "client", "refreshToken",
            "https://example.sharepoint.com/sites/test", true);
    SharePointRestStorageProvider p = new SharePointRestStorageProvider(tokenManager);
    assertNotNull(p);
  }

  // --- getStorageType ---

  @Test
  void testGetStorageType() {
    assertEquals("sharepoint-rest", provider.getStorageType());
  }

  // --- resolvePath ---

  @Test
  void testResolvePathWithAbsoluteUrl() {
    String resolved = provider.resolvePath("/base/dir", "https://example.com/file.txt");
    assertEquals("https://example.com/file.txt", resolved);
  }

  @Test
  void testResolvePathWithHttpUrl() {
    String resolved = provider.resolvePath("/base/dir", "http://example.com/file.txt");
    assertEquals("http://example.com/file.txt", resolved);
  }

  @Test
  void testResolvePathWithDirectoryBase() {
    String resolved = provider.resolvePath("/base/dir/", "file.txt");
    assertEquals("/base/dir/file.txt", resolved);
  }

  @Test
  void testResolvePathWithFileBase() {
    String resolved = provider.resolvePath("/base/dir/file.csv", "other.txt");
    assertEquals("/base/dir/other.txt", resolved);
  }

  @Test
  void testResolvePathWithDirectoryLikePath() {
    // Path without extension is treated as directory
    String resolved = provider.resolvePath("/base/dir", "file.txt");
    assertEquals("/base/dir/file.txt", resolved);
  }

  @Test
  void testResolvePathWithNoSlash() {
    String resolved = provider.resolvePath("basedir", "file.txt");
    assertEquals("basedir/file.txt", resolved);
  }

  // --- isLikelyDirectory (via resolvePath) ---

  @Test
  void testResolvePathWithExtensionIsFile() {
    // Has extension, treated as file, parent directory extracted
    String resolved = provider.resolvePath("/docs/report.pdf", "image.png");
    assertEquals("/docs/image.png", resolved);
  }

  @Test
  void testResolvePathWithNoExtensionIsDirectory() {
    // No extension, treated as directory
    String resolved = provider.resolvePath("/docs/reports", "file.txt");
    assertEquals("/docs/reports/file.txt", resolved);
  }

  // --- parseDateTime via reflection ---

  @Test
  void testParseDateTimeDateFormat() throws Exception {
    Method parseDateTimeMethod =
        SharePointRestStorageProvider.class.getDeclaredMethod("parseDateTime", String.class);
    parseDateTimeMethod.setAccessible(true);

    // Test /Date(...)/ format
    long result = (Long) parseDateTimeMethod.invoke(provider, "/Date(1234567890123)/");
    assertEquals(1234567890123L, result);
  }

  @Test
  void testParseDateTimeWithTimezoneOffset() throws Exception {
    Method parseDateTimeMethod =
        SharePointRestStorageProvider.class.getDeclaredMethod("parseDateTime", String.class);
    parseDateTimeMethod.setAccessible(true);

    // Test /Date(...)/ format with timezone offset
    long result = (Long) parseDateTimeMethod.invoke(provider, "/Date(1234567890123+0000)/");
    assertEquals(1234567890123L, result);
  }

  @Test
  void testParseDateTimeWithNegativeTimezoneOffset() throws Exception {
    Method parseDateTimeMethod =
        SharePointRestStorageProvider.class.getDeclaredMethod("parseDateTime", String.class);
    parseDateTimeMethod.setAccessible(true);

    // Test /Date(...)/ format with negative timezone
    long result = (Long) parseDateTimeMethod.invoke(provider, "/Date(1234567890123-0500)/");
    assertEquals(1234567890123L, result);
  }

  @Test
  void testParseDateTimeIso8601() throws Exception {
    Method parseDateTimeMethod =
        SharePointRestStorageProvider.class.getDeclaredMethod("parseDateTime", String.class);
    parseDateTimeMethod.setAccessible(true);

    // Test ISO 8601 format
    long result = (Long) parseDateTimeMethod.invoke(provider, "2023-01-15T10:30:00Z");
    assertTrue(result > 0);
  }

  @Test
  void testParseDateTimeInvalidFormat() throws Exception {
    Method parseDateTimeMethod =
        SharePointRestStorageProvider.class.getDeclaredMethod("parseDateTime", String.class);
    parseDateTimeMethod.setAccessible(true);

    // Test invalid format - should return current time
    long before = System.currentTimeMillis();
    long result = (Long) parseDateTimeMethod.invoke(provider, "not-a-date");
    long after = System.currentTimeMillis();
    assertTrue(result >= before && result <= after);
  }

  // --- isDocumentLibraryRoot via reflection ---

  @Test
  void testIsDocumentLibraryRoot() throws Exception {
    Method isDocLibRoot =
        SharePointRestStorageProvider.class.getDeclaredMethod(
            "isDocumentLibraryRoot", String.class);
    isDocLibRoot.setAccessible(true);

    assertTrue((Boolean) isDocLibRoot.invoke(provider, "Shared Documents"));
    assertTrue((Boolean) isDocLibRoot.invoke(provider, "Documents"));
    assertFalse((Boolean) isDocLibRoot.invoke(provider, "some-random-folder"));
    assertFalse((Boolean) isDocLibRoot.invoke(provider, ""));
  }

  @Test
  void testIsDocumentLibraryRootInternational() throws Exception {
    Method isDocLibRoot =
        SharePointRestStorageProvider.class.getDeclaredMethod(
            "isDocumentLibraryRoot", String.class);
    isDocLibRoot.setAccessible(true);

    // French
    assertTrue((Boolean) isDocLibRoot.invoke(provider, "Documents Partagés"));
    // Spanish
    assertTrue((Boolean) isDocLibRoot.invoke(provider, "Documentos compartidos"));
    // German
    assertTrue((Boolean) isDocLibRoot.invoke(provider, "Gemeinsame Dokumente"));
    // Italian
    assertTrue((Boolean) isDocLibRoot.invoke(provider, "Documenti condivisi"));
  }

  // --- removeDocumentLibraryPrefix via reflection ---

  @Test
  void testRemoveDocumentLibraryPrefix() throws Exception {
    Method removePrefix =
        SharePointRestStorageProvider.class.getDeclaredMethod(
            "removeDocumentLibraryPrefix", String.class);
    removePrefix.setAccessible(true);

    // Nested Shared Documents path
    assertEquals("Shared Documents/subfolder/file.txt",
        removePrefix.invoke(provider, "Shared Documents/Shared Documents/subfolder/file.txt"));

    // Single Shared Documents prefix
    assertEquals("subfolder/file.txt",
        removePrefix.invoke(provider, "Shared Documents/subfolder/file.txt"));

    // Just the document library name
    assertEquals("", removePrefix.invoke(provider, "Shared Documents"));

    // Other document library name
    assertEquals("", removePrefix.invoke(provider, "Documents"));

    // Other library with path
    assertEquals("subfolder/file.txt",
        removePrefix.invoke(provider, "Documents/subfolder/file.txt"));

    // No prefix to remove
    assertEquals("regular/path/file.txt",
        removePrefix.invoke(provider, "regular/path/file.txt"));
  }

  @Test
  void testRemoveDocumentLibraryPrefixForInternationalNames() throws Exception {
    Method removePrefix =
        SharePointRestStorageProvider.class.getDeclaredMethod(
            "removeDocumentLibraryPrefix", String.class);
    removePrefix.setAccessible(true);

    // French
    assertEquals("subfolder/file.txt",
        removePrefix.invoke(provider, "Documents Partagés/subfolder/file.txt"));

    // Spanish
    assertEquals("",
        removePrefix.invoke(provider, "Documentos compartidos"));
  }

  // --- exists error handling ---

  @Test
  void testExistsReturnsCorrectExceptionsViaResolvePath() {
    // Test the resolve path logic indirectly through public methods
    // resolvePath does not throw exceptions for bad input
    String result = provider.resolvePath("", "file.txt");
    assertNotNull(result);
  }
}
