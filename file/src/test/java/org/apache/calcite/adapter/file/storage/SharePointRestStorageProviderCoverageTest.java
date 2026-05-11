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
import java.lang.reflect.Field;
import java.lang.reflect.Method;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Coverage tests for {@link SharePointRestStorageProvider}.
 * Tests URL construction, path resolution, authentication token management,
 * and error handling via mocks and reflection.
 */
@Tag("unit")
class SharePointRestStorageProviderCoverageTest {

  private static final String SITE_URL = "https://contoso.sharepoint.com/sites/mysite";

  // ---------------------------------------------------------------
  // Constructor tests
  // ---------------------------------------------------------------

  @Test void testConstructorTrimTrailingSlash() throws Exception {
    SharePointTokenManager tokenManager = new SharePointTokenManager("token123",
        "https://contoso.sharepoint.com/sites/mysite/");
    SharePointRestStorageProvider provider = new SharePointRestStorageProvider(tokenManager);

    Field siteUrlField = SharePointRestStorageProvider.class.getDeclaredField("siteUrl");
    siteUrlField.setAccessible(true);
    String siteUrl = (String) siteUrlField.get(provider);
    assertFalse(siteUrl.endsWith("/"));
    assertEquals("https://contoso.sharepoint.com/sites/mysite", siteUrl);
  }

  @Test void testConstructorNoTrailingSlash() throws Exception {
    SharePointTokenManager tokenManager = new SharePointTokenManager("token123", SITE_URL);
    SharePointRestStorageProvider provider = new SharePointRestStorageProvider(tokenManager);

    Field siteUrlField = SharePointRestStorageProvider.class.getDeclaredField("siteUrl");
    siteUrlField.setAccessible(true);
    String siteUrl = (String) siteUrlField.get(provider);
    assertEquals(SITE_URL, siteUrl);
  }

  @Test void testConstructorWithStaticToken() {
    SharePointTokenManager tokenManager = new SharePointTokenManager("static_token", SITE_URL);
    SharePointRestStorageProvider provider = new SharePointRestStorageProvider(tokenManager);

    assertNotNull(provider);
    assertEquals("sharepoint-rest", provider.getStorageType());
  }

  @Test void testConstructorWithClientCredentials() throws Exception {
    SharePointTokenManager tokenManager = new SharePointTokenManager(
        "tenant-id", "client-id", "client-secret", SITE_URL);
    SharePointRestStorageProvider provider = new SharePointRestStorageProvider(tokenManager);

    Field tmField = SharePointRestStorageProvider.class.getDeclaredField("tokenManager");
    tmField.setAccessible(true);
    Object actualTm = tmField.get(provider);
    assertTrue(actualTm instanceof SharePointRestTokenManager);
  }

  @Test void testConstructorWithRestTokenManager() throws Exception {
    SharePointRestTokenManager tokenManager = new SharePointRestTokenManager(
        "tenant-id", "client-id", "client-secret", SITE_URL);
    SharePointRestStorageProvider provider = new SharePointRestStorageProvider(tokenManager);

    Field tmField = SharePointRestStorageProvider.class.getDeclaredField("tokenManager");
    tmField.setAccessible(true);
    Object actualTm = tmField.get(provider);
    assertTrue(actualTm instanceof SharePointRestTokenManager);
  }

  @Test void testConstructorWithStaticTokenNonRestManager() {
    SharePointTokenManager tokenManager = new SharePointTokenManager("static-token",
        "https://example.sharepoint.com");
    SharePointRestStorageProvider provider = new SharePointRestStorageProvider(tokenManager);
    assertEquals("sharepoint-rest", provider.getStorageType());
  }

  // ---------------------------------------------------------------
  // getStorageType tests
  // ---------------------------------------------------------------

  @Test void testGetStorageType() {
    SharePointTokenManager tokenManager = new SharePointTokenManager("token", SITE_URL);
    SharePointRestStorageProvider provider = new SharePointRestStorageProvider(tokenManager);
    assertEquals("sharepoint-rest", provider.getStorageType());
  }

  // ---------------------------------------------------------------
  // resolvePath tests
  // ---------------------------------------------------------------

  @Test void testResolvePathWithAbsoluteUrl() {
    SharePointRestStorageProvider provider = createProvider();
    String result = provider.resolvePath("/base/dir", "https://example.com/file.csv");
    assertEquals("https://example.com/file.csv", result);
  }

  @Test void testResolvePathWithHttpUrl() {
    SharePointRestStorageProvider provider = createProvider();
    String result = provider.resolvePath("/base/dir", "http://example.com/file.csv");
    assertEquals("http://example.com/file.csv", result);
  }

  @Test void testResolvePathWithDirectoryBase() {
    SharePointRestStorageProvider provider = createProvider();
    String result = provider.resolvePath("documents/reports", "sales.csv");
    assertEquals("documents/reports/sales.csv", result);
  }

  @Test void testResolvePathWithTrailingSlash() {
    SharePointRestStorageProvider provider = createProvider();
    String result = provider.resolvePath("documents/", "file.csv");
    assertEquals("documents/file.csv", result);
  }

  @Test void testResolvePathWithFileBase() {
    SharePointRestStorageProvider provider = createProvider();
    String result = provider.resolvePath("documents/index.html", "data.csv");
    assertEquals("documents/data.csv", result);
  }

  @Test void testResolvePathWithNestedDirectoryBase() {
    SharePointRestStorageProvider provider = createProvider();
    String result = provider.resolvePath("a/b/c", "file.txt");
    assertEquals("a/b/c/file.txt", result);
  }

  @Test void testResolvePathWithSimpleDirectoryBase() {
    SharePointRestStorageProvider provider = createProvider();
    String result = provider.resolvePath("mydir", "myfile.txt");
    assertEquals("mydir/myfile.txt", result);
  }

  @Test void testResolvePathWithSharedDocumentsBase() {
    SharePointRestStorageProvider provider = createProvider();
    String result = provider.resolvePath("Shared Documents/folder/file.csv", "other.csv");
    assertEquals("Shared Documents/folder/other.csv", result);
  }

  @Test void testResolvePathWithDeepNesting() {
    SharePointRestStorageProvider provider = createProvider();
    String result = provider.resolvePath("a/b/c/d/file.csv", "sibling.csv");
    assertEquals("a/b/c/d/sibling.csv", result);
  }

  // ---------------------------------------------------------------
  // exists / openReader error handling tests
  // ---------------------------------------------------------------

  @Test void testExistsThrowsOnNetworkError() {
    SharePointRestStorageProvider provider = createProvider();
    try {
      provider.exists("test-path.csv");
      fail("Should have thrown an exception since we cannot connect");
    } catch (IOException e) {
      assertNotNull(e.getMessage());
    }
  }

  @Test void testOpenReaderThrowsOnNetworkError() {
    SharePointRestStorageProvider provider = createProvider();
    assertThrows(IOException.class, () -> provider.openReader("test-path.csv"));
  }

  // ---------------------------------------------------------------
  // isLikelyDirectory tests (via reflection)
  // ---------------------------------------------------------------

  @Test void testIsLikelyDirectoryNoExtension() throws Exception {
    assertTrue(invokeIsLikelyDirectory(createProvider(), "folder/subfolder"));
  }

  @Test void testIsLikelyDirectoryWithExtension() throws Exception {
    assertFalse(invokeIsLikelyDirectory(createProvider(), "folder/file.csv"));
  }

  @Test void testIsLikelyDirectorySimpleName() throws Exception {
    assertTrue(invokeIsLikelyDirectory(createProvider(), "documents"));
  }

  @Test void testIsLikelyDirectoryHiddenFileWithExtension() throws Exception {
    assertFalse(invokeIsLikelyDirectory(createProvider(), ".gitignore"));
  }

  @Test void testIsLikelyDirectoryParquetFile() throws Exception {
    assertFalse(invokeIsLikelyDirectory(createProvider(), "data/table.parquet"));
  }

  // ---------------------------------------------------------------
  // removeDocumentLibraryPrefix tests (via reflection)
  // ---------------------------------------------------------------

  @Test void testRemoveDocLibPrefixSharedDocuments() throws Exception {
    assertEquals("subfolder/file.txt",
        invokeRemoveDocumentLibraryPrefix(createProvider(),
            "Shared Documents/subfolder/file.txt"));
  }

  @Test void testRemoveDocLibPrefixNestedSharedDocuments() throws Exception {
    assertEquals("Shared Documents/subfolder/file.txt",
        invokeRemoveDocumentLibraryPrefix(createProvider(),
            "Shared Documents/Shared Documents/subfolder/file.txt"));
  }

  @Test void testRemoveDocLibPrefixDocumentsRoot() throws Exception {
    assertEquals("",
        invokeRemoveDocumentLibraryPrefix(createProvider(), "Documents"));
  }

  @Test void testRemoveDocLibPrefixDocumentsWithPath() throws Exception {
    assertEquals("report.xlsx",
        invokeRemoveDocumentLibraryPrefix(createProvider(), "Documents/report.xlsx"));
  }

  @Test void testRemoveDocLibPrefixFrenchDocuments() throws Exception {
    assertEquals("",
        invokeRemoveDocumentLibraryPrefix(createProvider(), "Documents Partagés"));
  }

  @Test void testRemoveDocLibPrefixSpanishDocuments() throws Exception {
    assertEquals("archivo.xlsx",
        invokeRemoveDocumentLibraryPrefix(createProvider(),
            "Documentos compartidos/archivo.xlsx"));
  }

  @Test void testRemoveDocLibPrefixGermanDocuments() throws Exception {
    assertEquals("datei.xlsx",
        invokeRemoveDocumentLibraryPrefix(createProvider(),
            "Gemeinsame Dokumente/datei.xlsx"));
  }

  @Test void testRemoveDocLibPrefixNoMatch() throws Exception {
    assertEquals("custom_folder/file.txt",
        invokeRemoveDocumentLibraryPrefix(createProvider(), "custom_folder/file.txt"));
  }

  @Test void testRemoveDocLibPrefixSharedDocumentsExact() throws Exception {
    assertEquals("",
        invokeRemoveDocumentLibraryPrefix(createProvider(), "Shared Documents"));
  }

  // ---------------------------------------------------------------
  // isDocumentLibraryRoot tests (via reflection)
  // ---------------------------------------------------------------

  @Test void testIsDocumentLibraryRootSharedDocuments() throws Exception {
    assertTrue(invokeIsDocumentLibraryRoot(createProvider(), "Shared Documents"));
  }

  @Test void testIsDocumentLibraryRootDocuments() throws Exception {
    assertTrue(invokeIsDocumentLibraryRoot(createProvider(), "Documents"));
  }

  @Test void testIsDocumentLibraryRootCustomName() throws Exception {
    assertFalse(invokeIsDocumentLibraryRoot(createProvider(), "My Library"));
  }

  @Test void testIsDocumentLibraryRootChineseDocuments() throws Exception {
    assertTrue(invokeIsDocumentLibraryRoot(createProvider(), "\u5171\u4eab\u6587\u6863"));
  }

  @Test void testIsDocumentLibraryRootJapaneseDocuments() throws Exception {
    assertTrue(invokeIsDocumentLibraryRoot(createProvider(),
        "\u5171\u6709\u30c9\u30ad\u30e5\u30e1\u30f3\u30c8"));
  }

  @Test void testIsDocumentLibraryRootItalianDocuments() throws Exception {
    assertTrue(invokeIsDocumentLibraryRoot(createProvider(), "Documenti condivisi"));
  }

  // ---------------------------------------------------------------
  // parseDateTime tests (via reflection)
  // ---------------------------------------------------------------

  @Test void testParseDateTimeSharePointFormat() throws Exception {
    long result = invokeParseDateTime(createProvider(), "/Date(1234567890123)/");
    assertEquals(1234567890123L, result);
  }

  @Test void testParseDateTimeSharePointFormatWithTimezone() throws Exception {
    long result = invokeParseDateTime(createProvider(), "/Date(1234567890123+0000)/");
    assertEquals(1234567890123L, result);
  }

  @Test void testParseDateTimeSharePointFormatWithNegativeTimezone() throws Exception {
    long result = invokeParseDateTime(createProvider(), "/Date(1234567890123-0500)/");
    assertEquals(1234567890123L, result);
  }

  @Test void testParseDateTimeIso8601Format() throws Exception {
    long result = invokeParseDateTime(createProvider(), "2023-06-15T10:30:00Z");
    assertTrue(result > 0);
  }

  @Test void testParseDateTimeInvalidFormat() throws Exception {
    long before = System.currentTimeMillis();
    long result = invokeParseDateTime(createProvider(), "not-a-date");
    long after = System.currentTimeMillis();
    assertTrue(result >= before && result <= after);
  }

  // ---------------------------------------------------------------
  // webUrl field initialization tests
  // ---------------------------------------------------------------

  @Test void testWebUrlMatchesSiteUrl() throws Exception {
    SharePointTokenManager tokenManager = new SharePointTokenManager("token", SITE_URL);
    SharePointRestStorageProvider provider = new SharePointRestStorageProvider(tokenManager);

    Field webUrlField = SharePointRestStorageProvider.class.getDeclaredField("webUrl");
    webUrlField.setAccessible(true);
    assertEquals(SITE_URL, webUrlField.get(provider));
  }

  // ---------------------------------------------------------------
  // Helper methods
  // ---------------------------------------------------------------

  private SharePointRestStorageProvider createProvider() {
    SharePointTokenManager tokenManager = new SharePointTokenManager("token", SITE_URL);
    return new SharePointRestStorageProvider(tokenManager);
  }

  private boolean invokeIsLikelyDirectory(SharePointRestStorageProvider provider, String path)
      throws Exception {
    Method method = SharePointRestStorageProvider.class.getDeclaredMethod(
        "isLikelyDirectory", String.class);
    method.setAccessible(true);
    return (Boolean) method.invoke(provider, path);
  }

  private String invokeRemoveDocumentLibraryPrefix(SharePointRestStorageProvider provider,
      String path) throws Exception {
    Method method = SharePointRestStorageProvider.class.getDeclaredMethod(
        "removeDocumentLibraryPrefix", String.class);
    method.setAccessible(true);
    return (String) method.invoke(provider, path);
  }

  private boolean invokeIsDocumentLibraryRoot(SharePointRestStorageProvider provider,
      String path) throws Exception {
    Method method = SharePointRestStorageProvider.class.getDeclaredMethod(
        "isDocumentLibraryRoot", String.class);
    method.setAccessible(true);
    return (Boolean) method.invoke(provider, path);
  }

  private long invokeParseDateTime(SharePointRestStorageProvider provider,
      String dateTime) throws Exception {
    Method method = SharePointRestStorageProvider.class.getDeclaredMethod(
        "parseDateTime", String.class);
    method.setAccessible(true);
    return (Long) method.invoke(provider, dateTime);
  }
}
