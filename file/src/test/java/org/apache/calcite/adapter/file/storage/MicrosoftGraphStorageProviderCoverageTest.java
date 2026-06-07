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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Coverage tests for {@link MicrosoftGraphStorageProvider}.
 * Tests URL construction, path resolution, authentication, and error handling.
 */
@Tag("unit")
class MicrosoftGraphStorageProviderCoverageTest {

  private static final String SITE_URL = "https://contoso.sharepoint.com/sites/mysite";

  // ---------------------------------------------------------------
  // Constructor tests
  // ---------------------------------------------------------------

  @Test void testConstructorTrimTrailingSlash() throws Exception {
    SharePointTokenManager tokenManager = new SharePointTokenManager("token123",
        "https://contoso.sharepoint.com/sites/mysite/");
    MicrosoftGraphStorageProvider provider = new MicrosoftGraphStorageProvider(tokenManager);

    Field siteUrlField = MicrosoftGraphStorageProvider.class.getDeclaredField("siteUrl");
    siteUrlField.setAccessible(true);
    String siteUrl = (String) siteUrlField.get(provider);
    assertFalse(siteUrl.endsWith("/"));
    assertEquals("https://contoso.sharepoint.com/sites/mysite", siteUrl);
  }

  @Test void testConstructorNoTrailingSlash() throws Exception {
    SharePointTokenManager tokenManager = new SharePointTokenManager("token123", SITE_URL);
    MicrosoftGraphStorageProvider provider = new MicrosoftGraphStorageProvider(tokenManager);

    Field siteUrlField = MicrosoftGraphStorageProvider.class.getDeclaredField("siteUrl");
    siteUrlField.setAccessible(true);
    assertEquals(SITE_URL, siteUrlField.get(provider));
  }

  @Test void testConstructorWithStaticToken() {
    SharePointTokenManager tokenManager = new SharePointTokenManager("static_token", SITE_URL);
    MicrosoftGraphStorageProvider provider = new MicrosoftGraphStorageProvider(tokenManager);

    assertNotNull(provider);
    assertEquals("microsoft-graph", provider.getStorageType());
  }

  @Test void testConstructorWithClientCredentials() throws Exception {
    SharePointTokenManager tokenManager = new SharePointTokenManager(
        "tenant-id", "client-id", "client-secret", SITE_URL);
    MicrosoftGraphStorageProvider provider = new MicrosoftGraphStorageProvider(tokenManager);

    Field tmField = MicrosoftGraphStorageProvider.class.getDeclaredField("tokenManager");
    tmField.setAccessible(true);
    Object actualTm = tmField.get(provider);
    assertTrue(actualTm instanceof MicrosoftGraphTokenManager);
  }

  @Test void testConstructorWithGraphTokenManager() throws Exception {
    MicrosoftGraphTokenManager tokenManager = new MicrosoftGraphTokenManager(
        "tenant-id", "client-id", "client-secret", SITE_URL);
    MicrosoftGraphStorageProvider provider = new MicrosoftGraphStorageProvider(tokenManager);

    Field tmField = MicrosoftGraphStorageProvider.class.getDeclaredField("tokenManager");
    tmField.setAccessible(true);
    Object actualTm = tmField.get(provider);
    assertTrue(actualTm instanceof MicrosoftGraphTokenManager);
  }

  @Test void testConstructorWithStaticTokenManager() throws Exception {
    SharePointTokenManager tokenManager = new SharePointTokenManager("mytoken", SITE_URL);
    MicrosoftGraphStorageProvider provider = new MicrosoftGraphStorageProvider(tokenManager);

    Field tmField = MicrosoftGraphStorageProvider.class.getDeclaredField("tokenManager");
    tmField.setAccessible(true);
    Object actualTm = tmField.get(provider);
    assertNotNull(actualTm);
  }

  // ---------------------------------------------------------------
  // getStorageType tests
  // ---------------------------------------------------------------

  @Test void testGetStorageType() {
    MicrosoftGraphStorageProvider provider = createProvider();
    assertEquals("microsoft-graph", provider.getStorageType());
  }

  // ---------------------------------------------------------------
  // resolvePath tests
  // ---------------------------------------------------------------

  @Test void testResolvePathWithAbsoluteHttpsUrl() {
    MicrosoftGraphStorageProvider provider = createProvider();
    String result = provider.resolvePath("/base/dir", "https://example.com/file.csv");
    assertEquals("https://example.com/file.csv", result);
  }

  @Test void testResolvePathWithAbsoluteHttpUrl() {
    MicrosoftGraphStorageProvider provider = createProvider();
    String result = provider.resolvePath("/base/dir", "http://example.com/file.csv");
    assertEquals("http://example.com/file.csv", result);
  }

  @Test void testResolvePathWithDirectoryBase() {
    MicrosoftGraphStorageProvider provider = createProvider();
    String result = provider.resolvePath("documents/reports", "sales.csv");
    assertEquals("documents/reports/sales.csv", result);
  }

  @Test void testResolvePathWithTrailingSlashBase() {
    MicrosoftGraphStorageProvider provider = createProvider();
    String result = provider.resolvePath("documents/", "file.csv");
    assertEquals("documents/file.csv", result);
  }

  @Test void testResolvePathWithFileBase() {
    MicrosoftGraphStorageProvider provider = createProvider();
    String result = provider.resolvePath("documents/index.html", "data.csv");
    assertEquals("documents/data.csv", result);
  }

  @Test void testResolvePathWithNestedPath() {
    MicrosoftGraphStorageProvider provider = createProvider();
    String result = provider.resolvePath("a/b/c", "file.txt");
    assertEquals("a/b/c/file.txt", result);
  }

  @Test void testResolvePathWithSimpleBase() {
    MicrosoftGraphStorageProvider provider = createProvider();
    String result = provider.resolvePath("mydir", "myfile.txt");
    assertEquals("mydir/myfile.txt", result);
  }

  @Test void testResolvePathWithSharedDocumentsBase() {
    MicrosoftGraphStorageProvider provider = createProvider();
    String result = provider.resolvePath("Shared Documents/folder/file.csv", "other.csv");
    assertEquals("Shared Documents/folder/other.csv", result);
  }

  @Test void testResolvePathWithDeepNesting() {
    MicrosoftGraphStorageProvider provider = createProvider();
    String result = provider.resolvePath("a/b/c/d/file.csv", "sibling.csv");
    assertEquals("a/b/c/d/sibling.csv", result);
  }

  // ---------------------------------------------------------------
  // Error handling for network-dependent methods
  // ---------------------------------------------------------------

  @Test void testExistsThrowsOnNetworkError() {
    MicrosoftGraphStorageProvider provider = createProvider();
    try {
      provider.exists("test-path.csv");
      fail("Should have thrown an exception since we cannot connect");
    } catch (IOException e) {
      assertNotNull(e.getMessage());
    }
  }

  @Test void testOpenReaderThrowsOnNetworkError() {
    MicrosoftGraphStorageProvider provider = createProvider();
    assertThrows(IOException.class, () -> provider.openReader("test-path.csv"));
  }

  @Test void testListFilesThrowsOnNetworkError() {
    MicrosoftGraphStorageProvider provider = createProvider();
    assertThrows(IOException.class, () -> provider.listFiles("Shared Documents", false));
  }

  @Test void testIsDirectoryThrowsOnNetworkError() {
    MicrosoftGraphStorageProvider provider = createProvider();
    assertThrows(IOException.class, () -> provider.isDirectory("/some/path"));
  }

  @Test void testGetMetadataThrowsOnNetworkError() {
    MicrosoftGraphStorageProvider provider = createProvider();
    assertThrows(IOException.class, () -> provider.getMetadata("/some/file.csv"));
  }

  @Test void testOpenInputStreamThrowsOnNetworkError() {
    MicrosoftGraphStorageProvider provider = createProvider();
    assertThrows(IOException.class, () -> provider.openInputStream("/some/file.csv"));
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

  @Test void testIsLikelyDirectoryHiddenFile() throws Exception {
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
    assertEquals("", invokeRemoveDocumentLibraryPrefix(createProvider(), "Documents"));
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

  @Test void testRemoveDocLibPrefixItalianDocuments() throws Exception {
    assertEquals("file.xlsx",
        invokeRemoveDocumentLibraryPrefix(createProvider(),
            "Documenti condivisi/file.xlsx"));
  }

  @Test void testRemoveDocLibPrefixNoMatch() throws Exception {
    assertEquals("custom_folder/file.txt",
        invokeRemoveDocumentLibraryPrefix(createProvider(), "custom_folder/file.txt"));
  }

  @Test void testRemoveDocLibPrefixSharedDocumentsExact() throws Exception {
    assertEquals("", invokeRemoveDocumentLibraryPrefix(createProvider(), "Shared Documents"));
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

  @Test void testIsDocumentLibraryRootEmpty() throws Exception {
    assertFalse(invokeIsDocumentLibraryRoot(createProvider(), ""));
  }

  // ---------------------------------------------------------------
  // parseDateTime tests (via reflection)
  // ---------------------------------------------------------------

  @Test void testParseDateTimeIso8601() throws Exception {
    long result = invokeParseDateTime(createProvider(), "2023-06-15T10:30:00Z");
    assertTrue(result > 0);
  }

  @Test void testParseDateTimeIso8601WithMillis() throws Exception {
    long result = invokeParseDateTime(createProvider(), "2023-06-15T10:30:00.123Z");
    assertTrue(result > 0);
  }

  @Test void testParseDateTimeInvalidFormat() throws Exception {
    long before = System.currentTimeMillis();
    long result = invokeParseDateTime(createProvider(), "not-a-date");
    long after = System.currentTimeMillis();
    assertTrue(result >= before && result <= after);
  }

  @Test void testParseDateTimeEmpty() throws Exception {
    long before = System.currentTimeMillis();
    long result = invokeParseDateTime(createProvider(), "");
    long after = System.currentTimeMillis();
    assertTrue(result >= before && result <= after);
  }

  // ---------------------------------------------------------------
  // buildItemPath tests (via reflection)
  // ---------------------------------------------------------------

  @Test void testBuildItemPathNameOnly() throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    ObjectNode item = mapper.createObjectNode();
    item.put("name", "file.txt");

    String result = invokeBuildItemPath(createProvider(), item);
    assertEquals("file.txt", result);
  }

  @Test void testBuildItemPathWithParentReference() throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    ObjectNode item = mapper.createObjectNode();
    item.put("name", "file.txt");

    ObjectNode parentRef = mapper.createObjectNode();
    parentRef.put("path", "/drives/abc123/root:/documents/subfolder");
    item.set("parentReference", parentRef);

    String result = invokeBuildItemPath(createProvider(), item);
    assertEquals("documents/subfolder/file.txt", result);
  }

  @Test void testBuildItemPathWithRootParent() throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    ObjectNode item = mapper.createObjectNode();
    item.put("name", "root_file.txt");

    ObjectNode parentRef = mapper.createObjectNode();
    parentRef.put("path", "/drives/abc123/root:");
    item.set("parentReference", parentRef);

    String result = invokeBuildItemPath(createProvider(), item);
    assertEquals("root_file.txt", result);
  }

  @Test void testBuildItemPathWithLeadingSlashParent() throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    ObjectNode item = mapper.createObjectNode();
    item.put("name", "data.csv");

    ObjectNode parentRef = mapper.createObjectNode();
    parentRef.put("path", "/drives/abc123/root:/folder");
    item.set("parentReference", parentRef);

    String result = invokeBuildItemPath(createProvider(), item);
    assertEquals("folder/data.csv", result);
  }

  @Test void testBuildItemPathWithoutPathInParentRef() throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    ObjectNode item = mapper.createObjectNode();
    item.put("name", "orphan.txt");

    ObjectNode parentRef = mapper.createObjectNode();
    parentRef.put("id", "some-id");
    item.set("parentReference", parentRef);

    String result = invokeBuildItemPath(createProvider(), item);
    assertEquals("orphan.txt", result);
  }

  // ---------------------------------------------------------------
  // readAllBytes tests (via reflection)
  // ---------------------------------------------------------------

  @Test void testReadAllBytesEmpty() throws Exception {
    ByteArrayInputStream emptyStream = new ByteArrayInputStream(new byte[0]);
    byte[] result = invokeReadAllBytes(createProvider(), emptyStream);
    assertEquals(0, result.length);
  }

  @Test void testReadAllBytesSmallContent() throws Exception {
    byte[] content = "Hello, World!".getBytes(StandardCharsets.UTF_8);
    ByteArrayInputStream stream = new ByteArrayInputStream(content);
    byte[] result = invokeReadAllBytes(createProvider(), stream);
    assertArrayEquals(content, result);
  }

  @Test void testReadAllBytesLargeContent() throws Exception {
    // Create content larger than the internal buffer size (8192)
    byte[] content = new byte[20000];
    for (int i = 0; i < content.length; i++) {
      content[i] = (byte) (i % 256);
    }
    ByteArrayInputStream stream = new ByteArrayInputStream(content);
    byte[] result = invokeReadAllBytes(createProvider(), stream);
    assertArrayEquals(content, result);
  }

  // ---------------------------------------------------------------
  // Internal ID fields initial state tests
  // ---------------------------------------------------------------

  @Test void testSiteIdInitiallyNull() throws Exception {
    MicrosoftGraphStorageProvider provider = createProvider();
    Field siteIdField = MicrosoftGraphStorageProvider.class.getDeclaredField("siteId");
    siteIdField.setAccessible(true);
    assertNull(siteIdField.get(provider));
  }

  @Test void testDriveIdInitiallyNull() throws Exception {
    MicrosoftGraphStorageProvider provider = createProvider();
    Field driveIdField = MicrosoftGraphStorageProvider.class.getDeclaredField("driveId");
    driveIdField.setAccessible(true);
    assertNull(driveIdField.get(provider));
  }

  // ---------------------------------------------------------------
  // Helper methods
  // ---------------------------------------------------------------

  private MicrosoftGraphStorageProvider createProvider() {
    SharePointTokenManager tokenManager = new SharePointTokenManager("token", SITE_URL);
    return new MicrosoftGraphStorageProvider(tokenManager);
  }

  private boolean invokeIsLikelyDirectory(MicrosoftGraphStorageProvider provider, String path)
      throws Exception {
    Method method = MicrosoftGraphStorageProvider.class.getDeclaredMethod(
        "isLikelyDirectory", String.class);
    method.setAccessible(true);
    return (Boolean) method.invoke(provider, path);
  }

  private String invokeRemoveDocumentLibraryPrefix(MicrosoftGraphStorageProvider provider,
      String path) throws Exception {
    Method method = MicrosoftGraphStorageProvider.class.getDeclaredMethod(
        "removeDocumentLibraryPrefix", String.class);
    method.setAccessible(true);
    return (String) method.invoke(provider, path);
  }

  private boolean invokeIsDocumentLibraryRoot(MicrosoftGraphStorageProvider provider,
      String path) throws Exception {
    Method method = MicrosoftGraphStorageProvider.class.getDeclaredMethod(
        "isDocumentLibraryRoot", String.class);
    method.setAccessible(true);
    return (Boolean) method.invoke(provider, path);
  }

  private long invokeParseDateTime(MicrosoftGraphStorageProvider provider, String dateTime)
      throws Exception {
    Method method = MicrosoftGraphStorageProvider.class.getDeclaredMethod(
        "parseDateTime", String.class);
    method.setAccessible(true);
    return (Long) method.invoke(provider, dateTime);
  }

  private String invokeBuildItemPath(MicrosoftGraphStorageProvider provider, JsonNode item)
      throws Exception {
    Method method = MicrosoftGraphStorageProvider.class.getDeclaredMethod(
        "buildItemPath", JsonNode.class);
    method.setAccessible(true);
    return (String) method.invoke(provider, item);
  }

  private byte[] invokeReadAllBytes(MicrosoftGraphStorageProvider provider,
      InputStream inputStream) throws Exception {
    Method method = MicrosoftGraphStorageProvider.class.getDeclaredMethod(
        "readAllBytes", InputStream.class);
    method.setAccessible(true);
    return (byte[]) method.invoke(provider, inputStream);
  }
}
