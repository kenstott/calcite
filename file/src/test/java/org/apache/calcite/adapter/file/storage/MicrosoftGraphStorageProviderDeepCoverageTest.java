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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Deep coverage tests for MicrosoftGraphStorageProvider.
 * Focuses on pure logic methods that can be tested without HTTP connections.
 */
@Tag("unit")
public class MicrosoftGraphStorageProviderDeepCoverageTest {

  private MicrosoftGraphStorageProvider provider;
  private final ObjectMapper mapper = new ObjectMapper();

  @BeforeEach
  void setUp() {
    // Use static token constructor to avoid HTTP calls
    SharePointTokenManager tokenManager =
        new SharePointTokenManager("test-token", "https://example.sharepoint.com/sites/test");
    provider = new MicrosoftGraphStorageProvider(tokenManager);
  }

  // --- Constructor tests ---

  @Test
  void testConstructorWithTrailingSiteUrl() {
    SharePointTokenManager tokenManager =
        new SharePointTokenManager("test-token", "https://example.sharepoint.com/sites/test/");
    MicrosoftGraphStorageProvider p = new MicrosoftGraphStorageProvider(tokenManager);
    assertEquals("microsoft-graph", p.getStorageType());
  }

  @Test
  void testConstructorWithClientSecretTokenManager() {
    SharePointTokenManager tokenManager =
        new SharePointTokenManager("tenant", "client", "secret",
            "https://example.sharepoint.com/sites/test");
    MicrosoftGraphStorageProvider p = new MicrosoftGraphStorageProvider(tokenManager);
    assertNotNull(p);
    assertEquals("microsoft-graph", p.getStorageType());
  }

  @Test
  void testConstructorWithGraphTokenManager() {
    MicrosoftGraphTokenManager graphTokenManager =
        new MicrosoftGraphTokenManager("test-token",
            "https://example.sharepoint.com/sites/test");
    MicrosoftGraphStorageProvider p = new MicrosoftGraphStorageProvider(graphTokenManager);
    assertNotNull(p);
    assertEquals("microsoft-graph", p.getStorageType());
  }

  @Test
  void testConstructorWithNoClientSecret() {
    SharePointTokenManager tokenManager =
        new SharePointTokenManager("tenant", "client", "refreshToken",
            "https://example.sharepoint.com/sites/test", true);
    MicrosoftGraphStorageProvider p = new MicrosoftGraphStorageProvider(tokenManager);
    assertNotNull(p);
  }

  // --- getStorageType ---

  @Test
  void testGetStorageType() {
    assertEquals("microsoft-graph", provider.getStorageType());
  }

  // --- resolvePath ---

  @Test
  void testResolvePathWithAbsoluteHttps() {
    assertEquals("https://example.com/file.txt",
        provider.resolvePath("/base/dir", "https://example.com/file.txt"));
  }

  @Test
  void testResolvePathWithAbsoluteHttp() {
    assertEquals("http://example.com/file.txt",
        provider.resolvePath("/base/dir", "http://example.com/file.txt"));
  }

  @Test
  void testResolvePathWithDirectoryBase() {
    assertEquals("/base/dir/file.txt",
        provider.resolvePath("/base/dir/", "file.txt"));
  }

  @Test
  void testResolvePathWithFileBase() {
    assertEquals("/base/dir/other.txt",
        provider.resolvePath("/base/dir/file.csv", "other.txt"));
  }

  @Test
  void testResolvePathWithDirectoryLikePath() {
    assertEquals("/base/dir/file.txt",
        provider.resolvePath("/base/dir", "file.txt"));
  }

  @Test
  void testResolvePathWithFileBaseNoSlash() {
    assertEquals("basedir/file.txt",
        provider.resolvePath("basedir", "file.txt"));
  }

  // --- isLikelyDirectory via reflection ---

  @Test
  void testIsLikelyDirectory() throws Exception {
    Method isLikelyDir =
        MicrosoftGraphStorageProvider.class.getDeclaredMethod(
            "isLikelyDirectory", String.class);
    isLikelyDir.setAccessible(true);

    assertTrue((Boolean) isLikelyDir.invoke(provider, "documents"));
    assertTrue((Boolean) isLikelyDir.invoke(provider, "/path/to/folder"));
    assertFalse((Boolean) isLikelyDir.invoke(provider, "file.txt"));
    assertFalse((Boolean) isLikelyDir.invoke(provider, "/path/to/file.csv"));
  }

  // --- parseDateTime via reflection ---

  @Test
  void testParseDateTimeIso8601() throws Exception {
    Method parseDateTimeMethod =
        MicrosoftGraphStorageProvider.class.getDeclaredMethod("parseDateTime", String.class);
    parseDateTimeMethod.setAccessible(true);

    long result = (Long) parseDateTimeMethod.invoke(provider, "2023-06-15T14:30:00Z");
    assertTrue(result > 0);
  }

  @Test
  void testParseDateTimeInvalidFormat() throws Exception {
    Method parseDateTimeMethod =
        MicrosoftGraphStorageProvider.class.getDeclaredMethod("parseDateTime", String.class);
    parseDateTimeMethod.setAccessible(true);

    long before = System.currentTimeMillis();
    long result = (Long) parseDateTimeMethod.invoke(provider, "not-a-date");
    long after = System.currentTimeMillis();
    assertTrue(result >= before && result <= after);
  }

  // --- isDocumentLibraryRoot via reflection ---

  @Test
  void testIsDocumentLibraryRoot() throws Exception {
    Method isDocLibRoot =
        MicrosoftGraphStorageProvider.class.getDeclaredMethod(
            "isDocumentLibraryRoot", String.class);
    isDocLibRoot.setAccessible(true);

    assertTrue((Boolean) isDocLibRoot.invoke(provider, "Shared Documents"));
    assertTrue((Boolean) isDocLibRoot.invoke(provider, "Documents"));
    assertFalse((Boolean) isDocLibRoot.invoke(provider, "random-folder"));
    assertFalse((Boolean) isDocLibRoot.invoke(provider, ""));
  }

  @Test
  void testIsDocumentLibraryRootInternational() throws Exception {
    Method isDocLibRoot =
        MicrosoftGraphStorageProvider.class.getDeclaredMethod(
            "isDocumentLibraryRoot", String.class);
    isDocLibRoot.setAccessible(true);

    assertTrue((Boolean) isDocLibRoot.invoke(provider, "Documents Partagés"));
    assertTrue((Boolean) isDocLibRoot.invoke(provider, "Documentos compartidos"));
    assertTrue((Boolean) isDocLibRoot.invoke(provider, "Gemeinsame Dokumente"));
    assertTrue((Boolean) isDocLibRoot.invoke(provider, "Documenti condivisi"));
  }

  // --- removeDocumentLibraryPrefix via reflection ---

  @Test
  void testRemoveDocumentLibraryPrefix() throws Exception {
    Method removePrefix =
        MicrosoftGraphStorageProvider.class.getDeclaredMethod(
            "removeDocumentLibraryPrefix", String.class);
    removePrefix.setAccessible(true);

    assertEquals("Shared Documents/subfolder/file.txt",
        removePrefix.invoke(provider, "Shared Documents/Shared Documents/subfolder/file.txt"));

    assertEquals("subfolder/file.txt",
        removePrefix.invoke(provider, "Shared Documents/subfolder/file.txt"));

    assertEquals("", removePrefix.invoke(provider, "Shared Documents"));

    assertEquals("", removePrefix.invoke(provider, "Documents"));

    assertEquals("subfolder/file.txt",
        removePrefix.invoke(provider, "Documents/subfolder/file.txt"));

    assertEquals("regular/path/file.txt",
        removePrefix.invoke(provider, "regular/path/file.txt"));
  }

  // --- buildItemPath via reflection ---

  @Test
  void testBuildItemPathWithParentReference() throws Exception {
    Method buildItemPath =
        MicrosoftGraphStorageProvider.class.getDeclaredMethod(
            "buildItemPath", JsonNode.class);
    buildItemPath.setAccessible(true);

    ObjectNode item = mapper.createObjectNode();
    item.put("name", "file.txt");
    ObjectNode parentRef = mapper.createObjectNode();
    parentRef.put("path", "/drives/abc123/root:/subfolder");
    item.set("parentReference", parentRef);

    String result = (String) buildItemPath.invoke(provider, item);
    assertEquals("subfolder/file.txt", result);
  }

  @Test
  void testBuildItemPathWithEmptyParentPath() throws Exception {
    Method buildItemPath =
        MicrosoftGraphStorageProvider.class.getDeclaredMethod(
            "buildItemPath", JsonNode.class);
    buildItemPath.setAccessible(true);

    ObjectNode item = mapper.createObjectNode();
    item.put("name", "root-file.txt");
    ObjectNode parentRef = mapper.createObjectNode();
    parentRef.put("path", "/drives/abc123/root:");
    item.set("parentReference", parentRef);

    String result = (String) buildItemPath.invoke(provider, item);
    assertEquals("root-file.txt", result);
  }

  @Test
  void testBuildItemPathWithNoParentReference() throws Exception {
    Method buildItemPath =
        MicrosoftGraphStorageProvider.class.getDeclaredMethod(
            "buildItemPath", JsonNode.class);
    buildItemPath.setAccessible(true);

    ObjectNode item = mapper.createObjectNode();
    item.put("name", "standalone-file.txt");

    String result = (String) buildItemPath.invoke(provider, item);
    assertEquals("standalone-file.txt", result);
  }

  @Test
  void testBuildItemPathWithNoPathInParentReference() throws Exception {
    Method buildItemPath =
        MicrosoftGraphStorageProvider.class.getDeclaredMethod(
            "buildItemPath", JsonNode.class);
    buildItemPath.setAccessible(true);

    ObjectNode item = mapper.createObjectNode();
    item.put("name", "file.txt");
    ObjectNode parentRef = mapper.createObjectNode();
    parentRef.put("id", "some-id-only");
    item.set("parentReference", parentRef);

    String result = (String) buildItemPath.invoke(provider, item);
    assertEquals("file.txt", result);
  }

  // --- readAllBytes via reflection ---

  @Test
  void testReadAllBytes() throws Exception {
    Method readAllBytesMethod =
        MicrosoftGraphStorageProvider.class.getDeclaredMethod(
            "readAllBytes", java.io.InputStream.class);
    readAllBytesMethod.setAccessible(true);

    byte[] testData = "test data for reading".getBytes();
    java.io.InputStream is = new java.io.ByteArrayInputStream(testData);

    byte[] result = (byte[]) readAllBytesMethod.invoke(provider, is);
    assertEquals(testData.length, result.length);
    assertEquals("test data for reading", new String(result));
  }

  @Test
  void testReadAllBytesEmpty() throws Exception {
    Method readAllBytesMethod =
        MicrosoftGraphStorageProvider.class.getDeclaredMethod(
            "readAllBytes", java.io.InputStream.class);
    readAllBytesMethod.setAccessible(true);

    java.io.InputStream is = new java.io.ByteArrayInputStream(new byte[0]);

    byte[] result = (byte[]) readAllBytesMethod.invoke(provider, is);
    assertEquals(0, result.length);
  }
}
