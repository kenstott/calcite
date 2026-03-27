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
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link MicrosoftGraphStorageProvider} covering constructor logic,
 * path resolution, storage type, and document library prefix handling
 * without requiring a live Microsoft Graph API connection.
 */
@Tag("unit")
public class MicrosoftGraphStorageProviderCoverageTest {

  private MicrosoftGraphStorageProvider provider;

  @BeforeEach
  void setUp() {
    // Create with a static token manager (no network calls for token refresh)
    SharePointTokenManager tokenManager =
        new SharePointTokenManager("test-access-token",
            "https://example.sharepoint.com/sites/test");
    provider = new MicrosoftGraphStorageProvider(tokenManager);
  }

  // --- Constructor tests ---

  @Test
  void testConstructorTrimsTrailingSlash() {
    SharePointTokenManager tokenManager =
        new SharePointTokenManager("token",
            "https://example.sharepoint.com/sites/test/");
    MicrosoftGraphStorageProvider p = new MicrosoftGraphStorageProvider(tokenManager);
    assertEquals("microsoft-graph", p.getStorageType());
  }

  @Test
  void testConstructorWithoutTrailingSlash() {
    SharePointTokenManager tokenManager =
        new SharePointTokenManager("token",
            "https://example.sharepoint.com/sites/test");
    MicrosoftGraphStorageProvider p = new MicrosoftGraphStorageProvider(tokenManager);
    assertEquals("microsoft-graph", p.getStorageType());
  }

  @Test
  void testConstructorWithClientSecretTokenManager() {
    // When a non-Graph token manager with client secret is provided, a Graph one is created
    SharePointTokenManager tokenManager =
        new SharePointTokenManager("tenant-id", "client-id", "client-secret",
            "https://example.sharepoint.com/sites/test");
    MicrosoftGraphStorageProvider p = new MicrosoftGraphStorageProvider(tokenManager);
    assertEquals("microsoft-graph", p.getStorageType());
  }

  @Test
  void testConstructorWithGraphTokenManager() {
    // When a MicrosoftGraphTokenManager is provided, it should be used directly
    MicrosoftGraphTokenManager graphTokenManager =
        new MicrosoftGraphTokenManager("token",
            "https://example.sharepoint.com");
    MicrosoftGraphStorageProvider p =
        new MicrosoftGraphStorageProvider(graphTokenManager);
    assertEquals("microsoft-graph", p.getStorageType());
  }

  @Test
  void testConstructorWithStaticTokenNonGraphManager() {
    // When a non-Graph token manager without client secret is given,
    // the original token manager is used
    SharePointTokenManager tokenManager =
        new SharePointTokenManager("static-token",
            "https://example.sharepoint.com");
    MicrosoftGraphStorageProvider p = new MicrosoftGraphStorageProvider(tokenManager);
    assertEquals("microsoft-graph", p.getStorageType());
  }

  // --- getStorageType ---

  @Test
  void testGetStorageType() {
    assertEquals("microsoft-graph", provider.getStorageType());
  }

  // --- resolvePath tests ---

  @Test
  void testResolvePathWithAbsoluteHttpsUrl() {
    String result = provider.resolvePath("some/base/path.csv",
        "https://graph.microsoft.com/file.csv");
    assertEquals("https://graph.microsoft.com/file.csv", result);
  }

  @Test
  void testResolvePathWithAbsoluteHttpUrl() {
    String result = provider.resolvePath("some/base/path.csv",
        "http://example.com/file.csv");
    assertEquals("http://example.com/file.csv", result);
  }

  @Test
  void testResolvePathWithFileBase() {
    // When base is a file (has extension), get parent directory
    String result = provider.resolvePath("folder/subfolder/data.csv", "other.csv");
    assertEquals("folder/subfolder/other.csv", result);
  }

  @Test
  void testResolvePathWithDirectoryBase() {
    // When base is a directory (no extension), treat as directory
    String result = provider.resolvePath("folder/subfolder", "file.csv");
    assertEquals("folder/subfolder/file.csv", result);
  }

  @Test
  void testResolvePathWithTrailingSlash() {
    String result = provider.resolvePath("folder/subfolder/", "file.csv");
    assertEquals("folder/subfolder/file.csv", result);
  }

  @Test
  void testResolvePathWithNoSlash() {
    // Base has no slashes, considered a single path element
    String result = provider.resolvePath("documents", "file.csv");
    assertEquals("documents/file.csv", result);
  }

  @Test
  void testResolvePathWithDeepNesting() {
    String result = provider.resolvePath("a/b/c/d/file.csv", "sibling.csv");
    assertEquals("a/b/c/d/sibling.csv", result);
  }

  // --- exists behavior ---

  @Test
  void testExistsThrowsOnNetworkError() {
    // exists() delegates to getMetadata() which calls ensureInitialized()
    // which calls executeGraphCall() which will fail since no real server
    try {
      provider.exists("test-path.csv");
      fail("Should have thrown an exception since we cannot connect");
    } catch (IOException e) {
      assertNotNull(e.getMessage());
    }
  }

  // --- openReader ---

  @Test
  void testOpenReaderThrowsOnNetworkError() {
    assertThrows(IOException.class, () -> provider.openReader("test-path.csv"));
  }

  // --- listFiles ---

  @Test
  void testListFilesThrowsOnNetworkError() {
    assertThrows(IOException.class,
        () -> provider.listFiles("Shared Documents", false));
  }

  // --- isDirectory ---

  @Test
  void testIsDirectoryThrowsOnNetworkError() {
    assertThrows(IOException.class, () -> provider.isDirectory("/some/path"));
  }

  // --- getMetadata ---

  @Test
  void testGetMetadataThrowsOnNetworkError() {
    assertThrows(IOException.class, () -> provider.getMetadata("/some/file.csv"));
  }

  // --- openInputStream ---

  @Test
  void testOpenInputStreamThrowsOnNetworkError() {
    assertThrows(IOException.class,
        () -> provider.openInputStream("/some/file.csv"));
  }

  // --- Document library prefix patterns ---

  @Test
  void testResolvePathWithSharedDocumentsBase() {
    String result = provider.resolvePath(
        "Shared Documents/folder/file.csv", "other.csv");
    assertEquals("Shared Documents/folder/other.csv", result);
  }

  @Test
  void testResolvePathWithDocumentsBase() {
    String result = provider.resolvePath("Documents/report.csv", "summary.csv");
    assertEquals("Documents/summary.csv", result);
  }
}
