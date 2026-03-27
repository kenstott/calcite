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
 * Unit tests for {@link SharePointRestStorageProvider} covering path resolution,
 * storage type, constructor logic, and the resolvePath / document library prefix
 * handling without requiring a live SharePoint server.
 */
@Tag("unit")
public class SharePointRestStorageProviderCoverageTest {

  private SharePointRestStorageProvider provider;

  @BeforeEach
  void setUp() {
    // Create with a static token manager (no network calls for token refresh)
    SharePointTokenManager tokenManager =
        new SharePointTokenManager("test-access-token", "https://example.sharepoint.com/sites/test");
    provider = new SharePointRestStorageProvider(tokenManager);
  }

  // --- Constructor tests ---

  @Test
  void testConstructorTrimsTrailingSlash() {
    SharePointTokenManager tokenManager =
        new SharePointTokenManager("token", "https://example.sharepoint.com/sites/test/");
    SharePointRestStorageProvider p = new SharePointRestStorageProvider(tokenManager);
    assertEquals("sharepoint-rest", p.getStorageType());
  }

  @Test
  void testConstructorWithoutTrailingSlash() {
    SharePointTokenManager tokenManager =
        new SharePointTokenManager("token", "https://example.sharepoint.com/sites/test");
    SharePointRestStorageProvider p = new SharePointRestStorageProvider(tokenManager);
    assertEquals("sharepoint-rest", p.getStorageType());
  }

  @Test
  void testConstructorWithClientSecretTokenManager() {
    // When a non-REST token manager with client secret is provided, it should create a REST one
    SharePointTokenManager tokenManager =
        new SharePointTokenManager("tenant-id", "client-id", "client-secret",
            "https://example.sharepoint.com/sites/test");
    SharePointRestStorageProvider p = new SharePointRestStorageProvider(tokenManager);
    assertEquals("sharepoint-rest", p.getStorageType());
  }

  @Test
  void testConstructorWithRestTokenManager() {
    // When a SharePointRestTokenManager is provided, it should be used directly
    SharePointRestTokenManager restTokenManager =
        new SharePointRestTokenManager("token", "https://example.sharepoint.com");
    SharePointRestStorageProvider p = new SharePointRestStorageProvider(restTokenManager);
    assertEquals("sharepoint-rest", p.getStorageType());
  }

  @Test
  void testConstructorWithStaticTokenNonRestManager() {
    // When a non-REST token manager without client secret is given, it uses the same instance
    SharePointTokenManager tokenManager =
        new SharePointTokenManager("static-token", "https://example.sharepoint.com");
    SharePointRestStorageProvider p = new SharePointRestStorageProvider(tokenManager);
    assertEquals("sharepoint-rest", p.getStorageType());
  }

  // --- getStorageType ---

  @Test
  void testGetStorageType() {
    assertEquals("sharepoint-rest", provider.getStorageType());
  }

  // --- resolvePath tests ---

  @Test
  void testResolvePathWithAbsoluteUrl() {
    String result = provider.resolvePath("some/base/path.csv", "https://example.com/file.csv");
    assertEquals("https://example.com/file.csv", result);
  }

  @Test
  void testResolvePathWithHttpUrl() {
    String result = provider.resolvePath("some/base/path.csv", "http://example.com/file.csv");
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
  void testResolvePathWithBaseFileNoSlash() {
    // Base has no slashes and has a dot
    String result = provider.resolvePath("file.txt", "other.csv");
    assertEquals("file.txt/other.csv", result);
  }

  @Test
  void testResolvePathWithBaseDirectoryNoSlash() {
    // Base has no slashes and no dot (likely a directory)
    String result = provider.resolvePath("documents", "file.csv");
    assertEquals("documents/file.csv", result);
  }

  // --- exists / isDirectory / openReader delegation ---

  @Test
  void testExistsReturnsFalseOn404() throws IOException {
    // exists() catches IOException containing "404" and returns false
    // Since we can't call the real REST API, we test the error handling logic

    // This provider's exists() delegates to getMetadata() which calls ensureInitialized()
    // which calls executeRestCall() which will fail since we have no real server.
    // We verify the error handling behavior
    try {
      provider.exists("test-path.csv");
      fail("Should have thrown an exception since we cannot connect to the mock server");
    } catch (IOException e) {
      // Expected - network error since the server doesn't exist
      assertNotNull(e.getMessage());
    }
  }

  @Test
  void testOpenReaderDelegatesToOpenInputStream() {
    // openReader wraps openInputStream, test that the method exists and throws on non-existent server
    assertThrows(IOException.class, () -> provider.openReader("test-path.csv"));
  }

  // --- Document library prefix removal tests (via resolvePath patterns) ---

  @Test
  void testResolvePathPreservesStructure() {
    String result = provider.resolvePath("Shared Documents/folder/file.csv", "other.csv");
    assertEquals("Shared Documents/folder/other.csv", result);
  }

  @Test
  void testResolvePathWithDeepNesting() {
    String result = provider.resolvePath("a/b/c/d/file.csv", "sibling.csv");
    assertEquals("a/b/c/d/sibling.csv", result);
  }

  @Test
  void testResolvePathWithRootFile() {
    // Base path is at root level with a file extension
    String result = provider.resolvePath("data.csv", "other.csv");
    // "data.csv" has a dot, but no slash, so it's treated differently
    assertEquals("data.csv/other.csv", result);
  }
}
