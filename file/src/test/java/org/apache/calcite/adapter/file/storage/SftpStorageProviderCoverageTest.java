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
 * Unit tests for {@link SftpStorageProvider} covering constructor logic,
 * URI parsing, path resolution, storage type, and error handling
 * without requiring a live SFTP server.
 */
@Tag("unit")
public class SftpStorageProviderCoverageTest {

  private SftpStorageProvider provider;

  @BeforeEach
  void setUp() {
    provider = new SftpStorageProvider();
  }

  // --- Constructor tests ---

  @Test
  void testDefaultConstructor() {
    SftpStorageProvider p = new SftpStorageProvider();
    assertEquals("sftp", p.getStorageType());
  }

  @Test
  void testParameterizedConstructor() {
    SftpStorageProvider p = new SftpStorageProvider("testuser", "testpass",
        "/path/to/key", true);
    assertEquals("sftp", p.getStorageType());
  }

  @Test
  void testParameterizedConstructorWithNulls() {
    // null username falls back to system property; null privateKeyPath falls back to default
    SftpStorageProvider p = new SftpStorageProvider(null, null, null, false);
    assertEquals("sftp", p.getStorageType());
  }

  @Test
  void testParameterizedConstructorWithUsername() {
    SftpStorageProvider p = new SftpStorageProvider("myuser", "mypass",
        "/home/myuser/.ssh/id_rsa", false);
    assertEquals("sftp", p.getStorageType());
  }

  // --- getStorageType ---

  @Test
  void testGetStorageType() {
    assertEquals("sftp", provider.getStorageType());
  }

  // --- resolvePath tests ---

  @Test
  void testResolvePathWithAbsoluteSftpUrl() {
    String result = provider.resolvePath(
        "sftp://user@host.com/base/", "sftp://other@host2.com/file.txt");
    assertEquals("sftp://other@host2.com/file.txt", result);
  }

  @Test
  void testResolvePathWithRelativePath() {
    String result = provider.resolvePath(
        "sftp://user@host.com/base/", "file.txt");
    assertEquals("sftp://user@host.com/base/file.txt", result);
  }

  @Test
  void testResolvePathWithCustomPort() {
    String result = provider.resolvePath(
        "sftp://user@host.com:2222/base/", "file.txt");
    assertEquals("sftp://user@host.com:2222/base/file.txt", result);
  }

  @Test
  void testResolvePathWithFileBase() {
    // Base is a file path, should resolve relative to parent dir
    String result = provider.resolvePath(
        "sftp://user@host.com/path/to/file.csv", "other.csv");
    assertEquals("sftp://user@host.com/path/to/other.csv", result);
  }

  @Test
  void testResolvePathWithDeepPath() {
    String result = provider.resolvePath(
        "sftp://user@host.com/a/b/c/d/", "file.txt");
    assertEquals("sftp://user@host.com/a/b/c/d/file.txt", result);
  }

  @Test
  void testResolvePathFallbackOnInvalidUri() {
    // When URI parsing fails, it falls back to simple string concatenation
    String result = provider.resolvePath("invalid-uri", "file.txt");
    assertEquals("invalid-uri/file.txt", result);
  }

  @Test
  void testResolvePathFallbackWithTrailingSlash() {
    String result = provider.resolvePath("not-sftp/", "file.txt");
    assertEquals("not-sftp/file.txt", result);
  }

  @Test
  void testResolvePathWithSubdirectory() {
    String result = provider.resolvePath(
        "sftp://user@host.com/base/", "sub/dir/file.txt");
    assertEquals("sftp://user@host.com/base/sub/dir/file.txt", result);
  }

  @Test
  void testResolvePathWithDefaultPort() {
    // Port 22 (default) should not appear in the resolved URL
    String result = provider.resolvePath(
        "sftp://user@host.com/base/", "file.txt");
    assertEquals("sftp://user@host.com/base/file.txt", result);
  }

  @Test
  void testResolvePathWithNonDefaultPort() {
    // Non-default port should appear
    String result = provider.resolvePath(
        "sftp://user@host.com:3333/base/", "file.txt");
    assertEquals("sftp://user@host.com:3333/base/file.txt", result);
  }

  // --- URI parsing error handling ---

  @Test
  void testListFilesInvalidUri() {
    assertThrows(IOException.class, () -> provider.listFiles("not-sftp://host/path", false));
  }

  @Test
  void testListFilesNonSftpUri() {
    assertThrows(IOException.class, () -> provider.listFiles("/local/path", false));
  }

  @Test
  void testGetMetadataInvalidUri() {
    assertThrows(IOException.class, () -> provider.getMetadata("not-sftp://host/file.csv"));
  }

  @Test
  void testOpenInputStreamInvalidUri() {
    assertThrows(IOException.class, () -> provider.openInputStream("ftp://host/file.csv"));
  }

  @Test
  void testExistsInvalidUri() {
    assertThrows(IOException.class, () -> provider.exists("http://host/file.csv"));
  }

  @Test
  void testIsDirectoryInvalidUri() {
    assertThrows(IOException.class, () -> provider.isDirectory("garbage"));
  }

  @Test
  void testOpenReaderInvalidUri() {
    assertThrows(IOException.class, () -> provider.openReader("not-sftp://host/path"));
  }

  // --- Connection error handling (valid URI, no server) ---

  @Test
  void testListFilesConnectionError() {
    // Valid SFTP URI but no real server
    assertThrows(IOException.class,
        () -> provider.listFiles("sftp://user@nonexistent-host-12345.invalid/path/", false));
  }

  @Test
  void testGetMetadataConnectionError() {
    assertThrows(IOException.class,
        () -> provider.getMetadata("sftp://user@nonexistent-host-12345.invalid/file.csv"));
  }

  @Test
  void testExistsConnectionError() {
    assertThrows(IOException.class,
        () -> provider.exists("sftp://user@nonexistent-host-12345.invalid/file.csv"));
  }

  @Test
  void testIsDirectoryConnectionError() {
    assertThrows(IOException.class,
        () -> provider.isDirectory("sftp://user@nonexistent-host-12345.invalid/path"));
  }

  @Test
  void testOpenInputStreamConnectionError() {
    assertThrows(IOException.class,
        () -> provider.openInputStream("sftp://user@nonexistent-host-12345.invalid/file.csv"));
  }

  // --- URI parsing edge cases ---

  @Test
  void testUriWithPasswordInUrl() {
    // URL with user:pass format
    String result = provider.resolvePath(
        "sftp://user:pass@host.com/data/", "file.csv");
    assertEquals("sftp://user@host.com/data/file.csv", result);
  }

  @Test
  void testUriWithOnlyUsername() {
    String result = provider.resolvePath(
        "sftp://justuser@host.com/data/", "file.csv");
    assertEquals("sftp://justuser@host.com/data/file.csv", result);
  }

  @Test
  void testResolvePathWithRootBasePath() {
    String result = provider.resolvePath(
        "sftp://user@host.com/", "file.csv");
    assertEquals("sftp://user@host.com/file.csv", result);
  }
}
