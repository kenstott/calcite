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
 * Unit tests for {@link FtpStorageProvider} covering constructor, URI parsing,
 * path resolution, storage type, and error handling without requiring a live
 * FTP server.
 *
 * <p>Complements the existing FtpStorageProviderMockTest by testing additional
 * error scenarios, edge cases, and connection failure paths.
 */
@Tag("unit")
public class FtpStorageProviderCoverageTest {

  private FtpStorageProvider provider;

  @BeforeEach
  void setUp() {
    provider = new FtpStorageProvider();
  }

  // --- Constructor tests ---

  @Test
  void testDefaultConstructor() {
    FtpStorageProvider p = new FtpStorageProvider();
    assertEquals("ftp", p.getStorageType());
  }

  // --- getStorageType ---

  @Test
  void testGetStorageType() {
    assertEquals("ftp", provider.getStorageType());
  }

  // --- resolvePath tests ---

  @Test
  void testResolvePathWithAbsoluteFtpUrl() {
    String result = provider.resolvePath(
        "ftp://example.com/base/", "ftp://other.com/file.csv");
    assertEquals("ftp://other.com/file.csv", result);
  }

  @Test
  void testResolvePathWithRelativePath() {
    String result = provider.resolvePath(
        "ftp://example.com/base/", "file.csv");
    assertEquals("ftp://example.com:21/base/file.csv", result);
  }

  @Test
  void testResolvePathWithCustomPort() {
    String result = provider.resolvePath(
        "ftp://example.com:2121/base/", "file.csv");
    assertEquals("ftp://example.com:2121/base/file.csv", result);
  }

  @Test
  void testResolvePathWithFileBase() {
    // Base is a file path, should resolve relative to parent dir
    String result = provider.resolvePath(
        "ftp://example.com/path/to/file.csv", "other.csv");
    assertEquals("ftp://example.com:21/path/to/other.csv", result);
  }

  @Test
  void testResolvePathWithSubdirectory() {
    String result = provider.resolvePath(
        "ftp://example.com/base/", "sub/dir/file.csv");
    assertEquals("ftp://example.com:21/base/sub/dir/file.csv", result);
  }

  @Test
  void testResolvePathFallbackOnInvalidUri() {
    // When URI parsing fails, it falls back to simple concatenation
    String result = provider.resolvePath("invalid-uri", "file.txt");
    assertEquals("invalid-uri/file.txt", result);
  }

  @Test
  void testResolvePathFallbackWithTrailingSlash() {
    String result = provider.resolvePath("not-ftp/", "file.txt");
    assertEquals("not-ftp/file.txt", result);
  }

  @Test
  void testResolvePathWithRootPath() {
    String result = provider.resolvePath(
        "ftp://example.com/", "file.csv");
    assertEquals("ftp://example.com:21/file.csv", result);
  }

  @Test
  void testResolvePathWithCredentials() {
    String result = provider.resolvePath(
        "ftp://user:pass@example.com/data/", "file.csv");
    assertEquals("ftp://example.com:21/data/file.csv", result);
  }

  // --- URI parsing error handling ---

  @Test
  void testListFilesInvalidUri() {
    assertThrows(IOException.class,
        () -> provider.listFiles("not-ftp://host/path", false));
  }

  @Test
  void testListFilesLocalPath() {
    assertThrows(IOException.class,
        () -> provider.listFiles("/local/path", false));
  }

  @Test
  void testGetMetadataInvalidUri() {
    assertThrows(IOException.class,
        () -> provider.getMetadata("sftp://host/file.csv"));
  }

  @Test
  void testOpenInputStreamInvalidUri() {
    assertThrows(IOException.class,
        () -> provider.openInputStream("http://host/file.csv"));
  }

  @Test
  void testExistsInvalidUri() {
    assertThrows(IOException.class,
        () -> provider.exists("sftp://host/file.csv"));
  }

  @Test
  void testIsDirectoryInvalidUri() {
    assertThrows(IOException.class,
        () -> provider.isDirectory("garbage-path"));
  }

  @Test
  void testOpenReaderInvalidUri() {
    assertThrows(IOException.class,
        () -> provider.openReader("not-ftp://host/path"));
  }

  // --- Connection error handling (valid URI, no server) ---

  @Test
  void testListFilesConnectionError() {
    // Valid FTP URI but hostname resolves to nothing real
    assertThrows(IOException.class,
        () -> provider.listFiles(
            "ftp://nonexistent-host-12345.invalid/path/", false));
  }

  @Test
  void testGetMetadataConnectionError() {
    assertThrows(IOException.class,
        () -> provider.getMetadata(
            "ftp://nonexistent-host-12345.invalid/file.csv"));
  }

  @Test
  void testExistsConnectionError() {
    assertThrows(IOException.class,
        () -> provider.exists(
            "ftp://nonexistent-host-12345.invalid/file.csv"));
  }

  @Test
  void testIsDirectoryConnectionError() {
    assertThrows(IOException.class,
        () -> provider.isDirectory(
            "ftp://nonexistent-host-12345.invalid/path"));
  }

  @Test
  void testOpenInputStreamConnectionError() {
    assertThrows(IOException.class,
        () -> provider.openInputStream(
            "ftp://nonexistent-host-12345.invalid/file.csv"));
  }

  // --- URI parsing edge cases ---

  @Test
  void testUriWithAnonymousCredentials() {
    // Default FTP credentials
    String result = provider.resolvePath(
        "ftp://example.com/data/", "file.csv");
    assertEquals("ftp://example.com:21/data/file.csv", result);
  }

  @Test
  void testUriWithExplicitCredentials() {
    String result = provider.resolvePath(
        "ftp://admin:secret@example.com:990/secure/", "file.csv");
    assertEquals("ftp://example.com:990/secure/file.csv", result);
  }

  @Test
  void testUriWithOnlyUsername() {
    String result = provider.resolvePath(
        "ftp://justuser@example.com/data/", "file.csv");
    assertEquals("ftp://example.com:21/data/file.csv", result);
  }

  // --- Default operations ---

  @Test
  void testWriteFileThrowsUnsupported() {
    assertThrows(UnsupportedOperationException.class,
        () -> provider.writeFile("/test.txt", new byte[0]));
  }

  @Test
  void testDeleteThrowsUnsupported() {
    assertThrows(UnsupportedOperationException.class,
        () -> provider.delete("/test.txt"));
  }

  @Test
  void testCreateDirectoriesThrowsUnsupported() {
    assertThrows(UnsupportedOperationException.class,
        () -> provider.createDirectories("/test/dir"));
  }

  @Test
  void testCopyFileThrowsUnsupported() {
    assertThrows(UnsupportedOperationException.class,
        () -> provider.copyFile("/source.txt", "/dest.txt"));
  }

  @Test
  void testReadRangeThrowsUnsupported() {
    assertThrows(UnsupportedOperationException.class,
        () -> provider.readRange("ftp://host/file.csv", 0, 100));
  }
}
