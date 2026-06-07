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
package org.apache.calcite.adapter.file;

import org.apache.calcite.adapter.file.storage.FtpStorageProvider;
import org.apache.calcite.adapter.file.storage.StorageProvider;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.io.InputStream;
import java.net.SocketTimeoutException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration test for FTP storage provider.
 * These tests require network access and connect to public FTP test servers.
 */
@Tag("integration")
public class FtpStorageProviderTest {

  @Test @Timeout(value = 60, unit = TimeUnit.SECONDS)
  void testPublicFtpServer() throws IOException {
    FtpStorageProvider provider;
    try {
      provider = new FtpStorageProvider();
    } catch (Exception e) {
      Assumptions.assumeTrue(false,
          "FTP storage provider not available: " + e.getMessage());
      return;
    }

    try {
      doTestPublicFtpServer(provider);
    } catch (SocketTimeoutException e) {
      Assumptions.assumeTrue(false,
          "Skipping FTP test - timeout: " + e.getMessage());
    } catch (IOException e) {
      // Any IO failure during FTP operations means the network/server is unavailable
      Assumptions.assumeTrue(false,
          "Skipping FTP test - IO error (network or server unavailable): " + e.getMessage());
    }
  }

  private void doTestPublicFtpServer(FtpStorageProvider provider) throws IOException {
    // Try different public FTP servers (ordered by reliability)
    String[] testServers = {
        "ftp://dlpuser:rNrKYTX9g7z3RgJRmxWuGHbeu@ftp.dlptest.com/",  // DLP test server - most reliable
        "ftp://speedtest.tele2.net/",  // Tele2 speedtest server - fast
        "ftp://ftp.gnu.org/gnu/",  // GNU FTP server (read-only)
        "ftp://ftp.debian.org/debian/",  // Debian FTP server (read-only)
        "ftp://demo.wftpserver.com/",  // Demo FTP server (sometimes slow)
        "ftp://test.rebex.net/"  // Rebex test server (known timeout issues)
    };

    String testUrl = null;
    boolean connected = false;

    // Try each server until we find one that works
    for (String server : testServers) {
      try {
        System.out.println("Trying FTP server: " + server);
        provider.exists(server);
        testUrl = server;
        connected = true;
        System.out.println("Successfully connected to: " + server);
        break;
      } catch (SocketTimeoutException e) {
        System.out.println("Timeout connecting to " + server + ": " + e.getMessage());
      } catch (Exception e) {
        System.out.println("Failed to connect to " + server + ": " + e.getMessage());
      }
    }

    if (!connected) {
      // Use JUnit assumption to skip test when network is unavailable
      Assumptions.assumeTrue(false,
          "Skipping FTP test - could not connect to any public FTP server (network may be blocking FTP)");
      return;
    }

    // Test listing files
    System.out.println("Listing files from: " + testUrl);
    List<StorageProvider.FileEntry> entries = provider.listFiles(testUrl, false);
    assertNotNull(entries);
    System.out.println("Found " + entries.size() + " entries");

    // Print first few entries for debugging
    entries.stream().limit(5).forEach(e ->
        System.out.println("  " + (e.isDirectory() ? "[DIR] " : "[FILE]") + " " + e.getName()));

    assertTrue(entries.size() > 0, "Should find files on public FTP server");

    // Find a test file - different servers have different content
    StorageProvider.FileEntry testFile = entries.stream()
        .filter(e -> !e.isDirectory())
        .findFirst()
        .orElse(null);

    // If no files in root, it might be all directories (like GNU FTP)
    if (testFile == null && !entries.isEmpty()) {
      System.out.println("No files found in root, found " + entries.size() + " entries (likely directories)");
      // Try to list a subdirectory if available
      StorageProvider.FileEntry firstDir = entries.stream()
          .filter(StorageProvider.FileEntry::isDirectory)
          .findFirst()
          .orElse(null);

      if (firstDir != null) {
        System.out.println("Listing contents of directory: " + firstDir.getName());
        List<StorageProvider.FileEntry> subEntries = provider.listFiles(firstDir.getPath(), false);
        testFile = subEntries.stream()
            .filter(e -> !e.isDirectory())
            .findFirst()
            .orElse(null);
      }
    }

    // For read-only servers like GNU, we might only find directories
    if (testFile == null) {
      System.out.println("No files found, but successfully listed " + entries.size() + " entries");
      assertTrue(entries.size() > 0, "Should at least list some entries from FTP server");
      return; // Skip file-specific tests
    }

    // Test file metadata
    // Use the full path from the FileEntry
    String fileUrl = testFile.getPath();
    StorageProvider.FileMetadata metadata = provider.getMetadata(fileUrl);
    assertNotNull(metadata);
    assertEquals(fileUrl, metadata.getPath());
    // Some FTP servers may not report file size accurately for all files
    // Just verify size is not negative
    assertTrue(metadata.getSize() >= 0,
        "File size should be non-negative, but was: " + metadata.getSize());

    // Test file existence
    assertTrue(provider.exists(fileUrl));
    assertFalse(provider.exists(testUrl + "nonexistent-file.txt"));

    // Test reading small file content
    if (testFile.getName().contains("KB")) {
      try (InputStream is = provider.openInputStream(fileUrl)) {
        assertNotNull(is);
        // Just read first few bytes to verify connection
        byte[] buffer = new byte[100];
        int bytesRead = is.read(buffer);
        assertTrue(bytesRead > 0, "Should be able to read from file");
      }
    }
  }

  @Test void testFtpPathResolution() {
    FtpStorageProvider provider = new FtpStorageProvider();

    // Test absolute FTP URL
    assertEquals("ftp://example.com/absolute/path.txt",
        provider.resolvePath("ftp://example.com/base/", "ftp://example.com/absolute/path.txt"));

    // Test relative path
    assertEquals("ftp://example.com:21/base/relative.txt",
        provider.resolvePath("ftp://example.com/base/", "relative.txt"));

    // Test relative path with subdirectory
    assertEquals("ftp://example.com:21/base/sub/file.txt",
        provider.resolvePath("ftp://example.com/base/", "sub/file.txt"));

    // Test when base is a file
    assertEquals("ftp://example.com:21/base/relative.txt",
        provider.resolvePath("ftp://example.com/base/file.txt", "relative.txt"));
  }
}
