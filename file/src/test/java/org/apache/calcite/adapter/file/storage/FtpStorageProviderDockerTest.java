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

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Locale;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Docker-based integration tests for {@link FtpStorageProvider}.
 *
 * <p>These tests start a real FTP server via Docker ({@code delfer/alpine-ftp-server})
 * and exercise every public method of the FTP storage provider against it.
 *
 * <p>Prerequisites:
 * <ul>
 *   <li>Docker must be installed and running</li>
 *   <li>The {@code delfer/alpine-ftp-server:latest} image must be pulled</li>
 *   <li>Ports 2121 and 21000-21010 must be available</li>
 * </ul>
 */
@Tag("integration")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class FtpStorageProviderDockerTest {

  private static final String CONTAINER_NAME = "calcite-test-ftp";
  private static final String FTP_USER = "testuser";
  private static final String FTP_PASS = "testpass";
  private static final int FTP_PORT = 2121;
  private static final String BASE_URL =
      String.format(Locale.ROOT, "ftp://%s:%s@localhost:%d", FTP_USER, FTP_PASS, FTP_PORT);

  private static boolean containerStarted = false;

  @BeforeAll
  static void startContainer() throws Exception {
    // Check that Docker is available
    if (!isDockerAvailable()) {
      return;
    }

    // Remove any leftover container from a previous run
    runCommand("docker", "rm", "-f", CONTAINER_NAME);

    // Start the FTP container
    int exitCode = runCommand(
        "docker", "run", "-d",
        "--name", CONTAINER_NAME,
        "-p", FTP_PORT + ":21",
        "-p", "21000-21010:21000-21010",
        "-e", "USERS=" + FTP_USER + "|" + FTP_PASS,
        "-e", "ADDRESS=localhost",
        "-e", "MIN_PORT=21000",
        "-e", "MAX_PORT=21010",
        "delfer/alpine-ftp-server:latest");

    if (exitCode != 0) {
      return;
    }

    // Wait for the container to be healthy with retries
    containerStarted = waitForFtpReady(30);
    if (!containerStarted) {
      // Clean up if we could not get it ready
      runCommand("docker", "rm", "-f", CONTAINER_NAME);
      return;
    }

    // Upload test data files into the container
    uploadTestData();
  }

  @AfterAll
  static void stopContainer() {
    try {
      runCommand("docker", "rm", "-f", CONTAINER_NAME);
    } catch (Exception e) {
      // Ignore cleanup errors
    }
  }

  // ---------------------------------------------------------------------------
  // Test: getStorageType
  // ---------------------------------------------------------------------------

  @Test
  @Order(1)
  void testGetStorageType() {
    FtpStorageProvider provider = new FtpStorageProvider();
    assertEquals("ftp", provider.getStorageType());
  }

  // ---------------------------------------------------------------------------
  // Test: resolvePath (no Docker required)
  // ---------------------------------------------------------------------------

  @Test
  @Order(2)
  void testResolvePath() {
    FtpStorageProvider provider = new FtpStorageProvider();

    // Absolute URL is returned as-is
    assertEquals("ftp://other.com/absolute.txt",
        provider.resolvePath("ftp://example.com/base/", "ftp://other.com/absolute.txt"));

    // Relative path against a directory base
    assertEquals("ftp://example.com:21/base/file.csv",
        provider.resolvePath("ftp://example.com/base/", "file.csv"));

    // Relative path against a file base (parent directory is used)
    assertEquals("ftp://example.com:21/base/newfile.csv",
        provider.resolvePath("ftp://example.com/base/old.csv", "newfile.csv"));

    // With a custom port
    assertEquals("ftp://example.com:2121/data/report.csv",
        provider.resolvePath("ftp://example.com:2121/data/", "report.csv"));

    // Subdirectory relative path
    assertEquals("ftp://example.com:21/base/sub/dir/file.json",
        provider.resolvePath("ftp://example.com/base/", "sub/dir/file.json"));

    // Fallback path when base is not a valid FTP URI
    assertEquals("some/path/file.txt",
        provider.resolvePath("some/path/", "file.txt"));
  }

  // ---------------------------------------------------------------------------
  // Test: resolvePath fallback with invalid URI
  // ---------------------------------------------------------------------------

  @Test
  @Order(3)
  void testResolvePathFallbackNonFtpBase() {
    FtpStorageProvider provider = new FtpStorageProvider();

    // When base path is not an FTP URI, fallback concatenation is used
    assertEquals("/local/base/file.txt",
        provider.resolvePath("/local/base/", "file.txt"));
    assertEquals("/local/base/file.txt/other.txt",
        provider.resolvePath("/local/base/file.txt", "other.txt"));
  }

  // ---------------------------------------------------------------------------
  // Test: listFiles (non-recursive)
  // ---------------------------------------------------------------------------

  @Test
  @Order(10)
  void testListFilesNonRecursive() throws IOException {
    assumeContainerRunning();
    FtpStorageProvider provider = new FtpStorageProvider();

    List<StorageProvider.FileEntry> entries = provider.listFiles(BASE_URL + "/", false);
    assertNotNull(entries);
    assertTrue(entries.size() >= 3,
        "Expected at least 3 entries (test.csv, test.json, subdir), got " + entries.size());

    // Verify test.csv is present
    StorageProvider.FileEntry csvEntry = findEntry(entries, "test.csv");
    assertNotNull(csvEntry, "test.csv should be listed");
    assertFalse(csvEntry.isDirectory());
    assertTrue(csvEntry.getSize() > 0, "test.csv should have non-zero size");
    assertTrue(csvEntry.getLastModified() > 0, "Last modified should be > 0");
    assertTrue(csvEntry.getPath().startsWith("ftp://"), "Path should be a full FTP URI");

    // Verify test.json is present
    StorageProvider.FileEntry jsonEntry = findEntry(entries, "test.json");
    assertNotNull(jsonEntry, "test.json should be listed");
    assertFalse(jsonEntry.isDirectory());

    // Verify the subdirectory is present
    StorageProvider.FileEntry subdirEntry = findEntry(entries, "subdir");
    assertNotNull(subdirEntry, "subdir should be listed");
    assertTrue(subdirEntry.isDirectory());
  }

  // ---------------------------------------------------------------------------
  // Test: listFiles (recursive)
  // ---------------------------------------------------------------------------

  @Test
  @Order(11)
  void testListFilesRecursive() throws IOException {
    assumeContainerRunning();
    FtpStorageProvider provider = new FtpStorageProvider();

    List<StorageProvider.FileEntry> entries = provider.listFiles(BASE_URL + "/", true);
    assertNotNull(entries);

    // Should include files from subdirectory
    StorageProvider.FileEntry nestedFile = findEntry(entries, "nested.csv");
    assertNotNull(nestedFile,
        "nested.csv should be found in recursive listing");
    assertFalse(nestedFile.isDirectory());
    assertTrue(nestedFile.getPath().contains("subdir"),
        "Nested file path should contain subdir");
  }

  // ---------------------------------------------------------------------------
  // Test: exists
  // ---------------------------------------------------------------------------

  @Test
  @Order(20)
  void testExistsFile() throws IOException {
    assumeContainerRunning();
    FtpStorageProvider provider = new FtpStorageProvider();

    assertTrue(provider.exists(BASE_URL + "/test.csv"),
        "test.csv should exist");
    assertTrue(provider.exists(BASE_URL + "/test.json"),
        "test.json should exist");
  }

  @Test
  @Order(21)
  void testExistsDirectory() throws IOException {
    assumeContainerRunning();
    FtpStorageProvider provider = new FtpStorageProvider();

    assertTrue(provider.exists(BASE_URL + "/subdir/"),
        "subdir/ directory should exist");
  }

  @Test
  @Order(22)
  void testExistsNonExistentFile() throws IOException {
    assumeContainerRunning();
    FtpStorageProvider provider = new FtpStorageProvider();

    assertFalse(provider.exists(BASE_URL + "/no-such-file.txt"),
        "Non-existent file should return false");
  }

  // ---------------------------------------------------------------------------
  // Test: isDirectory
  // ---------------------------------------------------------------------------

  @Test
  @Order(25)
  void testIsDirectory() throws IOException {
    assumeContainerRunning();
    FtpStorageProvider provider = new FtpStorageProvider();

    assertTrue(provider.isDirectory(BASE_URL + "/subdir"),
        "subdir should be a directory");
    assertFalse(provider.isDirectory(BASE_URL + "/test.csv"),
        "test.csv should not be a directory");
  }

  // ---------------------------------------------------------------------------
  // Test: getMetadata
  // ---------------------------------------------------------------------------

  @Test
  @Order(30)
  void testGetMetadata() throws IOException {
    assumeContainerRunning();
    FtpStorageProvider provider = new FtpStorageProvider();

    String fileUrl = BASE_URL + "/test.csv";
    StorageProvider.FileMetadata metadata = provider.getMetadata(fileUrl);

    assertNotNull(metadata);
    assertEquals(fileUrl, metadata.getPath());
    assertTrue(metadata.getSize() > 0, "File size should be > 0");
    assertTrue(metadata.getLastModified() > 0, "Last modified should be > 0");
    assertEquals("application/octet-stream", metadata.getContentType());
    // FTP does not support ETags
    assertEquals(null, metadata.getEtag());
  }

  @Test
  @Order(31)
  void testGetMetadataNonExistentFile() {
    assumeContainerRunning();
    FtpStorageProvider provider = new FtpStorageProvider();

    assertThrows(IOException.class, () ->
        provider.getMetadata(BASE_URL + "/no-such-file.txt"));
  }

  // ---------------------------------------------------------------------------
  // Test: openInputStream
  // ---------------------------------------------------------------------------

  @Test
  @Order(40)
  void testOpenInputStreamCsv() throws IOException {
    assumeContainerRunning();
    FtpStorageProvider provider = new FtpStorageProvider();

    try (InputStream is = provider.openInputStream(BASE_URL + "/test.csv")) {
      assertNotNull(is);
      byte[] buf = new byte[4096];
      int totalRead = 0;
      int n;
      while ((n = is.read(buf, totalRead, buf.length - totalRead)) != -1) {
        totalRead += n;
        if (totalRead >= buf.length) {
          break;
        }
      }
      assertTrue(totalRead > 0, "Should have read some bytes");

      String content = new String(buf, 0, totalRead, StandardCharsets.UTF_8);
      assertTrue(content.contains("name"), "CSV should contain header 'name'");
      assertTrue(content.contains("Alice"), "CSV should contain data row 'Alice'");
    }
  }

  @Test
  @Order(41)
  void testOpenInputStreamJson() throws IOException {
    assumeContainerRunning();
    FtpStorageProvider provider = new FtpStorageProvider();

    try (InputStream is = provider.openInputStream(BASE_URL + "/test.json")) {
      assertNotNull(is);
      byte[] buf = new byte[4096];
      int totalRead = 0;
      int n;
      while ((n = is.read(buf, totalRead, buf.length - totalRead)) != -1) {
        totalRead += n;
        if (totalRead >= buf.length) {
          break;
        }
      }
      assertTrue(totalRead > 0, "Should have read some bytes");

      String content = new String(buf, 0, totalRead, StandardCharsets.UTF_8);
      assertTrue(content.contains("city"), "JSON should contain 'city'");
    }
  }

  @Test
  @Order(42)
  void testOpenInputStreamNonExistentFile() {
    assumeContainerRunning();
    FtpStorageProvider provider = new FtpStorageProvider();

    assertThrows(IOException.class, () ->
        provider.openInputStream(BASE_URL + "/no-such-file.txt"));
  }

  // ---------------------------------------------------------------------------
  // Test: openReader
  // ---------------------------------------------------------------------------

  @Test
  @Order(50)
  void testOpenReader() throws IOException {
    assumeContainerRunning();
    FtpStorageProvider provider = new FtpStorageProvider();

    try (Reader reader = provider.openReader(BASE_URL + "/test.csv")) {
      assertNotNull(reader);
      BufferedReader br = new BufferedReader(reader);
      String headerLine = br.readLine();
      assertNotNull(headerLine, "Should be able to read header line");
      assertTrue(headerLine.contains("name"), "Header should contain 'name'");

      String dataLine = br.readLine();
      assertNotNull(dataLine, "Should be able to read data line");
      assertTrue(dataLine.contains("Alice"), "Data should contain 'Alice'");
    }
  }

  // ---------------------------------------------------------------------------
  // Test: listFiles on a subdirectory
  // ---------------------------------------------------------------------------

  @Test
  @Order(60)
  void testListFilesSubdirectory() throws IOException {
    assumeContainerRunning();
    FtpStorageProvider provider = new FtpStorageProvider();

    List<StorageProvider.FileEntry> entries =
        provider.listFiles(BASE_URL + "/subdir", false);
    assertNotNull(entries);
    assertTrue(entries.size() >= 1,
        "subdir should contain at least nested.csv, got " + entries.size());

    StorageProvider.FileEntry nested = findEntry(entries, "nested.csv");
    assertNotNull(nested, "nested.csv should be in subdir listing");
  }

  // ---------------------------------------------------------------------------
  // Test: credentials in URI
  // ---------------------------------------------------------------------------

  @Test
  @Order(70)
  void testCredentialsInUri() throws IOException {
    assumeContainerRunning();
    FtpStorageProvider provider = new FtpStorageProvider();

    // URL with explicit credentials should work
    String url = String.format(Locale.ROOT,
        "ftp://%s:%s@localhost:%d/test.csv", FTP_USER, FTP_PASS, FTP_PORT);
    assertTrue(provider.exists(url), "Should authenticate via URI credentials");
  }

  // ---------------------------------------------------------------------------
  // Test: anonymous / bad credentials
  // ---------------------------------------------------------------------------

  @Test
  @Order(71)
  void testBadCredentials() {
    assumeContainerRunning();
    FtpStorageProvider provider = new FtpStorageProvider();

    String badUrl = String.format(Locale.ROOT,
        "ftp://baduser:badpass@localhost:%d/test.csv", FTP_PORT);
    assertThrows(IOException.class, () -> provider.exists(badUrl),
        "Bad credentials should throw IOException");
  }

  // ---------------------------------------------------------------------------
  // Test: invalid URI scheme
  // ---------------------------------------------------------------------------

  @Test
  @Order(80)
  void testInvalidUriScheme() {
    FtpStorageProvider provider = new FtpStorageProvider();

    assertThrows(IOException.class, () ->
        provider.listFiles("http://example.com/", false));
    assertThrows(IOException.class, () ->
        provider.exists("sftp://example.com/file.txt"));
    assertThrows(IOException.class, () ->
        provider.getMetadata("/local/path/file.txt"));
  }

  // ---------------------------------------------------------------------------
  // Test: connection to unreachable host
  // ---------------------------------------------------------------------------

  @Test
  @Order(81)
  void testConnectionToUnreachableHost() {
    FtpStorageProvider provider = new FtpStorageProvider();

    // Use a non-routable IP to trigger a connection timeout
    assertThrows(IOException.class, () ->
        provider.exists("ftp://testuser:testpass@192.0.2.1:2121/file.txt"));
  }

  // ---------------------------------------------------------------------------
  // Test: hasChanged (StorageProvider default method)
  // ---------------------------------------------------------------------------

  @Test
  @Order(90)
  void testHasChanged() throws IOException {
    assumeContainerRunning();
    FtpStorageProvider provider = new FtpStorageProvider();

    String fileUrl = BASE_URL + "/test.csv";

    // Get current metadata
    StorageProvider.FileMetadata metadata = provider.getMetadata(fileUrl);

    // File should not have changed compared to its own metadata
    assertFalse(provider.hasChanged(fileUrl, metadata),
        "File should not have changed when compared to its own metadata");

    // Null cached metadata should indicate change
    assertTrue(provider.hasChanged(fileUrl, null),
        "Null cached metadata should indicate change");

    // Metadata with different size should indicate change
    StorageProvider.FileMetadata differentSize = new StorageProvider.FileMetadata(
        fileUrl, metadata.getSize() + 100, metadata.getLastModified(),
        metadata.getContentType(), metadata.getEtag());
    assertTrue(provider.hasChanged(fileUrl, differentSize),
        "Different size should indicate change");
  }

  // ---------------------------------------------------------------------------
  // Test: readRange (default unsupported)
  // ---------------------------------------------------------------------------

  @Test
  @Order(91)
  void testReadRangeUnsupported() {
    FtpStorageProvider provider = new FtpStorageProvider();

    assertThrows(UnsupportedOperationException.class, () ->
        provider.readRange(BASE_URL + "/test.csv", 0, 10));
  }

  // ---------------------------------------------------------------------------
  // Test: writeFile (default unsupported)
  // ---------------------------------------------------------------------------

  @Test
  @Order(92)
  void testWriteFileUnsupported() {
    FtpStorageProvider provider = new FtpStorageProvider();

    assertThrows(UnsupportedOperationException.class, () ->
        provider.writeFile(BASE_URL + "/output.txt", "hello".getBytes(StandardCharsets.UTF_8)));
  }

  // ---------------------------------------------------------------------------
  // Test: delete (default unsupported)
  // ---------------------------------------------------------------------------

  @Test
  @Order(93)
  void testDeleteUnsupported() {
    FtpStorageProvider provider = new FtpStorageProvider();

    assertThrows(UnsupportedOperationException.class, () ->
        provider.delete(BASE_URL + "/test.csv"));
  }

  // ---------------------------------------------------------------------------
  // Test: copyFile (default unsupported)
  // ---------------------------------------------------------------------------

  @Test
  @Order(94)
  void testCopyFileUnsupported() {
    FtpStorageProvider provider = new FtpStorageProvider();

    assertThrows(UnsupportedOperationException.class, () ->
        provider.copyFile(BASE_URL + "/test.csv", BASE_URL + "/copy.csv"));
  }

  // ---------------------------------------------------------------------------
  // Test: normalizePath (static utility on StorageProvider)
  // ---------------------------------------------------------------------------

  @Test
  @Order(95)
  void testNormalizePath() {
    assertEquals(null, StorageProvider.normalizePath(null));
    assertEquals("ftp://server/path", StorageProvider.normalizePath("ftp://server/path"));
    assertEquals("s3a://bucket/key", StorageProvider.normalizePath("s3a:/bucket/key"));
    assertEquals("s3://bucket/key", StorageProvider.normalizePath("s3:/bucket/key"));
    assertEquals("hdfs://namenode/path", StorageProvider.normalizePath("hdfs:/namenode/path"));
    assertEquals("/local/path", StorageProvider.normalizePath("/local/path"));
  }

  // ---------------------------------------------------------------------------
  // Test: FtpInputStream wrapper close releases connection
  // ---------------------------------------------------------------------------

  @Test
  @Order(100)
  void testInputStreamClose() throws IOException {
    assumeContainerRunning();
    FtpStorageProvider provider = new FtpStorageProvider();

    // Open and close multiple streams to verify connections are released
    for (int i = 0; i < 3; i++) {
      try (InputStream is = provider.openInputStream(BASE_URL + "/test.csv")) {
        assertNotNull(is);
        // Read at least one byte
        int b = is.read();
        assertTrue(b >= 0, "Should be able to read at least one byte");
      }
    }
    // If we get here without IOException, connections are being released properly
  }

  // ---------------------------------------------------------------------------
  // Test: large file entry metadata consistency
  // ---------------------------------------------------------------------------

  @Test
  @Order(101)
  void testMetadataConsistencyWithListing() throws IOException {
    assumeContainerRunning();
    FtpStorageProvider provider = new FtpStorageProvider();

    List<StorageProvider.FileEntry> entries = provider.listFiles(BASE_URL + "/", false);
    StorageProvider.FileEntry csvEntry = findEntry(entries, "test.csv");
    assertNotNull(csvEntry);

    StorageProvider.FileMetadata metadata = provider.getMetadata(BASE_URL + "/test.csv");
    assertEquals(csvEntry.getSize(), metadata.getSize(),
        "Size from listing and metadata should match");
  }

  // =========================================================================
  // Helper methods
  // =========================================================================

  private void assumeContainerRunning() {
    Assumptions.assumeTrue(containerStarted,
        "Skipping: Docker FTP container is not running");
  }

  private static StorageProvider.FileEntry findEntry(
      List<StorageProvider.FileEntry> entries, String name) {
    for (StorageProvider.FileEntry e : entries) {
      if (name.equals(e.getName())) {
        return e;
      }
    }
    return null;
  }

  // =========================================================================
  // Docker helpers
  // =========================================================================

  private static boolean isDockerAvailable() {
    try {
      return runCommand("docker", "info") == 0;
    } catch (Exception e) {
      return false;
    }
  }

  private static int runCommand(String... cmd) throws Exception {
    Process p = new ProcessBuilder(cmd).redirectErrorStream(true).start();
    try (InputStream is = p.getInputStream()) {
      byte[] buf = new byte[1024];
      while (is.read(buf) != -1) {
        // drain output
      }
    }
    return p.waitFor();
  }

  private static String runCommandOutput(String... cmd) throws Exception {
    Process p = new ProcessBuilder(cmd).redirectErrorStream(true).start();
    StringBuilder sb = new StringBuilder();
    try (BufferedReader br =
             new BufferedReader(new InputStreamReader(p.getInputStream(),
                 StandardCharsets.UTF_8))) {
      String line;
      while ((line = br.readLine()) != null) {
        sb.append(line).append('\n');
      }
    }
    p.waitFor();
    return sb.toString().trim();
  }

  /**
   * Waits for the FTP server inside the container to become ready.
   * Tries connecting with the FTP client up to {@code maxWaitSeconds}.
   */
  private static boolean waitForFtpReady(int maxWaitSeconds) throws Exception {
    long deadline = System.currentTimeMillis() + maxWaitSeconds * 1000L;
    while (System.currentTimeMillis() < deadline) {
      // Use docker exec to check if vsftpd is listening
      int rc = runCommand("docker", "exec", CONTAINER_NAME, "ls", "/home/testuser");
      if (rc == 0) {
        // Give the FTP daemon a bit more time to bind the port
        Thread.sleep(2000);
        return true;
      }
      Thread.sleep(1000);
    }
    return false;
  }

  /**
   * Uploads test CSV and JSON files into the FTP container.
   */
  private static void uploadTestData() throws Exception {
    // Create test CSV content
    String csv = "name,age,city\nAlice,30,New York\nBob,25,San Francisco\nCarol,35,Chicago\n";
    // Create test JSON content
    String json = "[{\"city\":\"NYC\",\"pop\":8336817},{\"city\":\"LA\",\"pop\":3979576}]\n";
    // Create nested CSV
    String nestedCsv = "id,value\n1,alpha\n2,beta\n3,gamma\n";

    // Write files into the container using docker exec + shell redirection
    dockerExecWrite("/home/testuser/test.csv", csv);
    dockerExecWrite("/home/testuser/test.json", json);

    // Create subdirectory and nested file
    runCommand("docker", "exec", CONTAINER_NAME, "mkdir", "-p", "/home/testuser/subdir");
    dockerExecWrite("/home/testuser/subdir/nested.csv", nestedCsv);

    // Fix ownership so the FTP user can read the files
    runCommand("docker", "exec", CONTAINER_NAME,
        "chown", "-R", "1000:1000", "/home/testuser");
  }

  /**
   * Writes content to a file inside the Docker container using
   * {@code docker exec sh -c "cat > path"} with the content piped via stdin.
   */
  private static void dockerExecWrite(String path, String content) throws Exception {
    ProcessBuilder pb = new ProcessBuilder(
        "docker", "exec", "-i", CONTAINER_NAME,
        "sh", "-c", "cat > " + path);
    pb.redirectErrorStream(true);
    Process p = pb.start();
    p.getOutputStream().write(content.getBytes(StandardCharsets.UTF_8));
    p.getOutputStream().close();
    // drain stdout
    try (InputStream is = p.getInputStream()) {
      byte[] buf = new byte[1024];
      while (is.read(buf) != -1) {
        // drain
      }
    }
    p.waitFor();
  }
}
