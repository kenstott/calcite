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
 * Docker-based integration tests for {@link SftpStorageProvider}.
 *
 * <p>These tests start a real SFTP server via Docker ({@code atmoz/sftp})
 * and exercise every public method of the SFTP storage provider against it.
 *
 * <p>Prerequisites:
 * <ul>
 *   <li>Docker must be installed and running</li>
 *   <li>The {@code atmoz/sftp:latest} image must be pulled</li>
 *   <li>Port 2222 must be available</li>
 * </ul>
 */
@Tag("integration")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class SftpStorageProviderDockerTest {

  private static final String CONTAINER_NAME = "calcite-test-sftp";
  private static final String SFTP_USER = "testuser";
  private static final String SFTP_PASS = "testpass";
  private static final int SFTP_PORT = 2222;

  /** Base URL with credentials and non-default port. */
  private static final String BASE_URL =
      String.format(Locale.ROOT, "sftp://%s:%s@localhost:%d",
          SFTP_USER, SFTP_PASS, SFTP_PORT);

  private static boolean containerStarted = false;

  @BeforeAll
  static void startContainer() throws Exception {
    // Check that Docker is available
    if (!isDockerAvailable()) {
      return;
    }

    // Remove any leftover container from a previous run
    runCommand("docker", "rm", "-f", CONTAINER_NAME);

    // Start the SFTP container.
    // "testuser:testpass:::upload" creates user testuser with password testpass.
    // The user's home is /home/testuser and "upload" is a subdirectory owned
    // by the user that we can write to.
    int exitCode = runCommand(
        "docker", "run", "-d",
        "--name", CONTAINER_NAME,
        "-p", SFTP_PORT + ":22",
        "atmoz/sftp:latest",
        SFTP_USER + ":" + SFTP_PASS + ":::upload");

    if (exitCode != 0) {
      return;
    }

    // Wait for the SFTP server to be ready
    containerStarted = waitForSftpReady(30);
    if (!containerStarted) {
      runCommand("docker", "rm", "-f", CONTAINER_NAME);
      return;
    }

    // Upload test data into the writable "upload" directory
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
    SftpStorageProvider provider = createProvider();
    assertEquals("sftp", provider.getStorageType());
  }

  // ---------------------------------------------------------------------------
  // Test: resolvePath (no Docker required)
  // ---------------------------------------------------------------------------

  @Test
  @Order(2)
  void testResolvePath() {
    SftpStorageProvider provider = createProvider();

    // Absolute SFTP URL is returned as-is
    assertEquals("sftp://other@host2.com/file.txt",
        provider.resolvePath("sftp://user@host.com/base/", "sftp://other@host2.com/file.txt"));

    // Relative path against a directory base (default port 22 omitted)
    assertEquals("sftp://user@host.com/base/file.csv",
        provider.resolvePath("sftp://user@host.com/base/", "file.csv"));

    // Relative path against a file base
    assertEquals("sftp://user@host.com/base/newfile.csv",
        provider.resolvePath("sftp://user@host.com/base/old.csv", "newfile.csv"));

    // Non-default port is preserved
    assertEquals("sftp://user@host.com:2222/base/file.csv",
        provider.resolvePath("sftp://user@host.com:2222/base/", "file.csv"));

    // Subdirectory relative
    assertEquals("sftp://user@host.com/base/sub/dir/file.json",
        provider.resolvePath("sftp://user@host.com/base/", "sub/dir/file.json"));
  }

  @Test
  @Order(3)
  void testResolvePathFallback() {
    SftpStorageProvider provider = createProvider();

    // Non-SFTP base triggers fallback concatenation
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
    SftpStorageProvider provider = createProvider();

    String uploadDir = BASE_URL + "/upload/";
    List<StorageProvider.FileEntry> entries = provider.listFiles(uploadDir, false);

    assertNotNull(entries);
    assertTrue(entries.size() >= 3,
        "Expected at least 3 entries (test.csv, test.json, subdir), got " + entries.size());

    // Verify test.csv
    StorageProvider.FileEntry csvEntry = findEntry(entries, "test.csv");
    assertNotNull(csvEntry, "test.csv should be listed");
    assertFalse(csvEntry.isDirectory());
    assertTrue(csvEntry.getSize() > 0, "test.csv should have non-zero size");
    assertTrue(csvEntry.getLastModified() > 0, "Last modified should be > 0");
    assertTrue(csvEntry.getPath().startsWith("sftp://"), "Path should be a full SFTP URI");

    // Verify test.json
    StorageProvider.FileEntry jsonEntry = findEntry(entries, "test.json");
    assertNotNull(jsonEntry, "test.json should be listed");
    assertFalse(jsonEntry.isDirectory());

    // Verify subdirectory
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
    SftpStorageProvider provider = createProvider();

    String uploadDir = BASE_URL + "/upload/";
    List<StorageProvider.FileEntry> entries = provider.listFiles(uploadDir, true);

    assertNotNull(entries);
    // Should include nested file from subdir
    StorageProvider.FileEntry nestedFile = findEntry(entries, "nested.csv");
    assertNotNull(nestedFile,
        "nested.csv should be found in recursive listing");
    assertFalse(nestedFile.isDirectory());
    assertTrue(nestedFile.getPath().contains("subdir"),
        "Nested file path should contain 'subdir'");
  }

  // ---------------------------------------------------------------------------
  // Test: exists
  // ---------------------------------------------------------------------------

  @Test
  @Order(20)
  void testExistsFile() throws IOException {
    assumeContainerRunning();
    SftpStorageProvider provider = createProvider();

    assertTrue(provider.exists(BASE_URL + "/upload/test.csv"),
        "test.csv should exist");
    assertTrue(provider.exists(BASE_URL + "/upload/test.json"),
        "test.json should exist");
  }

  @Test
  @Order(21)
  void testExistsDirectory() throws IOException {
    assumeContainerRunning();
    SftpStorageProvider provider = createProvider();

    assertTrue(provider.exists(BASE_URL + "/upload"),
        "upload directory should exist");
    assertTrue(provider.exists(BASE_URL + "/upload/subdir"),
        "upload/subdir directory should exist");
  }

  @Test
  @Order(22)
  void testExistsNonExistentFile() throws IOException {
    assumeContainerRunning();
    SftpStorageProvider provider = createProvider();

    assertFalse(provider.exists(BASE_URL + "/upload/no-such-file.txt"),
        "Non-existent file should return false");
  }

  // ---------------------------------------------------------------------------
  // Test: isDirectory
  // ---------------------------------------------------------------------------

  @Test
  @Order(25)
  void testIsDirectory() throws IOException {
    assumeContainerRunning();
    SftpStorageProvider provider = createProvider();

    assertTrue(provider.isDirectory(BASE_URL + "/upload/subdir"),
        "subdir should be a directory");
    assertFalse(provider.isDirectory(BASE_URL + "/upload/test.csv"),
        "test.csv should not be a directory");
  }

  // ---------------------------------------------------------------------------
  // Test: getMetadata
  // ---------------------------------------------------------------------------

  @Test
  @Order(30)
  void testGetMetadata() throws IOException {
    assumeContainerRunning();
    SftpStorageProvider provider = createProvider();

    String fileUrl = BASE_URL + "/upload/test.csv";
    StorageProvider.FileMetadata metadata = provider.getMetadata(fileUrl);

    assertNotNull(metadata);
    assertEquals(fileUrl, metadata.getPath());
    assertTrue(metadata.getSize() > 0, "File size should be > 0");
    assertTrue(metadata.getLastModified() > 0, "Last modified should be > 0");
    assertEquals("application/octet-stream", metadata.getContentType());
    // SFTP does not support ETags
    assertEquals(null, metadata.getEtag());
  }

  @Test
  @Order(31)
  void testGetMetadataNonExistentFile() {
    assumeContainerRunning();
    SftpStorageProvider provider = createProvider();

    assertThrows(IOException.class, () ->
        provider.getMetadata(BASE_URL + "/upload/no-such-file.txt"));
  }

  // ---------------------------------------------------------------------------
  // Test: openInputStream
  // ---------------------------------------------------------------------------

  @Test
  @Order(40)
  void testOpenInputStreamCsv() throws IOException {
    assumeContainerRunning();
    SftpStorageProvider provider = createProvider();

    try (InputStream is = provider.openInputStream(BASE_URL + "/upload/test.csv")) {
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
    SftpStorageProvider provider = createProvider();

    try (InputStream is = provider.openInputStream(BASE_URL + "/upload/test.json")) {
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
    SftpStorageProvider provider = createProvider();

    assertThrows(IOException.class, () ->
        provider.openInputStream(BASE_URL + "/upload/no-such-file.txt"));
  }

  // ---------------------------------------------------------------------------
  // Test: openReader
  // ---------------------------------------------------------------------------

  @Test
  @Order(50)
  void testOpenReader() throws IOException {
    assumeContainerRunning();
    SftpStorageProvider provider = createProvider();

    try (Reader reader = provider.openReader(BASE_URL + "/upload/test.csv")) {
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
    SftpStorageProvider provider = createProvider();

    List<StorageProvider.FileEntry> entries =
        provider.listFiles(BASE_URL + "/upload/subdir", false);
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
    SftpStorageProvider provider = createProvider();

    // URL with explicit credentials should work
    String url = String.format(Locale.ROOT,
        "sftp://%s:%s@localhost:%d/upload/test.csv", SFTP_USER, SFTP_PASS, SFTP_PORT);
    assertTrue(provider.exists(url), "Should authenticate via URI credentials");
  }

  // ---------------------------------------------------------------------------
  // Test: bad credentials
  // ---------------------------------------------------------------------------

  @Test
  @Order(71)
  void testBadCredentials() {
    SftpStorageProvider provider = new SftpStorageProvider(
        "baduser", "badpass", null, false);

    String badUrl = String.format(Locale.ROOT,
        "sftp://baduser:badpass@localhost:%d/upload/test.csv", SFTP_PORT);

    assertThrows(IOException.class, () -> provider.exists(badUrl),
        "Bad credentials should throw IOException");
  }

  // ---------------------------------------------------------------------------
  // Test: invalid URI scheme
  // ---------------------------------------------------------------------------

  @Test
  @Order(80)
  void testInvalidUriScheme() {
    SftpStorageProvider provider = createProvider();

    assertThrows(IOException.class, () ->
        provider.listFiles("http://example.com/", false));
    assertThrows(IOException.class, () ->
        provider.exists("ftp://example.com/file.txt"));
    assertThrows(IOException.class, () ->
        provider.getMetadata("/local/path/file.txt"));
  }

  // ---------------------------------------------------------------------------
  // Test: connection to unreachable host
  // ---------------------------------------------------------------------------

  @Test
  @Order(81)
  void testConnectionToUnreachableHost() {
    SftpStorageProvider provider = createProvider();

    // Use a non-routable IP to trigger a connection timeout
    assertThrows(IOException.class, () ->
        provider.exists("sftp://testuser:testpass@192.0.2.1:2222/file.txt"));
  }

  // ---------------------------------------------------------------------------
  // Test: strict host key checking disabled
  // ---------------------------------------------------------------------------

  @Test
  @Order(82)
  void testStrictHostKeyCheckingDisabled() throws IOException {
    assumeContainerRunning();

    // Provider with strict host key checking explicitly disabled
    SftpStorageProvider provider = new SftpStorageProvider(
        SFTP_USER, SFTP_PASS, null, false);

    String url = String.format(Locale.ROOT,
        "sftp://%s:%s@localhost:%d/upload/test.csv", SFTP_USER, SFTP_PASS, SFTP_PORT);
    assertTrue(provider.exists(url),
        "Should connect when strict host key checking is disabled");
  }

  // ---------------------------------------------------------------------------
  // Test: constructor with password auth
  // ---------------------------------------------------------------------------

  @Test
  @Order(83)
  void testPasswordAuthConstructor() throws IOException {
    assumeContainerRunning();

    SftpStorageProvider provider = new SftpStorageProvider(
        SFTP_USER, SFTP_PASS, null, false);
    assertEquals("sftp", provider.getStorageType());

    // Verify it can actually connect
    String url = String.format(Locale.ROOT,
        "sftp://%s:%s@localhost:%d/upload", SFTP_USER, SFTP_PASS, SFTP_PORT);
    assertTrue(provider.exists(url));
  }

  // ---------------------------------------------------------------------------
  // Test: default constructor
  // ---------------------------------------------------------------------------

  @Test
  @Order(84)
  void testDefaultConstructor() {
    SftpStorageProvider provider = new SftpStorageProvider();
    assertEquals("sftp", provider.getStorageType());
  }

  // ---------------------------------------------------------------------------
  // Test: hasChanged (StorageProvider default method)
  // ---------------------------------------------------------------------------

  @Test
  @Order(90)
  void testHasChanged() throws IOException {
    assumeContainerRunning();
    SftpStorageProvider provider = createProvider();

    String fileUrl = BASE_URL + "/upload/test.csv";

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
    SftpStorageProvider provider = createProvider();

    assertThrows(UnsupportedOperationException.class, () ->
        provider.readRange(BASE_URL + "/upload/test.csv", 0, 10));
  }

  // ---------------------------------------------------------------------------
  // Test: writeFile (default unsupported)
  // ---------------------------------------------------------------------------

  @Test
  @Order(92)
  void testWriteFileUnsupported() {
    SftpStorageProvider provider = createProvider();

    assertThrows(UnsupportedOperationException.class, () ->
        provider.writeFile(BASE_URL + "/upload/out.txt",
            "hello".getBytes(StandardCharsets.UTF_8)));
  }

  // ---------------------------------------------------------------------------
  // Test: delete (default unsupported)
  // ---------------------------------------------------------------------------

  @Test
  @Order(93)
  void testDeleteUnsupported() {
    SftpStorageProvider provider = createProvider();

    assertThrows(UnsupportedOperationException.class, () ->
        provider.delete(BASE_URL + "/upload/test.csv"));
  }

  // ---------------------------------------------------------------------------
  // Test: copyFile (default unsupported)
  // ---------------------------------------------------------------------------

  @Test
  @Order(94)
  void testCopyFileUnsupported() {
    SftpStorageProvider provider = createProvider();

    assertThrows(UnsupportedOperationException.class, () ->
        provider.copyFile(BASE_URL + "/upload/test.csv",
            BASE_URL + "/upload/copy.csv"));
  }

  // ---------------------------------------------------------------------------
  // Test: SftpInputStream wrapper close releases resources
  // ---------------------------------------------------------------------------

  @Test
  @Order(100)
  void testInputStreamClose() throws IOException {
    assumeContainerRunning();
    SftpStorageProvider provider = createProvider();

    // Open and close multiple streams to verify sessions are released
    for (int i = 0; i < 3; i++) {
      try (InputStream is = provider.openInputStream(BASE_URL + "/upload/test.csv")) {
        assertNotNull(is);
        int b = is.read();
        assertTrue(b >= 0, "Should be able to read at least one byte");
      }
    }
    // If we get here without IOException, sessions are being released properly
  }

  // ---------------------------------------------------------------------------
  // Test: metadata consistency between listing and getMetadata
  // ---------------------------------------------------------------------------

  @Test
  @Order(101)
  void testMetadataConsistencyWithListing() throws IOException {
    assumeContainerRunning();
    SftpStorageProvider provider = createProvider();

    List<StorageProvider.FileEntry> entries =
        provider.listFiles(BASE_URL + "/upload/", false);
    StorageProvider.FileEntry csvEntry = findEntry(entries, "test.csv");
    assertNotNull(csvEntry);

    StorageProvider.FileMetadata metadata =
        provider.getMetadata(BASE_URL + "/upload/test.csv");
    assertEquals(csvEntry.getSize(), metadata.getSize(),
        "Size from listing and metadata should match");
  }

  // ---------------------------------------------------------------------------
  // Test: nested file download through subdirectory
  // ---------------------------------------------------------------------------

  @Test
  @Order(102)
  void testNestedFileDownload() throws IOException {
    assumeContainerRunning();
    SftpStorageProvider provider = createProvider();

    try (InputStream is =
             provider.openInputStream(BASE_URL + "/upload/subdir/nested.csv")) {
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
      assertTrue(totalRead > 0);
      String content = new String(buf, 0, totalRead, StandardCharsets.UTF_8);
      assertTrue(content.contains("alpha"), "Nested CSV should contain 'alpha'");
    }
  }

  // =========================================================================
  // Helper methods
  // =========================================================================

  private static SftpStorageProvider createProvider() {
    return new SftpStorageProvider(SFTP_USER, SFTP_PASS, null, false);
  }

  private void assumeContainerRunning() {
    Assumptions.assumeTrue(containerStarted,
        "Skipping: Docker SFTP container is not running");
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
   * Waits for the SFTP server inside the container to become ready.
   * Polls by attempting to run a command inside the container.
   */
  private static boolean waitForSftpReady(int maxWaitSeconds) throws Exception {
    long deadline = System.currentTimeMillis() + maxWaitSeconds * 1000L;
    while (System.currentTimeMillis() < deadline) {
      // Check if the SSH daemon is listening by testing the upload directory
      int rc = runCommand("docker", "exec", CONTAINER_NAME,
          "ls", "/home/testuser/upload");
      if (rc == 0) {
        // Give sshd a moment to finish binding
        Thread.sleep(2000);
        return true;
      }
      Thread.sleep(1000);
    }
    return false;
  }

  /**
   * Uploads test CSV and JSON files into the SFTP container.
   * Files go into /home/testuser/upload which is writable by the testuser.
   */
  private static void uploadTestData() throws Exception {
    // Test CSV
    String csv = "name,age,city\nAlice,30,New York\nBob,25,San Francisco\nCarol,35,Chicago\n";
    // Test JSON
    String json = "[{\"city\":\"NYC\",\"pop\":8336817},{\"city\":\"LA\",\"pop\":3979576}]\n";
    // Nested CSV
    String nestedCsv = "id,value\n1,alpha\n2,beta\n3,gamma\n";

    // Write files into the container
    dockerExecWrite("/home/testuser/upload/test.csv", csv);
    dockerExecWrite("/home/testuser/upload/test.json", json);

    // Create subdirectory and nested file
    runCommand("docker", "exec", CONTAINER_NAME,
        "mkdir", "-p", "/home/testuser/upload/subdir");
    dockerExecWrite("/home/testuser/upload/subdir/nested.csv", nestedCsv);

    // Fix ownership -- atmoz/sftp uses uid 1000 for the first user
    runCommand("docker", "exec", CONTAINER_NAME,
        "chown", "-R", "1000:1000", "/home/testuser/upload");
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
