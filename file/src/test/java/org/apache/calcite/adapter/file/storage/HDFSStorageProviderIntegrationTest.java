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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Integration tests for HDFSStorageProvider using embedded HDFS cluster.
 * These tests require more time and resources, so they're tagged as integration tests.
 */
@Tag("integration")
public class HDFSStorageProviderIntegrationTest {

  private MiniDFSCluster miniCluster;
  private Configuration conf;
  private FileSystem hdfs;
  private HDFSStorageProvider storageProvider;
  private java.io.File clusterBaseDir;

  @BeforeEach
  public void setUp() throws IOException {
    try {
      // Use a unique directory per test to avoid leftover state
      clusterBaseDir = java.nio.file.Files.createTempDirectory("hdfs-test-").toFile();

      // Configure embedded HDFS cluster with isolated data directory
      conf = new Configuration();
      conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, clusterBaseDir.getAbsolutePath());
      conf.set("hadoop.security.authentication", "simple");
      conf.set("hadoop.security.authorization", "false");

      MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(conf);
      builder.nameNodePort(0); // Use random port
      builder.numDataNodes(1);

      miniCluster = builder.build();
      miniCluster.waitActive();

      // Get filesystem and create storage provider
      hdfs = miniCluster.getFileSystem();
      conf = hdfs.getConf();

      storageProvider = new HDFSStorageProvider(hdfs, conf);

      // Create test directory structure
      hdfs.mkdirs(new Path("/test"));
      hdfs.mkdirs(new Path("/test/data"));
      hdfs.mkdirs(new Path("/test/empty"));
    } catch (Exception e) {
      System.err.println("HDFS setUp failed: " + e.getClass().getName() + ": " + e.getMessage());
      e.printStackTrace(System.err);
      assumeTrue(false,
          "Mini HDFS cluster not available: " + e.getMessage());
    }
  }

  @AfterEach
  public void tearDown() throws IOException {
    if (storageProvider != null) {
      storageProvider.close();
    }
    if (hdfs != null) {
      hdfs.close();
    }
    if (miniCluster != null) {
      miniCluster.shutdown();
    }
    if (clusterBaseDir != null && clusterBaseDir.exists()) {
      deleteDirectory(clusterBaseDir);
    }
  }

  @Test public void testCreateAndReadFile() throws IOException {
    String testPath = "/test/data/sample.txt";
    String content = "Hello HDFS Integration Test!";

    // Write file
    storageProvider.writeFile(testPath, content.getBytes(StandardCharsets.UTF_8));

    // Verify file exists
    assertTrue(storageProvider.exists(testPath));
    assertFalse(storageProvider.isDirectory(testPath));

    // Read file content
    try (InputStream inputStream = storageProvider.openInputStream(testPath);
         ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {

      byte[] buffer = new byte[1024];
      int bytesRead;
      while ((bytesRead = inputStream.read(buffer)) != -1) {
        outputStream.write(buffer, 0, bytesRead);
      }

      String readContent = outputStream.toString(StandardCharsets.UTF_8.name());
      assertEquals(content, readContent);
    }
  }

  @Test public void testFileMetadata() throws IOException {
    String testPath = "/test/data/metadata.json";
    String jsonContent = "{\"name\":\"test\",\"value\":123}";

    storageProvider.writeFile(testPath, jsonContent.getBytes(StandardCharsets.UTF_8));

    StorageProvider.FileMetadata metadata = storageProvider.getMetadata(testPath);

    assertNotNull(metadata);
    assertEquals(testPath, metadata.getPath());
    assertEquals(jsonContent.length(), metadata.getSize());
    assertEquals("application/json", metadata.getContentType());
    assertNotNull(metadata.getEtag());
    assertTrue(metadata.getLastModified() > 0);
  }

  @Test public void testDirectoryOperations() throws IOException {
    String dirPath = "/test/new-directory";

    // Create directory
    storageProvider.createDirectories(dirPath);

    // Verify directory exists
    assertTrue(storageProvider.exists(dirPath));
    assertTrue(storageProvider.isDirectory(dirPath));

    // Create files in directory
    String file1Path = dirPath + "/file1.txt";
    String file2Path = dirPath + "/file2.csv";

    storageProvider.writeFile(file1Path, "Content 1".getBytes(StandardCharsets.UTF_8));
    storageProvider.writeFile(file2Path, "name,value\ntest,123".getBytes(StandardCharsets.UTF_8));

    // List directory contents
    List<StorageProvider.FileEntry> entries = storageProvider.listFiles(dirPath, false);

    assertEquals(2, entries.size());

    // Verify entries (order may vary)
    boolean foundFile1 = false;
    boolean foundFile2 = false;

    for (StorageProvider.FileEntry entry : entries) {
      if (entry.getName().equals("file1.txt")) {
        foundFile1 = true;
        assertFalse(entry.isDirectory());
        assertEquals("Content 1".length(), entry.getSize());
      } else if (entry.getName().equals("file2.csv")) {
        foundFile2 = true;
        assertFalse(entry.isDirectory());
        assertEquals("name,value\ntest,123".length(), entry.getSize());
      }
    }

    assertTrue(foundFile1);
    assertTrue(foundFile2);
  }

  @Test public void testRecursiveDirectoryListing() throws IOException {
    // Create nested directory structure
    String basePath = "/test/recursive";
    storageProvider.createDirectories(basePath + "/level1/level2");

    // Create files at different levels
    storageProvider.writeFile(basePath + "/root-file.txt", "root".getBytes());
    storageProvider.writeFile(basePath + "/level1/level1-file.txt", "level1".getBytes());
    storageProvider.writeFile(basePath + "/level1/level2/level2-file.txt", "level2".getBytes());

    // Non-recursive listing should show only immediate children
    List<StorageProvider.FileEntry> nonRecursive = storageProvider.listFiles(basePath, false);
    assertEquals(2, nonRecursive.size()); // root-file.txt and level1/

    // Recursive listing should show all files
    List<StorageProvider.FileEntry> recursive = storageProvider.listFiles(basePath, true);
    assertEquals(3, recursive.size()); // All three files (directories not included in recursive file listing)

    // Verify all files are found
    boolean foundRootFile = false;
    boolean foundLevel1File = false;
    boolean foundLevel2File = false;

    for (StorageProvider.FileEntry entry : recursive) {
      if (entry.getPath().contains("root-file.txt")) {
        foundRootFile = true;
      } else if (entry.getPath().contains("level1-file.txt")) {
        foundLevel1File = true;
      } else if (entry.getPath().contains("level2-file.txt")) {
        foundLevel2File = true;
      }
    }

    assertTrue(foundRootFile);
    assertTrue(foundLevel1File);
    assertTrue(foundLevel2File);
  }

  @Test public void testDeleteOperations() throws IOException {
    String filePath = "/test/data/to-delete.txt";
    String dirPath = "/test/dir-to-delete";

    // Create file and directory
    storageProvider.writeFile(filePath, "delete me".getBytes());
    storageProvider.createDirectories(dirPath);
    storageProvider.writeFile(dirPath + "/nested-file.txt", "nested".getBytes());

    // Verify they exist
    assertTrue(storageProvider.exists(filePath));
    assertTrue(storageProvider.exists(dirPath));
    assertTrue(storageProvider.exists(dirPath + "/nested-file.txt"));

    // Delete file
    assertTrue(storageProvider.delete(filePath));
    assertFalse(storageProvider.exists(filePath));

    // Delete directory (should be recursive)
    assertTrue(storageProvider.delete(dirPath));
    assertFalse(storageProvider.exists(dirPath));
    assertFalse(storageProvider.exists(dirPath + "/nested-file.txt"));

    // Try to delete non-existent file
    assertFalse(storageProvider.delete("/test/nonexistent.txt"));
  }

  @Test public void testCopyOperation() throws IOException {
    String sourcePath = "/test/data/source.txt";
    String destPath = "/test/data/destination.txt";
    String content = "Content to copy";

    // Create source file
    storageProvider.writeFile(sourcePath, content.getBytes(StandardCharsets.UTF_8));

    // Copy file - may fail on certain volume filesystems (e.g., T9 external volume)
    try {
      storageProvider.copyFile(sourcePath, destPath);
    } catch (IOException e) {
      assumeTrue(false,
          "Copy operation not supported on this filesystem: " + e.getMessage());
      return;
    }

    // Verify both files exist
    assertTrue(storageProvider.exists(sourcePath));
    assertTrue(storageProvider.exists(destPath));

    // Verify content is identical (Java 8 compatible - no transferTo)
    try (InputStream sourceStream = storageProvider.openInputStream(sourcePath);
         InputStream destStream = storageProvider.openInputStream(destPath);
         ByteArrayOutputStream sourceContent = new ByteArrayOutputStream();
         ByteArrayOutputStream destContent = new ByteArrayOutputStream()) {

      copyStream(sourceStream, sourceContent);
      copyStream(destStream, destContent);

      assertEquals(sourceContent.toString(), destContent.toString());
    }
  }

  @Test public void testPathResolution() {
    String hdfsUri = hdfs.getUri().toString();

    // Test resolving relative paths with HDFS URIs
    String resolved = storageProvider.resolvePath(hdfsUri + "/base", "relative/file.txt");
    assertEquals(hdfsUri + "/base/relative/file.txt", resolved);

    // Test absolute path override
    String absolute = storageProvider.resolvePath("/base", hdfsUri + "/absolute/file.txt");
    assertEquals(hdfsUri + "/absolute/file.txt", absolute);
  }

  @Test public void testLargeFile() throws IOException {
    String largePath = "/test/data/large-file.txt";

    // Create 1MB of test data
    StringBuilder content = new StringBuilder();
    String chunk = "This is a chunk of data that will be repeated many times.\n";
    int chunks = 1024 * 1024 / chunk.length(); // ~1MB

    for (int i = 0; i < chunks; i++) {
      content.append(chunk);
    }

    byte[] largeContent = content.toString().getBytes(StandardCharsets.UTF_8);

    // Write large file
    storageProvider.writeFile(largePath, largeContent);

    // Verify file size
    StorageProvider.FileMetadata metadata = storageProvider.getMetadata(largePath);
    assertEquals(largeContent.length, metadata.getSize());

    // Read back and verify first few bytes (Java 8 compatible - no readNBytes)
    try (InputStream inputStream = storageProvider.openInputStream(largePath)) {
      byte[] buffer = new byte[chunk.length()];
      int totalRead = 0;
      while (totalRead < buffer.length) {
        int read = inputStream.read(buffer, totalRead, buffer.length - totalRead);
        if (read == -1) {
          break;
        }
        totalRead += read;
      }
      String firstChunkStr = new String(buffer, 0, totalRead, StandardCharsets.UTF_8);
      assertEquals(chunk, firstChunkStr);
    }
  }

  @Test public void testHasChangedDetection() throws IOException {
    String testPath = "/test/data/change-detection.txt";
    String initialContent = "Initial content";

    // Create initial file
    storageProvider.writeFile(testPath, initialContent.getBytes());
    StorageProvider.FileMetadata initialMetadata = storageProvider.getMetadata(testPath);

    // File shouldn't have changed
    assertFalse(storageProvider.hasChanged(testPath, initialMetadata));

    // Wait a bit to ensure timestamp difference
    try {
      Thread.sleep(1100); // Wait more than 1 second for timestamp precision
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }

    // Modify file
    String newContent = "Modified content";
    storageProvider.writeFile(testPath, newContent.getBytes());

    // File should have changed
    assertTrue(storageProvider.hasChanged(testPath, initialMetadata));

    // Get new metadata - file shouldn't have changed compared to itself
    StorageProvider.FileMetadata newMetadata = storageProvider.getMetadata(testPath);
    assertFalse(storageProvider.hasChanged(testPath, newMetadata));
  }

  // -----------------------------------------------------------------------
  // FILE-064: an hdfs:// directory routes to the HDFS provider via fs.defaultFS;
  //           recursive catalog enumeration + byte-exact read, asserted at the seam.
  //
  // These tests build the provider from a Configuration whose fs.defaultFS is the
  // running MiniDFSCluster namenode URI (NOT the (FileSystem, Configuration) test
  // constructor), to prove the fs.defaultFS routing seam the way production resolves
  // the namenode. NOTE: Hadoop Simple auth (OS user) is the default here; no Kerberos.
  // -----------------------------------------------------------------------

  /**
   * Builds an {@link HDFSStorageProvider} whose namenode comes from {@code fs.defaultFS} set to
   * the running cluster's URI. This is the routing seam FILE-064 asserts.
   */
  private HDFSStorageProvider createDefaultFsProvider() throws IOException {
    Configuration providerConf = new Configuration();
    providerConf.set("fs.defaultFS", hdfs.getUri().toString());
    return new HDFSStorageProvider(providerConf);
  }

  /** Reads all bytes from a stream. */
  private static byte[] readAll(InputStream is) throws IOException {
    ByteArrayOutputStream buf = new ByteArrayOutputStream();
    byte[] tmp = new byte[4096];
    int n;
    while ((n = is.read(tmp)) != -1) {
      buf.write(tmp, 0, n);
    }
    return buf.toByteArray();
  }

  /** Fully-qualifies an absolute HDFS path with the cluster namenode authority. */
  private String qualify(String absolutePath) {
    return hdfs.getUri().toString() + absolutePath;
  }

  /**
   * Writes a known set of files (including some under nested HDFS prefixes) via the cluster
   * filesystem, then asserts that {@link HDFSStorageProvider#listFiles} returns exactly that
   * catalog under an {@code hdfs://} directory (recursive, flattened enumeration) and that
   * {@link HDFSStorageProvider#openInputStream} returns the exact bytes of one file. The
   * provider's namenode is resolved from {@code fs.defaultFS}.
   */
  @Tag("FILE-064")
  @Tag("integration")
  @Test public void listFilesReturnsRecursiveCatalogAndOpenInputStreamReturnsExactBytes()
      throws IOException {
    String base = "/file-064/catalog";

    // Known fixture set: two at the root of the prefix, three under nested prefixes.
    String rootA = base + "/root-a.csv";
    String rootB = base + "/root-b.json";
    String nested1 = base + "/sub1/nested-1.csv";
    String nested2 = base + "/sub1/sub2/nested-2.csv";
    String nested3 = base + "/sub3/nested-3.txt";

    String knownContent = "id,name\n1,alice\n2,bob\n3,charlie\n";

    // Write fixtures across nested HDFS paths via the cluster filesystem (mkdirs + create).
    hdfs.mkdirs(new Path(base + "/sub1/sub2"));
    hdfs.mkdirs(new Path(base + "/sub3"));
    storageProvider.writeFile(rootA, knownContent.getBytes(StandardCharsets.UTF_8));
    storageProvider.writeFile(rootB, "{\"k\":\"v\"}".getBytes(StandardCharsets.UTF_8));
    storageProvider.writeFile(nested1, "n1\n".getBytes(StandardCharsets.UTF_8));
    storageProvider.writeFile(nested2, "n2\n".getBytes(StandardCharsets.UTF_8));
    storageProvider.writeFile(nested3, "n3\n".getBytes(StandardCharsets.UTF_8));

    HDFSStorageProvider provider = createDefaultFsProvider();
    try {
      // The provider must route a directory starting hdfs:// through the namenode from
      // fs.defaultFS. List recursively against the fully-qualified hdfs:// directory.
      String hdfsDir = qualify(base);
      List<StorageProvider.FileEntry> entries = provider.listFiles(hdfsDir, true);

      Set<String> actualPaths = new HashSet<>();
      for (StorageProvider.FileEntry entry : entries) {
        assertFalse(entry.isDirectory(),
            "recursive listing should not include directory entries: " + entry.getPath());
        actualPaths.add(entry.getPath());
      }

      // Provider emits fully-qualified hdfs://host:port/... paths (FileStatus.toUri()).
      Set<String> expectedPaths = new HashSet<>();
      expectedPaths.add(qualify(rootA));
      expectedPaths.add(qualify(rootB));
      expectedPaths.add(qualify(nested1));
      expectedPaths.add(qualify(nested2));
      expectedPaths.add(qualify(nested3));

      assertEquals(expectedPaths, actualPaths,
          "recursive catalog set must match the written files exactly");

      // Byte-for-byte read of one file, asserted at the provider seam.
      byte[] expectedBytes = knownContent.getBytes(StandardCharsets.UTF_8);
      try (InputStream is = provider.openInputStream(qualify(rootA))) {
        byte[] actualBytes = readAll(is);
        assertArrayEquals(expectedBytes, actualBytes,
            "openInputStream must return the exact written bytes");
      }
    } finally {
      provider.close();
    }
  }

  /**
   * Proves the routing seam directly: the provider built from {@code fs.defaultFS} resolves the
   * MiniDFSCluster namenode (its underlying FileSystem URI equals the cluster URI), and an
   * {@code hdfs://} file path it never saw before is readable through it.
   */
  @Tag("FILE-064")
  @Tag("integration")
  @Test public void providerResolvesNamenodeFromDefaultFsAndReadsHdfsUri() throws IOException {
    String path = "/file-064/seam/only.txt";
    String content = "namenode-from-default-fs\n";
    storageProvider.writeFile(path, content.getBytes(StandardCharsets.UTF_8));

    HDFSStorageProvider provider = createDefaultFsProvider();
    try {
      // The provider's filesystem was resolved from fs.defaultFS = the cluster namenode.
      assertNotNull(provider.getFileSystem());
      assertEquals(URI.create(hdfs.getUri().toString()), provider.getFileSystem().getUri(),
          "provider namenode must come from fs.defaultFS (the cluster URI)");

      String hdfsFile = qualify(path);
      assertTrue(provider.exists(hdfsFile), "provider must see the hdfs:// fixture file");
      assertFalse(provider.isDirectory(hdfsFile));

      try (InputStream is = provider.openInputStream(hdfsFile)) {
        assertEquals(content, new String(readAll(is), StandardCharsets.UTF_8),
            "read through the fs.defaultFS-resolved provider must return exact bytes");
      }
    } finally {
      provider.close();
    }
  }

  /** Java 8 compatible stream copy (replacement for InputStream.transferTo). */
  private static void copyStream(InputStream in, ByteArrayOutputStream out) throws IOException {
    byte[] buffer = new byte[4096];
    int bytesRead;
    while ((bytesRead = in.read(buffer)) != -1) {
      out.write(buffer, 0, bytesRead);
    }
  }

  private void deleteDirectory(java.io.File dir) {
    if (dir.isDirectory()) {
      java.io.File[] files = dir.listFiles();
      if (files != null) {
        for (java.io.File file : files) {
          deleteDirectory(file);
        }
      }
    }
    dir.delete();
  }
}
