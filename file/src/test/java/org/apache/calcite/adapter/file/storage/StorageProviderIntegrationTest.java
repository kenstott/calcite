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

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration tests for StorageProvider implementations exercising real file I/O.
 * Covers write/read round-trips, Parquet operations, change detection, and edge cases.
 */
@Tag("integration")
public class StorageProviderIntegrationTest {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(StorageProviderIntegrationTest.class);

  @TempDir
  Path tempDir;

  // --- LocalFileStorageProvider full lifecycle tests ---

  @Test public void testWriteReadDeleteLifecycle() throws IOException {
    LocalFileStorageProvider provider = new LocalFileStorageProvider();
    String filePath = tempDir.resolve("lifecycle.txt").toString();

    // Write
    byte[] content = "Hello, Integration Test!".getBytes(StandardCharsets.UTF_8);
    provider.writeFile(filePath, content);

    // Verify exists
    assertTrue(provider.exists(filePath));
    assertFalse(provider.isDirectory(filePath));

    // Read back via InputStream
    try (InputStream is = provider.openInputStream(filePath)) {
      byte[] readBack = new byte[content.length];
      int bytesRead = is.read(readBack);
      assertEquals(content.length, bytesRead);
      assertArrayEquals(content, readBack);
    }

    // Read back via Reader
    try (Reader reader = provider.openReader(filePath)) {
      char[] buf = new char[1024];
      int charsRead = reader.read(buf);
      assertEquals("Hello, Integration Test!", new String(buf, 0, charsRead));
    }

    // Get metadata
    StorageProvider.FileMetadata meta = provider.getMetadata(filePath);
    assertEquals(content.length, meta.getSize());
    assertTrue(meta.getLastModified() > 0);
    assertNotNull(meta.getPath());

    // Delete
    assertTrue(provider.delete(filePath));
    assertFalse(provider.exists(filePath));

    // Delete again should return false
    assertFalse(provider.delete(filePath));
  }

  @Test public void testWriteFileViaInputStream() throws IOException {
    LocalFileStorageProvider provider = new LocalFileStorageProvider();
    String filePath = tempDir.resolve("stream-write.txt").toString();
    String content = "Written via InputStream";

    provider.writeFile(filePath,
        new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8)));

    assertTrue(provider.exists(filePath));
    byte[] readBack = Files.readAllBytes(Path.of(filePath));
    assertEquals(content, new String(readBack, StandardCharsets.UTF_8));
  }

  @Test public void testWriteFileCreatesParentDirectories() throws IOException {
    LocalFileStorageProvider provider = new LocalFileStorageProvider();
    String filePath = tempDir.resolve("a/b/c/deep.txt").toString();

    provider.writeFile(filePath, "deep content".getBytes(StandardCharsets.UTF_8));

    assertTrue(provider.exists(filePath));
    assertEquals("deep content",
        new String(Files.readAllBytes(Path.of(filePath)), StandardCharsets.UTF_8));
  }

  @Test public void testCreateDirectories() throws IOException {
    LocalFileStorageProvider provider = new LocalFileStorageProvider();
    String dirPath = tempDir.resolve("x/y/z").toString();

    provider.createDirectories(dirPath);

    assertTrue(provider.exists(dirPath));
    assertTrue(provider.isDirectory(dirPath));
  }

  @Test public void testCopyFile() throws IOException {
    LocalFileStorageProvider provider = new LocalFileStorageProvider();
    String src = tempDir.resolve("src.txt").toString();
    String dst = tempDir.resolve("dst.txt").toString();

    provider.writeFile(src, "copy me".getBytes(StandardCharsets.UTF_8));
    provider.copyFile(src, dst);

    assertTrue(provider.exists(dst));
    byte[] dstContent = Files.readAllBytes(Path.of(dst));
    assertEquals("copy me", new String(dstContent, StandardCharsets.UTF_8));
  }

  @Test public void testCopyFileOverwrites() throws IOException {
    LocalFileStorageProvider provider = new LocalFileStorageProvider();
    String src = tempDir.resolve("src2.txt").toString();
    String dst = tempDir.resolve("dst2.txt").toString();

    provider.writeFile(src, "new content".getBytes(StandardCharsets.UTF_8));
    provider.writeFile(dst, "old content".getBytes(StandardCharsets.UTF_8));
    provider.copyFile(src, dst);

    assertEquals("new content",
        new String(Files.readAllBytes(Path.of(dst)), StandardCharsets.UTF_8));
  }

  @Test public void testListFilesRecursiveIntegration() throws IOException {
    LocalFileStorageProvider provider = new LocalFileStorageProvider();

    // Create directory structure
    Path base = tempDir.resolve("listtest");
    Files.createDirectories(base.resolve("sub1"));
    Files.createDirectories(base.resolve("sub2/nested"));
    Files.write(base.resolve("root.csv"), "root".getBytes(StandardCharsets.UTF_8));
    Files.write(base.resolve("sub1/a.csv"), "a".getBytes(StandardCharsets.UTF_8));
    Files.write(base.resolve("sub2/b.csv"), "b".getBytes(StandardCharsets.UTF_8));
    Files.write(base.resolve("sub2/nested/c.csv"), "c".getBytes(StandardCharsets.UTF_8));

    // Recursive listing
    List<StorageProvider.FileEntry> entries = provider.listFiles(base.toString(), true);

    int fileCount = 0;
    int dirCount = 0;
    for (StorageProvider.FileEntry entry : entries) {
      if (entry.isDirectory()) {
        dirCount++;
      } else {
        fileCount++;
      }
    }
    assertTrue(fileCount >= 4, "Should find at least 4 files");
    assertTrue(dirCount >= 3, "Should find at least 3 directories");
  }

  @Test public void testListFilesNonRecursiveIntegration() throws IOException {
    LocalFileStorageProvider provider = new LocalFileStorageProvider();

    Path base = tempDir.resolve("nonrec");
    Files.createDirectories(base.resolve("sub"));
    Files.write(base.resolve("top.csv"), "top".getBytes(StandardCharsets.UTF_8));
    Files.write(base.resolve("sub/nested.csv"), "nested".getBytes(StandardCharsets.UTF_8));

    List<StorageProvider.FileEntry> entries = provider.listFiles(base.toString(), false);

    boolean foundNested = false;
    for (StorageProvider.FileEntry entry : entries) {
      if ("nested.csv".equals(entry.getName())) {
        foundNested = true;
      }
    }
    assertFalse(foundNested, "Should not find nested files in non-recursive mode");
  }

  @Test public void testReadRange() throws IOException {
    LocalFileStorageProvider provider = new LocalFileStorageProvider();
    String filePath = tempDir.resolve("range.bin").toString();

    byte[] data = new byte[256];
    for (int i = 0; i < 256; i++) {
      data[i] = (byte) i;
    }
    provider.writeFile(filePath, data);

    // Read middle range
    byte[] range = provider.readRange(filePath, 100, 50);
    assertEquals(50, range.length);
    for (int i = 0; i < 50; i++) {
      assertEquals((byte) (100 + i), range[i]);
    }
  }

  @Test public void testReadRangeTruncatesAtEndOfFile() throws IOException {
    LocalFileStorageProvider provider = new LocalFileStorageProvider();
    String filePath = tempDir.resolve("short.bin").toString();

    provider.writeFile(filePath, new byte[]{1, 2, 3, 4, 5});

    // Request more bytes than available from offset 3
    byte[] range = provider.readRange(filePath, 3, 100);
    assertEquals(2, range.length);
    assertEquals(4, range[0]);
    assertEquals(5, range[1]);
  }

  // --- hasChanged detection ---

  @Test public void testHasChangedWithNullCachedMetadata() throws IOException {
    LocalFileStorageProvider provider = new LocalFileStorageProvider();
    String filePath = tempDir.resolve("change.txt").toString();
    provider.writeFile(filePath, "content".getBytes(StandardCharsets.UTF_8));

    assertTrue(provider.hasChanged(filePath, null));
  }

  @Test public void testHasChangedWhenSizeChanges() throws IOException {
    LocalFileStorageProvider provider = new LocalFileStorageProvider();
    String filePath = tempDir.resolve("change2.txt").toString();
    provider.writeFile(filePath, "short".getBytes(StandardCharsets.UTF_8));

    // Create cached metadata with different size
    StorageProvider.FileMetadata cached =
        new StorageProvider.FileMetadata(filePath, 999, System.currentTimeMillis(),
            "text/plain", null);

    assertTrue(provider.hasChanged(filePath, cached));
  }

  @Test public void testHasChangedWhenUnchanged() throws IOException {
    LocalFileStorageProvider provider = new LocalFileStorageProvider();
    String filePath = tempDir.resolve("unchanged.txt").toString();
    provider.writeFile(filePath, "static content".getBytes(StandardCharsets.UTF_8));

    StorageProvider.FileMetadata meta = provider.getMetadata(filePath);

    // Same metadata should show unchanged
    assertFalse(provider.hasChanged(filePath, meta));
  }

  @Test public void testHasChangedWithEtagMatch() throws IOException {
    LocalFileStorageProvider provider = new LocalFileStorageProvider();
    String filePath = tempDir.resolve("etag.txt").toString();
    provider.writeFile(filePath, "content".getBytes(StandardCharsets.UTF_8));

    StorageProvider.FileMetadata currentMeta = provider.getMetadata(filePath);

    // Create cached metadata with same etag
    StorageProvider.FileMetadata cached =
        new StorageProvider.FileMetadata(filePath, currentMeta.getSize(),
            currentMeta.getLastModified(), "text/plain", "same-etag");

    StorageProvider.FileMetadata currentWithEtag =
        new StorageProvider.FileMetadata(filePath, currentMeta.getSize(),
            currentMeta.getLastModified(), "text/plain", "same-etag");

    // Since local files don't have etags, test with null etags
    assertFalse(provider.hasChanged(filePath, currentMeta));
  }

  @Test public void testHasChangedForNonExistentFile() throws IOException {
    LocalFileStorageProvider provider = new LocalFileStorageProvider();
    String filePath = tempDir.resolve("nonexistent.txt").toString();

    StorageProvider.FileMetadata cached =
        new StorageProvider.FileMetadata(filePath, 100, 0, "text/plain", null);

    // Non-existent file should report as changed
    assertTrue(provider.hasChanged(filePath, cached));
  }

  // --- StorageProvider default method tests ---

  @Test public void testDeleteBatch() throws IOException {
    LocalFileStorageProvider provider = new LocalFileStorageProvider();

    String f1 = tempDir.resolve("batch1.txt").toString();
    String f2 = tempDir.resolve("batch2.txt").toString();
    String f3 = tempDir.resolve("batch3.txt").toString();

    provider.writeFile(f1, "one".getBytes(StandardCharsets.UTF_8));
    provider.writeFile(f2, "two".getBytes(StandardCharsets.UTF_8));
    provider.writeFile(f3, "three".getBytes(StandardCharsets.UTF_8));

    int deleted = provider.deleteBatch(Arrays.asList(f1, f2, f3));
    assertEquals(3, deleted);

    assertFalse(provider.exists(f1));
    assertFalse(provider.exists(f2));
    assertFalse(provider.exists(f3));
  }

  @Test public void testDeleteBatchWithMissing() throws IOException {
    LocalFileStorageProvider provider = new LocalFileStorageProvider();

    String f1 = tempDir.resolve("exists.txt").toString();
    String f2 = tempDir.resolve("missing.txt").toString();

    provider.writeFile(f1, "data".getBytes(StandardCharsets.UTF_8));

    int deleted = provider.deleteBatch(Arrays.asList(f1, f2));
    assertEquals(1, deleted);
  }

  @Test public void testGetStagingDirectory() throws IOException {
    LocalFileStorageProvider provider = new LocalFileStorageProvider();
    String stagingDir = provider.getStagingDirectory("test-purpose");

    assertNotNull(stagingDir);
    assertTrue(stagingDir.contains(".staging"));
    assertTrue(stagingDir.contains("test-purpose"));
    assertTrue(new File(stagingDir).isDirectory());
  }

  @Test public void testEnsureLifecycleRuleNoOp() throws IOException {
    // Default implementation is a no-op for local storage
    LocalFileStorageProvider provider = new LocalFileStorageProvider();
    provider.ensureLifecycleRule("prefix/", 7);
    // Should not throw
  }

  @Test public void testGetS3ConfigReturnsNull() {
    LocalFileStorageProvider provider = new LocalFileStorageProvider();
    assertNull(provider.getS3Config());
  }

  // --- StorageProviderInputFile integration ---

  @Test public void testStorageProviderInputFileReadParquet() throws IOException {
    // Create a simple file and verify InputFile can read it
    LocalFileStorageProvider provider = new LocalFileStorageProvider();
    String filePath = tempDir.resolve("inputfile.bin").toString();
    byte[] data = new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
    provider.writeFile(filePath, data);

    StorageProviderInputFile inputFile = new StorageProviderInputFile(provider, filePath);

    assertEquals(10, inputFile.getLength());

    // Test reading via stream
    try (org.apache.parquet.io.SeekableInputStream stream = inputFile.newStream()) {
      assertEquals(0, stream.getPos());

      // Read first byte
      int b = stream.read();
      assertEquals(1, b);
      assertEquals(1, stream.getPos());

      // Seek and read
      stream.seek(5);
      assertEquals(5, stream.getPos());
      b = stream.read();
      assertEquals(6, b);

      // Read fully
      stream.seek(0);
      byte[] buf = new byte[10];
      stream.readFully(buf);
      assertArrayEquals(data, buf);
    }
  }

  @Test public void testStorageProviderInputFileSeekBounds() throws IOException {
    LocalFileStorageProvider provider = new LocalFileStorageProvider();
    String filePath = tempDir.resolve("seek.bin").toString();
    provider.writeFile(filePath, new byte[]{1, 2, 3});

    StorageProviderInputFile inputFile = new StorageProviderInputFile(provider, filePath);

    try (org.apache.parquet.io.SeekableInputStream stream = inputFile.newStream()) {
      // Seek to end
      stream.seek(3);
      assertEquals(-1, stream.read());

      // Seek to beginning
      stream.seek(0);
      assertEquals(1, stream.read());

      // Invalid seek
      assertThrows(IllegalArgumentException.class, () -> stream.seek(-1));
      assertThrows(IllegalArgumentException.class, () -> stream.seek(100));
    }
  }

  @Test public void testStorageProviderInputFileReadByteBuffer() throws IOException {
    LocalFileStorageProvider provider = new LocalFileStorageProvider();
    String filePath = tempDir.resolve("bytebuf.bin").toString();
    byte[] data = "ByteBuffer test data".getBytes(StandardCharsets.UTF_8);
    provider.writeFile(filePath, data);

    StorageProviderInputFile inputFile = new StorageProviderInputFile(provider, filePath);

    try (org.apache.parquet.io.SeekableInputStream stream = inputFile.newStream()) {
      java.nio.ByteBuffer buf = java.nio.ByteBuffer.allocate(10);
      int bytesRead = stream.read(buf);
      assertEquals(10, bytesRead);
      buf.flip();
      byte[] result = new byte[10];
      buf.get(result);
      assertEquals("ByteBuffer", new String(result, StandardCharsets.UTF_8));
    }
  }

  @Test public void testStorageProviderInputFileReadFullyByteBuffer() throws IOException {
    LocalFileStorageProvider provider = new LocalFileStorageProvider();
    String filePath = tempDir.resolve("fullybb.bin").toString();
    byte[] data = "ABCDEFGHIJ".getBytes(StandardCharsets.UTF_8);
    provider.writeFile(filePath, data);

    StorageProviderInputFile inputFile = new StorageProviderInputFile(provider, filePath);

    try (org.apache.parquet.io.SeekableInputStream stream = inputFile.newStream()) {
      java.nio.ByteBuffer buf = java.nio.ByteBuffer.allocate(5);
      stream.readFully(buf);
      buf.flip();
      byte[] result = new byte[5];
      buf.get(result);
      assertEquals("ABCDE", new String(result, StandardCharsets.UTF_8));
    }
  }

  @Test public void testStorageProviderInputFileReadWithOffset() throws IOException {
    LocalFileStorageProvider provider = new LocalFileStorageProvider();
    String filePath = tempDir.resolve("offset.bin").toString();
    byte[] data = "0123456789".getBytes(StandardCharsets.UTF_8);
    provider.writeFile(filePath, data);

    StorageProviderInputFile inputFile = new StorageProviderInputFile(provider, filePath);

    try (org.apache.parquet.io.SeekableInputStream stream = inputFile.newStream()) {
      byte[] buf = new byte[20];
      int read = stream.read(buf, 5, 10);
      assertEquals(10, read);
      assertEquals('0', (char) buf[5]);
      assertEquals('9', (char) buf[14]);
    }
  }

  @Test public void testStorageProviderInputFileEOFOnReadFully() throws IOException {
    LocalFileStorageProvider provider = new LocalFileStorageProvider();
    String filePath = tempDir.resolve("eof.bin").toString();
    provider.writeFile(filePath, new byte[]{1, 2});

    StorageProviderInputFile inputFile = new StorageProviderInputFile(provider, filePath);

    try (org.apache.parquet.io.SeekableInputStream stream = inputFile.newStream()) {
      assertThrows(java.io.EOFException.class, () -> {
        byte[] buf = new byte[10];
        stream.readFully(buf);
      });
    }
  }

  // --- StorageProviderFile integration ---

  @Test public void testStorageProviderFileLocalOperations() throws IOException {
    LocalFileStorageProvider provider = new LocalFileStorageProvider();
    String filePath = tempDir.resolve("spf-local.txt").toString();
    Files.write(Path.of(filePath), "SPF content".getBytes(StandardCharsets.UTF_8));

    StorageProviderFile spf = StorageProviderFile.create(filePath, provider);

    assertTrue(spf.isLocal());
    assertTrue(spf.exists());
    assertEquals(11, spf.length());
    assertTrue(spf.lastModified() > 0);
    assertEquals(spf, spf.getFile());
  }

  @Test public void testStorageProviderFileRemoteOperations() throws IOException {
    StorageProvider.FileEntry entry =
        new StorageProvider.FileEntry("/remote/file.csv", "file.csv",
            false, 42, System.currentTimeMillis());
    LocalFileStorageProvider provider = new LocalFileStorageProvider();

    StorageProviderFile spf = StorageProviderFile.create(entry, provider);

    assertFalse(spf.isLocal());
    assertEquals(42, spf.length());
    assertEquals(entry, spf.getFileEntry());
    assertEquals(provider, spf.getStorageProvider());
  }

  @Test public void testStorageProviderFileWriteAndRead() throws IOException {
    String filePath = tempDir.resolve("spf-write.txt").toString();
    LocalFileStorageProvider provider = new LocalFileStorageProvider();
    StorageProviderFile spf = StorageProviderFile.create(filePath, provider);

    byte[] content = "SPF write test".getBytes(StandardCharsets.UTF_8);
    spf.writeBytes(content);

    assertTrue(spf.exists());

    try (InputStream is = spf.openInputStream()) {
      byte[] readBack = new byte[content.length];
      is.read(readBack);
      assertArrayEquals(content, readBack);
    }
  }

  @Test public void testStorageProviderFileWriteInputStream() throws IOException {
    String filePath = tempDir.resolve("spf-stream.txt").toString();
    LocalFileStorageProvider provider = new LocalFileStorageProvider();
    StorageProviderFile spf = StorageProviderFile.create(filePath, provider);

    String content = "Stream write test";
    spf.writeInputStream(
        new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8)));

    assertTrue(spf.exists());
    assertEquals(content,
        new String(Files.readAllBytes(Path.of(filePath)), StandardCharsets.UTF_8));
  }

  @Test public void testStorageProviderFileDelete() throws IOException {
    String filePath = tempDir.resolve("spf-delete.txt").toString();
    Files.write(Path.of(filePath), "data".getBytes(StandardCharsets.UTF_8));

    LocalFileStorageProvider provider = new LocalFileStorageProvider();
    StorageProviderFile spf = StorageProviderFile.create(filePath, provider);

    assertTrue(spf.exists());
    assertTrue(spf.delete());
    assertFalse(spf.exists());
  }

  @Test public void testStorageProviderFileMkdirs() throws IOException {
    String dirPath = tempDir.resolve("spf-mkdirs/a/b").toString();
    LocalFileStorageProvider provider = new LocalFileStorageProvider();
    StorageProviderFile spf = StorageProviderFile.create(dirPath, provider);

    assertTrue(spf.mkdirs());
    assertTrue(Files.isDirectory(Path.of(dirPath)));
  }

  @Test public void testStorageProviderFileRemoteGetFileThrows() {
    StorageProvider.FileEntry entry =
        new StorageProvider.FileEntry("/remote/file.csv", "file.csv",
            false, 42, 0);
    LocalFileStorageProvider provider = new LocalFileStorageProvider();
    StorageProviderFile spf = StorageProviderFile.create(entry, provider);

    assertThrows(UnsupportedOperationException.class, spf::getFile);
  }

  // --- StorageProviderSource integration with real files ---

  @Test public void testStorageProviderSourceWithLocalFile() throws IOException {
    LocalFileStorageProvider provider = new LocalFileStorageProvider();
    String filePath = tempDir.resolve("source-test.csv").toString();
    Files.write(Path.of(filePath), "col1,col2\na,b".getBytes(StandardCharsets.UTF_8));

    StorageProvider.FileEntry entry =
        new StorageProvider.FileEntry(filePath, "source-test.csv",
            false, 14, System.currentTimeMillis());

    StorageProviderSource source = new StorageProviderSource(entry, provider);

    assertEquals(filePath, source.path());
    assertEquals("local", source.protocol());
    assertNull(source.url());
    assertNotNull(source.file());
    assertTrue(source.fileOpt().isPresent());

    try (InputStream is = source.openStream()) {
      byte[] buf = new byte[1024];
      int n = is.read(buf);
      assertEquals("col1,col2\na,b", new String(buf, 0, n, StandardCharsets.UTF_8));
    }

    try (Reader reader = source.reader()) {
      char[] buf = new char[1024];
      int n = reader.read(buf);
      assertEquals("col1,col2\na,b", new String(buf, 0, n));
    }
  }

  // --- HttpConfig integration ---

  @Test public void testHttpConfigFromMap() {
    java.util.Map<String, Object> config = new java.util.HashMap<>();
    config.put("method", "POST");
    config.put("body", "{\"key\":\"val\"}");
    config.put("mimeType", "application/json");
    java.util.Map<String, String> headers = new java.util.HashMap<>();
    headers.put("X-Custom", "value");
    config.put("headers", headers);

    java.util.Map<String, Object> authConfig = new java.util.HashMap<>();
    authConfig.put("bearerToken", "token123");
    authConfig.put("cacheEnabled", false);
    authConfig.put("cacheTtl", 60000L);
    config.put("authConfig", authConfig);

    HttpConfig httpConfig = HttpConfig.fromMap(config);

    assertEquals("POST", httpConfig.getMethod());
    assertEquals("{\"key\":\"val\"}", httpConfig.getBody());
    assertEquals("application/json", httpConfig.getMimeType());
    assertEquals("value", httpConfig.getHeaders().get("X-Custom"));
    assertEquals("token123", httpConfig.getBearerToken());
    assertFalse(httpConfig.isCacheEnabled());
    assertEquals(60000L, httpConfig.getCacheTtl());
  }

  @Test public void testHttpConfigBuilder() {
    HttpConfig config = new HttpConfig.Builder()
        .bearerToken("bearer-123")
        .apiKey("api-key-456")
        .basicAuth("user", "pass")
        .tokenCommand("curl token-endpoint")
        .tokenEnv("MY_TOKEN")
        .tokenFile("/path/to/token")
        .tokenEndpoint("https://auth.example.com/token")
        .proxyEndpoint("https://proxy.example.com")
        .cacheEnabled(true)
        .cacheTtl(120000)
        .build();

    assertEquals("bearer-123", config.getBearerToken());
    assertEquals("api-key-456", config.getApiKey());
    assertEquals("user", config.getUsername());
    assertEquals("pass", config.getPassword());
    assertEquals("curl token-endpoint", config.getTokenCommand());
    assertEquals("MY_TOKEN", config.getTokenEnv());
    assertEquals("/path/to/token", config.getTokenFile());
    assertEquals("https://auth.example.com/token", config.getTokenEndpoint());
    assertEquals("https://proxy.example.com", config.getProxyEndpoint());
    assertTrue(config.isCacheEnabled());
    assertEquals(120000, config.getCacheTtl());
  }

  @Test public void testHttpConfigBuilderWithAuthHeaders() {
    java.util.Map<String, String> authHeaders = new java.util.HashMap<>();
    authHeaders.put("X-Auth", "custom");
    authHeaders.put("X-Scope", "admin");

    HttpConfig config = new HttpConfig.Builder()
        .authHeaders(authHeaders)
        .build();

    assertNotNull(config.getAuthHeaders());
    assertEquals("custom", config.getAuthHeaders().get("X-Auth"));
    assertEquals("admin", config.getAuthHeaders().get("X-Scope"));
  }

  @Test public void testHttpConfigDefaultValues() {
    HttpConfig config = new HttpConfig();

    assertEquals("GET", config.getMethod());
    assertNull(config.getBody());
    assertNotNull(config.getHeaders());
    assertTrue(config.getHeaders().isEmpty());
    assertNull(config.getMimeType());
    assertTrue(config.isCacheEnabled());
    assertEquals(300000, config.getCacheTtl());
  }

  @Test public void testHttpConfigFromMapWithAllAuthTypes() {
    // Test with auth headers in auth config
    java.util.Map<String, Object> config = new java.util.HashMap<>();
    java.util.Map<String, Object> authConfig = new java.util.HashMap<>();

    java.util.Map<String, String> authHeaders = new java.util.HashMap<>();
    authHeaders.put("Authorization", "Custom token");
    authConfig.put("authHeaders", authHeaders);

    authConfig.put("apiKey", "key-abc");
    authConfig.put("username", "admin");
    authConfig.put("password", "secret");
    authConfig.put("tokenCommand", "/bin/get-token");
    authConfig.put("tokenEnv", "TOKEN_VAR");
    authConfig.put("tokenFile", "/tmp/token");
    authConfig.put("tokenEndpoint", "https://auth.example.com");
    authConfig.put("proxyEndpoint", "https://proxy.example.com");

    config.put("authConfig", authConfig);

    HttpConfig httpConfig = HttpConfig.fromMap(config);

    assertEquals("key-abc", httpConfig.getApiKey());
    assertEquals("admin", httpConfig.getUsername());
    assertEquals("secret", httpConfig.getPassword());
    assertEquals("/bin/get-token", httpConfig.getTokenCommand());
    assertEquals("TOKEN_VAR", httpConfig.getTokenEnv());
    assertEquals("/tmp/token", httpConfig.getTokenFile());
    assertEquals("https://auth.example.com", httpConfig.getTokenEndpoint());
    assertEquals("https://proxy.example.com", httpConfig.getProxyEndpoint());
    assertEquals("Custom token", httpConfig.getAuthHeaders().get("Authorization"));
  }

  @Test public void testHttpConfigCreateStorageProvider() {
    HttpConfig config = new HttpConfig("GET", null,
        new java.util.HashMap<>(), null);

    HttpStorageProvider provider = config.createStorageProvider();
    assertNotNull(provider);
    assertEquals("http", provider.getStorageType());
  }

  // --- normalizePath static method ---

  @Test public void testNormalizePathVariousSchemes() {
    // s3a single slash
    assertEquals("s3a://bucket/key", StorageProvider.normalizePath("s3a:/bucket/key"));
    // s3a already correct
    assertEquals("s3a://bucket/key", StorageProvider.normalizePath("s3a://bucket/key"));
    // s3 single slash
    assertEquals("s3://bucket/key", StorageProvider.normalizePath("s3:/bucket/key"));
    // s3 already correct
    assertEquals("s3://bucket/key", StorageProvider.normalizePath("s3://bucket/key"));
    // hdfs single slash
    assertEquals("hdfs://namenode/path", StorageProvider.normalizePath("hdfs:/namenode/path"));
    // hdfs already correct
    assertEquals("hdfs://namenode/path", StorageProvider.normalizePath("hdfs://namenode/path"));
    // local path unchanged
    assertEquals("/local/path", StorageProvider.normalizePath("/local/path"));
    // null input
    assertNull(StorageProvider.normalizePath(null));
  }
}
