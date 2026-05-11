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
package org.apache.calcite.adapter.file.etl.cache;

import org.apache.calcite.adapter.file.storage.StorageProvider;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Unit tests for {@link CacheResolver}.
 */
@Tag("unit")
public class CacheResolverTest {

  @TempDir
  Path tempDir;

  @Test void testResolveFromLocalCache() throws IOException {
    Path cacheDir = tempDir.resolve("cache");
    Files.createDirectories(cacheDir);

    // Create a cached file locally
    Path cachedFile = cacheDir.resolve("data/file.csv");
    Files.createDirectories(cachedFile.getParent());
    Files.write(cachedFile, "cached content".getBytes(StandardCharsets.UTF_8));

    CacheResolver resolver = new CacheResolver(cacheDir.toString(), null, "schema");

    String result = resolver.resolve("data/file.csv");

    assertNotNull(result);
    assertEquals(cacheDir.toString() + "/data/file.csv", result);
  }

  @Test void testResolveReturnsNullForMissingLocalFile() {
    Path cacheDir = tempDir.resolve("cache");

    CacheResolver resolver = new CacheResolver(cacheDir.toString(), null, "schema");

    String result = resolver.resolve("data/nonexistent.csv");

    assertNull(result);
  }

  @Test void testResolveReturnsNullWhenNoStorageProvider() throws IOException {
    Path cacheDir = tempDir.resolve("cache");
    Files.createDirectories(cacheDir);

    CacheResolver resolver = new CacheResolver(cacheDir.toString(), null, "schema");

    String result = resolver.resolve("not-local.csv");

    assertNull(result);
  }

  @Test void testResolveFromBundledEntry() throws IOException {
    Path cacheDir = tempDir.resolve("cache");
    Files.createDirectories(cacheDir);

    // Create a mock storage provider with bundle data
    MockBundleStorageProvider provider = new MockBundleStorageProvider();

    // Set up index file listing a bundled entry
    String indexContent =
        "{\"key\":\"data/bundled.csv\",\"offset\":0,\"length\":14,\"ts\":1000}\n";
    provider.addFile("bundles/schema/run-001.idx.jsonl", indexContent);

    // Set up bundle content
    provider.addFile("bundles/schema/run-001.bin", "bundled content");

    // Set up range read to return the right bytes
    provider.addRangeData("bundles/schema/run-001.bin", 0, 14, "bundled content");

    CacheResolver resolver = new CacheResolver(cacheDir.toString(), provider, "schema");

    String result = resolver.resolve("data/bundled.csv");

    assertNotNull(result);
    // Verify the file was promoted to local cache
    Path localFile = cacheDir.resolve("data/bundled.csv");
    assertTrue(Files.exists(localFile));
    String content = new String(Files.readAllBytes(localFile), StandardCharsets.UTF_8);
    assertEquals("bundled content", content);
  }

  @Test void testResolveFromIndividualObject() throws IOException {
    Path cacheDir = tempDir.resolve("cache");
    Files.createDirectories(cacheDir);

    MockBundleStorageProvider provider = new MockBundleStorageProvider();

    // Index with individual object entry
    String indexContent =
        "{\"key\":\"shapes/large.shp\",\"storage\":\"object\",\"length\":11,\"ts\":1000}\n";
    provider.addFile("bundles/schema/run-001.idx.jsonl", indexContent);

    // Set up the individual object
    provider.addFile("objects/schema/shapes/large.shp", "large object");

    CacheResolver resolver = new CacheResolver(cacheDir.toString(), provider, "schema");

    String result = resolver.resolve("shapes/large.shp");

    assertNotNull(result);
    // Verify the file was promoted to local cache
    Path localFile = cacheDir.resolve("shapes/large.shp");
    assertTrue(Files.exists(localFile));
    String content = new String(Files.readAllBytes(localFile), StandardCharsets.UTF_8);
    assertEquals("large object", content);
  }

  @Test void testResolveReturnsNullWhenNotInIndex() throws IOException {
    Path cacheDir = tempDir.resolve("cache");
    Files.createDirectories(cacheDir);

    MockBundleStorageProvider provider = new MockBundleStorageProvider();

    // Empty index
    String indexContent =
        "{\"key\":\"other.csv\",\"offset\":0,\"length\":5,\"ts\":1000}\n";
    provider.addFile("bundles/schema/run-001.idx.jsonl", indexContent);

    CacheResolver resolver = new CacheResolver(cacheDir.toString(), provider, "schema");

    String result = resolver.resolve("not-in-index.csv");

    assertNull(result);
  }

  @Test void testResolvePrefersLocalOverRemote() throws IOException {
    Path cacheDir = tempDir.resolve("cache");
    Files.createDirectories(cacheDir);

    // Create local file
    Path localFile = cacheDir.resolve("file.csv");
    Files.write(localFile, "local content".getBytes(StandardCharsets.UTF_8));

    // Set up remote with different content
    MockBundleStorageProvider provider = new MockBundleStorageProvider();
    String indexContent =
        "{\"key\":\"file.csv\",\"offset\":0,\"length\":14,\"ts\":1000}\n";
    provider.addFile("bundles/schema/run-001.idx.jsonl", indexContent);
    provider.addRangeData("bundles/schema/run-001.bin", 0, 14, "remote content");

    CacheResolver resolver = new CacheResolver(cacheDir.toString(), provider, "schema");

    String result = resolver.resolve("file.csv");

    // Should return local path without touching remote
    assertNotNull(result);
    String content = new String(Files.readAllBytes(localFile), StandardCharsets.UTF_8);
    assertEquals("local content", content);
  }

  @Test void testResolveIgnoresEmptyLocalFile() throws IOException {
    Path cacheDir = tempDir.resolve("cache");
    Files.createDirectories(cacheDir);

    // Create empty local file (0 bytes)
    Path emptyFile = cacheDir.resolve("empty.csv");
    Files.createFile(emptyFile);

    MockBundleStorageProvider provider = new MockBundleStorageProvider();
    String indexContent =
        "{\"key\":\"empty.csv\",\"offset\":0,\"length\":4,\"ts\":1000}\n";
    provider.addFile("bundles/schema/run-001.idx.jsonl", indexContent);
    provider.addRangeData("bundles/schema/run-001.bin", 0, 4, "data");

    CacheResolver resolver = new CacheResolver(cacheDir.toString(), provider, "schema");

    String result = resolver.resolve("empty.csv");

    // Should fetch from remote since local file is empty
    assertNotNull(result);
    String content = new String(Files.readAllBytes(emptyFile), StandardCharsets.UTF_8);
    assertEquals("data", content);
  }

  @Test void testResolveHandlesRemoteFailure() throws IOException {
    Path cacheDir = tempDir.resolve("cache");
    Files.createDirectories(cacheDir);

    // Provider that fails on range reads
    MockBundleStorageProvider provider = new MockBundleStorageProvider() {
      @Override
      public byte[] readRange(String path, long offset, long length) throws IOException {
        throw new IOException("Simulated network failure");
      }
    };

    String indexContent =
        "{\"key\":\"file.csv\",\"offset\":0,\"length\":4,\"ts\":1000}\n";
    provider.addFile("bundles/schema/run-001.idx.jsonl", indexContent);

    CacheResolver resolver = new CacheResolver(cacheDir.toString(), provider, "schema");

    // Should return null when remote fetch fails
    String result = resolver.resolve("file.csv");
    assertNull(result);
  }

  @Test void testResolveWithEmptyIndex() throws IOException {
    Path cacheDir = tempDir.resolve("cache");
    Files.createDirectories(cacheDir);

    // Provider with no index files
    MockBundleStorageProvider provider = new MockBundleStorageProvider();

    CacheResolver resolver = new CacheResolver(cacheDir.toString(), provider, "schema");

    // Should return null for everything
    assertNull(resolver.resolve("anything.csv"));
    // Second call should also work (tests lazy loading / caching of null index)
    assertNull(resolver.resolve("other.csv"));
  }

  @Test void testResolveLazyIndexLoading() throws IOException {
    Path cacheDir = tempDir.resolve("cache");
    Files.createDirectories(cacheDir);

    MockBundleStorageProvider provider = new MockBundleStorageProvider();
    String indexContent =
        "{\"key\":\"file.csv\",\"offset\":0,\"length\":4,\"ts\":1000}\n";
    provider.addFile("bundles/schema/run-001.idx.jsonl", indexContent);
    provider.addRangeData("bundles/schema/run-001.bin", 0, 4, "data");

    CacheResolver resolver = new CacheResolver(cacheDir.toString(), provider, "schema");

    // First resolve triggers index loading
    String result1 = resolver.resolve("file.csv");
    assertNotNull(result1);

    // Remove the local file so we know second call uses cached index
    Files.delete(cacheDir.resolve("file.csv"));

    // Second resolve should use the already-loaded index
    String result2 = resolver.resolve("file.csv");
    assertNotNull(result2);
  }

  private static boolean assertTrue(boolean condition) {
    org.junit.jupiter.api.Assertions.assertTrue(condition);
    return condition;
  }

  /**
   * StorageProvider that supports bundle operations for testing CacheResolver.
   */
  private static class MockBundleStorageProvider implements StorageProvider {
    private final Map<String, String> files = new HashMap<String, String>();
    private final Map<String, byte[]> rangeData = new HashMap<String, byte[]>();

    void addFile(String path, String content) {
      files.put(path, content);
    }

    void addRangeData(String path, long offset, long length, String data) {
      rangeData.put(path + ":" + offset + ":" + length, data.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public byte[] readRange(String path, long offset, long length) throws IOException {
      byte[] data = rangeData.get(path + ":" + offset + ":" + length);
      if (data != null) {
        return data;
      }
      // Fall back to reading from file content
      String content = files.get(path);
      if (content == null) {
        throw new IOException("File not found: " + path);
      }
      byte[] bytes = content.getBytes(StandardCharsets.UTF_8);
      int off = (int) offset;
      int len = (int) Math.min(length, bytes.length - off);
      byte[] result = new byte[len];
      System.arraycopy(bytes, off, result, 0, len);
      return result;
    }

    @Override
    public List<FileEntry> listFiles(String path, boolean recursive) throws IOException {
      List<FileEntry> entries = new ArrayList<FileEntry>();
      for (String filePath : files.keySet()) {
        if (filePath.startsWith(path)) {
          String name = filePath.substring(filePath.lastIndexOf('/') + 1);
          entries.add(new FileEntry(filePath, name, false,
              files.get(filePath).length(), 0));
        }
      }
      return entries;
    }

    @Override
    public FileMetadata getMetadata(String path) throws IOException {
      String content = files.get(path);
      if (content == null) {
        throw new IOException("File not found: " + path);
      }
      return new FileMetadata(path, content.length(), 0, "application/octet-stream", null);
    }

    @Override
    public InputStream openInputStream(String path) throws IOException {
      String content = files.get(path);
      if (content == null) {
        throw new IOException("File not found: " + path);
      }
      return new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public Reader openReader(String path) throws IOException {
      return new java.io.InputStreamReader(openInputStream(path), StandardCharsets.UTF_8);
    }

    @Override
    public boolean exists(String path) {
      return files.containsKey(path);
    }

    @Override
    public boolean isDirectory(String path) {
      return false;
    }

    @Override
    public String getStorageType() {
      return "mock-bundle";
    }

    @Override
    public String resolvePath(String basePath, String relativePath) {
      return basePath + "/" + relativePath;
    }
  }
}
