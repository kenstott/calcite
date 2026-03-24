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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link BundleArchiver}.
 */
@Tag("unit")
public class BundleArchiverTest {

  @TempDir
  Path tempDir;

  @Test void testDefaultSizeThreshold() {
    assertEquals(1L * 1024 * 1024, BundleArchiver.DEFAULT_SIZE_THRESHOLD);
  }

  @Test void testArchiveNonExistentDirectory() {
    RecordingStorageProvider provider = new RecordingStorageProvider();

    BundleArchiver.archive("/nonexistent/dir", provider, "schema", "run-001");

    // No files should be written since directory doesn't exist
    assertTrue(provider.writtenFiles.isEmpty());
  }

  @Test void testArchiveEmptyDirectory() throws IOException {
    Path cacheDir = tempDir.resolve("empty-cache");
    Files.createDirectories(cacheDir);

    RecordingStorageProvider provider = new RecordingStorageProvider();

    BundleArchiver.archive(cacheDir.toString(), provider, "schema", "run-001");

    // No files should be written since directory is empty
    assertTrue(provider.writtenFiles.isEmpty());
  }

  @Test void testArchiveSmallFiles() throws IOException {
    Path cacheDir = tempDir.resolve("cache");
    Files.createDirectories(cacheDir);

    // Create small files (below default threshold)
    Files.write(cacheDir.resolve("file1.csv"), "data1".getBytes(StandardCharsets.UTF_8));
    Files.write(cacheDir.resolve("file2.csv"), "data2".getBytes(StandardCharsets.UTF_8));

    RecordingStorageProvider provider = new RecordingStorageProvider();

    BundleArchiver.archive(cacheDir.toString(), provider, "myschema", "run-20260310T1423");

    // Should produce a chunked bundle file and an index file
    assertTrue(provider.writtenFiles.containsKey("bundles/myschema/run-20260310T1423-001.bin"));
    assertTrue(provider.writtenFiles.containsKey("bundles/myschema/run-20260310T1423.idx.jsonl"));

    // Verify index content
    String indexContent = new String(
        provider.writtenFiles.get("bundles/myschema/run-20260310T1423.idx.jsonl"),
        StandardCharsets.UTF_8);
    assertTrue(indexContent.contains("file1.csv"));
    assertTrue(indexContent.contains("file2.csv"));

    // Verify bundle content contains both files' data
    byte[] bundleContent = provider.writtenFiles.get("bundles/myschema/run-20260310T1423-001.bin");
    String bundleStr = new String(bundleContent, StandardCharsets.UTF_8);
    assertTrue(bundleStr.contains("data1"));
    assertTrue(bundleStr.contains("data2"));
  }

  @Test void testArchiveLargeFiles() throws IOException {
    Path cacheDir = tempDir.resolve("cache");
    Files.createDirectories(cacheDir);

    // Create a "large" file (above threshold)
    byte[] largeContent = new byte[100];
    for (int i = 0; i < 100; i++) {
      largeContent[i] = (byte) ('A' + (i % 26));
    }
    Files.write(cacheDir.resolve("large.shp"), largeContent);

    RecordingStorageProvider provider = new RecordingStorageProvider();

    // Use very low threshold so our file is "large"
    BundleArchiver.archive(cacheDir.toString(), provider, "schema", "run-001", 50);

    // Large file should be uploaded individually
    assertTrue(provider.writtenFiles.containsKey("objects/schema/large.shp"));

    // Index should contain the entry
    assertTrue(provider.writtenFiles.containsKey("bundles/schema/run-001.idx.jsonl"));
    String indexContent = new String(
        provider.writtenFiles.get("bundles/schema/run-001.idx.jsonl"),
        StandardCharsets.UTF_8);
    assertTrue(indexContent.contains("large.shp"));
    assertTrue(indexContent.contains("\"storage\":\"object\""));
  }

  @Test void testArchiveMixedFiles() throws IOException {
    Path cacheDir = tempDir.resolve("cache");
    Files.createDirectories(cacheDir);

    // Small file
    Files.write(cacheDir.resolve("small.csv"), "small data".getBytes(StandardCharsets.UTF_8));

    // Large file
    byte[] largeContent = new byte[200];
    java.util.Arrays.fill(largeContent, (byte) 'X');
    Files.write(cacheDir.resolve("big.shp"), largeContent);

    RecordingStorageProvider provider = new RecordingStorageProvider();

    BundleArchiver.archive(cacheDir.toString(), provider, "schema", "run-001", 100);

    // Bundle for small files (chunked naming: run-001-001.bin)
    assertTrue(provider.writtenFiles.containsKey("bundles/schema/run-001-001.bin"));

    // Individual upload for large file
    assertTrue(provider.writtenFiles.containsKey("objects/schema/big.shp"));

    // Index should contain both
    String indexContent = new String(
        provider.writtenFiles.get("bundles/schema/run-001.idx.jsonl"),
        StandardCharsets.UTF_8);
    assertTrue(indexContent.contains("small.csv"));
    assertTrue(indexContent.contains("big.shp"));
  }

  @Test void testArchiveNestedDirectories() throws IOException {
    Path cacheDir = tempDir.resolve("cache");
    Path subDir = cacheDir.resolve("subdir");
    Files.createDirectories(subDir);

    Files.write(cacheDir.resolve("top.csv"), "top level".getBytes(StandardCharsets.UTF_8));
    Files.write(subDir.resolve("nested.csv"), "nested data".getBytes(StandardCharsets.UTF_8));

    RecordingStorageProvider provider = new RecordingStorageProvider();

    BundleArchiver.archive(cacheDir.toString(), provider, "schema", "run-001");

    // Both files should be archived
    String indexContent = new String(
        provider.writtenFiles.get("bundles/schema/run-001.idx.jsonl"),
        StandardCharsets.UTF_8);
    assertTrue(indexContent.contains("top.csv"));
    assertTrue(indexContent.contains("nested.csv"));
  }

  @Test void testArchiveIgnoresEmptyFiles() throws IOException {
    Path cacheDir = tempDir.resolve("cache");
    Files.createDirectories(cacheDir);

    // Create an empty file (0 bytes) - use a unique name to avoid macOS metadata confusion
    Path emptyFile = cacheDir.resolve("zerolen_file.dat");
    Files.createFile(emptyFile);
    assertEquals(0, emptyFile.toFile().length());

    // Create a non-empty file
    Files.write(cacheDir.resolve("nonempty.csv"), "data".getBytes(StandardCharsets.UTF_8));

    RecordingStorageProvider provider = new RecordingStorageProvider();

    BundleArchiver.archive(cacheDir.toString(), provider, "schema", "run-001");

    String indexContent = new String(
        provider.writtenFiles.get("bundles/schema/run-001.idx.jsonl"),
        StandardCharsets.UTF_8);
    // Only non-empty file should be archived
    assertFalse(indexContent.contains("\"key\":\"zerolen_file.dat\""),
        "Zero-length file should not appear in index. Index: " + indexContent);
    assertTrue(indexContent.contains("nonempty.csv"));
  }

  @Test void testArchiveHandlesWriteFailure() throws IOException {
    Path cacheDir = tempDir.resolve("cache");
    Files.createDirectories(cacheDir);
    Files.write(cacheDir.resolve("file.csv"), "data".getBytes(StandardCharsets.UTF_8));

    // Provider that throws on write
    StorageProvider failingProvider = new RecordingStorageProvider() {
      @Override
      public void writeFile(String path, byte[] content) throws IOException {
        throw new IOException("Simulated write failure");
      }
    };

    // Should not throw - failures are logged but not propagated
    BundleArchiver.archive(cacheDir.toString(), failingProvider, "schema", "run-001");
  }

  /**
   * StorageProvider that records all write operations for verification.
   */
  private static class RecordingStorageProvider implements StorageProvider {
    final Map<String, byte[]> writtenFiles = new HashMap<String, byte[]>();

    @Override
    public void writeFile(String path, byte[] content) throws IOException {
      writtenFiles.put(path, content);
    }

    @Override
    public List<FileEntry> listFiles(String path, boolean recursive) throws IOException {
      return new ArrayList<FileEntry>();
    }

    @Override
    public FileMetadata getMetadata(String path) throws IOException {
      throw new IOException("Not supported");
    }

    @Override
    public InputStream openInputStream(String path) throws IOException {
      byte[] content = writtenFiles.get(path);
      if (content == null) {
        throw new IOException("File not found: " + path);
      }
      return new ByteArrayInputStream(content);
    }

    @Override
    public Reader openReader(String path) throws IOException {
      return new java.io.InputStreamReader(openInputStream(path), StandardCharsets.UTF_8);
    }

    @Override
    public boolean exists(String path) {
      return writtenFiles.containsKey(path);
    }

    @Override
    public boolean isDirectory(String path) {
      return false;
    }

    @Override
    public String getStorageType() {
      return "recording";
    }

    @Override
    public String resolvePath(String basePath, String relativePath) {
      return basePath + "/" + relativePath;
    }
  }
}
