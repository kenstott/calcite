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

import org.apache.calcite.adapter.file.etl.cache.BundleEntry;
import org.apache.calcite.adapter.file.etl.cache.BundleIndex;
import org.apache.calcite.adapter.file.etl.cache.CacheResolver;
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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * FILE-091 / FILE-092 — exact-assertion recode of the file-adapter raw-cache tests (hermetic;
 * in-memory fake StorageProvider, no live S3). FILE-091 pins BundleArchiver chunking + JSONL index
 * round-trip + empty-file exclusion; FILE-092 pins CacheResolver local-first tier order, empty-local
 * fall-through, and remote-bundle promotion to local.
 */
@Tag("unit")
public class RawCacheRequirementsTest {

  @TempDir
  Path tempDir;

  @Test @Tag("FILE-091")
  void bundlesSmallFilesIntoChunkedBinWithIndexAndIgnoresEmpty() throws IOException {
    Path cacheDir = tempDir.resolve("cache");
    Files.createDirectories(cacheDir);

    byte[] data1 = "data1".getBytes(StandardCharsets.UTF_8);
    byte[] data2 = "second-file".getBytes(StandardCharsets.UTF_8);
    Files.write(cacheDir.resolve("file1.csv"), data1);
    Files.write(cacheDir.resolve("file2.csv"), data2);

    Path emptyFile = cacheDir.resolve("zerolen_file.dat");
    Files.createFile(emptyFile);
    assertEquals(0, emptyFile.toFile().length());

    RecordingStorageProvider provider = new RecordingStorageProvider();
    org.apache.calcite.adapter.file.etl.cache.BundleArchiver.archive(
        cacheDir.toString(), provider, "myschema", "run-001");

    byte[] chunk = provider.writtenFiles.get("bundles/myschema/run-001-001.bin");
    byte[] indexBytes = provider.writtenFiles.get("bundles/myschema/run-001.idx.jsonl");
    assertNotNull(chunk, "expected chunk bundles/myschema/run-001-001.bin");
    assertNotNull(indexBytes, "expected index bundles/myschema/run-001.idx.jsonl");
    assertEquals(data1.length + data2.length, chunk.length);

    BundleIndex parsed = new BundleIndex();
    parsed.mergeFromJsonl(new String(indexBytes, StandardCharsets.UTF_8), "run-001-001.bin");

    assertEquals(2, parsed.size());
    assertFalse(parsed.contains("zerolen_file.dat"), "zero-length file must not be indexed");

    BundleEntry e1 = parsed.get("file1.csv");
    BundleEntry e2 = parsed.get("file2.csv");
    assertNotNull(e1);
    assertNotNull(e2);
    assertTrue(e1.isBundled());
    assertTrue(e2.isBundled());
    assertEquals("run-001-001.bin", e1.getBundleFile());
    assertEquals(data1.length, e1.getLength());
    assertEquals(data2.length, e2.getLength());
    // Each entry's recorded [offset,length) slice must equal its own bytes, regardless of the
    // (filesystem-dependent) packing order of the two files within the chunk.
    assertEquals("data1", sliceUtf8(chunk, e1.getOffset(), e1.getLength()));
    assertEquals("second-file", sliceUtf8(chunk, e2.getOffset(), e2.getLength()));
    // The two slices are contiguous and non-overlapping: offsets are exactly {0, otherLength}.
    long lo = Math.min(e1.getOffset(), e2.getOffset());
    long hi = Math.max(e1.getOffset(), e2.getOffset());
    assertEquals(0, lo, "first packed file starts at offset 0");
    assertTrue(hi == data1.length || hi == data2.length, "second file starts right after the first");
  }

  @Test @Tag("FILE-091")
  void indexJsonlRoundTripIsLosslessAndLaterOverridesEarlier() {
    BundleIndex original = new BundleIndex();
    original.put("bundled.csv", BundleEntry.bundled("run.bin", 0, 100, 1000));
    original.put("individual.shp", BundleEntry.individual(5000, 2000));

    BundleIndex restored = new BundleIndex();
    restored.mergeFromJsonl(original.toJsonl(), "run.bin");
    assertEquals(original.size(), restored.size());

    BundleEntry rb = restored.get("bundled.csv");
    assertNotNull(rb);
    assertTrue(rb.isBundled());
    assertEquals("run.bin", rb.getBundleFile());
    assertEquals(0, rb.getOffset());
    assertEquals(100, rb.getLength());

    BundleEntry ri = restored.get("individual.shp");
    assertNotNull(ri);
    assertTrue(ri.isIndividualObject());
    assertEquals(5000, ri.getLength());

    BundleIndex merged = new BundleIndex();
    merged.mergeFromJsonl("{\"key\":\"shared.csv\",\"offset\":0,\"length\":100,\"ts\":1000}\n", "run1.bin");
    merged.mergeFromJsonl("{\"key\":\"shared.csv\",\"offset\":500,\"length\":200,\"ts\":2000}\n", "run2.bin");
    assertEquals(1, merged.size());
    BundleEntry winner = merged.get("shared.csv");
    assertNotNull(winner);
    assertEquals("run2.bin", winner.getBundleFile());
    assertEquals(500, winner.getOffset());
    assertEquals(200, winner.getLength());
  }

  @Test @Tag("FILE-092")
  void localNonEmptyFileIsHitAndRemoteIsNotConsulted() throws IOException {
    Path cacheDir = tempDir.resolve("cache");
    Files.createDirectories(cacheDir);
    Path localFile = cacheDir.resolve("file.csv");
    Files.write(localFile, "local content".getBytes(StandardCharsets.UTF_8));

    MockBundleStorageProvider provider = new MockBundleStorageProvider();
    provider.addFile("bundles/schema/run-001.idx.jsonl",
        "{\"key\":\"file.csv\",\"offset\":0,\"length\":14,\"ts\":1000}\n");
    provider.addRangeData("bundles/schema/run-001.bin", 0, 14, "remote content");

    CacheResolver resolver = new CacheResolver(cacheDir.toString(), provider, "schema");
    String result = resolver.resolve("file.csv");

    assertNotNull(result);
    assertEquals(cacheDir.toString() + "/file.csv", result);
    assertEquals("local content", new String(Files.readAllBytes(localFile), StandardCharsets.UTF_8));
  }

  @Test @Tag("FILE-092")
  void emptyLocalFileIsNotHitAndFallsThroughToRemoteBundle() throws IOException {
    Path cacheDir = tempDir.resolve("cache");
    Files.createDirectories(cacheDir);
    Path emptyFile = cacheDir.resolve("empty.csv");
    Files.createFile(emptyFile);
    assertEquals(0, emptyFile.toFile().length());

    MockBundleStorageProvider provider = new MockBundleStorageProvider();
    provider.addFile("bundles/schema/run-001.idx.jsonl",
        "{\"key\":\"empty.csv\",\"offset\":0,\"length\":4,\"ts\":1000}\n");
    provider.addRangeData("bundles/schema/run-001.bin", 0, 4, "data");

    CacheResolver resolver = new CacheResolver(cacheDir.toString(), provider, "schema");
    String result = resolver.resolve("empty.csv");

    assertNotNull(result);
    assertEquals("data", new String(Files.readAllBytes(emptyFile), StandardCharsets.UTF_8));
  }

  @Test @Tag("FILE-092")
  void resolveFromRemoteBundlePromotesBytesToLocal() throws IOException {
    Path cacheDir = tempDir.resolve("cache");
    Files.createDirectories(cacheDir);

    MockBundleStorageProvider provider = new MockBundleStorageProvider();
    provider.addFile("bundles/schema/run-001.idx.jsonl",
        "{\"key\":\"data/bundled.csv\",\"offset\":0,\"length\":15,\"ts\":1000}\n");
    provider.addRangeData("bundles/schema/run-001.bin", 0, 15, "bundled content");

    CacheResolver resolver = new CacheResolver(cacheDir.toString(), provider, "schema");
    String result = resolver.resolve("data/bundled.csv");

    assertNotNull(result);
    Path localFile = cacheDir.resolve("data/bundled.csv");
    assertTrue(Files.exists(localFile), "remote bytes should be promoted to local");
    assertEquals("bundled content", new String(Files.readAllBytes(localFile), StandardCharsets.UTF_8));
  }

  @Test @Tag("FILE-092")
  void resolveReturnsNullWhenNeitherLocalNorInIndex() throws IOException {
    Path cacheDir = tempDir.resolve("cache");
    Files.createDirectories(cacheDir);
    MockBundleStorageProvider provider = new MockBundleStorageProvider();
    provider.addFile("bundles/schema/run-001.idx.jsonl",
        "{\"key\":\"other.csv\",\"offset\":0,\"length\":5,\"ts\":1000}\n");
    CacheResolver resolver = new CacheResolver(cacheDir.toString(), provider, "schema");
    assertNull(resolver.resolve("not-in-index.csv"));
  }

  // -------------------------------------------------- helpers --------------------------------

  private static String sliceUtf8(byte[] data, long offset, long length) {
    byte[] slice = new byte[(int) length];
    System.arraycopy(data, (int) offset, slice, 0, (int) length);
    return new String(slice, StandardCharsets.UTF_8);
  }

  private static class RecordingStorageProvider implements StorageProvider {
    final Map<String, byte[]> writtenFiles = new HashMap<String, byte[]>();

    @Override public void writeFile(String path, byte[] content) {
      writtenFiles.put(path, content);
    }

    @Override public List<FileEntry> listFiles(String path, boolean recursive) {
      return new ArrayList<FileEntry>();
    }

    @Override public FileMetadata getMetadata(String path) throws IOException {
      throw new IOException("Not supported");
    }

    @Override public InputStream openInputStream(String path) throws IOException {
      byte[] content = writtenFiles.get(path);
      if (content == null) {
        throw new IOException("File not found: " + path);
      }
      return new ByteArrayInputStream(content);
    }

    @Override public Reader openReader(String path) throws IOException {
      return new java.io.InputStreamReader(openInputStream(path), StandardCharsets.UTF_8);
    }

    @Override public boolean exists(String path) {
      return writtenFiles.containsKey(path);
    }

    @Override public boolean isDirectory(String path) {
      return false;
    }

    @Override public String getStorageType() {
      return "recording";
    }

    @Override public String resolvePath(String basePath, String relativePath) {
      return basePath + "/" + relativePath;
    }
  }

  private static class MockBundleStorageProvider implements StorageProvider {
    private final Map<String, String> files = new HashMap<String, String>();
    private final Map<String, byte[]> rangeData = new HashMap<String, byte[]>();

    void addFile(String path, String content) {
      files.put(path, content);
    }

    void addRangeData(String path, long offset, long length, String data) {
      rangeData.put(path + ":" + offset + ":" + length, data.getBytes(StandardCharsets.UTF_8));
    }

    @Override public byte[] readRange(String path, long offset, long length) throws IOException {
      byte[] data = rangeData.get(path + ":" + offset + ":" + length);
      if (data != null) {
        return data;
      }
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

    @Override public void writeFile(String path, byte[] content) {
      files.put(path, new String(content, StandardCharsets.UTF_8));
    }

    @Override public List<FileEntry> listFiles(String path, boolean recursive) {
      List<FileEntry> entries = new ArrayList<FileEntry>();
      for (String filePath : files.keySet()) {
        if (filePath.startsWith(path)) {
          String name = filePath.substring(filePath.lastIndexOf('/') + 1);
          entries.add(new FileEntry(filePath, name, false, files.get(filePath).length(), 0));
        }
      }
      return entries;
    }

    @Override public FileMetadata getMetadata(String path) throws IOException {
      String content = files.get(path);
      if (content == null) {
        throw new IOException("File not found: " + path);
      }
      return new FileMetadata(path, content.length(), 0, "application/octet-stream", null);
    }

    @Override public InputStream openInputStream(String path) throws IOException {
      String content = files.get(path);
      if (content == null) {
        throw new IOException("File not found: " + path);
      }
      return new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8));
    }

    @Override public Reader openReader(String path) throws IOException {
      return new java.io.InputStreamReader(openInputStream(path), StandardCharsets.UTF_8);
    }

    @Override public boolean exists(String path) {
      return files.containsKey(path);
    }

    @Override public boolean isDirectory(String path) {
      return false;
    }

    @Override public String getStorageType() {
      return "mock-bundle";
    }

    @Override public String resolvePath(String basePath, String relativePath) {
      return basePath + "/" + relativePath;
    }
  }
}
