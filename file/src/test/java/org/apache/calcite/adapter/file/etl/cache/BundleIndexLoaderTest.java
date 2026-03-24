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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link BundleIndexLoader}.
 */
@Tag("unit")
public class BundleIndexLoaderTest {

  @Test void testLoadWithNoIndexFiles() throws IOException {
    StorageProvider provider = new InMemoryStorageProvider();

    BundleIndex index = BundleIndexLoader.load(provider, "test-schema");

    assertNotNull(index);
    assertEquals(0, index.size());
  }

  @Test void testLoadSingleIndexFile() throws IOException {
    InMemoryStorageProvider provider = new InMemoryStorageProvider();

    String indexContent =
        "{\"key\":\"data/file1.csv\",\"offset\":0,\"length\":100,\"ts\":1000}\n"
        + "{\"key\":\"data/file2.csv\",\"offset\":100,\"length\":200,\"ts\":2000}\n";

    provider.addFile("bundles/myschema/run-20260310T1423.idx.jsonl", indexContent);

    BundleIndex index = BundleIndexLoader.load(provider, "myschema");

    assertEquals(2, index.size());
    assertNotNull(index.get("data/file1.csv"));
    assertNotNull(index.get("data/file2.csv"));

    // Verify the bundle file name was derived correctly
    BundleEntry entry = index.get("data/file1.csv");
    assertEquals("run-20260310T1423.bin", entry.getBundleFile());
  }

  @Test void testLoadMergesMultipleIndexFiles() throws IOException {
    InMemoryStorageProvider provider = new InMemoryStorageProvider();

    // First run
    String index1 =
        "{\"key\":\"file.csv\",\"offset\":0,\"length\":100,\"ts\":1000}\n"
        + "{\"key\":\"old.csv\",\"offset\":100,\"length\":50,\"ts\":1000}\n";
    provider.addFile("bundles/schema/run-20260310T1400.idx.jsonl", index1);

    // Second run - overrides file.csv with newer version
    String index2 =
        "{\"key\":\"file.csv\",\"offset\":0,\"length\":150,\"ts\":2000}\n"
        + "{\"key\":\"new.csv\",\"offset\":150,\"length\":75,\"ts\":2000}\n";
    provider.addFile("bundles/schema/run-20260310T1500.idx.jsonl", index2);

    BundleIndex index = BundleIndexLoader.load(provider, "schema");

    assertEquals(3, index.size());

    // file.csv should have the later version (from run2)
    BundleEntry fileEntry = index.get("file.csv");
    assertEquals(150, fileEntry.getLength());
    assertEquals(2000, fileEntry.getTimestamp());
    assertEquals("run-20260310T1500.bin", fileEntry.getBundleFile());

    // old.csv from first run still present
    assertNotNull(index.get("old.csv"));

    // new.csv from second run present
    assertNotNull(index.get("new.csv"));
  }

  @Test void testLoadSkipsNonIndexFiles() throws IOException {
    InMemoryStorageProvider provider = new InMemoryStorageProvider();

    provider.addFile("bundles/schema/run-20260310T1423.bin", "binary data");
    provider.addFile("bundles/schema/run-20260310T1423.idx.jsonl",
        "{\"key\":\"file.csv\",\"offset\":0,\"length\":100,\"ts\":1000}\n");
    provider.addFile("bundles/schema/other.txt", "not an index");

    BundleIndex index = BundleIndexLoader.load(provider, "schema");

    assertEquals(1, index.size());
  }

  @Test void testLoadHandlesIOExceptionOnListFiles() throws IOException {
    StorageProvider provider = new FailingStorageProvider(true, false);

    BundleIndex index = BundleIndexLoader.load(provider, "schema");

    assertNotNull(index);
    assertEquals(0, index.size());
  }

  @Test void testLoadHandlesIOExceptionOnReadFile() throws IOException {
    InMemoryStorageProvider baseProvider = new InMemoryStorageProvider();
    baseProvider.addFile("bundles/schema/run-20260310T1423.idx.jsonl", "dummy");

    StorageProvider provider = new FailingStorageProvider(false, true) {
      @Override
      public List<FileEntry> listFiles(String path, boolean recursive) throws IOException {
        return baseProvider.listFiles(path, recursive);
      }
    };

    BundleIndex index = BundleIndexLoader.load(provider, "schema");

    // Should still return an index, just empty because the read failed
    assertNotNull(index);
    assertEquals(0, index.size());
  }

  @Test void testLoadSortsIndexFilesByName() throws IOException {
    InMemoryStorageProvider provider = new InMemoryStorageProvider();

    // Add files in reverse order
    provider.addFile("bundles/schema/run-20260310T1500.idx.jsonl",
        "{\"key\":\"file.csv\",\"offset\":0,\"length\":200,\"ts\":2000}\n");
    provider.addFile("bundles/schema/run-20260310T1400.idx.jsonl",
        "{\"key\":\"file.csv\",\"offset\":0,\"length\":100,\"ts\":1000}\n");

    BundleIndex index = BundleIndexLoader.load(provider, "schema");

    // Should process in sorted order, so later timestamp wins
    BundleEntry entry = index.get("file.csv");
    assertEquals(200, entry.getLength());
    assertEquals(2000, entry.getTimestamp());
  }

  /**
   * Simple in-memory StorageProvider for testing.
   */
  private static class InMemoryStorageProvider implements StorageProvider {
    private final List<String> filePaths = new ArrayList<String>();
    private final java.util.Map<String, String> fileContents =
        new java.util.HashMap<String, String>();

    void addFile(String path, String content) {
      filePaths.add(path);
      fileContents.put(path, content);
    }

    @Override
    public List<FileEntry> listFiles(String path, boolean recursive) throws IOException {
      List<FileEntry> entries = new ArrayList<FileEntry>();
      for (String filePath : filePaths) {
        if (filePath.startsWith(path)) {
          String name = filePath.substring(filePath.lastIndexOf('/') + 1);
          entries.add(new FileEntry(filePath, name, false,
              fileContents.get(filePath).length(), 0));
        }
      }
      return entries;
    }

    @Override
    public FileMetadata getMetadata(String path) throws IOException {
      String content = fileContents.get(path);
      if (content == null) {
        throw new IOException("File not found: " + path);
      }
      return new FileMetadata(path, content.length(), 0, "application/octet-stream", null);
    }

    @Override
    public InputStream openInputStream(String path) throws IOException {
      String content = fileContents.get(path);
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
      return fileContents.containsKey(path);
    }

    @Override
    public boolean isDirectory(String path) {
      return false;
    }

    @Override
    public String getStorageType() {
      return "memory";
    }

    @Override
    public String resolvePath(String basePath, String relativePath) {
      return basePath + "/" + relativePath;
    }
  }

  /**
   * StorageProvider that fails on specific operations.
   */
  private static class FailingStorageProvider implements StorageProvider {
    private final boolean failOnList;
    private final boolean failOnRead;

    FailingStorageProvider(boolean failOnList, boolean failOnRead) {
      this.failOnList = failOnList;
      this.failOnRead = failOnRead;
    }

    @Override
    public List<FileEntry> listFiles(String path, boolean recursive) throws IOException {
      if (failOnList) {
        throw new IOException("Simulated list failure");
      }
      return new ArrayList<FileEntry>();
    }

    @Override
    public FileMetadata getMetadata(String path) throws IOException {
      throw new IOException("Not supported");
    }

    @Override
    public InputStream openInputStream(String path) throws IOException {
      if (failOnRead) {
        throw new IOException("Simulated read failure");
      }
      return new ByteArrayInputStream(new byte[0]);
    }

    @Override
    public Reader openReader(String path) throws IOException {
      throw new IOException("Not supported");
    }

    @Override
    public boolean exists(String path) {
      return false;
    }

    @Override
    public boolean isDirectory(String path) {
      return false;
    }

    @Override
    public String getStorageType() {
      return "failing";
    }

    @Override
    public String resolvePath(String basePath, String relativePath) {
      return basePath + "/" + relativePath;
    }
  }
}
