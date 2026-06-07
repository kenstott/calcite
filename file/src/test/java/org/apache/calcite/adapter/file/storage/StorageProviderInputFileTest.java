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

import org.apache.parquet.io.SeekableInputStream;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Unit tests for {@link StorageProviderInputFile}.
 */
@Tag("unit")
public class StorageProviderInputFileTest {

  @TempDir
  Path tempDir;

  @Test void testGetLength() throws IOException {
    byte[] content = "Hello, Parquet!".getBytes(StandardCharsets.UTF_8);
    InMemoryProvider provider = new InMemoryProvider();
    provider.addFile("test.parquet", content);

    StorageProviderInputFile inputFile =
        new StorageProviderInputFile(provider, "test.parquet");

    assertEquals(content.length, inputFile.getLength());
  }

  @Test void testGetLengthCached() throws IOException {
    byte[] content = "data".getBytes(StandardCharsets.UTF_8);
    InMemoryProvider provider = new InMemoryProvider();
    provider.addFile("test.parquet", content);

    StorageProviderInputFile inputFile =
        new StorageProviderInputFile(provider, "test.parquet");

    // Call twice - second call should use cached length
    long len1 = inputFile.getLength();
    long len2 = inputFile.getLength();
    assertEquals(len1, len2);
  }

  @Test void testNewStreamAndRead() throws IOException {
    byte[] content = "ABCDEFGHIJ".getBytes(StandardCharsets.UTF_8);
    InMemoryProvider provider = new InMemoryProvider();
    provider.addFile("test.parquet", content);

    StorageProviderInputFile inputFile =
        new StorageProviderInputFile(provider, "test.parquet");

    try (SeekableInputStream stream = inputFile.newStream()) {
      assertEquals(0, stream.getPos());

      // Read single byte
      int b = stream.read();
      assertEquals('A', b);
      assertEquals(1, stream.getPos());
    }
  }

  @Test void testStreamSeek() throws IOException {
    byte[] content = "ABCDEFGHIJ".getBytes(StandardCharsets.UTF_8);
    InMemoryProvider provider = new InMemoryProvider();
    provider.addFile("test.parquet", content);

    StorageProviderInputFile inputFile =
        new StorageProviderInputFile(provider, "test.parquet");

    try (SeekableInputStream stream = inputFile.newStream()) {
      // Seek to position 5
      stream.seek(5);
      assertEquals(5, stream.getPos());

      int b = stream.read();
      assertEquals('F', b);

      // Seek backwards
      stream.seek(0);
      assertEquals(0, stream.getPos());

      b = stream.read();
      assertEquals('A', b);
    }
  }

  @Test void testStreamSeekInvalidPosition() throws IOException {
    byte[] content = "short".getBytes(StandardCharsets.UTF_8);
    InMemoryProvider provider = new InMemoryProvider();
    provider.addFile("test.parquet", content);

    StorageProviderInputFile inputFile =
        new StorageProviderInputFile(provider, "test.parquet");

    try (SeekableInputStream stream = inputFile.newStream()) {
      // Negative position
      assertThrows(IllegalArgumentException.class, () -> stream.seek(-1));

      // Beyond content length
      assertThrows(IllegalArgumentException.class,
          () -> stream.seek(content.length + 1));
    }
  }

  @Test void testStreamReadFully() throws IOException {
    byte[] content = "Hello, World!".getBytes(StandardCharsets.UTF_8);
    InMemoryProvider provider = new InMemoryProvider();
    provider.addFile("test.parquet", content);

    StorageProviderInputFile inputFile =
        new StorageProviderInputFile(provider, "test.parquet");

    try (SeekableInputStream stream = inputFile.newStream()) {
      byte[] result = new byte[content.length];
      stream.readFully(result);

      assertArrayEquals(content, result);
      assertEquals(content.length, stream.getPos());
    }
  }

  @Test void testStreamReadFullyPartial() throws IOException {
    byte[] content = "ABCDEFGHIJ".getBytes(StandardCharsets.UTF_8);
    InMemoryProvider provider = new InMemoryProvider();
    provider.addFile("test.parquet", content);

    StorageProviderInputFile inputFile =
        new StorageProviderInputFile(provider, "test.parquet");

    try (SeekableInputStream stream = inputFile.newStream()) {
      byte[] result = new byte[10];
      stream.readFully(result, 2, 5);

      assertEquals('A', result[2]);
      assertEquals('E', result[6]);
    }
  }

  @Test void testStreamReadFullyEOF() throws IOException {
    byte[] content = "short".getBytes(StandardCharsets.UTF_8);
    InMemoryProvider provider = new InMemoryProvider();
    provider.addFile("test.parquet", content);

    StorageProviderInputFile inputFile =
        new StorageProviderInputFile(provider, "test.parquet");

    try (SeekableInputStream stream = inputFile.newStream()) {
      byte[] result = new byte[100]; // More than content length
      assertThrows(EOFException.class, () -> stream.readFully(result));
    }
  }

  @Test void testStreamReadByteBuffer() throws IOException {
    byte[] content = "ByteBuffer Test".getBytes(StandardCharsets.UTF_8);
    InMemoryProvider provider = new InMemoryProvider();
    provider.addFile("test.parquet", content);

    StorageProviderInputFile inputFile =
        new StorageProviderInputFile(provider, "test.parquet");

    try (SeekableInputStream stream = inputFile.newStream()) {
      ByteBuffer buf = ByteBuffer.allocate(10);
      int bytesRead = stream.read(buf);

      assertEquals(10, bytesRead);
      buf.flip();
      byte[] result = new byte[10];
      buf.get(result);
      assertEquals("ByteBuffer", new String(result, StandardCharsets.UTF_8));
    }
  }

  @Test void testStreamReadByteBufferAtEnd() throws IOException {
    byte[] content = "end".getBytes(StandardCharsets.UTF_8);
    InMemoryProvider provider = new InMemoryProvider();
    provider.addFile("test.parquet", content);

    StorageProviderInputFile inputFile =
        new StorageProviderInputFile(provider, "test.parquet");

    try (SeekableInputStream stream = inputFile.newStream()) {
      // Read all content
      stream.seek(content.length);

      ByteBuffer buf = ByteBuffer.allocate(10);
      int bytesRead = stream.read(buf);

      assertEquals(-1, bytesRead);
    }
  }

  @Test void testStreamReadFullyByteBuffer() throws IOException {
    byte[] content = "Full ByteBuffer".getBytes(StandardCharsets.UTF_8);
    InMemoryProvider provider = new InMemoryProvider();
    provider.addFile("test.parquet", content);

    StorageProviderInputFile inputFile =
        new StorageProviderInputFile(provider, "test.parquet");

    try (SeekableInputStream stream = inputFile.newStream()) {
      ByteBuffer buf = ByteBuffer.allocate(content.length);
      stream.readFully(buf);

      buf.flip();
      byte[] result = new byte[content.length];
      buf.get(result);
      assertArrayEquals(content, result);
    }
  }

  @Test void testStreamReadFullyByteBufferEOF() throws IOException {
    byte[] content = "short".getBytes(StandardCharsets.UTF_8);
    InMemoryProvider provider = new InMemoryProvider();
    provider.addFile("test.parquet", content);

    StorageProviderInputFile inputFile =
        new StorageProviderInputFile(provider, "test.parquet");

    try (SeekableInputStream stream = inputFile.newStream()) {
      ByteBuffer buf = ByteBuffer.allocate(100); // More than content
      assertThrows(EOFException.class, () -> stream.readFully(buf));
    }
  }

  @Test void testStreamReadBulk() throws IOException {
    byte[] content = "Bulk Read Test Data".getBytes(StandardCharsets.UTF_8);
    InMemoryProvider provider = new InMemoryProvider();
    provider.addFile("test.parquet", content);

    StorageProviderInputFile inputFile =
        new StorageProviderInputFile(provider, "test.parquet");

    try (SeekableInputStream stream = inputFile.newStream()) {
      byte[] result = new byte[10];
      int bytesRead = stream.read(result, 0, 10);

      assertEquals(10, bytesRead);
      assertEquals("Bulk Read ", new String(result, StandardCharsets.UTF_8));
    }
  }

  @Test void testStreamReadBulkAtEnd() throws IOException {
    byte[] content = "end".getBytes(StandardCharsets.UTF_8);
    InMemoryProvider provider = new InMemoryProvider();
    provider.addFile("test.parquet", content);

    StorageProviderInputFile inputFile =
        new StorageProviderInputFile(provider, "test.parquet");

    try (SeekableInputStream stream = inputFile.newStream()) {
      stream.seek(content.length);

      byte[] result = new byte[10];
      int bytesRead = stream.read(result, 0, 10);

      assertEquals(-1, bytesRead);
    }
  }

  @Test void testStreamReadSingleByteAtEnd() throws IOException {
    byte[] content = "X".getBytes(StandardCharsets.UTF_8);
    InMemoryProvider provider = new InMemoryProvider();
    provider.addFile("test.parquet", content);

    StorageProviderInputFile inputFile =
        new StorageProviderInputFile(provider, "test.parquet");

    try (SeekableInputStream stream = inputFile.newStream()) {
      assertEquals('X', stream.read());
      assertEquals(-1, stream.read()); // EOF
    }
  }

  /**
   * In-memory StorageProvider for testing.
   */
  private static class InMemoryProvider implements StorageProvider {
    private final java.util.Map<String, byte[]> files =
        new java.util.HashMap<String, byte[]>();

    void addFile(String path, byte[] content) {
      files.put(path, content);
    }

    void addFile(String path, String content) {
      files.put(path, content.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public List<FileEntry> listFiles(String path, boolean recursive) {
      return new ArrayList<FileEntry>();
    }

    @Override
    public FileMetadata getMetadata(String path) throws IOException {
      byte[] content = files.get(path);
      if (content == null) {
        throw new IOException("File not found: " + path);
      }
      return new FileMetadata(path, content.length, 0, "application/octet-stream", null);
    }

    @Override
    public InputStream openInputStream(String path) throws IOException {
      byte[] content = files.get(path);
      if (content == null) {
        throw new IOException("File not found: " + path);
      }
      return new ByteArrayInputStream(content);
    }

    @Override
    public Reader openReader(String path) throws IOException {
      byte[] content = files.get(path);
      if (content == null) {
        throw new IOException("File not found: " + path);
      }
      return new java.io.InputStreamReader(
          new ByteArrayInputStream(content), StandardCharsets.UTF_8);
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
      return "memory";
    }

    @Override
    public String resolvePath(String basePath, String relativePath) {
      return basePath + "/" + relativePath;
    }
  }
}
