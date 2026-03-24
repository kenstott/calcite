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

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Deep coverage tests for {@link LocalFileStorageProvider}
 * covering file:// URI handling, cleanup, copy, resolve, and edge cases.
 */
@Tag("unit")
class LocalFileStorageProviderDeepTest {

  @TempDir
  Path tempDir;

  private final LocalFileStorageProvider provider = new LocalFileStorageProvider();

  // --- File URI handling ---

  @Test void testOpenInputStreamFileUri() throws IOException {
    Path file = tempDir.resolve("uri_test.txt");
    Files.write(file, "uri content".getBytes(StandardCharsets.UTF_8));

    String fileUri = file.toUri().toString();
    try (InputStream is = provider.openInputStream(fileUri)) {
      byte[] buf = new byte[11];
      int read = is.read(buf);
      assertEquals(11, read);
      assertEquals("uri content", new String(buf, StandardCharsets.UTF_8));
    }
  }

  @Test void testOpenInputStreamFileUriNotExists() {
    String fileUri = "file:///nonexistent_dir_xyz/nope.txt";
    assertThrows(IOException.class, () -> provider.openInputStream(fileUri));
  }

  @Test void testOpenReaderFileUri() throws IOException {
    Path file = tempDir.resolve("reader_uri.txt");
    Files.write(file, "reader data".getBytes(StandardCharsets.UTF_8));

    String fileUri = file.toUri().toString();
    try (Reader reader = provider.openReader(fileUri)) {
      char[] buf = new char[11];
      reader.read(buf);
      assertEquals("reader data", new String(buf));
    }
  }

  @Test void testOpenReaderFileUriNotExists() {
    String fileUri = "file:///nonexistent_dir_xyz/nope.txt";
    assertThrows(IOException.class, () -> provider.openReader(fileUri));
  }

  @Test void testExistsFileUri() throws IOException {
    Path file = tempDir.resolve("exists_uri.txt");
    Files.write(file, "data".getBytes());

    assertTrue(provider.exists(file.toUri().toString()));
    assertFalse(provider.exists("file:///nonexistent_dir_xyz/nope.txt"));
  }

  @Test void testIsDirectoryFileUri() throws IOException {
    assertTrue(provider.isDirectory(tempDir.toUri().toString()));
    assertFalse(provider.isDirectory("file:///nonexistent_dir_xyz"));
  }

  // --- resolvePath ---

  @Test void testResolvePathRelative() {
    Path baseDir = tempDir.resolve("base");
    String result = provider.resolvePath(baseDir.toString(), "sub/file.csv");
    assertTrue(result.endsWith("sub/file.csv"));
    assertTrue(result.contains("base"));
  }

  @Test void testResolvePathAbsolute() {
    String result = provider.resolvePath("/base", "/absolute/path.csv");
    assertEquals("/absolute/path.csv", result);
  }

  @Test void testResolvePathEmptyRelative() {
    String result = provider.resolvePath("/base/dir", "");
    assertEquals("/base/dir", result);
  }

  @Test void testResolvePathNullRelative() {
    String result = provider.resolvePath("/base/dir", null);
    assertEquals("/base/dir", result);
  }

  @Test void testResolvePathBaseIsFile() throws IOException {
    Path baseFile = tempDir.resolve("base.csv");
    Files.write(baseFile, "data".getBytes());

    String result = provider.resolvePath(baseFile.toString(), "other.csv");
    assertTrue(result.endsWith("other.csv"));
    // Should resolve against parent directory of the file
    assertFalse(result.contains("base.csv"));
  }

  // --- listFiles ---

  @Test void testListFilesRecursive() throws IOException {
    Path dir = tempDir.resolve("list_test");
    Files.createDirectories(dir);
    Files.write(dir.resolve("a.csv"), "a".getBytes());
    Path subDir = dir.resolve("sub");
    Files.createDirectories(subDir);
    Files.write(subDir.resolve("b.csv"), "b".getBytes());

    List<StorageProvider.FileEntry> entries = provider.listFiles(dir.toString(), true);
    assertTrue(entries.size() >= 3); // a.csv, sub (dir), b.csv
    boolean hasB = entries.stream().anyMatch(e -> e.getName().equals("b.csv"));
    assertTrue(hasB);
  }

  @Test void testListFilesNonRecursive() throws IOException {
    Path dir = tempDir.resolve("list_nonrec");
    Files.createDirectories(dir);
    Files.write(dir.resolve("a.csv"), "a".getBytes());
    Path subDir = dir.resolve("sub");
    Files.createDirectories(subDir);
    Files.write(subDir.resolve("b.csv"), "b".getBytes());

    List<StorageProvider.FileEntry> entries = provider.listFiles(dir.toString(), false);
    // Should have a.csv and sub directory, but NOT b.csv
    boolean hasB = entries.stream().anyMatch(e -> e.getName().equals("b.csv"));
    assertFalse(hasB);
  }

  @Test void testListFilesNonExistentDirectory() {
    assertThrows(IOException.class,
        () -> provider.listFiles("/nonexistent_dir_xyz", false));
  }

  // --- writeFile ---

  @Test void testWriteFileBytes() throws IOException {
    Path file = tempDir.resolve("write_bytes.txt");
    provider.writeFile(file.toString(), "content".getBytes(StandardCharsets.UTF_8));

    assertTrue(Files.exists(file));
    assertEquals("content", new String(Files.readAllBytes(file), StandardCharsets.UTF_8));
  }

  @Test void testWriteFileBytesCreatesParentDir() throws IOException {
    Path file = tempDir.resolve("new_dir/write_bytes.txt");
    provider.writeFile(file.toString(), "content".getBytes(StandardCharsets.UTF_8));

    assertTrue(Files.exists(file));
  }

  @Test void testWriteFileStream() throws IOException {
    Path file = tempDir.resolve("write_stream.txt");
    ByteArrayInputStream bais = new ByteArrayInputStream("stream content".getBytes(StandardCharsets.UTF_8));
    provider.writeFile(file.toString(), bais);

    assertTrue(Files.exists(file));
    assertEquals("stream content", new String(Files.readAllBytes(file), StandardCharsets.UTF_8));
  }

  // --- copyFile ---

  @Test void testCopyFile() throws IOException {
    Path source = tempDir.resolve("source.txt");
    Path dest = tempDir.resolve("dest.txt");
    Files.write(source, "copy me".getBytes(StandardCharsets.UTF_8));

    provider.copyFile(source.toString(), dest.toString());

    assertTrue(Files.exists(dest));
    assertEquals("copy me", new String(Files.readAllBytes(dest), StandardCharsets.UTF_8));
  }

  @Test void testCopyFileNonExistentSource() {
    assertThrows(IOException.class,
        () -> provider.copyFile("/nonexistent.txt", tempDir.resolve("dest.txt").toString()));
  }

  @Test void testCopyFileCreatesParentDir() throws IOException {
    Path source = tempDir.resolve("copy_source.txt");
    Path dest = tempDir.resolve("new_copy_dir/dest.txt");
    Files.write(source, "data".getBytes());

    provider.copyFile(source.toString(), dest.toString());
    assertTrue(Files.exists(dest));
  }

  // --- delete ---

  @Test void testDeleteFile() throws IOException {
    Path file = tempDir.resolve("delete_me.txt");
    Files.write(file, "data".getBytes());

    assertTrue(provider.delete(file.toString()));
    assertFalse(Files.exists(file));
  }

  @Test void testDeleteNonExistent() throws IOException {
    assertFalse(provider.delete(tempDir.resolve("nope.txt").toString()));
  }

  @Test void testDeleteEmptyDirectory() throws IOException {
    Path dir = tempDir.resolve("empty_dir");
    Files.createDirectories(dir);

    assertTrue(provider.delete(dir.toString()));
    assertFalse(Files.exists(dir));
  }

  // --- createDirectories ---

  @Test void testCreateDirectories() throws IOException {
    Path dir = tempDir.resolve("a/b/c");
    provider.createDirectories(dir.toString());
    assertTrue(Files.exists(dir));
    assertTrue(Files.isDirectory(dir));
  }

  // --- cleanupMacosMetadata ---

  @Test void testCleanupMacosMetadata() throws IOException {
    Path dir = tempDir.resolve("cleanup_test");
    Files.createDirectories(dir);
    Files.write(dir.resolve("._metadata"), "junk".getBytes());
    Files.write(dir.resolve(".DS_Store"), "junk".getBytes());
    Files.write(dir.resolve("data.parquet"), "real".getBytes());
    Files.write(dir.resolve("backup~"), "old".getBytes());
    Files.write(dir.resolve("temp.tmp"), "temp".getBytes());
    Files.write(dir.resolve("check.crc"), "crc".getBytes());

    provider.cleanupMacosMetadata(dir.toString());

    // Metadata files should be deleted
    assertFalse(Files.exists(dir.resolve("._metadata")));
    assertFalse(Files.exists(dir.resolve(".DS_Store")));
    assertFalse(Files.exists(dir.resolve("backup~")));
    assertFalse(Files.exists(dir.resolve("temp.tmp")));
    assertFalse(Files.exists(dir.resolve("check.crc")));
    // Real data should remain
    assertTrue(Files.exists(dir.resolve("data.parquet")));
  }

  @Test void testCleanupMacosMetadataRecursive() throws IOException {
    Path dir = tempDir.resolve("cleanup_recursive");
    Path sub = dir.resolve("sub");
    Files.createDirectories(sub);
    Files.write(sub.resolve("._sub_meta"), "junk".getBytes());
    Files.write(sub.resolve("real.csv"), "data".getBytes());

    provider.cleanupMacosMetadata(dir.toString());

    assertFalse(Files.exists(sub.resolve("._sub_meta")));
    assertTrue(Files.exists(sub.resolve("real.csv")));
  }

  @Test void testCleanupMacosMetadataNonExistentDir() throws IOException {
    // Should not throw
    provider.cleanupMacosMetadata("/nonexistent_dir_xyz");
  }

  @Test void testCleanupMacosMetadataOnFile() throws IOException {
    Path file = tempDir.resolve("not_a_dir.txt");
    Files.write(file, "data".getBytes());
    // Should not throw
    provider.cleanupMacosMetadata(file.toString());
  }

  // --- getMetadata ---

  @Test void testGetMetadata() throws IOException {
    Path file = tempDir.resolve("meta.txt");
    Files.write(file, "content".getBytes(StandardCharsets.UTF_8));

    StorageProvider.FileMetadata meta = provider.getMetadata(file.toString());
    assertNotNull(meta);
    assertEquals(7, meta.getSize());
    assertTrue(meta.getLastModified() > 0);
    // Local files don't have ETags
    assertEquals(null, meta.getEtag());
  }

  @Test void testGetMetadataNonExistent() {
    assertThrows(IOException.class,
        () -> provider.getMetadata("/nonexistent_dir_xyz/nope.txt"));
  }

  // --- readRange edge cases ---

  @Test void testReadRangeBeyondFileLength() throws IOException {
    Path file = tempDir.resolve("short.txt");
    Files.write(file, "AB".getBytes());

    byte[] result = provider.readRange(file.toString(), 0, 100);
    // Should return trimmed array
    assertEquals(2, result.length);
    assertEquals("AB", new String(result, StandardCharsets.UTF_8));
  }

  @Test void testReadRangeOffset() throws IOException {
    Path file = tempDir.resolve("offset.txt");
    Files.write(file, "0123456789".getBytes(StandardCharsets.UTF_8));

    byte[] result = provider.readRange(file.toString(), 5, 3);
    assertEquals("567", new String(result, StandardCharsets.UTF_8));
  }

  // --- writeFile with file:// URI ---

  @Test void testWriteFileBytesFileUri() throws IOException {
    Path file = tempDir.resolve("uri_write.txt");
    provider.writeFile(file.toUri().toString(), "uri bytes".getBytes(StandardCharsets.UTF_8));
    assertTrue(Files.exists(file));
    assertEquals("uri bytes", new String(Files.readAllBytes(file), StandardCharsets.UTF_8));
  }
}
