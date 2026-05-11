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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Deep coverage tests for {@link StorageProviderFile} covering
 * local and remote file operations, factory methods, and edge cases.
 */
@Tag("unit")
class StorageProviderFileDeepTest {

  @TempDir
  File tempDir;

  @Test void testCreateLocalFile() {
    StorageProviderFile file = StorageProviderFile.create(
        tempDir.getAbsolutePath() + "/test.csv", new LocalFileStorageProvider());
    assertTrue(file.isLocal());
    assertNotNull(file.getFile());
  }

  @Test void testCreateLocalAbsolutePath() {
    StorageProviderFile file = StorageProviderFile.create(
        "/absolute/path/test.csv", new LocalFileStorageProvider());
    assertTrue(file.isLocal());
  }

  @Test void testCreateRemoteFile() {
    StorageProvider.FileEntry entry =
        new StorageProvider.FileEntry("s3://bucket/key.csv", "key.csv", false, 1024, 1000L);
    StorageProviderFile file = StorageProviderFile.create(entry, new LocalFileStorageProvider());
    assertFalse(file.isLocal());
    assertNotNull(file.getStorageProvider());
    assertNotNull(file.getFileEntry());
  }

  @Test void testLocalFileExists() throws IOException {
    File testFile = new File(tempDir, "exists.txt");
    Files.write(testFile.toPath(), "data".getBytes());

    StorageProviderFile spFile = StorageProviderFile.create(
        testFile.getAbsolutePath(), new LocalFileStorageProvider());
    assertTrue(spFile.exists());
  }

  @Test void testLocalFileNotExists() {
    StorageProviderFile spFile = StorageProviderFile.create(
        new File(tempDir, "nope.txt").getAbsolutePath(), new LocalFileStorageProvider());
    assertFalse(spFile.exists());
  }

  @Test void testLocalFileLastModified() throws IOException {
    File testFile = new File(tempDir, "mod.txt");
    Files.write(testFile.toPath(), "data".getBytes());

    StorageProviderFile spFile = StorageProviderFile.create(
        testFile.getAbsolutePath(), new LocalFileStorageProvider());
    assertTrue(spFile.lastModified() > 0);
  }

  @Test void testLocalFileLength() throws IOException {
    File testFile = new File(tempDir, "len.txt");
    Files.write(testFile.toPath(), "data".getBytes());

    StorageProviderFile spFile = StorageProviderFile.create(
        testFile.getAbsolutePath(), new LocalFileStorageProvider());
    assertEquals(4, spFile.length());
  }

  @Test void testLocalFileDelete() throws IOException {
    File testFile = new File(tempDir, "del.txt");
    Files.write(testFile.toPath(), "data".getBytes());

    StorageProviderFile spFile = StorageProviderFile.create(
        testFile.getAbsolutePath(), new LocalFileStorageProvider());
    assertTrue(spFile.delete());
    assertFalse(testFile.exists());
  }

  @Test void testLocalFileMkdirs() {
    StorageProviderFile spFile = StorageProviderFile.create(
        new File(tempDir, "sub/dir/file.txt").getAbsolutePath(), new LocalFileStorageProvider());
    assertTrue(spFile.mkdirs());
  }

  @Test void testLocalOpenInputStream() throws IOException {
    File testFile = new File(tempDir, "stream.txt");
    Files.write(testFile.toPath(), "hello".getBytes());

    StorageProviderFile spFile = StorageProviderFile.create(
        testFile.getAbsolutePath(), new LocalFileStorageProvider());
    try (InputStream is = spFile.openInputStream()) {
      byte[] buf = new byte[5];
      is.read(buf);
      assertArrayEquals("hello".getBytes(), buf);
    }
  }

  @Test void testLocalWriteBytes() throws IOException {
    File testFile = new File(tempDir, "write.txt");
    StorageProviderFile spFile = StorageProviderFile.create(
        testFile.getAbsolutePath(), new LocalFileStorageProvider());
    spFile.writeBytes("written".getBytes());
    assertEquals("written", new String(Files.readAllBytes(testFile.toPath())));
  }

  @Test void testLocalWriteInputStream() throws IOException {
    File testFile = new File(tempDir, "write_stream.txt");
    StorageProviderFile spFile = StorageProviderFile.create(
        testFile.getAbsolutePath(), new LocalFileStorageProvider());

    java.io.ByteArrayInputStream bais = new java.io.ByteArrayInputStream("streamed".getBytes());
    spFile.writeInputStream(bais);
    assertEquals("streamed", new String(Files.readAllBytes(testFile.toPath())));
  }

  @Test void testRemoteFileGetFileThrows() {
    StorageProvider.FileEntry entry =
        new StorageProvider.FileEntry("s3://bucket/key.csv", "key.csv", false, 1024, 1000L);
    StorageProviderFile file = StorageProviderFile.create(entry, new LocalFileStorageProvider());
    assertThrows(UnsupportedOperationException.class, file::getFile);
  }

  @Test void testRemoteFileLastModified() {
    StorageProvider.FileEntry entry =
        new StorageProvider.FileEntry("s3://bucket/key.csv", "key.csv", false, 1024, 5000L);
    StorageProviderFile file = StorageProviderFile.create(entry, new LocalFileStorageProvider());
    assertEquals(5000L, file.lastModified());
  }

  @Test void testRemoteFileLength() {
    StorageProvider.FileEntry entry =
        new StorageProvider.FileEntry("s3://bucket/key.csv", "key.csv", false, 2048, 1000L);
    StorageProviderFile file = StorageProviderFile.create(entry, new LocalFileStorageProvider());
    assertEquals(2048, file.length());
  }

  @Test void testRemoteFileExists() {
    StorageProvider.FileEntry entry =
        new StorageProvider.FileEntry("/nonexistent", "nonexistent", false, 100, 100L);
    // Uses storageProvider.exists() which will check local filesystem
    StorageProviderFile file = StorageProviderFile.create(entry, new LocalFileStorageProvider());
    assertFalse(file.exists());
  }

  @Test void testEnsureParentDirsLocal() throws IOException {
    File testFile = new File(tempDir, "nested/dir/file.txt");
    StorageProviderFile spFile = StorageProviderFile.create(
        testFile.getAbsolutePath(), new LocalFileStorageProvider());
    spFile.ensureParentDirs();
    assertTrue(testFile.getParentFile().exists());
  }

  @Test void testCreateWithFileEntryPathPrefix() {
    // Test with path not starting with /
    StorageProvider.FileEntry entry =
        new StorageProvider.FileEntry("relative/path.csv", "path.csv", false, 100, 100L);
    StorageProviderFile file = StorageProviderFile.create(entry, new LocalFileStorageProvider());
    assertNotNull(file);
  }
}
