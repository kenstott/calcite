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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.Method;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Deep coverage tests for FtpStorageProvider.
 * Focuses on URI parsing, path resolution, and logic methods testable without FTP.
 */
@Tag("unit")
public class FtpStorageProviderDeepCoverageTest {

  private FtpStorageProvider provider;

  @BeforeEach
  void setUp() {
    provider = new FtpStorageProvider();
  }

  // --- Constructor ---

  @Test
  void testDefaultConstructor() {
    FtpStorageProvider p = new FtpStorageProvider();
    assertEquals("ftp", p.getStorageType());
  }

  // --- getStorageType ---

  @Test
  void testGetStorageType() {
    assertEquals("ftp", provider.getStorageType());
  }

  // --- resolvePath ---

  @Test
  void testResolvePathAbsoluteFtp() {
    assertEquals("ftp://other.com/path.txt",
        provider.resolvePath("ftp://host.com/base/", "ftp://other.com/path.txt"));
  }

  @Test
  void testResolvePathRelativeWithDirectory() {
    String resolved = provider.resolvePath("ftp://host.com/base/dir/", "file.txt");
    assertEquals("ftp://host.com:21/base/dir/file.txt", resolved);
  }

  @Test
  void testResolvePathRelativeWithFile() {
    String resolved = provider.resolvePath("ftp://host.com/base/dir/old.csv", "new.txt");
    assertEquals("ftp://host.com:21/base/dir/new.txt", resolved);
  }

  @Test
  void testResolvePathWithCustomPort() {
    String resolved = provider.resolvePath("ftp://host.com:2121/base/", "file.txt");
    assertEquals("ftp://host.com:2121/base/file.txt", resolved);
  }

  @Test
  void testResolvePathFallbackOnInvalidUri() {
    // Invalid base URI - should fallback to string concatenation
    String resolved = provider.resolvePath("not-a-uri", "file.txt");
    assertEquals("not-a-uri/file.txt", resolved);
  }

  @Test
  void testResolvePathFallbackWithTrailingSlash() {
    String resolved = provider.resolvePath("not-a-uri/", "file.txt");
    assertEquals("not-a-uri/file.txt", resolved);
  }

  @Test
  void testResolvePathNoSlashInBase() {
    // Base path with no directory slash (path is just /)
    String resolved = provider.resolvePath("ftp://host.com/singlepath", "other.txt");
    assertEquals("ftp://host.com:21/other.txt", resolved);
  }

  // --- parseFtpUri via reflection ---

  @Test
  void testParseFtpUriBasic() throws Exception {
    Method parseMethod = FtpStorageProvider.class.getDeclaredMethod("parseFtpUri", String.class);
    parseMethod.setAccessible(true);

    Object uri = parseMethod.invoke(provider, "ftp://host.com/path/file.txt");
    assertNotNull(uri);
  }

  @Test
  void testParseFtpUriWithPort() throws Exception {
    Method parseMethod = FtpStorageProvider.class.getDeclaredMethod("parseFtpUri", String.class);
    parseMethod.setAccessible(true);

    Object uri = parseMethod.invoke(provider, "ftp://host.com:2121/path/file.txt");
    assertNotNull(uri);
  }

  @Test
  void testParseFtpUriWithCredentials() throws Exception {
    Method parseMethod = FtpStorageProvider.class.getDeclaredMethod("parseFtpUri", String.class);
    parseMethod.setAccessible(true);

    Object uri = parseMethod.invoke(provider, "ftp://user:pass@host.com/path/file.txt");
    assertNotNull(uri);
  }

  @Test
  void testParseFtpUriWithUsernameOnly() throws Exception {
    Method parseMethod = FtpStorageProvider.class.getDeclaredMethod("parseFtpUri", String.class);
    parseMethod.setAccessible(true);

    Object uri = parseMethod.invoke(provider, "ftp://justuser@host.com/path/file.txt");
    assertNotNull(uri);
  }

  @Test
  void testParseFtpUriNoUserInfo() throws Exception {
    Method parseMethod = FtpStorageProvider.class.getDeclaredMethod("parseFtpUri", String.class);
    parseMethod.setAccessible(true);

    // Anonymous access - default user/pass
    Object uri = parseMethod.invoke(provider, "ftp://host.com/path/file.txt");
    assertNotNull(uri);
  }

  @Test
  void testParseFtpUriInvalid() throws Exception {
    Method parseMethod = FtpStorageProvider.class.getDeclaredMethod("parseFtpUri", String.class);
    parseMethod.setAccessible(true);

    try {
      parseMethod.invoke(provider, "http://not-ftp/path");
      assertTrue(false, "Expected IOException");
    } catch (java.lang.reflect.InvocationTargetException e) {
      assertTrue(e.getCause() instanceof IOException);
      assertTrue(e.getCause().getMessage().contains("Invalid FTP URI"));
    }
  }

  // --- readAllBytes via reflection ---

  @Test
  void testReadAllBytes() throws Exception {
    Method readAllBytesMethod =
        FtpStorageProvider.class.getDeclaredMethod("readAllBytes", java.io.InputStream.class);
    readAllBytesMethod.setAccessible(true);

    byte[] testData = "ftp test data".getBytes();
    java.io.InputStream is = new java.io.ByteArrayInputStream(testData);

    byte[] result = (byte[]) readAllBytesMethod.invoke(provider, is);
    assertEquals(testData.length, result.length);
    assertEquals("ftp test data", new String(result));
  }

  @Test
  void testReadAllBytesEmpty() throws Exception {
    Method readAllBytesMethod =
        FtpStorageProvider.class.getDeclaredMethod("readAllBytes", java.io.InputStream.class);
    readAllBytesMethod.setAccessible(true);

    java.io.InputStream is = new java.io.ByteArrayInputStream(new byte[0]);

    byte[] result = (byte[]) readAllBytesMethod.invoke(provider, is);
    assertEquals(0, result.length);
  }

  // --- Error handling for invalid URIs on public methods ---

  @Test
  void testListFilesInvalidUri() {
    assertThrows(IOException.class, () -> provider.listFiles("not-ftp://path", false));
  }

  @Test
  void testGetMetadataInvalidUri() {
    assertThrows(IOException.class, () -> provider.getMetadata("not-ftp://path"));
  }

  @Test
  void testOpenInputStreamInvalidUri() {
    assertThrows(IOException.class, () -> provider.openInputStream("not-ftp://path"));
  }

  @Test
  void testExistsInvalidUri() {
    assertThrows(IOException.class, () -> provider.exists("not-ftp://path"));
  }

  @Test
  void testIsDirectoryInvalidUri() {
    assertThrows(IOException.class, () -> provider.isDirectory("not-ftp://path"));
  }

  // --- FtpInputStream inner class via reflection ---

  @Test
  void testFtpInputStreamClassExists() throws Exception {
    Class<?> ftpInputStreamClass = null;
    for (Class<?> clazz : FtpStorageProvider.class.getDeclaredClasses()) {
      if (clazz.getSimpleName().equals("FtpInputStream")) {
        ftpInputStreamClass = clazz;
        break;
      }
    }
    assertNotNull(ftpInputStreamClass, "FtpInputStream inner class should exist");

    assertNotNull(ftpInputStreamClass.getDeclaredMethod("read"));
    assertNotNull(ftpInputStreamClass.getDeclaredMethod("read", byte[].class, int.class, int.class));
    assertNotNull(ftpInputStreamClass.getDeclaredMethod("close"));
  }

  // --- FtpUri inner class via reflection ---

  @Test
  void testFtpUriClassExists() throws Exception {
    Class<?> ftpUriClass = null;
    for (Class<?> clazz : FtpStorageProvider.class.getDeclaredClasses()) {
      if (clazz.getSimpleName().equals("FtpUri")) {
        ftpUriClass = clazz;
        break;
      }
    }
    assertNotNull(ftpUriClass, "FtpUri inner class should exist");
  }
}
