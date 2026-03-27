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
 * Deep coverage tests for SftpStorageProvider.
 * Focuses on URI parsing, path resolution, and logic methods testable without SSH.
 */
@Tag("unit")
public class SftpStorageProviderDeepCoverageTest {

  private SftpStorageProvider provider;

  @BeforeEach
  void setUp() {
    provider = new SftpStorageProvider();
  }

  // --- Constructor tests ---

  @Test
  void testDefaultConstructor() {
    SftpStorageProvider p = new SftpStorageProvider();
    assertEquals("sftp", p.getStorageType());
  }

  @Test
  void testConfigConstructor() {
    SftpStorageProvider p = new SftpStorageProvider("myuser", "mypass", null, true);
    assertEquals("sftp", p.getStorageType());
  }

  @Test
  void testConfigConstructorWithNullUsername() {
    // Should default to system user
    SftpStorageProvider p = new SftpStorageProvider(null, "pass", null, false);
    assertEquals("sftp", p.getStorageType());
  }

  @Test
  void testConfigConstructorWithPrivateKey() {
    SftpStorageProvider p = new SftpStorageProvider("user", null, "/home/.ssh/id_rsa", true);
    assertEquals("sftp", p.getStorageType());
  }

  // --- getStorageType ---

  @Test
  void testGetStorageType() {
    assertEquals("sftp", provider.getStorageType());
  }

  // --- resolvePath ---

  @Test
  void testResolvePathAbsoluteSftp() {
    assertEquals("sftp://other@host2.com/file.txt",
        provider.resolvePath("sftp://user@host.com/base/", "sftp://other@host2.com/file.txt"));
  }

  @Test
  void testResolvePathRelativeWithDirectory() {
    String resolved = provider.resolvePath("sftp://user@host.com/base/dir/", "file.txt");
    assertEquals("sftp://user@host.com/base/dir/file.txt", resolved);
  }

  @Test
  void testResolvePathRelativeWithFile() {
    String resolved = provider.resolvePath("sftp://user@host.com/base/dir/old.csv", "new.txt");
    assertEquals("sftp://user@host.com/base/dir/new.txt", resolved);
  }

  @Test
  void testResolvePathWithPort() {
    String resolved = provider.resolvePath("sftp://user@host.com:2222/base/", "file.txt");
    assertEquals("sftp://user@host.com:2222/base/file.txt", resolved);
  }

  @Test
  void testResolvePathFallbackOnInvalidUri() {
    // Invalid base URI - should fallback to string concat
    String resolved = provider.resolvePath("not-a-uri", "file.txt");
    assertEquals("not-a-uri/file.txt", resolved);
  }

  @Test
  void testResolvePathFallbackWithTrailingSlash() {
    String resolved = provider.resolvePath("not-a-uri/", "file.txt");
    assertEquals("not-a-uri/file.txt", resolved);
  }

  @Test
  void testResolvePathNoLastSlash() {
    // resolvePath when base path has no slash at all
    String resolved = provider.resolvePath("sftp://user@host.com/singlefile", "other.txt");
    // singlefile has no extension so treated as directory? Let's see: lastSlash is after host
    // The path is /singlefile, lastSlash is at index 0
    assertEquals("sftp://user@host.com/other.txt", resolved);
  }

  // --- parseSftpUri via reflection ---

  @Test
  void testParseSftpUriBasic() throws Exception {
    Method parseMethod =
        SftpStorageProvider.class.getDeclaredMethod("parseSftpUri", String.class);
    parseMethod.setAccessible(true);

    Object uri = parseMethod.invoke(provider, "sftp://user@host.com/path/to/file.txt");
    assertNotNull(uri);
  }

  @Test
  void testParseSftpUriWithPort() throws Exception {
    Method parseMethod =
        SftpStorageProvider.class.getDeclaredMethod("parseSftpUri", String.class);
    parseMethod.setAccessible(true);

    Object uri = parseMethod.invoke(provider, "sftp://user@host.com:2222/path/file.txt");
    assertNotNull(uri);
  }

  @Test
  void testParseSftpUriWithPassword() throws Exception {
    Method parseMethod =
        SftpStorageProvider.class.getDeclaredMethod("parseSftpUri", String.class);
    parseMethod.setAccessible(true);

    Object uri = parseMethod.invoke(provider, "sftp://user:password@host.com/path/file.txt");
    assertNotNull(uri);
  }

  @Test
  void testParseSftpUriWithUsernameOnly() throws Exception {
    Method parseMethod =
        SftpStorageProvider.class.getDeclaredMethod("parseSftpUri", String.class);
    parseMethod.setAccessible(true);

    Object uri = parseMethod.invoke(provider, "sftp://justuser@host.com/path/file.txt");
    assertNotNull(uri);
  }

  @Test
  void testParseSftpUriNoUserInfo() throws Exception {
    Method parseMethod =
        SftpStorageProvider.class.getDeclaredMethod("parseSftpUri", String.class);
    parseMethod.setAccessible(true);

    Object uri = parseMethod.invoke(provider, "sftp://host.com/path/file.txt");
    assertNotNull(uri);
  }

  @Test
  void testParseSftpUriInvalid() throws Exception {
    Method parseMethod =
        SftpStorageProvider.class.getDeclaredMethod("parseSftpUri", String.class);
    parseMethod.setAccessible(true);

    try {
      parseMethod.invoke(provider, "http://not-sftp/path");
      assertTrue(false, "Expected IOException");
    } catch (java.lang.reflect.InvocationTargetException e) {
      assertTrue(e.getCause() instanceof IOException);
      assertTrue(e.getCause().getMessage().contains("Invalid SFTP URI"));
    }
  }

  // --- findDefaultPrivateKey via reflection ---

  @Test
  void testFindDefaultPrivateKey() throws Exception {
    Method findKeyMethod =
        SftpStorageProvider.class.getDeclaredMethod("findDefaultPrivateKey");
    findKeyMethod.setAccessible(true);

    // Result depends on system state, but should not throw
    Object result = findKeyMethod.invoke(provider);
    // Could be null or a valid path - either is fine
  }

  // --- readAllBytes via reflection ---

  @Test
  void testReadAllBytes() throws Exception {
    Method readAllBytesMethod =
        SftpStorageProvider.class.getDeclaredMethod("readAllBytes", java.io.InputStream.class);
    readAllBytesMethod.setAccessible(true);

    byte[] testData = "sftp test data".getBytes();
    java.io.InputStream is = new java.io.ByteArrayInputStream(testData);

    byte[] result = (byte[]) readAllBytesMethod.invoke(provider, is);
    assertEquals(testData.length, result.length);
    assertEquals("sftp test data", new String(result));
  }

  @Test
  void testReadAllBytesEmpty() throws Exception {
    Method readAllBytesMethod =
        SftpStorageProvider.class.getDeclaredMethod("readAllBytes", java.io.InputStream.class);
    readAllBytesMethod.setAccessible(true);

    java.io.InputStream is = new java.io.ByteArrayInputStream(new byte[0]);

    byte[] result = (byte[]) readAllBytesMethod.invoke(provider, is);
    assertEquals(0, result.length);
  }

  // --- Error handling for invalid URIs ---

  @Test
  void testListFilesInvalidUri() {
    assertThrows(IOException.class, () -> provider.listFiles("not-sftp://path", false));
  }

  @Test
  void testGetMetadataInvalidUri() {
    assertThrows(IOException.class, () -> provider.getMetadata("not-sftp://path"));
  }

  @Test
  void testOpenInputStreamInvalidUri() {
    assertThrows(IOException.class, () -> provider.openInputStream("not-sftp://path"));
  }

  @Test
  void testExistsInvalidUri() {
    assertThrows(IOException.class, () -> provider.exists("not-sftp://path"));
  }

  @Test
  void testIsDirectoryInvalidUri() {
    assertThrows(IOException.class, () -> provider.isDirectory("not-sftp://path"));
  }

  // --- SftpInputStream inner class via reflection ---

  @Test
  void testSftpInputStreamRead() throws Exception {
    // Access the inner class SftpInputStream
    Class<?> sftpInputStreamClass = null;
    for (Class<?> clazz : SftpStorageProvider.class.getDeclaredClasses()) {
      if (clazz.getSimpleName().equals("SftpInputStream")) {
        sftpInputStreamClass = clazz;
        break;
      }
    }
    assertNotNull(sftpInputStreamClass, "SftpInputStream inner class should exist");

    // Test that the class has the expected methods
    assertNotNull(sftpInputStreamClass.getDeclaredMethod("read"));
    assertNotNull(sftpInputStreamClass.getDeclaredMethod("read", byte[].class, int.class, int.class));
    assertNotNull(sftpInputStreamClass.getDeclaredMethod("close"));
  }
}
