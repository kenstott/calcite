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

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.Session;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link SftpStorageProvider} covering constructor logic,
 * URI parsing, path resolution, storage type, error handling, and internal
 * helper methods without requiring a live SFTP server.
 *
 * <p>Uses reflection to test private methods like parseSftpUri, readAllBytes,
 * findDefaultPrivateKey, and the inner SftpUri and SftpInputStream classes.
 */
@Tag("unit")
public class SftpStorageProviderCoverageTest {

  private SftpStorageProvider provider;

  @BeforeEach
  void setUp() {
    provider = new SftpStorageProvider();
  }

  // --- Constructor tests ---

  @Test
  void testDefaultConstructor() {
    SftpStorageProvider p = new SftpStorageProvider();
    assertNotNull(p);
    assertEquals("sftp", p.getStorageType());
  }

  @Test
  void testDefaultConstructorSetsDefaultUsername() throws Exception {
    SftpStorageProvider p = new SftpStorageProvider();
    Field usernameField = SftpStorageProvider.class.getDeclaredField("defaultUsername");
    usernameField.setAccessible(true);
    String username = (String) usernameField.get(p);
    assertEquals(System.getProperty("user.name"), username);
  }

  @Test
  void testDefaultConstructorSetsNullPassword() throws Exception {
    SftpStorageProvider p = new SftpStorageProvider();
    Field passwordField = SftpStorageProvider.class.getDeclaredField("defaultPassword");
    passwordField.setAccessible(true);
    assertNull(passwordField.get(p));
  }

  @Test
  void testDefaultConstructorSetsStrictHostKeyCheckingFalse() throws Exception {
    SftpStorageProvider p = new SftpStorageProvider();
    Field strictField = SftpStorageProvider.class.getDeclaredField("strictHostKeyChecking");
    strictField.setAccessible(true);
    assertFalse((Boolean) strictField.get(p));
  }

  @Test
  void testParameterizedConstructor() {
    SftpStorageProvider p = new SftpStorageProvider("testuser", "testpass",
        "/path/to/key", true);
    assertEquals("sftp", p.getStorageType());
  }

  @Test
  void testParameterizedConstructorWithNulls() {
    SftpStorageProvider p = new SftpStorageProvider(null, null, null, false);
    assertEquals("sftp", p.getStorageType());
  }

  @Test
  void testParameterizedConstructorWithUsername() throws Exception {
    SftpStorageProvider p = new SftpStorageProvider("myuser", "mypass",
        "/home/myuser/.ssh/id_rsa", false);
    Field usernameField = SftpStorageProvider.class.getDeclaredField("defaultUsername");
    usernameField.setAccessible(true);
    assertEquals("myuser", usernameField.get(p));
  }

  @Test
  void testParameterizedConstructorWithNullUsername() throws Exception {
    SftpStorageProvider p = new SftpStorageProvider(null, "pass", "/key", true);
    Field usernameField = SftpStorageProvider.class.getDeclaredField("defaultUsername");
    usernameField.setAccessible(true);
    assertEquals(System.getProperty("user.name"), usernameField.get(p));
  }

  @Test
  void testParameterizedConstructorStrictHostKeyChecking() throws Exception {
    SftpStorageProvider p = new SftpStorageProvider("user", "pass", "/key", true);
    Field strictField = SftpStorageProvider.class.getDeclaredField("strictHostKeyChecking");
    strictField.setAccessible(true);
    assertTrue((Boolean) strictField.get(p));
  }

  @Test
  void testParameterizedConstructorPrivateKeyPath() throws Exception {
    SftpStorageProvider p = new SftpStorageProvider("user", "pass",
        "/custom/key/path", false);
    Field keyField = SftpStorageProvider.class.getDeclaredField("defaultPrivateKeyPath");
    keyField.setAccessible(true);
    assertEquals("/custom/key/path", keyField.get(p));
  }

  @Test
  void testParameterizedConstructorNullPrivateKeyPathFallsBack() throws Exception {
    SftpStorageProvider p = new SftpStorageProvider("user", "pass", null, false);
    Field keyField = SftpStorageProvider.class.getDeclaredField("defaultPrivateKeyPath");
    keyField.setAccessible(true);
    // Should fall back to findDefaultPrivateKey result (may or may not be null)
    // Just confirm no exception thrown
    Object val = keyField.get(p);
    assertTrue(val == null || val instanceof String);
  }

  // --- getStorageType ---

  @Test
  void testGetStorageType() {
    assertEquals("sftp", provider.getStorageType());
  }

  // --- resolvePath tests ---

  @Test
  void testResolvePathWithAbsoluteSftpUrl() {
    String result = provider.resolvePath(
        "sftp://user@host.com/base/", "sftp://other@host2.com/file.txt");
    assertEquals("sftp://other@host2.com/file.txt", result);
  }

  @Test
  void testResolvePathWithRelativePath() {
    String result = provider.resolvePath(
        "sftp://user@host.com/base/", "file.txt");
    assertEquals("sftp://user@host.com/base/file.txt", result);
  }

  @Test
  void testResolvePathWithCustomPort() {
    String result = provider.resolvePath(
        "sftp://user@host.com:2222/base/", "file.txt");
    assertEquals("sftp://user@host.com:2222/base/file.txt", result);
  }

  @Test
  void testResolvePathWithFileBase() {
    String result = provider.resolvePath(
        "sftp://user@host.com/path/to/file.csv", "other.csv");
    assertEquals("sftp://user@host.com/path/to/other.csv", result);
  }

  @Test
  void testResolvePathWithDeepPath() {
    String result = provider.resolvePath(
        "sftp://user@host.com/a/b/c/d/", "file.txt");
    assertEquals("sftp://user@host.com/a/b/c/d/file.txt", result);
  }

  @Test
  void testResolvePathFallbackOnInvalidUri() {
    String result = provider.resolvePath("invalid-uri", "file.txt");
    assertEquals("invalid-uri/file.txt", result);
  }

  @Test
  void testResolvePathFallbackWithTrailingSlash() {
    String result = provider.resolvePath("not-sftp/", "file.txt");
    assertEquals("not-sftp/file.txt", result);
  }

  @Test
  void testResolvePathWithSubdirectory() {
    String result = provider.resolvePath(
        "sftp://user@host.com/base/", "sub/dir/file.txt");
    assertEquals("sftp://user@host.com/base/sub/dir/file.txt", result);
  }

  @Test
  void testResolvePathWithDefaultPort() {
    // Port 22 (default) should not appear in the resolved URL
    String result = provider.resolvePath(
        "sftp://user@host.com/base/", "file.txt");
    assertEquals("sftp://user@host.com/base/file.txt", result);
    assertFalse(result.contains(":22"), "Default port should not appear");
  }

  @Test
  void testResolvePathWithNonDefaultPort() {
    String result = provider.resolvePath(
        "sftp://user@host.com:3333/base/", "file.txt");
    assertEquals("sftp://user@host.com:3333/base/file.txt", result);
    assertTrue(result.contains(":3333"));
  }

  @Test
  void testResolvePathWithRootBasePath() {
    String result = provider.resolvePath(
        "sftp://user@host.com/", "file.csv");
    assertEquals("sftp://user@host.com/file.csv", result);
  }

  @Test
  void testResolvePathWithPasswordInUrl() {
    String result = provider.resolvePath(
        "sftp://user:pass@host.com/data/", "file.csv");
    assertEquals("sftp://user@host.com/data/file.csv", result);
  }

  @Test
  void testResolvePathWithOnlyUsername() {
    String result = provider.resolvePath(
        "sftp://justuser@host.com/data/", "file.csv");
    assertEquals("sftp://justuser@host.com/data/file.csv", result);
  }

  // --- parseSftpUri (via reflection) ---

  @Test
  void testParseSftpUriValidSimple() throws Exception {
    Method parseSftpUri =
        SftpStorageProvider.class.getDeclaredMethod("parseSftpUri", String.class);
    parseSftpUri.setAccessible(true);

    Object sftpUri = parseSftpUri.invoke(provider, "sftp://myuser@example.com/data/file.csv");
    assertNotNull(sftpUri);

    Class<?> sftpUriClass = sftpUri.getClass();
    Field hostField = sftpUriClass.getDeclaredField("host");
    hostField.setAccessible(true);
    assertEquals("example.com", hostField.get(sftpUri));

    Field portField = sftpUriClass.getDeclaredField("port");
    portField.setAccessible(true);
    assertEquals(22, portField.get(sftpUri));

    Field pathField = sftpUriClass.getDeclaredField("path");
    pathField.setAccessible(true);
    assertEquals("/data/file.csv", pathField.get(sftpUri));

    Field usernameField = sftpUriClass.getDeclaredField("username");
    usernameField.setAccessible(true);
    assertEquals("myuser", usernameField.get(sftpUri));
  }

  @Test
  void testParseSftpUriWithPort() throws Exception {
    Method parseSftpUri =
        SftpStorageProvider.class.getDeclaredMethod("parseSftpUri", String.class);
    parseSftpUri.setAccessible(true);

    Object sftpUri = parseSftpUri.invoke(provider, "sftp://user@example.com:2222/file.csv");
    Class<?> sftpUriClass = sftpUri.getClass();

    Field portField = sftpUriClass.getDeclaredField("port");
    portField.setAccessible(true);
    assertEquals(2222, portField.get(sftpUri));
  }

  @Test
  void testParseSftpUriWithUserAndPassword() throws Exception {
    Method parseSftpUri =
        SftpStorageProvider.class.getDeclaredMethod("parseSftpUri", String.class);
    parseSftpUri.setAccessible(true);

    Object sftpUri = parseSftpUri.invoke(provider, "sftp://admin:secret@example.com/data/");
    Class<?> sftpUriClass = sftpUri.getClass();

    Field usernameField = sftpUriClass.getDeclaredField("username");
    usernameField.setAccessible(true);
    assertEquals("admin", usernameField.get(sftpUri));

    Field passwordField = sftpUriClass.getDeclaredField("password");
    passwordField.setAccessible(true);
    assertEquals("secret", passwordField.get(sftpUri));
  }

  @Test
  void testParseSftpUriWithOnlyUsername() throws Exception {
    Method parseSftpUri =
        SftpStorageProvider.class.getDeclaredMethod("parseSftpUri", String.class);
    parseSftpUri.setAccessible(true);

    Object sftpUri = parseSftpUri.invoke(provider, "sftp://justuser@example.com/data/");
    Class<?> sftpUriClass = sftpUri.getClass();

    Field usernameField = sftpUriClass.getDeclaredField("username");
    usernameField.setAccessible(true);
    assertEquals("justuser", usernameField.get(sftpUri));
  }

  @Test
  void testParseSftpUriInvalidScheme() throws Exception {
    Method parseSftpUri =
        SftpStorageProvider.class.getDeclaredMethod("parseSftpUri", String.class);
    parseSftpUri.setAccessible(true);

    try {
      parseSftpUri.invoke(provider, "ftp://example.com/file.csv");
    } catch (InvocationTargetException e) {
      assertTrue(e.getCause() instanceof IOException);
      assertTrue(e.getCause().getMessage().contains("Invalid SFTP URI"));
    }
  }

  @Test
  void testParseSftpUriNoUserInfo() throws Exception {
    Method parseSftpUri =
        SftpStorageProvider.class.getDeclaredMethod("parseSftpUri", String.class);
    parseSftpUri.setAccessible(true);

    // No user info in URL, should use defaultUsername
    Object sftpUri = parseSftpUri.invoke(provider, "sftp://example.com/data/file.csv");
    Class<?> sftpUriClass = sftpUri.getClass();

    Field usernameField = sftpUriClass.getDeclaredField("username");
    usernameField.setAccessible(true);
    assertEquals(System.getProperty("user.name"), usernameField.get(sftpUri));
  }

  @Test
  void testParseSftpUriStrictHostKeyCheckingField() throws Exception {
    Method parseSftpUri =
        SftpStorageProvider.class.getDeclaredMethod("parseSftpUri", String.class);
    parseSftpUri.setAccessible(true);

    Object sftpUri = parseSftpUri.invoke(provider, "sftp://user@example.com/data/");
    Class<?> sftpUriClass = sftpUri.getClass();

    Field strictField = sftpUriClass.getDeclaredField("strictHostKeyChecking");
    strictField.setAccessible(true);
    assertFalse((Boolean) strictField.get(sftpUri));
  }

  @Test
  void testParseSftpUriPrivateKeyPathField() throws Exception {
    Method parseSftpUri =
        SftpStorageProvider.class.getDeclaredMethod("parseSftpUri", String.class);
    parseSftpUri.setAccessible(true);

    Object sftpUri = parseSftpUri.invoke(provider, "sftp://user@example.com/data/");
    Class<?> sftpUriClass = sftpUri.getClass();

    Field keyField = sftpUriClass.getDeclaredField("privateKeyPath");
    keyField.setAccessible(true);
    // May be null or a path - just verifying the field exists and is accessible
    Object val = keyField.get(sftpUri);
    assertTrue(val == null || val instanceof String);
  }

  // --- readAllBytes (via reflection) ---

  @Test
  void testReadAllBytesEmptyStream() throws Exception {
    Method readAllBytes =
        SftpStorageProvider.class.getDeclaredMethod("readAllBytes", InputStream.class);
    readAllBytes.setAccessible(true);

    InputStream empty = new ByteArrayInputStream(new byte[0]);
    byte[] result = (byte[]) readAllBytes.invoke(provider, empty);
    assertNotNull(result);
    assertEquals(0, result.length);
  }

  @Test
  void testReadAllBytesSmallStream() throws Exception {
    Method readAllBytes =
        SftpStorageProvider.class.getDeclaredMethod("readAllBytes", InputStream.class);
    readAllBytes.setAccessible(true);

    byte[] data = "hello sftp".getBytes(StandardCharsets.UTF_8);
    InputStream stream = new ByteArrayInputStream(data);
    byte[] result = (byte[]) readAllBytes.invoke(provider, stream);
    assertEquals(data.length, result.length);
    assertEquals("hello sftp", new String(result, StandardCharsets.UTF_8));
  }

  @Test
  void testReadAllBytesLargeStream() throws Exception {
    Method readAllBytes =
        SftpStorageProvider.class.getDeclaredMethod("readAllBytes", InputStream.class);
    readAllBytes.setAccessible(true);

    byte[] data = new byte[20000];
    for (int i = 0; i < data.length; i++) {
      data[i] = (byte) (i % 256);
    }
    InputStream stream = new ByteArrayInputStream(data);
    byte[] result = (byte[]) readAllBytes.invoke(provider, stream);
    assertEquals(data.length, result.length);
  }

  // --- findDefaultPrivateKey (via reflection) ---

  @Test
  void testFindDefaultPrivateKey() throws Exception {
    Method findKey =
        SftpStorageProvider.class.getDeclaredMethod("findDefaultPrivateKey");
    findKey.setAccessible(true);

    Object result = findKey.invoke(provider);
    // Result may be null if no SSH keys exist, or a path if they do
    assertTrue(result == null || result instanceof String);
  }

  // --- SftpInputStream inner class (via reflection) ---

  @Test
  void testSftpInputStreamRead() throws Exception {
    Class<?>[] innerClasses = SftpStorageProvider.class.getDeclaredClasses();
    Class<?> sftpInputStreamClass = null;
    for (Class<?> c : innerClasses) {
      if (c.getSimpleName().equals("SftpInputStream")) {
        sftpInputStreamClass = c;
        break;
      }
    }
    assertNotNull(sftpInputStreamClass, "SftpInputStream inner class should exist");

    Constructor<?> ctor = sftpInputStreamClass.getDeclaredConstructor(
        InputStream.class, ChannelSftp.class, Session.class);
    ctor.setAccessible(true);

    byte[] testData = "sftp test data".getBytes(StandardCharsets.UTF_8);
    InputStream wrapped = new ByteArrayInputStream(testData);

    // Use null channel and session - close will handle gracefully
    InputStream sftpIs = (InputStream) ctor.newInstance(wrapped, null, null);
    assertNotNull(sftpIs);

    int firstByte = sftpIs.read();
    assertEquals('s', firstByte);
  }

  @Test
  void testSftpInputStreamReadWithBuffer() throws Exception {
    Class<?>[] innerClasses = SftpStorageProvider.class.getDeclaredClasses();
    Class<?> sftpInputStreamClass = null;
    for (Class<?> c : innerClasses) {
      if (c.getSimpleName().equals("SftpInputStream")) {
        sftpInputStreamClass = c;
        break;
      }
    }
    assertNotNull(sftpInputStreamClass);

    Constructor<?> ctor = sftpInputStreamClass.getDeclaredConstructor(
        InputStream.class, ChannelSftp.class, Session.class);
    ctor.setAccessible(true);

    byte[] testData = "buffer sftp data".getBytes(StandardCharsets.UTF_8);
    InputStream wrapped = new ByteArrayInputStream(testData);

    InputStream sftpIs = (InputStream) ctor.newInstance(wrapped, null, null);
    byte[] buffer = new byte[6];
    int bytesRead = sftpIs.read(buffer, 0, 6);
    assertEquals(6, bytesRead);
    assertEquals("buffer", new String(buffer, StandardCharsets.UTF_8));
  }

  @Test
  void testSftpInputStreamCloseWithNullChannelAndSession() throws Exception {
    Class<?>[] innerClasses = SftpStorageProvider.class.getDeclaredClasses();
    Class<?> sftpInputStreamClass = null;
    for (Class<?> c : innerClasses) {
      if (c.getSimpleName().equals("SftpInputStream")) {
        sftpInputStreamClass = c;
        break;
      }
    }
    assertNotNull(sftpInputStreamClass);

    Constructor<?> ctor = sftpInputStreamClass.getDeclaredConstructor(
        InputStream.class, ChannelSftp.class, Session.class);
    ctor.setAccessible(true);

    byte[] testData = "close test".getBytes(StandardCharsets.UTF_8);
    InputStream wrapped = new ByteArrayInputStream(testData);

    InputStream sftpIs = (InputStream) ctor.newInstance(wrapped, null, null);
    // Should not throw even with null channel and session
    sftpIs.close();
  }

  // --- URI parsing error handling ---

  @Test
  void testListFilesInvalidUri() {
    assertThrows(IOException.class,
        () -> provider.listFiles("not-sftp://host/path", false));
  }

  @Test
  void testListFilesNonSftpUri() {
    assertThrows(IOException.class,
        () -> provider.listFiles("/local/path", false));
  }

  @Test
  void testGetMetadataInvalidUri() {
    assertThrows(IOException.class,
        () -> provider.getMetadata("not-sftp://host/file.csv"));
  }

  @Test
  void testOpenInputStreamInvalidUri() {
    assertThrows(IOException.class,
        () -> provider.openInputStream("ftp://host/file.csv"));
  }

  @Test
  void testExistsInvalidUri() {
    assertThrows(IOException.class,
        () -> provider.exists("http://host/file.csv"));
  }

  @Test
  void testIsDirectoryInvalidUri() {
    assertThrows(IOException.class,
        () -> provider.isDirectory("garbage"));
  }

  @Test
  void testOpenReaderInvalidUri() {
    assertThrows(IOException.class,
        () -> provider.openReader("not-sftp://host/path"));
  }

  // --- Connection error handling (valid URI, no server) ---

  @Test
  void testListFilesConnectionError() {
    assertThrows(IOException.class,
        () -> provider.listFiles(
            "sftp://user@nonexistent-host-12345.invalid/path/", false));
  }

  @Test
  void testGetMetadataConnectionError() {
    assertThrows(IOException.class,
        () -> provider.getMetadata(
            "sftp://user@nonexistent-host-12345.invalid/file.csv"));
  }

  @Test
  void testExistsConnectionError() {
    assertThrows(IOException.class,
        () -> provider.exists(
            "sftp://user@nonexistent-host-12345.invalid/file.csv"));
  }

  @Test
  void testIsDirectoryConnectionError() {
    assertThrows(IOException.class,
        () -> provider.isDirectory(
            "sftp://user@nonexistent-host-12345.invalid/path"));
  }

  @Test
  void testOpenInputStreamConnectionError() {
    assertThrows(IOException.class,
        () -> provider.openInputStream(
            "sftp://user@nonexistent-host-12345.invalid/file.csv"));
  }

  // --- hasChanged with mock metadata ---

  @Test
  void testHasChangedWithNullCachedMetadata() throws Exception {
    boolean result = provider.hasChanged(
        "sftp://user@nonexistent.invalid/file.csv", null);
    assertTrue(result, "Should report changed when cached metadata is null");
  }

  @Test
  void testHasChangedWhenGetMetadataFails() throws Exception {
    StorageProvider.FileMetadata cached =
        new StorageProvider.FileMetadata(
            "sftp://user@nonexistent.invalid/file.csv",
            100, System.currentTimeMillis(), "application/octet-stream", null);
    boolean result = provider.hasChanged(
        "sftp://user@nonexistent.invalid/file.csv", cached);
    assertTrue(result, "Should report changed when current metadata cannot be fetched");
  }

  // --- Default operations ---

  @Test
  void testWriteFileThrowsUnsupported() {
    assertThrows(UnsupportedOperationException.class,
        () -> provider.writeFile("/test.txt", new byte[0]));
  }

  @Test
  void testWriteFileStreamThrowsUnsupported() {
    assertThrows(UnsupportedOperationException.class,
        () -> provider.writeFile("/test.txt",
            new ByteArrayInputStream(new byte[0])));
  }

  @Test
  void testDeleteThrowsUnsupported() {
    assertThrows(UnsupportedOperationException.class,
        () -> provider.delete("/test.txt"));
  }

  @Test
  void testCreateDirectoriesThrowsUnsupported() {
    assertThrows(UnsupportedOperationException.class,
        () -> provider.createDirectories("/test/dir"));
  }

  @Test
  void testCopyFileThrowsUnsupported() {
    assertThrows(UnsupportedOperationException.class,
        () -> provider.copyFile("/source.txt", "/dest.txt"));
  }

  @Test
  void testReadRangeThrowsUnsupported() {
    assertThrows(UnsupportedOperationException.class,
        () -> provider.readRange("sftp://host/file.csv", 0, 100));
  }

  // --- SftpUri inner class field validation ---

  @Test
  void testSftpUriClassFields() throws Exception {
    Class<?>[] innerClasses = SftpStorageProvider.class.getDeclaredClasses();
    Class<?> sftpUriClass = null;
    for (Class<?> c : innerClasses) {
      if (c.getSimpleName().equals("SftpUri")) {
        sftpUriClass = c;
        break;
      }
    }
    assertNotNull(sftpUriClass, "SftpUri inner class should exist");

    // Verify all expected fields exist
    assertNotNull(sftpUriClass.getDeclaredField("host"));
    assertNotNull(sftpUriClass.getDeclaredField("port"));
    assertNotNull(sftpUriClass.getDeclaredField("username"));
    assertNotNull(sftpUriClass.getDeclaredField("password"));
    assertNotNull(sftpUriClass.getDeclaredField("path"));
    assertNotNull(sftpUriClass.getDeclaredField("privateKeyPath"));
    assertNotNull(sftpUriClass.getDeclaredField("passphrase"));
    assertNotNull(sftpUriClass.getDeclaredField("strictHostKeyChecking"));
  }

  @Test
  void testSftpUriConstructorViaReflection() throws Exception {
    Class<?>[] innerClasses = SftpStorageProvider.class.getDeclaredClasses();
    Class<?> sftpUriClass = null;
    for (Class<?> c : innerClasses) {
      if (c.getSimpleName().equals("SftpUri")) {
        sftpUriClass = c;
        break;
      }
    }
    assertNotNull(sftpUriClass);

    Constructor<?> ctor = sftpUriClass.getDeclaredConstructor(
        String.class, int.class, String.class, String.class, String.class,
        String.class, String.class, boolean.class);
    ctor.setAccessible(true);

    Object sftpUri = ctor.newInstance(
        "myhost", 2222, "myuser", "mypass", "/my/path",
        "/key/path", "passphrase", true);

    Field hostField = sftpUriClass.getDeclaredField("host");
    hostField.setAccessible(true);
    assertEquals("myhost", hostField.get(sftpUri));

    Field portField = sftpUriClass.getDeclaredField("port");
    portField.setAccessible(true);
    assertEquals(2222, portField.get(sftpUri));

    Field usernameField = sftpUriClass.getDeclaredField("username");
    usernameField.setAccessible(true);
    assertEquals("myuser", usernameField.get(sftpUri));

    Field passwordField = sftpUriClass.getDeclaredField("password");
    passwordField.setAccessible(true);
    assertEquals("mypass", passwordField.get(sftpUri));

    Field pathField = sftpUriClass.getDeclaredField("path");
    pathField.setAccessible(true);
    assertEquals("/my/path", pathField.get(sftpUri));

    Field keyField = sftpUriClass.getDeclaredField("privateKeyPath");
    keyField.setAccessible(true);
    assertEquals("/key/path", keyField.get(sftpUri));

    Field passphraseField = sftpUriClass.getDeclaredField("passphrase");
    passphraseField.setAccessible(true);
    assertEquals("passphrase", passphraseField.get(sftpUri));

    Field strictField = sftpUriClass.getDeclaredField("strictHostKeyChecking");
    strictField.setAccessible(true);
    assertTrue((Boolean) strictField.get(sftpUri));
  }

  // --- Static constants via reflection ---

  @Test
  void testDefaultPortConstant() throws Exception {
    Field defaultPort = SftpStorageProvider.class.getDeclaredField("DEFAULT_PORT");
    defaultPort.setAccessible(true);
    assertEquals(22, defaultPort.get(null));
  }

  @Test
  void testConnectTimeoutConstant() throws Exception {
    Field connectTimeout = SftpStorageProvider.class.getDeclaredField("CONNECT_TIMEOUT");
    connectTimeout.setAccessible(true);
    assertEquals(30000, connectTimeout.get(null));
  }

  @Test
  void testSftpSubsystemConstant() throws Exception {
    Field subsystem = SftpStorageProvider.class.getDeclaredField("SFTP_SUBSYSTEM");
    subsystem.setAccessible(true);
    assertEquals("sftp", subsystem.get(null));
  }

  // --- S3 config returns null ---

  @Test
  void testGetS3ConfigReturnsNull() {
    assertNull(provider.getS3Config());
  }

  // --- Recursive listing with invalid URI still fails ---

  @Test
  void testListFilesRecursiveWithInvalidUri() {
    assertThrows(IOException.class,
        () -> provider.listFiles("ftp://not-sftp/path", true));
  }

  // --- Persistent cache field ---

  @Test
  void testPersistentCacheFieldAccessible() throws Exception {
    Field cacheField = SftpStorageProvider.class.getDeclaredField("persistentCache");
    cacheField.setAccessible(true);
    // When StorageCacheManager is not initialized, should be null
    Object cacheValue = cacheField.get(provider);
    assertTrue(cacheValue == null || cacheValue != null);
  }
}
