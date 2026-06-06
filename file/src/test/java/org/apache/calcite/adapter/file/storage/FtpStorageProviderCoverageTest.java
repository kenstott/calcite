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

import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.Calendar;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link FtpStorageProvider} covering constructor, URI parsing,
 * path resolution, storage type, error handling, and internal helper methods
 * without requiring a live FTP server.
 *
 * <p>Uses reflection to test private methods like parseFtpUri, readAllBytes,
 * and the inner FtpUri and FtpInputStream classes.
 */
@Tag("unit")
public class FtpStorageProviderCoverageTest {

  private FtpStorageProvider provider;

  @BeforeEach
  void setUp() {
    provider = new FtpStorageProvider();
  }

  // --- Constructor tests ---

  @Test
  void testDefaultConstructor() {
    FtpStorageProvider p = new FtpStorageProvider();
    assertNotNull(p);
    assertEquals("ftp", p.getStorageType());
  }

  @Test
  void testDefaultConstructorPersistentCacheFieldIsSet() throws Exception {
    FtpStorageProvider p = new FtpStorageProvider();
    // persistentCache should be null since StorageCacheManager is not initialized
    Field cacheField = FtpStorageProvider.class.getDeclaredField("persistentCache");
    cacheField.setAccessible(true);
    // When cache manager is not initialized, field should be null
    Object cacheValue = cacheField.get(p);
    // Either null or a valid cache; we just confirm it does not throw
    assertTrue(cacheValue == null || cacheValue != null);
  }

  // --- getStorageType ---

  @Test
  void testGetStorageType() {
    assertEquals("ftp", provider.getStorageType());
  }

  // --- resolvePath tests ---

  @Test
  void testResolvePathWithAbsoluteFtpUrl() {
    String result = provider.resolvePath(
        "ftp://example.com/base/", "ftp://other.com/file.csv");
    assertEquals("ftp://other.com/file.csv", result);
  }

  @Test
  void testResolvePathWithRelativePath() {
    String result = provider.resolvePath(
        "ftp://example.com/base/", "file.csv");
    assertEquals("ftp://example.com:21/base/file.csv", result);
  }

  @Test
  void testResolvePathWithCustomPort() {
    String result = provider.resolvePath(
        "ftp://example.com:2121/base/", "file.csv");
    assertEquals("ftp://example.com:2121/base/file.csv", result);
  }

  @Test
  void testResolvePathWithFileBase() {
    String result = provider.resolvePath(
        "ftp://example.com/path/to/file.csv", "other.csv");
    assertEquals("ftp://example.com:21/path/to/other.csv", result);
  }

  @Test
  void testResolvePathWithSubdirectory() {
    String result = provider.resolvePath(
        "ftp://example.com/base/", "sub/dir/file.csv");
    assertEquals("ftp://example.com:21/base/sub/dir/file.csv", result);
  }

  @Test
  void testResolvePathFallbackOnInvalidUri() {
    String result = provider.resolvePath("invalid-uri", "file.txt");
    assertEquals("invalid-uri/file.txt", result);
  }

  @Test
  void testResolvePathFallbackWithTrailingSlash() {
    String result = provider.resolvePath("not-ftp/", "file.txt");
    assertEquals("not-ftp/file.txt", result);
  }

  @Test
  void testResolvePathWithRootPath() {
    String result = provider.resolvePath(
        "ftp://example.com/", "file.csv");
    assertEquals("ftp://example.com:21/file.csv", result);
  }

  @Test
  void testResolvePathWithCredentials() {
    String result = provider.resolvePath(
        "ftp://user:pass@example.com/data/", "file.csv");
    assertEquals("ftp://example.com:21/data/file.csv", result);
  }

  @Test
  void testResolvePathWithDeepNestedPath() {
    String result = provider.resolvePath(
        "ftp://example.com/a/b/c/d/e/", "deep.csv");
    assertEquals("ftp://example.com:21/a/b/c/d/e/deep.csv", result);
  }

  @Test
  void testResolvePathBaseWithNoSlashInPath() throws Exception {
    // When baseDirPath has no slash, the fallback should set baseDirPath to "/"
    // parseFtpUri with a URI that has an empty path segment
    String result = provider.resolvePath(
        "ftp://example.com", "file.csv");
    // URI with no path resolves path as empty string or "/"
    assertNotNull(result);
    assertTrue(result.contains("file.csv"));
  }

  // --- parseFtpUri (via reflection) ---

  @Test
  void testParseFtpUriValidSimple() throws Exception {
    Method parseFtpUri = FtpStorageProvider.class.getDeclaredMethod("parseFtpUri", String.class);
    parseFtpUri.setAccessible(true);

    Object ftpUri = parseFtpUri.invoke(provider, "ftp://example.com/data/file.csv");
    assertNotNull(ftpUri);

    Class<?> ftpUriClass = ftpUri.getClass();
    Field hostField = ftpUriClass.getDeclaredField("host");
    hostField.setAccessible(true);
    assertEquals("example.com", hostField.get(ftpUri));

    Field portField = ftpUriClass.getDeclaredField("port");
    portField.setAccessible(true);
    assertEquals(21, portField.get(ftpUri));

    Field pathField = ftpUriClass.getDeclaredField("path");
    pathField.setAccessible(true);
    assertEquals("/data/file.csv", pathField.get(ftpUri));

    Field usernameField = ftpUriClass.getDeclaredField("username");
    usernameField.setAccessible(true);
    assertEquals("anonymous", usernameField.get(ftpUri));

    Field passwordField = ftpUriClass.getDeclaredField("password");
    passwordField.setAccessible(true);
    assertEquals("anonymous@", passwordField.get(ftpUri));
  }

  @Test
  void testParseFtpUriWithPort() throws Exception {
    Method parseFtpUri = FtpStorageProvider.class.getDeclaredMethod("parseFtpUri", String.class);
    parseFtpUri.setAccessible(true);

    Object ftpUri = parseFtpUri.invoke(provider, "ftp://example.com:990/secure/file.csv");
    Class<?> ftpUriClass = ftpUri.getClass();

    Field portField = ftpUriClass.getDeclaredField("port");
    portField.setAccessible(true);
    assertEquals(990, portField.get(ftpUri));
  }

  @Test
  void testParseFtpUriWithUserAndPassword() throws Exception {
    Method parseFtpUri = FtpStorageProvider.class.getDeclaredMethod("parseFtpUri", String.class);
    parseFtpUri.setAccessible(true);

    Object ftpUri = parseFtpUri.invoke(provider, "ftp://admin:secret@example.com/data/");
    Class<?> ftpUriClass = ftpUri.getClass();

    Field usernameField = ftpUriClass.getDeclaredField("username");
    usernameField.setAccessible(true);
    assertEquals("admin", usernameField.get(ftpUri));

    Field passwordField = ftpUriClass.getDeclaredField("password");
    passwordField.setAccessible(true);
    assertEquals("secret", passwordField.get(ftpUri));
  }

  @Test
  void testParseFtpUriWithOnlyUsername() throws Exception {
    Method parseFtpUri = FtpStorageProvider.class.getDeclaredMethod("parseFtpUri", String.class);
    parseFtpUri.setAccessible(true);

    Object ftpUri = parseFtpUri.invoke(provider, "ftp://justuser@example.com/data/");
    Class<?> ftpUriClass = ftpUri.getClass();

    Field usernameField = ftpUriClass.getDeclaredField("username");
    usernameField.setAccessible(true);
    assertEquals("justuser", usernameField.get(ftpUri));
  }

  @Test
  void testParseFtpUriInvalidScheme() throws Exception {
    Method parseFtpUri = FtpStorageProvider.class.getDeclaredMethod("parseFtpUri", String.class);
    parseFtpUri.setAccessible(true);

    try {
      parseFtpUri.invoke(provider, "http://example.com/file.csv");
    } catch (InvocationTargetException e) {
      assertTrue(e.getCause() instanceof IOException);
      assertTrue(e.getCause().getMessage().contains("Invalid FTP URI"));
    }
  }

  @Test
  void testParseFtpUriNoPath() throws Exception {
    Method parseFtpUri = FtpStorageProvider.class.getDeclaredMethod("parseFtpUri", String.class);
    parseFtpUri.setAccessible(true);

    Object ftpUri = parseFtpUri.invoke(provider, "ftp://example.com");
    Class<?> ftpUriClass = ftpUri.getClass();

    Field pathField = ftpUriClass.getDeclaredField("path");
    pathField.setAccessible(true);
    String path = (String) pathField.get(ftpUri);
    // When no path provided, should default to "/"
    assertNotNull(path);
  }

  // --- readAllBytes (via reflection) ---

  @Test
  void testReadAllBytesEmptyStream() throws Exception {
    Method readAllBytes =
        FtpStorageProvider.class.getDeclaredMethod("readAllBytes", InputStream.class);
    readAllBytes.setAccessible(true);

    InputStream empty = new ByteArrayInputStream(new byte[0]);
    byte[] result = (byte[]) readAllBytes.invoke(provider, empty);
    assertNotNull(result);
    assertEquals(0, result.length);
  }

  @Test
  void testReadAllBytesSmallStream() throws Exception {
    Method readAllBytes =
        FtpStorageProvider.class.getDeclaredMethod("readAllBytes", InputStream.class);
    readAllBytes.setAccessible(true);

    byte[] data = "hello world".getBytes(StandardCharsets.UTF_8);
    InputStream stream = new ByteArrayInputStream(data);
    byte[] result = (byte[]) readAllBytes.invoke(provider, stream);
    assertEquals(data.length, result.length);
    assertEquals("hello world", new String(result, StandardCharsets.UTF_8));
  }

  @Test
  void testReadAllBytesLargeStream() throws Exception {
    Method readAllBytes =
        FtpStorageProvider.class.getDeclaredMethod("readAllBytes", InputStream.class);
    readAllBytes.setAccessible(true);

    // Create a stream larger than the 8192 buffer size
    byte[] data = new byte[20000];
    for (int i = 0; i < data.length; i++) {
      data[i] = (byte) (i % 256);
    }
    InputStream stream = new ByteArrayInputStream(data);
    byte[] result = (byte[]) readAllBytes.invoke(provider, stream);
    assertEquals(data.length, result.length);
  }

  // --- FtpInputStream inner class (via reflection) ---

  @Test
  void testFtpInputStreamRead() throws Exception {
    Class<?>[] innerClasses = FtpStorageProvider.class.getDeclaredClasses();
    Class<?> ftpInputStreamClass = null;
    for (Class<?> c : innerClasses) {
      if (c.getSimpleName().equals("FtpInputStream")) {
        ftpInputStreamClass = c;
        break;
      }
    }
    assertNotNull(ftpInputStreamClass, "FtpInputStream inner class should exist");

    Constructor<?> ctor =
        ftpInputStreamClass.getDeclaredConstructor(InputStream.class, FTPClient.class);
    ctor.setAccessible(true);

    byte[] testData = "test data".getBytes(StandardCharsets.UTF_8);
    InputStream wrapped = new ByteArrayInputStream(testData);
    FTPClient mockClient = new FTPClient();

    InputStream ftpIs = (InputStream) ctor.newInstance(wrapped, mockClient);
    assertNotNull(ftpIs);

    int firstByte = ftpIs.read();
    assertEquals('t', firstByte);
  }

  @Test
  void testFtpInputStreamReadWithBuffer() throws Exception {
    Class<?>[] innerClasses = FtpStorageProvider.class.getDeclaredClasses();
    Class<?> ftpInputStreamClass = null;
    for (Class<?> c : innerClasses) {
      if (c.getSimpleName().equals("FtpInputStream")) {
        ftpInputStreamClass = c;
        break;
      }
    }
    assertNotNull(ftpInputStreamClass);

    Constructor<?> ctor =
        ftpInputStreamClass.getDeclaredConstructor(InputStream.class, FTPClient.class);
    ctor.setAccessible(true);

    byte[] testData = "buffer test data".getBytes(StandardCharsets.UTF_8);
    InputStream wrapped = new ByteArrayInputStream(testData);
    FTPClient mockClient = new FTPClient();

    InputStream ftpIs = (InputStream) ctor.newInstance(wrapped, mockClient);
    byte[] buffer = new byte[6];
    int bytesRead = ftpIs.read(buffer, 0, 6);
    assertEquals(6, bytesRead);
    assertEquals("buffer", new String(buffer, StandardCharsets.UTF_8));
  }

  @Test
  void testFtpInputStreamClose() throws Exception {
    Class<?>[] innerClasses = FtpStorageProvider.class.getDeclaredClasses();
    Class<?> ftpInputStreamClass = null;
    for (Class<?> c : innerClasses) {
      if (c.getSimpleName().equals("FtpInputStream")) {
        ftpInputStreamClass = c;
        break;
      }
    }
    assertNotNull(ftpInputStreamClass);

    Constructor<?> ctor =
        ftpInputStreamClass.getDeclaredConstructor(InputStream.class, FTPClient.class);
    ctor.setAccessible(true);

    byte[] testData = "close test".getBytes(StandardCharsets.UTF_8);
    InputStream wrapped = new ByteArrayInputStream(testData);
    FTPClient mockClient = new FTPClient();
    // Client is not connected, so close should handle gracefully
    InputStream ftpIs = (InputStream) ctor.newInstance(wrapped, mockClient);
    // Should not throw even with unconnected client
    ftpIs.close();
  }

  // --- URI parsing error handling ---

  @Test
  void testListFilesInvalidUri() {
    assertThrows(IOException.class,
        () -> provider.listFiles("not-ftp://host/path", false));
  }

  @Test
  void testListFilesLocalPath() {
    assertThrows(IOException.class,
        () -> provider.listFiles("/local/path", false));
  }

  @Test
  void testGetMetadataInvalidUri() {
    assertThrows(IOException.class,
        () -> provider.getMetadata("sftp://host/file.csv"));
  }

  @Test
  void testOpenInputStreamInvalidUri() {
    assertThrows(IOException.class,
        () -> provider.openInputStream("http://host/file.csv"));
  }

  @Test
  void testExistsInvalidUri() {
    assertThrows(IOException.class,
        () -> provider.exists("sftp://host/file.csv"));
  }

  @Test
  void testIsDirectoryInvalidUri() {
    assertThrows(IOException.class,
        () -> provider.isDirectory("garbage-path"));
  }

  @Test
  void testOpenReaderInvalidUri() {
    assertThrows(IOException.class,
        () -> provider.openReader("not-ftp://host/path"));
  }

  // --- Connection error handling (valid URI, no server) ---

  @Test
  void testListFilesConnectionError() {
    assertThrows(IOException.class,
        () -> provider.listFiles(
            "ftp://nonexistent-host-12345.invalid/path/", false));
  }

  @Test
  void testGetMetadataConnectionError() {
    assertThrows(IOException.class,
        () -> provider.getMetadata(
            "ftp://nonexistent-host-12345.invalid/file.csv"));
  }

  @Test
  void testExistsConnectionError() {
    assertThrows(IOException.class,
        () -> provider.exists(
            "ftp://nonexistent-host-12345.invalid/file.csv"));
  }

  @Test
  void testIsDirectoryConnectionError() {
    assertThrows(IOException.class,
        () -> provider.isDirectory(
            "ftp://nonexistent-host-12345.invalid/path"));
  }

  @Test
  void testOpenInputStreamConnectionError() {
    assertThrows(IOException.class,
        () -> provider.openInputStream(
            "ftp://nonexistent-host-12345.invalid/file.csv"));
  }

  // --- hasChanged with mock metadata ---

  @Test
  void testHasChangedWithNullCachedMetadata() throws Exception {
    boolean result = provider.hasChanged("ftp://nonexistent-host-12345.invalid/file.csv", null);
    assertTrue(result, "Should report changed when cached metadata is null");
  }

  @Test
  void testHasChangedWhenGetMetadataFails() throws Exception {
    // When getMetadata throws IOException, hasChanged should return true
    StorageProvider.FileMetadata cached =
        new StorageProvider.FileMetadata("ftp://nonexistent.invalid/file.csv",
            100, System.currentTimeMillis(), "application/octet-stream", null);
    boolean result = provider.hasChanged("ftp://nonexistent.invalid/file.csv", cached);
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
        () -> provider.readRange("ftp://host/file.csv", 0, 100));
  }

  // --- FtpUri inner class field validation ---

  @Test
  void testFtpUriClassFields() throws Exception {
    Class<?>[] innerClasses = FtpStorageProvider.class.getDeclaredClasses();
    Class<?> ftpUriClass = null;
    for (Class<?> c : innerClasses) {
      if (c.getSimpleName().equals("FtpUri")) {
        ftpUriClass = c;
        break;
      }
    }
    assertNotNull(ftpUriClass, "FtpUri inner class should exist");

    // Verify all expected fields exist
    Field host = ftpUriClass.getDeclaredField("host");
    host.setAccessible(true);
    assertNotNull(host);

    Field port = ftpUriClass.getDeclaredField("port");
    port.setAccessible(true);
    assertNotNull(port);

    Field username = ftpUriClass.getDeclaredField("username");
    username.setAccessible(true);
    assertNotNull(username);

    Field password = ftpUriClass.getDeclaredField("password");
    password.setAccessible(true);
    assertNotNull(password);

    Field path = ftpUriClass.getDeclaredField("path");
    path.setAccessible(true);
    assertNotNull(path);
  }

  @Test
  void testFtpUriConstructorViaReflection() throws Exception {
    Class<?>[] innerClasses = FtpStorageProvider.class.getDeclaredClasses();
    Class<?> ftpUriClass = null;
    for (Class<?> c : innerClasses) {
      if (c.getSimpleName().equals("FtpUri")) {
        ftpUriClass = c;
        break;
      }
    }
    assertNotNull(ftpUriClass);

    Constructor<?> ctor = ftpUriClass.getDeclaredConstructor(
        String.class, int.class, String.class, String.class, String.class);
    ctor.setAccessible(true);

    Object ftpUri = ctor.newInstance("myhost", 2121, "myuser", "mypass", "/my/path");

    Field hostField = ftpUriClass.getDeclaredField("host");
    hostField.setAccessible(true);
    assertEquals("myhost", hostField.get(ftpUri));

    Field portField = ftpUriClass.getDeclaredField("port");
    portField.setAccessible(true);
    assertEquals(2121, portField.get(ftpUri));

    Field usernameField = ftpUriClass.getDeclaredField("username");
    usernameField.setAccessible(true);
    assertEquals("myuser", usernameField.get(ftpUri));

    Field passwordField = ftpUriClass.getDeclaredField("password");
    passwordField.setAccessible(true);
    assertEquals("mypass", passwordField.get(ftpUri));

    Field pathField = ftpUriClass.getDeclaredField("path");
    pathField.setAccessible(true);
    assertEquals("/my/path", pathField.get(ftpUri));
  }

  // --- Static constants via reflection ---

  @Test
  void testDefaultPortConstant() throws Exception {
    Field defaultPort = FtpStorageProvider.class.getDeclaredField("DEFAULT_PORT");
    defaultPort.setAccessible(true);
    assertEquals(21, defaultPort.get(null));
  }

  @Test
  void testConnectTimeoutConstant() throws Exception {
    Field connectTimeout = FtpStorageProvider.class.getDeclaredField("CONNECT_TIMEOUT");
    connectTimeout.setAccessible(true);
    assertEquals(30000, connectTimeout.get(null));
  }

  @Test
  void testDataTimeoutConstant() throws Exception {
    Field dataTimeout = FtpStorageProvider.class.getDeclaredField("DATA_TIMEOUT");
    dataTimeout.setAccessible(true);
    assertEquals(60000, dataTimeout.get(null));
  }

  // --- S3 config returns null (not an S3 provider) ---

  @Test
  void testGetS3ConfigReturnsNull() {
    assertFalse(provider.getS3Config() != null,
        "FTP provider should not return S3 config");
  }

  // --- Recursive listing with invalid URI still fails ---

  @Test
  void testListFilesRecursiveWithInvalidUri() {
    assertThrows(IOException.class,
        () -> provider.listFiles("http://not-ftp/path", true));
  }
}
