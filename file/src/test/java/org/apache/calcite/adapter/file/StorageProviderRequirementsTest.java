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

import org.apache.calcite.adapter.file.storage.FtpStorageProvider;
import org.apache.calcite.adapter.file.storage.LocalFileStorageProvider;
import org.apache.calcite.adapter.file.storage.S3StorageProvider;
import org.apache.calcite.adapter.file.storage.SftpStorageProvider;
import org.apache.calcite.adapter.file.storage.StorageProvider;
import org.apache.calcite.adapter.file.storage.StorageProviderFactory;

import com.amazonaws.services.s3.AmazonS3;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * FILE-046 / FILE-113 / FILE-115 / FILE-116 — exact, hermetic goldens recoding the weak
 * storage-provider tests (the {@code assertNotNull}/{@code instanceof}-only coverage tests) into
 * precise contract assertions.
 *
 * <p>Strictly hermetic: no FTP/SFTP/S3/HTTP/HDFS server is ever contacted. Every assertion is on
 * factory dispatch, constructor/config validation, parse-time defaults, or local-filesystem
 * behavior against a {@link TempDir}. Where a documented default (FTP/SFTP default ports,
 * SFTP {@code strictHostKeyChecking}, S3 region/path-style) is not exposed through a public
 * accessor, it is read directly from the declaring field/built client so the assertion remains
 * exact rather than degrading to a smoke test.
 */
@Tag("unit")
public class StorageProviderRequirementsTest {

  // ============================================================ FILE-046 =====================
  // StorageProviderFactory.createFromType returns the right concrete provider per storage type.

  @Test @Tag("FILE-046") void factoryLocalAndFileTypesYieldLocalProvider() {
    StorageProviderFactory.clearCache();
    StorageProvider local = StorageProviderFactory.createFromType("local", null);
    StorageProvider file = StorageProviderFactory.createFromType("file", null);

    assertTrue(local instanceof LocalFileStorageProvider,
        "type 'local' must yield LocalFileStorageProvider");
    assertTrue(file instanceof LocalFileStorageProvider,
        "type 'file' must yield LocalFileStorageProvider");
    assertEquals("local", local.getStorageType());
    assertEquals("local", file.getStorageType());
    // "local" and "file" share the same cache key, so the factory returns the identical instance.
    assertSame(local, file, "'local' and 'file' share one cached LocalFileStorageProvider");
  }

  @Test @Tag("FILE-046") void factoryHttpAndHttpsTypesYieldHttpProvider() {
    StorageProviderFactory.clearCache();
    StorageProvider http = StorageProviderFactory.createFromType("http", null);
    StorageProvider https = StorageProviderFactory.createFromType("https", null);

    assertEquals("http", http.getStorageType(), "type 'http' must yield the http provider");
    assertEquals("http", https.getStorageType(), "type 'https' must yield the http provider");
    assertSame(http, https, "'http' and 'https' share one cached HttpStorageProvider");
  }

  @Test @Tag("FILE-046") void factoryFtpAndFtpsTypesYieldFtpProvider() {
    StorageProviderFactory.clearCache();
    StorageProvider ftp = StorageProviderFactory.createFromType("ftp", null);
    StorageProvider ftps = StorageProviderFactory.createFromType("ftps", null);

    assertTrue(ftp instanceof FtpStorageProvider, "type 'ftp' must yield FtpStorageProvider");
    assertTrue(ftps instanceof FtpStorageProvider, "type 'ftps' must yield FtpStorageProvider");
    assertEquals("ftp", ftp.getStorageType());
    assertSame(ftp, ftps, "'ftp' and 'ftps' share one cached FtpStorageProvider");
  }

  @Test @Tag("FILE-046") void factorySftpTypeYieldsSftpProvider() {
    StorageProviderFactory.clearCache();
    StorageProvider sftp = StorageProviderFactory.createFromType("sftp", null);

    assertTrue(sftp instanceof SftpStorageProvider, "type 'sftp' must yield SftpStorageProvider");
    assertEquals("sftp", sftp.getStorageType());
  }

  @Test @Tag("FILE-046") void factoryHdfsTypeYieldsHdfsProvider() {
    StorageProviderFactory.clearCache();
    // No Hadoop NameNode is contacted by construction; a bare Configuration is sufficient.
    StorageProvider hdfs = StorageProviderFactory.createFromType("hdfs", null);

    assertNotNull(hdfs, "type 'hdfs' must yield a provider");
    assertEquals("hdfs", hdfs.getStorageType(), "type 'hdfs' must yield the hdfs provider");
  }

  @Test @Tag("FILE-046") void factoryUnknownTypeThrowsIllegalArgument() {
    StorageProviderFactory.clearCache();
    IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
        () -> StorageProviderFactory.createFromType("nosuchscheme", null));
    assertTrue(ex.getMessage().contains("Unsupported storage type: nosuchscheme"),
        "message must name the unsupported type (got '" + ex.getMessage() + "')");
  }

  // ============================================================ FILE-113 =====================
  // LocalFileStorageProvider.listFiles error contract + FileEntry directory/file shape.

  @Test @Tag("FILE-113") void listFilesOnMissingPathThrowsDoesNotExist(@TempDir Path dir) {
    LocalFileStorageProvider provider = new LocalFileStorageProvider();
    Path missing = dir.resolve("not-here");
    IOException ex = assertThrows(IOException.class,
        () -> provider.listFiles(missing.toString(), false));
    assertTrue(ex.getMessage().contains("does not exist"),
        "missing-directory message must contain 'does not exist' (got '" + ex.getMessage() + "')");
  }

  @Test @Tag("FILE-113") void listFilesOnRegularFileThrowsDoesNotExist(@TempDir Path dir)
      throws IOException {
    LocalFileStorageProvider provider = new LocalFileStorageProvider();
    Path file = dir.resolve("plain.txt");
    Files.write(file, "hello".getBytes(StandardCharsets.UTF_8));

    // listFiles requires a directory; a regular file is rejected with the same contract message.
    IOException ex = assertThrows(IOException.class,
        () -> provider.listFiles(file.toString(), false));
    assertTrue(ex.getMessage().contains("does not exist"),
        "non-directory message must contain 'does not exist' (got '" + ex.getMessage() + "')");
  }

  @Test @Tag("FILE-113") void fileEntryReportsDirectoryAndFileShape(@TempDir Path dir)
      throws IOException {
    LocalFileStorageProvider provider = new LocalFileStorageProvider();

    // One subdirectory and one file with a known, non-zero length.
    Files.createDirectory(dir.resolve("subdir"));
    byte[] payload = "0123456789".getBytes(StandardCharsets.UTF_8); // exactly 10 bytes
    Path file = dir.resolve("data.bin");
    Files.write(file, payload);

    List<StorageProvider.FileEntry> entries = provider.listFiles(dir.toString(), false);
    assertEquals(2, entries.size(), "listing must contain exactly the subdir and the file");

    StorageProvider.FileEntry dirEntry = findByName(entries, "subdir");
    StorageProvider.FileEntry fileEntry = findByName(entries, "data.bin");

    assertTrue(dirEntry.isDirectory(), "subdir entry must report isDirectory()==true");
    assertEquals(0L, dirEntry.getSize(), "directory entry size must be 0");

    assertFalse(fileEntry.isDirectory(), "file entry must report isDirectory()==false");
    assertEquals(payload.length, fileEntry.getSize(),
        "file entry size must equal the real file length");
    assertEquals(Files.size(file), fileEntry.getSize(),
        "file entry size must match Files.size()");
  }

  private static StorageProvider.FileEntry findByName(List<StorageProvider.FileEntry> entries,
      String name) {
    for (StorageProvider.FileEntry e : entries) {
      if (name.equals(e.getName())) {
        return e;
      }
    }
    throw new AssertionError("no FileEntry named '" + name + "' in listing");
  }

  // ============================================================ FILE-115 =====================
  // FTP/SFTP documented default ports + SFTP strictHostKeyChecking default. Parse/construct only —
  // no socket is ever opened. The constants are private, so they are read from their declaring
  // fields to keep the assertion exact (a parse path would require a live connection to observe).

  @Test @Tag("FILE-115") void ftpDefaultPortIs21() throws Exception {
    int defaultPort = readStaticIntField(FtpStorageProvider.class, "DEFAULT_PORT");
    assertEquals(21, defaultPort, "FTP default port must be 21");
  }

  @Test @Tag("FILE-115") void sftpDefaultPortIs22() throws Exception {
    int defaultPort = readStaticIntField(SftpStorageProvider.class, "DEFAULT_PORT");
    assertEquals(22, defaultPort, "SFTP default port must be 22");
  }

  @Test @Tag("FILE-115") void sftpStrictHostKeyCheckingDefaultsFalse() throws Exception {
    // Default constructor performs no connection; it only initializes local defaults.
    SftpStorageProvider provider = new SftpStorageProvider();
    Field f = SftpStorageProvider.class.getDeclaredField("strictHostKeyChecking");
    f.setAccessible(true);
    boolean strict = f.getBoolean(provider);
    assertFalse(strict, "SFTP strictHostKeyChecking must default to false");
  }

  private static int readStaticIntField(Class<?> owner, String name) throws Exception {
    Field f = owner.getDeclaredField(name);
    f.setAccessible(true);
    return f.getInt(null);
  }

  // ============================================================ FILE-116 =====================
  // S3StorageProvider construction/config validation. No real S3 calls: the client is built
  // locally (the AWS SDK builder makes no network call) and never invoked. Without a "directory"
  // operand the constructor performs no bucket existence check, so no request is issued.

  @Test @Tag("FILE-116") void s3MissingCredentialsThrowsIllegalArgument() {
    Map<String, Object> noAccessKey = new HashMap<>();
    noAccessKey.put("secretAccessKey", "secret");
    assertThrows(IllegalArgumentException.class, () -> new S3StorageProvider(noAccessKey),
        "missing accessKeyId must throw IllegalArgumentException");

    Map<String, Object> noSecret = new HashMap<>();
    noSecret.put("accessKeyId", "key");
    assertThrows(IllegalArgumentException.class, () -> new S3StorageProvider(noSecret),
        "missing secretAccessKey must throw IllegalArgumentException");
  }

  @Test @Tag("FILE-116") void s3WithEndpointEnablesPathStyleAccess() throws Exception {
    Map<String, Object> config = new HashMap<>();
    config.put("accessKeyId", "key");
    config.put("secretAccessKey", "secret");
    config.put("endpoint", "http://localhost:9000"); // MinIO-style endpoint; never contacted
    // No "directory" -> no ensureBucketExists() -> no network.
    S3StorageProvider provider = new S3StorageProvider(config);

    assertTrue(isPathStyleAccessEnabled(provider),
        "a non-null endpoint must enable path-style access");
  }

  @Test @Tag("FILE-116") void s3RegionDefaultsToUsEast1WhenAbsent() throws Exception {
    Map<String, Object> config = new HashMap<>();
    config.put("accessKeyId", "key");
    config.put("secretAccessKey", "secret");
    config.put("endpoint", "http://localhost:9000"); // endpoint config carries the signing region
    // region intentionally absent.
    S3StorageProvider provider = new S3StorageProvider(config);

    AmazonS3 client = readS3Client(provider);
    Method getRegionName = client.getClass().getMethod("getRegionName");
    String region = (String) getRegionName.invoke(client);
    assertEquals("us-east-1", region, "region must default to us-east-1 when absent from config");
  }

  /** Reads the private {@code s3Client} field from an {@link S3StorageProvider} (no network). */
  private static AmazonS3 readS3Client(S3StorageProvider provider) throws Exception {
    Field f = S3StorageProvider.class.getDeclaredField("s3Client");
    f.setAccessible(true);
    return (AmazonS3) f.get(provider);
  }

  /**
   * Reads {@code AmazonS3Client.clientOptions.isPathStyleAccess()} via reflection. Path-style is
   * applied to the builder and not surfaced through any public provider accessor, so the built
   * client's stored option is the only exact, hermetic source of truth.
   */
  private static boolean isPathStyleAccessEnabled(S3StorageProvider provider) throws Exception {
    AmazonS3 client = readS3Client(provider);
    Field optionsField = client.getClass().getDeclaredField("clientOptions");
    optionsField.setAccessible(true);
    Object options = optionsField.get(client);
    assertNotNull(options, "built S3 client must carry S3ClientOptions");
    Method isPathStyle = options.getClass().getMethod("isPathStyleAccess");
    return (Boolean) isPathStyle.invoke(options);
  }
}
