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
import org.apache.calcite.adapter.file.storage.HttpConfig;
import org.apache.calcite.adapter.file.storage.HttpStorageProvider;
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
import java.io.InputStream;
import java.io.Reader;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
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
 *
 * <p>FILE-118: {@code StorageProvider.hasChanged} change-detection — null cached → changed; size,
 * then ETag (when both present, short-circuiting), then lastModified with a strict 1000ms tolerance;
 * any IOException → changed. Asserted against an in-memory provider double with fixed epoch constants.
 *
 * <p>FILE-114: {@code HttpStorageProvider} — listFiles always UnsupportedOperationException,
 * isDirectory always false, {@code HttpConfig.cacheTtl} default 300000ms, and the bearer&gt;apiKey&gt;
 * basic auth precedence (read back from the request properties applyAuth sets, no socket opened).
 * The hard-coded timeouts/User-Agent and the 200/201/304 response-code accept-set are reachable only
 * through a live request and are deferred to the HTTP live-service test (FILE-066 seam).
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

  // ============================================================ FILE-118 =====================
  // StorageProvider.hasChanged: null cached -> changed; size, then ETag (both present, short-circuit),
  // then lastModified with a strict 1000ms tolerance; IOException -> changed. Fixed epoch constants.

  private static final long BASE = 1_700_000_000_000L;
  private static final String PATH = "/test/file.csv";

  /** In-memory provider whose getMetadata returns a caller-set "current" metadata (or throws). */
  private static final class HasChangedProvider implements StorageProvider {
    private StorageProvider.FileMetadata current;
    private boolean throwOnGetMetadata;

    @Override public StorageProvider.FileMetadata getMetadata(String path) throws IOException {
      if (throwOnGetMetadata) {
        throw new IOException("forced getMetadata failure");
      }
      return current;
    }

    @Override public List<StorageProvider.FileEntry> listFiles(String path, boolean recursive) {
      return Collections.emptyList();
    }

    @Override public InputStream openInputStream(String path) {
      throw new UnsupportedOperationException();
    }

    @Override public Reader openReader(String path) {
      throw new UnsupportedOperationException();
    }

    @Override public boolean exists(String path) {
      return true;
    }

    @Override public boolean isDirectory(String path) {
      return false;
    }

    @Override public String getStorageType() {
      return "test";
    }

    @Override public String resolvePath(String basePath, String relativePath) {
      return basePath + "/" + relativePath;
    }
  }

  private static StorageProvider.FileMetadata md(long size, long lastModified, String etag) {
    return new StorageProvider.FileMetadata(PATH, size, lastModified, null, etag);
  }

  @Test @Tag("FILE-118") void nullCachedMetadataIsAlwaysChanged() throws IOException {
    // Null cached returns before any getMetadata call (provider has no current set).
    assertTrue(new HasChangedProvider().hasChanged(PATH, null),
        "a null cached metadata must always be treated as changed");
  }

  @Test @Tag("FILE-118") void differingSizeIsChangedBeforeEtagCheck() throws IOException {
    HasChangedProvider p = new HasChangedProvider();
    p.current = md(200, BASE, "same");                 // identical etag, but size differs
    assertTrue(p.hasChanged(PATH, md(100, BASE, "same")),
        "a size difference is changed and is checked before the etag");
  }

  @Test @Tag("FILE-118") void sameSizeSameEtagIsUnchanged() throws IOException {
    HasChangedProvider p = new HasChangedProvider();
    p.current = md(100, BASE, "etag-same");
    assertFalse(p.hasChanged(PATH, md(100, BASE, "etag-same")),
        "equal size and equal etag is unchanged");
  }

  @Test @Tag("FILE-118") void differingEtagIsChanged() throws IOException {
    HasChangedProvider p = new HasChangedProvider();
    p.current = md(100, BASE, "etag-new");
    assertTrue(p.hasChanged(PATH, md(100, BASE, "etag-old")),
        "same size but different etag is changed");
  }

  @Test @Tag("FILE-118") void etagWinsOverLastModifiedWhenBothPresent() throws IOException {
    HasChangedProvider p = new HasChangedProvider();
    p.current = md(100, BASE + 10_000, "etag-same");   // 10s lastModified gap, far beyond tolerance
    assertFalse(p.hasChanged(PATH, md(100, BASE, "etag-same")),
        "equal etags short-circuit before lastModified, so a large mtime gap is ignored");
  }

  @Test @Tag("FILE-118") void oneEtagNullFallsThroughToLastModified() throws IOException {
    HasChangedProvider p = new HasChangedProvider();
    p.current = md(100, BASE, "etag-abc");
    assertFalse(p.hasChanged(PATH, md(100, BASE, null)),
        "a null etag on either side skips the etag block; equal lastModified is unchanged");
  }

  @Test @Tag("FILE-118") void lastModifiedAtToleranceBoundaryIsUnchanged() throws IOException {
    HasChangedProvider p = new HasChangedProvider();
    p.current = md(100, BASE, null);
    // diff of exactly 1000ms is NOT changed (boundary is strict >).
    assertFalse(p.hasChanged(PATH, md(100, BASE - 1000, null)),
        "lastModified delta of exactly 1000ms is within tolerance (strict >)");
  }

  @Test @Tag("FILE-118") void lastModifiedBeyondToleranceIsChangedBothDirections() throws IOException {
    HasChangedProvider p = new HasChangedProvider();
    p.current = md(100, BASE, null);
    assertTrue(p.hasChanged(PATH, md(100, BASE - 1001, null)),
        "lastModified delta of 1001ms exceeds tolerance");
    assertTrue(p.hasChanged(PATH, md(100, BASE + 1001, null)),
        "Math.abs: a 1001ms delta in either direction exceeds tolerance");
  }

  @Test @Tag("FILE-118") void ioExceptionFromGetMetadataAssumesChanged() throws IOException {
    HasChangedProvider p = new HasChangedProvider();
    p.throwOnGetMetadata = true;
    assertTrue(p.hasChanged(PATH, md(100, BASE, null)),
        "an IOException reading current metadata is treated as changed");
  }

  // ============================================================ FILE-114 =====================
  // HttpStorageProvider structural contract + auth precedence. Hermetic: no socket is opened —
  // applyAuth only sets request properties, read back from a lazily-created (never connected) conn.

  @Test @Tag("FILE-114") void httpListFilesAlwaysUnsupported() {
    HttpStorageProvider p = new HttpStorageProvider();
    UnsupportedOperationException ex = assertThrows(UnsupportedOperationException.class,
        () -> p.listFiles("http://example.com/", true));
    assertEquals("HTTP storage does not support directory listing", ex.getMessage());
  }

  @Test @Tag("FILE-114") void httpIsDirectoryAlwaysFalse() throws IOException {
    HttpStorageProvider p = new HttpStorageProvider();
    assertFalse(p.isDirectory("http://example.com/dir/"), "HTTP isDirectory is always false");
    assertFalse(p.isDirectory("http://example.com/file.csv"));
  }

  @Test @Tag("FILE-114") void httpConfigCacheTtlDefaults300000() {
    assertEquals(300000L, new HttpConfig.Builder().build().getCacheTtl(),
        "HttpConfig cacheTtl must default to 300000ms");
  }

  /**
   * Records request properties applyAuth sets. A real {@code sun.net} HttpURLConnection filters the
   * {@code Authorization}/{@code Proxy-Authorization} headers out of getRequestProperty for security,
   * so we capture the set calls directly to verify exactly what applyAuth applied.
   */
  private static final class RecordingConnection extends HttpURLConnection {
    private final Map<String, String> props = new HashMap<String, String>();

    RecordingConnection() throws Exception {
      super(new URI("http://example.com/x").toURL());
    }

    @Override public void setRequestProperty(String key, String value) {
      props.put(key, value);
    }

    @Override public String getRequestProperty(String key) {
      return props.get(key);
    }

    @Override public void connect() { }

    @Override public void disconnect() { }

    @Override public boolean usingProxy() {
      return false;
    }
  }

  /** Invokes the private applyAuth on a recording conn (no socket) and returns it for readback. */
  private static RecordingConnection applyAuthTo(HttpConfig config) throws Exception {
    HttpStorageProvider p =
        new HttpStorageProvider("GET", null, new HashMap<String, String>(), null, config);
    RecordingConnection conn = new RecordingConnection();
    Method m = HttpStorageProvider.class.getDeclaredMethod("applyAuth", HttpURLConnection.class);
    m.setAccessible(true);
    m.invoke(p, conn);
    return conn;
  }

  @Test @Tag("FILE-114") void authBearerWinsOverApiKeyAndBasic() throws Exception {
    HttpConfig config =
        new HttpConfig.Builder().bearerToken("tok").apiKey("k").basicAuth("u", "pw").build();
    HttpURLConnection conn = applyAuthTo(config);
    assertEquals("Bearer tok", conn.getRequestProperty("Authorization"),
        "bearer token wins over apiKey and basic");
    assertNull(conn.getRequestProperty("X-API-Key"), "apiKey header must not be set when bearer wins");
  }

  @Test @Tag("FILE-114") void authApiKeyWinsOverBasic() throws Exception {
    HttpConfig config = new HttpConfig.Builder().apiKey("k").basicAuth("u", "pw").build();
    HttpURLConnection conn = applyAuthTo(config);
    assertEquals("k", conn.getRequestProperty("X-API-Key"), "apiKey wins over basic");
    assertNull(conn.getRequestProperty("Authorization"), "basic header must not be set when apiKey wins");
  }

  @Test @Tag("FILE-114") void authBasicWhenOnlyBasicPresent() throws Exception {
    HttpConfig config = new HttpConfig.Builder().basicAuth("u", "pw").build();
    HttpURLConnection conn = applyAuthTo(config);
    String expected =
        "Basic " + Base64.getEncoder().encodeToString("u:pw".getBytes(StandardCharsets.UTF_8));
    assertEquals(expected, conn.getRequestProperty("Authorization"), "basic auth header");
  }
}
