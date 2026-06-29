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

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Hermetic requirements tests for file-adapter storage-provider SELECTION / ROUTING.
 *
 * <p>These tests assert ONLY the routing decision — which {@link StorageProvider}
 * implementation the {@link StorageProviderFactory} dispatch chooses for a given
 * schema-level {@code storageType} or per-table URL scheme. They never drive a real
 * transfer and never require a remote service to connect; the byte/catalog goldens
 * per provider live in the per-provider integration tests
 * (S3StorageProviderIntegrationTest, HDFSStorageProviderIntegrationTest,
 * SftpFtpStorageProviderIntegrationTest, HttpStorageProvider via
 * HtmlCrawlerHttpPostRequirementsTest).
 *
 * <p>Dispatch APIs under test:
 * <ul>
 *   <li>{@link StorageProviderFactory#createFromType(String, java.util.Map)}
 *       — schema-level {@code storageType} seam.</li>
 *   <li>{@link StorageProviderFactory#createFromUrl(String)}
 *       — per-table URL-scheme auto-detection seam.</li>
 * </ul>
 */
@Tag("unit")
public class StorageSelectionRequirementsTest {

  /**
   * Returns an S3 config with credentials but NO {@code directory} key, so the
   * S3StorageProvider builds its (lazy, non-connecting) client and skips the
   * bucket-existence network call. Endpoint is a dummy host that is never contacted.
   */
  private static Map<String, Object> s3CredsNoDirectory() {
    Map<String, Object> config = new HashMap<>();
    config.put("accessKeyId", "test-key");
    config.put("secretAccessKey", "test-secret");
    config.put("endpoint", "http://127.0.0.1:1");
    config.put("region", "us-east-1");
    return config;
  }

  // -------------------------------------------------------------------------
  // FILE-045 : storage-provider SELECTION priority
  // -------------------------------------------------------------------------

  /**
   * FILE-045 (1): a schema-level {@code storageType} forces ALL files through that
   * provider via {@link StorageProviderFactory#createFromType}. The dispatch keys only
   * on the type string; any {@code directory} present in the config does not redirect
   * the choice to local — there is no per-file mixing within a schema.
   *
   * <p>NOTE: the "the {@code directory} operand is ignored for provider choice / no
   * mixing within a schema" rule is enforced at the FileSchema wiring layer, which is
   * not cleanly reachable hermetically. Here we assert the reachable seam: createFromType
   * dispatches purely on the type string and a non-local type stays non-local even when a
   * (local-looking) directory is also supplied.
   */
  @Test
  @Tag("FILE-045")
  public void schemaLevelStorageTypeForcesProviderRegardlessOfDirectory() {
    Map<String, Object> httpConfigWithLocalDir = new HashMap<>();
    httpConfigWithLocalDir.put("directory", "/var/data/local-looking-path");

    StorageProvider http =
        StorageProviderFactory.createFromType("http", httpConfigWithLocalDir);
    // storageType wins: a local-looking directory does NOT route this to local.
    assertEquals("http", http.getStorageType(),
        "schema-level storageType=http must force the HTTP provider regardless of directory");
    assertTrue(http instanceof HttpStorageProvider);

    Map<String, Object> s3Config = s3CredsNoDirectory();
    StorageProvider s3 = StorageProviderFactory.createFromType("s3", s3Config);
    assertEquals("s3", s3.getStorageType(),
        "schema-level storageType=s3 must force the S3 provider");
    assertTrue(s3 instanceof S3StorageProvider);
  }

  /**
   * FILE-045 (2): per-table URL SCHEME auto-detection through
   * {@link StorageProviderFactory#createFromUrl}.
   * http(s):// -> HTTP, ftp:// -> FTP, sftp:// -> SFTP, hdfs:// -> HDFS,
   * file:// and a bare /path -> local.
   *
   * <p>NOTE: s3:// is intentionally NOT auto-detectable from a URL alone —
   * {@code createFromUrl} throws IllegalArgumentException because S3 requires explicit
   * credentials (asserted in {@link #s3SchemeFromUrlRequiresExplicitCredentials()}).
   */
  @Test
  @Tag("FILE-045")
  public void perTableUrlSchemeAutoDetection() {
    assertEquals("http",
        StorageProviderFactory.createFromUrl("http://example.com/data.csv").getStorageType());
    assertEquals("http",
        StorageProviderFactory.createFromUrl("https://example.com/data.csv").getStorageType());
    assertEquals("ftp",
        StorageProviderFactory.createFromUrl("ftp://host/data.csv").getStorageType());
    assertEquals("sftp",
        StorageProviderFactory.createFromUrl("sftp://host/data.csv").getStorageType());
    assertEquals("hdfs",
        StorageProviderFactory.createFromUrl("hdfs://namenode:8020/data.csv").getStorageType());

    assertEquals("local",
        StorageProviderFactory.createFromUrl("file:///tmp/data.csv").getStorageType());
  }

  /**
   * FILE-045 (3): the default — a bare path or empty/no scheme — is the local provider.
   */
  @Test
  @Tag("FILE-045")
  public void defaultIsLocal() {
    assertEquals("local",
        StorageProviderFactory.createFromUrl("/tmp/data.csv").getStorageType());
    assertEquals("local",
        StorageProviderFactory.createFromUrl("relative/path/data.csv").getStorageType());
    assertEquals("local",
        StorageProviderFactory.createFromUrl("").getStorageType());
    assertEquals("local",
        StorageProviderFactory.createFromUrl(null).getStorageType());

    // And the explicit storageType seam defaults to local when type is absent/empty.
    assertEquals("local",
        StorageProviderFactory.createFromType(null, null).getStorageType());
    assertEquals("local",
        StorageProviderFactory.createFromType("", null).getStorageType());
    assertEquals("local",
        StorageProviderFactory.createFromType("local", null).getStorageType());
    assertEquals("local",
        StorageProviderFactory.createFromType("file", null).getStorageType());
  }

  /**
   * FILE-045: s3:// cannot be auto-selected from a URL alone — the routing decision is
   * to refuse (S3 requires explicit credentials via storageConfig), not to guess.
   */
  @Test
  @Tag("FILE-045")
  public void s3SchemeFromUrlRequiresExplicitCredentials() {
    assertThrows(IllegalArgumentException.class,
        () -> StorageProviderFactory.createFromUrl("s3://bucket/key.csv"));
  }

  // -------------------------------------------------------------------------
  // FILE-024 : each remote provider type reachable through the same seam
  // -------------------------------------------------------------------------

  /**
   * FILE-024: every remote provider type is reachable through the same
   * {@link StorageProviderFactory} seam, producing the correct provider CLASS.
   * (Live byte/catalog behavior is covered by the per-provider integration tests.)
   */
  @Test
  @Tag("FILE-024")
  public void factoryProducesCorrectProviderClassPerScheme() {
    // http / https
    assertTrue(StorageProviderFactory.createFromUrl("https://h/f") instanceof HttpStorageProvider);
    assertTrue(StorageProviderFactory.createFromType("http", null) instanceof HttpStorageProvider);

    // ftp / ftps
    assertTrue(StorageProviderFactory.createFromUrl("ftp://h/f") instanceof FtpStorageProvider);
    assertTrue(StorageProviderFactory.createFromType("ftps", null) instanceof FtpStorageProvider);

    // sftp
    assertTrue(StorageProviderFactory.createFromUrl("sftp://h/f") instanceof SftpStorageProvider);
    assertTrue(StorageProviderFactory.createFromType("sftp", null) instanceof SftpStorageProvider);

    // hdfs
    assertTrue(StorageProviderFactory.createFromUrl("hdfs://n:8020/f") instanceof HDFSStorageProvider);
    assertTrue(StorageProviderFactory.createFromType("hdfs", null) instanceof HDFSStorageProvider);

    // s3 (via explicit-credentials type seam; URL alone is refused per FILE-045)
    assertTrue(StorageProviderFactory.createFromType("s3", s3CredsNoDirectory())
        instanceof S3StorageProvider);

    // local
    assertTrue(StorageProviderFactory.createFromType("local", null)
        instanceof LocalFileStorageProvider);

    // NOTE: SharePoint is pending live creds and is not asserted here (its branch in
    // createFromType requires a real siteUrl + auth material).
  }
}
