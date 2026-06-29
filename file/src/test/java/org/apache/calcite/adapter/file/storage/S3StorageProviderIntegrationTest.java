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

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration test that validates {@link S3StorageProvider} against a real MinIO
 * S3-compatible object store running in a Testcontainers-managed Docker container.
 *
 * <p>This is the TEMPLATE for the live-service bucket: the Testcontainers + provider
 * wiring established here (custom endpoint, path-style access, static credentials) is
 * the same wiring used against any S3-compatible service.
 *
 * <p>The container is started once for the class in {@link #startMinio()} and torn
 * down in {@link #stopMinio()}. A control-plane {@link AmazonS3} client (built the
 * same way the provider builds its own client) is used only to create the bucket and
 * to upload the known fixture objects; every assertion exercises the provider itself.
 *
 * <p>Run with:
 * {@code ./gradlew :file:test -PincludeTags="FILE-023,FILE-065" --console=plain}
 */
@Tag("integration")
@Execution(ExecutionMode.SAME_THREAD)
public class S3StorageProviderIntegrationTest {

  private static final String IMAGE = "minio/minio:latest";
  private static final int MINIO_PORT = 9000;
  private static final String ACCESS_KEY = "minioadmin";
  private static final String SECRET_KEY = "minioadmin";
  private static final String REGION = "us-east-1";
  private static final String BUCKET = "calcite-it-bucket";

  /** Manual lifecycle (started in @BeforeAll, stopped in @AfterAll). */
  private static GenericContainer<?> minio;

  /** Custom endpoint of the running MinIO container, e.g. http://localhost:32812. */
  private static String endpoint;

  /** Control-plane client used ONLY to create the bucket + upload fixtures. */
  private static AmazonS3 controlClient;

  // -----------------------------------------------------------------------
  // Lifecycle
  // -----------------------------------------------------------------------

  @BeforeAll
  static void startMinio() {
    minio = new GenericContainer<>(DockerImageName.parse(IMAGE))
        .withCommand("server", "/data")
        .withEnv("MINIO_ROOT_USER", ACCESS_KEY)
        .withEnv("MINIO_ROOT_PASSWORD", SECRET_KEY)
        .withExposedPorts(MINIO_PORT)
        .waitingFor(
            Wait.forHttp("/minio/health/ready")
                .forPort(MINIO_PORT)
                .withStartupTimeout(Duration.ofMinutes(2)));
    minio.start();

    endpoint = "http://" + minio.getHost() + ":" + minio.getMappedPort(MINIO_PORT);

    // Build the control-plane client exactly the way the provider does: custom
    // endpoint + path-style + static creds. This proves the wiring before any test.
    controlClient = AmazonS3ClientBuilder.standard()
        .withEndpointConfiguration(
            new AwsClientBuilder.EndpointConfiguration(endpoint, REGION))
        .withCredentials(
            new AWSStaticCredentialsProvider(
                new BasicAWSCredentials(ACCESS_KEY, SECRET_KEY)))
        .withPathStyleAccessEnabled(true)
        .build();

    if (!controlClient.doesBucketExistV2(BUCKET)) {
      controlClient.createBucket(BUCKET);
    }
  }

  @AfterAll
  static void stopMinio() {
    if (controlClient != null) {
      controlClient.shutdown();
      controlClient = null;
    }
    if (minio != null) {
      minio.stop();
      minio = null;
    }
  }

  // -----------------------------------------------------------------------
  // Helpers
  // -----------------------------------------------------------------------

  /** Uploads an object with the given key and UTF-8 content via the control client. */
  private static void putObject(String key, String content) {
    byte[] bytes = content.getBytes(StandardCharsets.UTF_8);
    ObjectMetadata meta = new ObjectMetadata();
    meta.setContentLength(bytes.length);
    meta.setContentType("text/csv");
    controlClient.putObject(BUCKET, key, new ByteArrayInputStream(bytes), meta);
  }

  /**
   * Builds an {@link S3StorageProvider} configured with the MinIO custom endpoint,
   * path-style access (implied by the provider whenever an endpoint is present) and
   * static accessKey/secretKey credentials, rooted at the given prefix.
   */
  private static S3StorageProvider createProvider(String prefix) {
    Map<String, Object> config = new HashMap<>();
    config.put("accessKeyId", ACCESS_KEY);
    config.put("secretAccessKey", SECRET_KEY);
    config.put("endpoint", endpoint);
    config.put("region", REGION);
    config.put("directory", "s3://" + BUCKET + "/" + prefix);
    return new S3StorageProvider(config);
  }

  /** Reads all bytes from a stream. */
  private static byte[] readAll(InputStream is) throws IOException {
    ByteArrayOutputStream buf = new ByteArrayOutputStream();
    byte[] tmp = new byte[4096];
    int n;
    while ((n = is.read(tmp)) != -1) {
      buf.write(tmp, 0, n);
    }
    return buf.toByteArray();
  }

  // -----------------------------------------------------------------------
  // FILE-023: recursive catalog enumeration + byte-exact read
  // -----------------------------------------------------------------------

  /**
   * Uploads a known set of objects (including some under nested prefixes), then asserts
   * that {@link S3StorageProvider#listFiles} returns exactly that catalog under a prefix
   * (recursive enumeration of keys) and that {@link S3StorageProvider#openInputStream}
   * returns the exact bytes of one object.
   */
  @Tag("FILE-023")
  @Tag("integration")
  @Test
  void listFilesReturnsRecursiveCatalogAndOpenInputStreamReturnsExactBytes()
      throws IOException {
    String base = "file-023/catalog/";

    // Known fixture set: two at the root of the prefix, three under nested prefixes.
    String rootA = "root-a.csv";
    String rootB = "root-b.json";
    String nested1 = "sub1/nested-1.csv";
    String nested2 = "sub1/sub2/nested-2.csv";
    String nested3 = "sub3/nested-3.txt";

    String knownContent = "id,name\n1,alice\n2,bob\n3,charlie\n";

    putObject(base + rootA, knownContent);
    putObject(base + rootB, "{\"k\":\"v\"}");
    putObject(base + nested1, "n1\n");
    putObject(base + nested2, "n2\n");
    putObject(base + nested3, "n3\n");

    S3StorageProvider provider = createProvider(base);

    // Recursive listing must enumerate every key under the prefix (flattened).
    List<StorageProvider.FileEntry> entries =
        provider.listFiles("s3://" + BUCKET + "/" + base, true);

    Set<String> actualPaths = new HashSet<>();
    for (StorageProvider.FileEntry entry : entries) {
      assertTrue(!entry.isDirectory(),
          "recursive listing should not include directory entries: " + entry.getPath());
      actualPaths.add(entry.getPath());
    }

    Set<String> expectedPaths = new HashSet<>();
    expectedPaths.add("s3://" + BUCKET + "/" + base + rootA);
    expectedPaths.add("s3://" + BUCKET + "/" + base + rootB);
    expectedPaths.add("s3://" + BUCKET + "/" + base + nested1);
    expectedPaths.add("s3://" + BUCKET + "/" + base + nested2);
    expectedPaths.add("s3://" + BUCKET + "/" + base + nested3);

    assertEquals(expectedPaths, actualPaths,
        "recursive catalog set must match the uploaded keys exactly");

    // Byte-for-byte read of one object, asserted at the provider seam.
    byte[] expectedBytes = knownContent.getBytes(StandardCharsets.UTF_8);
    try (InputStream is =
             provider.openInputStream("s3://" + BUCKET + "/" + base + rootA)) {
      byte[] actualBytes = readAll(is);
      assertArrayEquals(expectedBytes, actualBytes,
          "openInputStream must return the exact uploaded bytes");
    }
  }

  // -----------------------------------------------------------------------
  // FILE-065: custom endpoint (MinIO) + path-style + storageConfig credentials
  // -----------------------------------------------------------------------

  /**
   * Proves the provider works against a CUSTOM ENDPOINT (the MinIO container) with
   * path-style access and credentials supplied via storageConfig accessKey/secretKey:
   * configured that way, it successfully lists and reads an object. Path-style access
   * is what makes {@code http://host:port/bucket/key} addressing work against MinIO
   * (vs. virtual-host {@code bucket.host}); a successful list/read here proves it.
   */
  @Tag("FILE-065")
  @Tag("integration")
  @Test
  void customEndpointWithPathStyleAndCredentialsListsAndReads() throws IOException {
    String base = "file-065/endpoint/";
    String key = "custom-endpoint.csv";
    String content = "col1,col2\nval1,val2\n";
    putObject(base + key, content);

    // Provider configured with the MinIO endpoint + path-style + storageConfig creds.
    S3StorageProvider provider = createProvider(base);

    // The provider stored the custom endpoint in its S3 config (proves wiring).
    Map<String, String> s3Config = provider.getS3Config();
    assertNotNull(s3Config, "provider should expose its S3 config");
    assertEquals(endpoint, s3Config.get("endpoint"),
        "provider must be configured with the custom MinIO endpoint");
    assertEquals(ACCESS_KEY, s3Config.get("accessKeyId"));
    assertEquals(SECRET_KEY, s3Config.get("secretAccessKey"));

    // List against the custom endpoint with path-style addressing.
    List<StorageProvider.FileEntry> entries =
        provider.listFiles("s3://" + BUCKET + "/" + base, true);
    assertEquals(1, entries.size(),
        "custom-endpoint list must return the single uploaded object");
    assertEquals("s3://" + BUCKET + "/" + base + key, entries.get(0).getPath());

    // Read back through the custom endpoint and verify exact bytes.
    try (InputStream is =
             provider.openInputStream("s3://" + BUCKET + "/" + base + key)) {
      assertEquals(content, new String(readAll(is), StandardCharsets.UTF_8),
          "custom-endpoint read must return the exact uploaded content");
    }
  }
}
