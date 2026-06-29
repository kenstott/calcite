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

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;

/**
 * Shared, dedicated, EPHEMERAL MinIO test fixture.
 *
 * <p>Spins up ONE MinIO container ({@code minio/minio:latest}) for the whole test
 * suite using the singleton-container pattern: a single static
 * {@link GenericContainer} is started on first use (guarded by a class-init lock)
 * and torn down via a JVM shutdown hook so it is reliably stopped and removed at
 * suite end. This guarantees the S3 integration tests NEVER touch a developer's
 * real/standing MinIO (or R2) on {@code http://localhost:9000}.
 *
 * <p>The container is exposed on a RANDOM mapped host port (never the fixed 9000),
 * with credentials {@code minioadmin/minioadmin} and region {@code us-east-1}.
 *
 * <p>The container config here is the exact config established by
 * {@code S3StorageProviderIntegrationTest}: {@code server /data}, root user/password
 * env vars, exposed port 9000, HTTP readiness probe on {@code /minio/health/ready}.
 *
 * <p>Usage from a test's {@code @BeforeAll}:
 * <pre>
 *   String endpoint = MinioTestContainer.endpoint();
 *   MinioTestContainer.createBucket("test-bucket");
 * </pre>
 */
public final class MinioTestContainer {

  private static final String IMAGE = "minio/minio:latest";
  private static final int MINIO_PORT = 9000;
  private static final String ACCESS_KEY = "minioadmin";
  private static final String SECRET_KEY = "minioadmin";
  private static final String REGION = "us-east-1";

  /** The single shared container, started once and removed via shutdown hook. */
  private static final GenericContainer<?> CONTAINER;

  /** The endpoint of the running container, e.g. http://localhost:32812. */
  private static final String ENDPOINT;

  static {
    CONTAINER = new GenericContainer<>(DockerImageName.parse(IMAGE))
        .withCommand("server", "/data")
        .withEnv("MINIO_ROOT_USER", ACCESS_KEY)
        .withEnv("MINIO_ROOT_PASSWORD", SECRET_KEY)
        .withExposedPorts(MINIO_PORT)
        .waitingFor(
            Wait.forHttp("/minio/health/ready")
                .forPort(MINIO_PORT)
                .withStartupTimeout(Duration.ofMinutes(2)));
    CONTAINER.start();
    ENDPOINT = "http://" + CONTAINER.getHost() + ":"
        + CONTAINER.getMappedPort(MINIO_PORT);

    // Reliably stop/remove the ephemeral container at suite end.
    Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
      @Override public void run() {
        if (CONTAINER.isRunning()) {
          CONTAINER.stop();
        }
      }
    }));
  }

  private MinioTestContainer() {
  }

  /** Forces class initialization (and therefore container startup). */
  public static void ensureStarted() {
    // Referencing ENDPOINT triggers the static initializer if not yet run.
    if (ENDPOINT == null) {
      throw new IllegalStateException("MinIO container failed to initialize");
    }
  }

  /** Endpoint of the running ephemeral MinIO, e.g. {@code http://localhost:32812}. */
  public static String endpoint() {
    return ENDPOINT;
  }

  /** Access key for the ephemeral MinIO ({@code minioadmin}). */
  public static String accessKey() {
    return ACCESS_KEY;
  }

  /** Secret key for the ephemeral MinIO ({@code minioadmin}). */
  public static String secretKey() {
    return SECRET_KEY;
  }

  /** Region for the ephemeral MinIO ({@code us-east-1}). */
  public static String region() {
    return REGION;
  }

  /**
   * Creates the given bucket in the ephemeral MinIO if it does not already exist,
   * using a path-style control-plane client built the same way the provider builds
   * its own client (custom endpoint + path-style + static creds).
   */
  public static void createBucket(String bucket) {
    AmazonS3 client = newClient();
    try {
      if (!client.doesBucketExistV2(bucket)) {
        client.createBucket(bucket);
      }
    } finally {
      client.shutdown();
    }
  }

  /**
   * Builds an {@link AmazonS3} client targeting the ephemeral MinIO with path-style
   * access and static credentials. The caller owns the returned client's lifecycle.
   */
  public static AmazonS3 newClient() {
    return AmazonS3ClientBuilder.standard()
        .withEndpointConfiguration(
            new AwsClientBuilder.EndpointConfiguration(ENDPOINT, REGION))
        .withCredentials(
            new AWSStaticCredentialsProvider(
                new BasicAWSCredentials(ACCESS_KEY, SECRET_KEY)))
        .withPathStyleAccessEnabled(true)
        .build();
  }
}
