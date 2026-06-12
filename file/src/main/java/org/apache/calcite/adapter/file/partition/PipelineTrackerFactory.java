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
package org.apache.calcite.adapter.file.partition;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;

/**
 * Factory for creating {@link PipelineTracker} instances based on backend type.
 *
 * <p>Supports the following backends:
 * <ul>
 *   <li>{@code duckdb} (default) - Local DuckDB-based tracking via
 *       {@link DuckDBPartitionStatusStore}</li>
 *   <li>{@code s3} - S3 hive-partitioned append-only parquet via
 *       {@link S3HivePipelineTracker}</li>
 *   <li>{@code pg} / {@code postgres} - PostgreSQL-based tracking via
 *       {@link PGPipelineTracker}</li>
 *   <li>{@code noop} - No tracking (forces full rebuild every time)</li>
 * </ul>
 *
 * <p>Backend selection can be configured via:
 * <ol>
 *   <li>Schema operand: {@code "trackerBackend": "s3"}</li>
 *   <li>Environment variable: {@code CALCITE_TRACKER_BACKEND=s3}</li>
 *   <li>Default: {@code "duckdb"}</li>
 * </ol>
 */
public final class PipelineTrackerFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(PipelineTrackerFactory.class);

  /** Environment variable for tracker backend selection. */
  private static final String ENV_TRACKER_BACKEND = "CALCITE_TRACKER_BACKEND";

  private PipelineTrackerFactory() {
    // Utility class
  }

  /**
   * Create a PipelineTracker for the given backend and base directory.
   *
   * @param backend        Backend type: "duckdb", "s3", "pg", "postgres", "noop"
   * @param baseDirectory  Base directory for local backends (DuckDB file location)
   * @param config         Additional configuration (S3 bucket, PG connection, etc.)
   * @return PipelineTracker instance
   */
  public static PipelineTracker create(String backend, String baseDirectory,
      Map<String, String> config) {
    String resolvedBackend = resolveBackend(backend);

    LOGGER.info("Creating PipelineTracker: backend={}, baseDirectory={}",
        resolvedBackend, baseDirectory);

    switch (resolvedBackend) {
    case "duckdb":
      return DuckDBPartitionStatusStore.getInstance(baseDirectory);

    case "s3":
      return createS3Tracker(baseDirectory, config);

    case "pg":
    case "postgres":
      return createPGTracker(config);

    case "noop":
      return PipelineTracker.NOOP_PIPELINE;

    default:
      LOGGER.warn("Unknown tracker backend '{}', falling back to duckdb", resolvedBackend);
      return DuckDBPartitionStatusStore.getInstance(baseDirectory);
    }
  }

  /**
   * Create a PipelineTracker using backend from environment or default.
   *
   * @param baseDirectory  Base directory for local backends
   * @return PipelineTracker instance
   */
  public static PipelineTracker create(String baseDirectory) {
    return create(null, baseDirectory, Collections.<String, String>emptyMap());
  }

  /**
   * Create a PipelineTracker using operand configuration.
   *
   * @param operand        Schema operand map (may contain trackerBackend, trackerConfig)
   * @param baseDirectory  Base directory for local backends
   * @return PipelineTracker instance
   */
  @SuppressWarnings("unchecked")
  public static PipelineTracker createFromOperand(Map<String, Object> operand,
      String baseDirectory) {
    String backend = (String) operand.get("trackerBackend");
    Map<String, String> config = new java.util.HashMap<>();
    Object trackerConfig = operand.get("trackerConfig");
    if (trackerConfig instanceof Map) {
      config.putAll((Map<String, String>) trackerConfig);
    }
    // Merge s3Config credentials into tracker config if not already set
    Object s3Config = operand.get("s3Config");
    if (s3Config instanceof Map) {
      Map<String, String> s3 = (Map<String, String>) s3Config;
      for (String key : new String[]{"accessKeyId", "secretAccessKey", "endpoint", "region"}) {
        if (!config.containsKey(key) && s3.containsKey(key)) {
          config.put(key, s3.get(key));
        }
      }
    }
    // Namespace the tracker by schema so each schema's per-key markers live under their own
    // prefix (<bucket>/<schema>/year=*/source_key=*). Without this, every schema shares the flat
    // <bucket>/year=*/source_key=* space, so a high-fan-out schema (e.g. crime per-ORI) bloats
    // the shared year partition for everyone — a single year=YYYY directory reached 160k+ marker
    // files, making the read-time preload list and per-run compaction grind for minutes. With the
    // schema prefix, each partition is bounded and compaction is scoped to one schema.
    Object dataSource = operand.get("dataSource");
    if (dataSource instanceof String && !((String) dataSource).isEmpty()) {
      config.put("schema", (String) dataSource);
    }
    return create(backend, baseDirectory, config);
  }

  /**
   * Resolve backend string, falling back to environment variable then default.
   */
  private static String resolveBackend(String backend) {
    if (backend != null && !backend.isEmpty()) {
      return backend.toLowerCase();
    }
    String envBackend = System.getenv(ENV_TRACKER_BACKEND);
    if (envBackend != null && !envBackend.isEmpty()) {
      return envBackend.toLowerCase();
    }
    return "duckdb";
  }

  @SuppressWarnings("UnusedVariable")
  private static PipelineTracker createS3Tracker(String baseDirectory,
      Map<String, String> config) {
    String bucket = config.get("bucket");
    if (bucket == null) {
      bucket = System.getenv("CALCITE_TRACKER_S3_BUCKET");
    }
    if (bucket == null) {
      throw new IllegalArgumentException(
          "S3 tracker requires 'bucket' in trackerConfig or CALCITE_TRACKER_S3_BUCKET env var");
    }
    // Scope the tracker under the schema directory (set by createFromOperand from the operand's
    // dataSource) so per-key markers never share a flat year partition across schemas.
    String schema = config.get("schema");
    if (schema != null && !schema.isEmpty()) {
      bucket = bucket.endsWith("/") ? bucket + schema : bucket + "/" + schema;
    }
    String endpoint = config.get("endpoint");
    return new S3HivePipelineTracker(bucket, endpoint, config);
  }

  private static PipelineTracker createPGTracker(Map<String, String> config) {
    try {
      String jdbcUrl = config.get("jdbcUrl");
      if (jdbcUrl == null) {
        jdbcUrl = System.getenv("CALCITE_TRACKER_PG_URL");
      }
      if (jdbcUrl == null) {
        throw new IllegalArgumentException(
            "PG tracker requires 'jdbcUrl' in trackerConfig or CALCITE_TRACKER_PG_URL env var");
      }
      String user = config.get("user");
      if (user == null) {
        user = System.getenv("CALCITE_TRACKER_PG_USER");
      }
      String password = config.get("password");
      if (password == null) {
        password = System.getenv("CALCITE_TRACKER_PG_PASSWORD");
      }
      return new PGPipelineTracker(jdbcUrl, user, password);
    } catch (Exception e) {
      LOGGER.error("Failed to create PG tracker: {}", e.getMessage());
      throw new RuntimeException("Failed to create PG tracker", e);
    }
  }
}
