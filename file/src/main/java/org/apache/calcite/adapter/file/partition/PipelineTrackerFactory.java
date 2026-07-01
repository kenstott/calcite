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
 *   <li>{@code readonly} (default) - Reads report "untracked"; any write throws
 *       (via {@link ReadOnlyPipelineTracker}). This is the fail-closed default:
 *       a tracked write requires an explicitly configured backend, so a run
 *       launched without one cannot silently corrupt resume state.</li>
 *   <li>{@code pg} / {@code postgres} - PostgreSQL-based tracking via
 *       {@link PGPipelineTracker}. The multi-writer store for the concurrent pool.</li>
 *   <li>{@code duckdb} - Local DuckDB-based tracking via
 *       {@link DuckDBPartitionStatusStore}. Single-writer; explicit opt-in only.</li>
 *   <li>{@code s3} - S3 hive-partitioned append-only parquet via
 *       {@link S3HivePipelineTracker} (legacy).</li>
 *   <li>{@code noop} - No tracking, writes silently swallowed (forces full
 *       rebuild every time). Distinct from {@code readonly}, which throws on write.</li>
 * </ul>
 *
 * <p>Backend selection can be configured via:
 * <ol>
 *   <li>Schema operand: {@code "trackerBackend": "pg"}</li>
 *   <li>Environment variable: {@code CALCITE_TRACKER_BACKEND=pg}</li>
 *   <li>Default: {@code "readonly"} (fail closed — no writable backend, no writes)</li>
 * </ol>
 *
 * <p>An unrecognized backend is a hard error, not a silent fallback to a writable
 * store: misconfiguration must fail loudly rather than quietly start tracking
 * somewhere unexpected.
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
    case "readonly":
    case "read-only":
      return new ReadOnlyPipelineTracker();

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
      throw new IllegalArgumentException(
          "Unknown tracker backend '" + resolvedBackend + "'. Valid backends: "
          + "readonly (default), pg/postgres, duckdb, s3, noop. Writes require an explicitly "
          + "configured writable backend; an unknown value is never silently treated as writable.");
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
    // PG tracker namespace = the parquet bucket/directory, so dq and prod (which use different
    // parquet buckets) get isolated tracker schemas in one database. Consumed only by the pg
    // backend (mapped to a PG schema); ignored by the others.
    Object directory = operand.get("directory");
    if (directory instanceof String && !((String) directory).isEmpty()) {
      config.put("namespace", (String) directory);
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
    // Fail closed: with no backend configured anywhere, hand back a read-only tracker
    // rather than a writable local DuckDB. ETL must opt in to a writable backend.
    return "readonly";
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
      // Isolate dq vs prod by the parquet bucket: the tracker lives in a PG schema derived from it.
      return new PGPipelineTracker(jdbcUrl, user, password, config.get("namespace"));
    } catch (Exception e) {
      LOGGER.error("Failed to create PG tracker: {}", e.getMessage());
      throw new RuntimeException("Failed to create PG tracker", e);
    }
  }
}
