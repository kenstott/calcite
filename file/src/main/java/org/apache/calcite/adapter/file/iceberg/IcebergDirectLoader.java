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
package org.apache.calcite.adapter.file.iceberg;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.StaticTableOperations;
import org.apache.iceberg.Table;
import org.apache.iceberg.hadoop.HadoopFileIO;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

/**
 * Shared read-only table loading for the standalone repair/verification CLIs in this package
 * ({@link ClosureVerifier}, {@link ClosureResolver}). Prefers {@code metadata/version-hint.text}
 * — the same authoritative pointer {@link S3FileIOTableOperations} uses for the live
 * application's own reads and commits — falling back to picking the highest-numbered
 * {@code vN.metadata.json} only when the hint is unreadable (rather than relying on
 * {@code HadoopTableOperations.findVersion()}, which fails on R2/MinIO because a missing key
 * returns 403, not 404 — the same reason {@link CompactionRunner#loadTableDirect} exists).
 *
 * <p>Trusting the highest filename unconditionally (this class's original behavior) picks an
 * orphaned {@code vN.metadata.json} left behind by a lost commit race — Iceberg's protocol
 * tolerates exactly this by design, the file is never referenced by anything once its commit
 * loses the race — and fails to parse it, since an aborted commit's metadata.json can be
 * empty or truncated. version-hint.text is what actually names the live version.
 *
 * <p>Kept separate from CompactionRunner's copy: that one backs a read-write table used for
 * commits, and these tools must stay strictly read-only regardless of what CompactionRunner
 * does with its own loaded table.
 */
final class IcebergDirectLoader {

  private IcebergDirectLoader() {}

  static Table loadReadOnly(Configuration conf, String tablePath, String tableName)
      throws Exception {
    FileSystem fs = FileSystem.get(new java.net.URI(tablePath), conf);
    Path metadataDir = new Path(tablePath + "/metadata");
    String latestMetadataPath = resolveMetadataPath(fs, metadataDir);

    HadoopFileIO fileIO = new HadoopFileIO(conf);
    StaticTableOperations ops = new StaticTableOperations(latestMetadataPath, fileIO);
    return new BaseTable(ops, tableName);
  }

  /** The version number implied by the {@code vN.metadata.json} loaded, e.g. from a table path. */
  static int currentVersion(Configuration conf, String tablePath) throws Exception {
    FileSystem fs = FileSystem.get(new java.net.URI(tablePath), conf);
    Path metadataDir = new Path(tablePath + "/metadata");
    String metadataPath = resolveMetadataPath(fs, metadataDir);
    String name = new Path(metadataPath).getName();
    return Integer.parseInt(name.substring(1, name.indexOf('.')));
  }

  /**
   * Resolves the current {@code vN.metadata.json} path, preferring version-hint.text over the
   * highest-numbered-filename scan. See the class javadoc for why the scan alone is unsafe and
   * why it remains as the fallback.
   */
  private static String resolveMetadataPath(FileSystem fs, Path metadataDir) throws Exception {
    Path hintPath = new Path(metadataDir, "version-hint.text");
    try {
      if (fs.exists(hintPath)) {
        try (BufferedReader reader = new BufferedReader(
            new InputStreamReader(fs.open(hintPath), StandardCharsets.UTF_8))) {
          String line = reader.readLine();
          if (line != null && !line.trim().isEmpty()) {
            int v = Integer.parseInt(line.trim());
            Path candidate = new Path(metadataDir, "v" + v + ".metadata.json");
            if (fs.exists(candidate)) {
              return candidate.toString();
            }
          }
        }
      }
    } catch (Exception e) {
      // version-hint.text missing, unreadable, or names a file that no longer exists — fall
      // through to the directory scan below rather than fail (matches this class's existing
      // tolerance for R2/MinIO's 403-not-404 behavior on a missing key).
    }

    int maxVersion = -1;
    String latestMetadataPath = null;
    for (FileStatus file : fs.listStatus(metadataDir)) {
      String name = file.getPath().getName();
      if (name.startsWith("v") && name.endsWith(".metadata.json")) {
        try {
          int v = Integer.parseInt(name.substring(1, name.indexOf('.')));
          if (v > maxVersion) {
            maxVersion = v;
            latestMetadataPath = file.getPath().toString();
          }
        } catch (NumberFormatException nfe) {
          // skip non-numeric version file names
        }
      }
    }
    if (latestMetadataPath == null) {
      throw new IllegalStateException("No metadata files found in " + metadataDir);
    }
    return latestMetadataPath;
  }

  static Configuration buildHadoopConf() {
    Configuration conf = new Configuration();
    String accessKey = System.getenv("AWS_ACCESS_KEY_ID");
    String secretKey = System.getenv("AWS_SECRET_ACCESS_KEY");
    String endpoint = System.getenv("AWS_ENDPOINT_OVERRIDE");
    if (accessKey != null) {
      conf.set("fs.s3a.access.key", accessKey);
    }
    if (secretKey != null) {
      conf.set("fs.s3a.secret.key", secretKey);
    }
    if (endpoint != null) {
      conf.set("fs.s3a.endpoint", endpoint);
      conf.set("fs.s3a.path.style.access", "true");
      conf.set("fs.s3a.change.detection.mode", "none");
      conf.set("fs.s3a.change.detection.version.required", "false");
      // R2 rejects AWS region names outright ("Must be one of: wnam, enam, ... auto") — it
      // ignores region for routing but the S3A/SDK client still requires a value it accepts.
      conf.set("fs.s3a.endpoint.region", "auto");
    }
    conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
    conf.set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
    return conf;
  }
}
