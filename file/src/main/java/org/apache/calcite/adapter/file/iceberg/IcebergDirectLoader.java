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

/**
 * Shared read-only table loading for the standalone repair/verification CLIs in this package
 * ({@link ClosureVerifier}, {@link ClosureResolver}). Lists the metadata directory directly and
 * picks the highest {@code vN.metadata.json} rather than relying on
 * {@code HadoopTableOperations.findVersion()}, which fails on R2/MinIO because a missing key
 * returns 403, not 404 — the same reason {@link CompactionRunner#loadTableDirect} exists.
 * Kept separate from CompactionRunner's copy: that one backs a read-write table used for
 * commits, and these tools must stay strictly read-only regardless of what CompactionRunner
 * does with its own loaded table.
 */
final class IcebergDirectLoader {

  private IcebergDirectLoader() {}

  static Table loadReadOnly(Configuration conf, String tablePath, String tableName)
      throws Exception {
    FileSystem fs = FileSystem.get(new java.net.URI(tablePath), conf);
    Path metadataDir = new Path(tablePath + "/metadata");

    int maxVersion = 0;
    String latestMetadataPath = null;
    FileStatus[] files = fs.listStatus(metadataDir);
    for (FileStatus file : files) {
      String name = file.getPath().getName();
      if (name.startsWith("v") && name.endsWith(".metadata.json")) {
        String numStr = name.substring(1, name.indexOf('.'));
        try {
          int v = Integer.parseInt(numStr);
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

    HadoopFileIO fileIO = new HadoopFileIO(conf);
    StaticTableOperations ops = new StaticTableOperations(latestMetadataPath, fileIO);
    return new BaseTable(ops, tableName);
  }

  /** The version number implied by the {@code vN.metadata.json} loaded, e.g. from a table path. */
  static int currentVersion(Configuration conf, String tablePath) throws Exception {
    FileSystem fs = FileSystem.get(new java.net.URI(tablePath), conf);
    Path metadataDir = new Path(tablePath + "/metadata");
    int maxVersion = -1;
    for (FileStatus file : fs.listStatus(metadataDir)) {
      String name = file.getPath().getName();
      if (name.startsWith("v") && name.endsWith(".metadata.json")) {
        try {
          int v = Integer.parseInt(name.substring(1, name.indexOf('.')));
          maxVersion = Math.max(maxVersion, v);
        } catch (NumberFormatException nfe) {
          // skip non-numeric version file names
        }
      }
    }
    if (maxVersion < 0) {
      throw new IllegalStateException("No metadata files found in " + metadataDir);
    }
    return maxVersion;
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
