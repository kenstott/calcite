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

import org.apache.iceberg.BaseTable;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StaticTableOperations;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.aws.s3.S3FileIO;
import org.apache.iceberg.io.FileIO;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * Loads path-based Iceberg tables from S3/MinIO using Iceberg's {@link S3FileIO}
 * (AWS SDK v2), so the read path needs neither hadoop-aws nor the AWS SDK v1
 * {@code S3AFileSystem}.
 *
 * <p>This mirrors the {@code HadoopTables} layout — {@code metadata/version-hint.text}
 * holds the current version number {@code N}, and the table metadata lives at
 * {@code metadata/v{N}.metadata.json}. The metadata's internal paths are written as
 * {@code s3a://} by the HadoopFileIO ETL writer; {@code S3FileIO} accepts the
 * {@code s3}/{@code s3a}/{@code s3n} schemes interchangeably, so a table written via
 * HadoopFileIO reads back unchanged through S3FileIO.
 *
 * <p>Read-only: uses {@link StaticTableOperations}, which never writes metadata.
 */
public final class S3FileIOTables {
  private S3FileIOTables() {
  }

  /**
   * Loads an Iceberg table from an {@code s3://} (or {@code s3a://}) path via S3FileIO.
   *
   * @param tablePath table root, e.g. {@code s3://bucket/schema/table}
   * @param s3aConfig the {@code fs.s3a.*} credential map built by FileSchema (may be null)
   * @return a read-only Iceberg {@link Table}
   */
  public static Table load(String tablePath, Map<String, String> s3aConfig) {
    S3FileIO io = newIO(s3aConfig);
    String root = stripTrailingSlash(tablePath);
    String version = readVersionHint(io, root);
    String metadataLocation = root + "/metadata/v" + version + ".metadata.json";
    StaticTableOperations ops = new StaticTableOperations(metadataLocation, io);
    return new BaseTable(ops, tableName(root));
  }

  /**
   * Builds and initializes an {@link S3FileIO} from a credential map (either hadoop
   * {@code fs.s3a.*} keys or the plain {@code accessKeyId}/{@code secretAccessKey}/{@code endpoint}
   * keys). Shared by the read-only and writable table paths.
   */
  static S3FileIO newIO(Map<String, String> s3Config) {
    S3FileIO io = new S3FileIO();
    io.initialize(toS3Properties(s3Config));
    return io;
  }

  /**
   * Creates a new writable Iceberg table at {@code tablePath} via S3FileIO (AWS SDK v2), writing the
   * initial {@code v0.metadata.json} + {@code version-hint.text} directly — no HadoopCatalog.
   *
   * @param tablePath table root, e.g. {@code s3://bucket/schema/table}
   * @param s3Config the credential map (may be null)
   * @param schema the table schema
   * @param spec the partition spec (unpartitioned when null)
   * @param properties table properties (empty when null)
   * @return an appendable Iceberg {@link Table}
   */
  public static Table create(String tablePath, Map<String, String> s3Config, Schema schema,
      PartitionSpec spec, Map<String, String> properties) {
    S3FileIO io = newIO(s3Config);
    String root = stripTrailingSlash(tablePath);
    S3FileIOTableOperations ops = new S3FileIOTableOperations(root, io);
    TableMetadata metadata =
        TableMetadata.newTableMetadata(schema,
            spec != null ? spec : PartitionSpec.unpartitioned(),
            root,
            properties != null ? properties : java.util.Collections.<String, String>emptyMap());
    ops.commit(null, metadata);
    return new BaseTable(ops, tableName(root));
  }

  /**
   * Loads an existing Iceberg table as a writable/appendable table via S3FileIO — unlike the
   * read-only {@link #load} (which uses {@link StaticTableOperations}), the returned table supports
   * {@code newAppend()}/{@code commit()} because it is backed by {@link S3FileIOTableOperations}.
   *
   * @param tablePath table root, e.g. {@code s3://bucket/schema/table}
   * @param s3Config the credential map (may be null)
   * @return an appendable Iceberg {@link Table}
   */
  public static Table loadWritable(String tablePath, Map<String, String> s3Config) {
    S3FileIO io = newIO(s3Config);
    String root = stripTrailingSlash(tablePath);
    S3FileIOTableOperations ops = new S3FileIOTableOperations(root, io);
    ops.refresh();
    return new BaseTable(ops, tableName(root));
  }

  /**
   * Returns whether an Iceberg table exists at {@code tablePath}, by probing for
   * {@code metadata/version-hint.text}.
   *
   * @param tablePath table root, e.g. {@code s3://bucket/schema/table}
   * @param s3Config the credential map (may be null)
   * @return true if the version hint is present, false if it is absent
   */
  public static boolean exists(String tablePath, Map<String, String> s3Config) {
    S3FileIO io = newIO(s3Config);
    String root = stripTrailingSlash(tablePath);
    String hintPath = root + "/metadata/version-hint.text";
    try (InputStream is = io.newInputFile(hintPath).newStream();
         BufferedReader reader =
             new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {
      String line = reader.readLine();
      return line != null && !line.trim().isEmpty();
    } catch (Exception e) {
      // Absent hint file (no table yet).
      return false;
    }
  }

  /**
   * Translates a credential map to Iceberg S3FileIO {@code s3.*} properties. Accepts
   * either hadoop {@code fs.s3a.*} keys (as FileSchema builds) or the plain
   * {@code accessKeyId}/{@code secretAccessKey}/{@code endpoint} keys (as the file
   * adapter's S3StorageProvider config uses).
   */
  static Map<String, String> toS3Properties(Map<String, String> src) {
    Map<String, String> props = new HashMap<>();
    if (src == null) {
      return props;
    }
    putFirst(props, "s3.access-key-id", src, "fs.s3a.access.key", "accessKeyId");
    putFirst(props, "s3.secret-access-key", src, "fs.s3a.secret.key", "secretAccessKey");
    putFirst(props, "s3.endpoint", src, "fs.s3a.endpoint", "endpoint");
    String pathStyle = firstNonEmpty(src, "fs.s3a.path.style.access", "pathStyleAccess");
    props.put("s3.path-style-access", pathStyle != null ? pathStyle : "true");
    // AWS SDK v2 requires a region even against a custom endpoint; MinIO/R2 ignore it.
    String region = firstNonEmpty(src, "fs.s3a.endpoint.region", "region");
    props.put("client.region", region != null && !region.isEmpty() ? region : "us-east-1");
    return props;
  }

  private static void putFirst(Map<String, String> dst, String dstKey,
      Map<String, String> src, String... srcKeys) {
    String v = firstNonEmpty(src, srcKeys);
    if (v != null) {
      dst.put(dstKey, v);
    }
  }

  private static String firstNonEmpty(Map<String, String> src, String... keys) {
    for (String k : keys) {
      String v = src.get(k);
      if (v != null && !v.isEmpty()) {
        return v;
      }
    }
    return null;
  }

  private static String readVersionHint(FileIO io, String root) {
    String hintPath = root + "/metadata/version-hint.text";
    try (InputStream is = io.newInputFile(hintPath).newStream();
         BufferedReader reader =
             new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {
      String line = reader.readLine();
      if (line == null || line.trim().isEmpty()) {
        throw new IllegalStateException("Empty Iceberg version-hint.text at " + hintPath);
      }
      return line.trim();
    } catch (IOException e) {
      throw new RuntimeException("Failed to read Iceberg version-hint.text at " + hintPath, e);
    }
  }

  private static String stripTrailingSlash(String path) {
    return path.endsWith("/") ? path.substring(0, path.length() - 1) : path;
  }

  private static String tableName(String root) {
    int slash = root.lastIndexOf('/');
    return slash >= 0 ? root.substring(slash + 1) : root;
  }
}
