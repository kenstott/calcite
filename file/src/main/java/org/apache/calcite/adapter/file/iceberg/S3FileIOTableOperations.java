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

import org.apache.iceberg.LocationProviders;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.aws.s3.S3FileIO;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.io.PositionOutputStream;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Collections;

/**
 * A writable {@link TableOperations} backed by Iceberg's {@link S3FileIO} (AWS SDK v2),
 * so the ETL create/append/commit path needs neither hadoop-aws nor the AWS SDK v1
 * {@code S3AFileSystem}/{@code HadoopFileIO}.
 *
 * <p>This mirrors the {@code HadoopTables}/{@code HadoopCatalog} on-disk layout —
 * {@code metadata/version-hint.text} holds the current version number {@code N}, and the table
 * metadata lives at {@code metadata/v{N}.metadata.json}. On commit we read version {@code N}, write
 * {@code v{N+1}.metadata.json}, then overwrite the version hint. This is a plain read-then-write
 * (no atomic rename / compare-and-swap): {@link CrossProcessCommitLock} already serializes commits
 * to one committer per table per host, so no optimistic-concurrency check is required.
 */
public class S3FileIOTableOperations implements TableOperations {
  private final String location;
  private final S3FileIO io;
  private TableMetadata current;
  private int version = -1;
  private boolean refreshed;

  /**
   * @param location table root, e.g. {@code s3://bucket/schema/table}
   * @param io an initialized S3FileIO
   */
  public S3FileIOTableOperations(String location, S3FileIO io) {
    this.location = stripTrailingSlash(location);
    this.io = io;
  }

  @Override public TableMetadata current() {
    if (current == null && !refreshed) {
      refresh();
    }
    return current;
  }

  @Override public TableMetadata refresh() {
    refreshed = true;
    String hintPath = location + "/metadata/version-hint.text";
    String hint = readVersionHint(hintPath);
    if (hint == null) {
      current = null;
      version = -1;
      return null;
    }
    int n = Integer.parseInt(hint);
    String metadataLocation = location + "/metadata/v" + n + ".metadata.json";
    current = TableMetadataParser.read(io, io.newInputFile(metadataLocation));
    version = n;
    return current;
  }

  @Override public void commit(TableMetadata base, TableMetadata metadata) {
    int next = (base == null) ? 0 : version + 1;
    try {
      String metadataLocation = location + "/metadata/v" + next + ".metadata.json";
      TableMetadataParser.write(metadata, io.newOutputFile(metadataLocation));

      String hintPath = location + "/metadata/version-hint.text";
      PositionOutputStream out = io.newOutputFile(hintPath).createOrOverwrite();
      try {
        out.write(String.valueOf(next).getBytes(StandardCharsets.UTF_8));
      } finally {
        out.close();
      }

      current = metadata;
      version = next;
      refreshed = true;
    } catch (CommitFailedException e) {
      throw e;
    } catch (Exception e) {
      throw new CommitFailedException(e,
          "Failed to commit Iceberg metadata v%d for table at %s", next, location);
    }
  }

  @Override public FileIO io() {
    return io;
  }

  @Override public String metadataFileLocation(String fileName) {
    return location + "/metadata/" + fileName;
  }

  @Override public LocationProvider locationProvider() {
    return LocationProviders.locationsFor(location,
        current != null ? current.properties() : Collections.<String, String>emptyMap());
  }

  /**
   * Reads {@code metadata/version-hint.text}, returning the trimmed version number, or {@code null}
   * when the hint file is absent/empty (i.e. no table exists yet at this location).
   */
  private String readVersionHint(String hintPath) {
    try (InputStream is = io.newInputFile(hintPath).newStream();
         BufferedReader reader =
             new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {
      String line = reader.readLine();
      if (line == null || line.trim().isEmpty()) {
        return null;
      }
      return line.trim();
    } catch (Exception e) {
      // Absent hint file (no table yet) — treat as "no current metadata".
      return null;
    }
  }

  private static String stripTrailingSlash(String path) {
    return path.endsWith("/") ? path.substring(0, path.length() - 1) : path;
  }
}
