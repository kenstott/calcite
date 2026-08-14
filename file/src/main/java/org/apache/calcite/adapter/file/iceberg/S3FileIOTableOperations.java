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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
 * metadata lives at {@code metadata/v{N}.metadata.json}. On commit we read version {@code N},
 * write {@code v{N+1}.metadata.json}, read it back to confirm it is durably present and
 * parseable, then overwrite the version hint. This is a plain read-write-verify-write (no
 * atomic rename / compare-and-swap): {@link CrossProcessCommitLock} already serializes commits
 * to one committer per table per host, so no optimistic-concurrency check is required. The
 * read-back is what a compare-and-swap store would get for free — without it, a client-side
 * write that returns successfully but isn't yet durably readable (or a crash between the PUT
 * request and its response) leaves version-hint.text free to advance onto a file that doesn't
 * exist, only surfacing later as a table that silently "won't load".
 */
public class S3FileIOTableOperations implements TableOperations {
  private static final Logger LOGGER = LoggerFactory.getLogger(S3FileIOTableOperations.class);

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
      LOGGER.debug("refresh: no version-hint.text at {} — table does not exist yet", location);
      current = null;
      version = -1;
      return null;
    }
    int n = Integer.parseInt(hint);
    String metadataLocation = location + "/metadata/v" + n + ".metadata.json";
    LOGGER.debug("refresh: {} -> hint={}, reading {}", location, n, metadataLocation);
    current = TableMetadataParser.read(io, io.newInputFile(metadataLocation));
    version = n;
    LOGGER.debug("refresh: {} -> loaded v{}, snapshot={}", location, n,
        current.currentSnapshot() != null ? current.currentSnapshot().snapshotId() : "none");
    return current;
  }

  /** Threshold above which a single commit sub-step logs a WARN even on success — an unusually
   * slow write/read is the most plausible precursor to a client-observed "succeeded" that
   * wasn't actually durable yet (see the verification-gate comment below). */
  private static final long SLOW_STEP_MS = 3000;

  @Override public void commit(TableMetadata base, TableMetadata metadata) {
    int previous = version;
    int next = (base == null) ? 0 : version + 1;
    String metadataLocation = location + "/metadata/v" + next + ".metadata.json";
    long commitStartNanos = System.nanoTime();
    // Named so the catch block's diagnostic log line can say exactly which of the three
    // sub-steps failed, without three separate try/catch blocks changing the thrown-exception
    // behavior below (still one CommitFailedException either way).
    String step = "write metadata.json";
    long stepStartNanos = commitStartNanos;
    try {
      LOGGER.debug("commit: {} starting v{} -> v{} (base snapshot={}, new snapshot={})",
          location, previous, next,
          base != null && base.currentSnapshot() != null ? base.currentSnapshot().snapshotId() : "none",
          metadata.currentSnapshot() != null ? metadata.currentSnapshot().snapshotId() : "none");

      TableMetadataParser.write(metadata, io.newOutputFile(metadataLocation));
      logStep(location, step, stepStartNanos, "wrote " + metadataLocation);

      // Verification gate: a write returning without an exception is not proof the object
      // is durably present and readable (S3-compatible stores can surface a write that
      // "succeeded" client-side but isn't yet consistently readable, and a crash between the
      // PUT request and its response leaves the same ambiguity). Read the metadata.json back
      // BEFORE the pointer can be advanced to reference it -- this is the one gate standing
      // between a partial/undurable write and version-hint.text pointing at a file that
      // doesn't (yet, or ever) exist, which otherwise surfaces later as a table that "won't
      // load" (see IcebergMaintenanceRunner#repairVersionHint, the emergency repair for
      // exactly this state). Failing here throws before version-hint.text is ever touched,
      // so the pointer still references the last known-good version -- no dangling reference,
      // and the next commit attempt retries this same version number since `version` was
      // never advanced.
      step = "verify metadata.json read-back";
      stepStartNanos = System.nanoTime();
      TableMetadata verified = TableMetadataParser.read(io, io.newInputFile(metadataLocation));
      logStep(location, step, stepStartNanos, "verified readable (snapshot="
          + (verified.currentSnapshot() != null ? verified.currentSnapshot().snapshotId() : "none") + ")");

      step = "write version-hint.text";
      stepStartNanos = System.nanoTime();
      String hintPath = location + "/metadata/version-hint.text";
      PositionOutputStream out = io.newOutputFile(hintPath).createOrOverwrite();
      try {
        out.write(String.valueOf(next).getBytes(StandardCharsets.UTF_8));
      } finally {
        out.close();
      }
      logStep(location, step, stepStartNanos, hintPath + " -> v" + next);

      current = metadata;
      version = next;
      refreshed = true;
      LOGGER.debug("commit: {} complete v{} -> v{} in {}ms", location, previous, next,
          (System.nanoTime() - commitStartNanos) / 1_000_000);
    } catch (CommitFailedException e) {
      LOGGER.error("commit: {} FAILED at step '{}' ({}ms into that step) for v{} -> v{} "
          + "(metadataLocation={}): {}", location, step,
          (System.nanoTime() - stepStartNanos) / 1_000_000, previous, next, metadataLocation,
          e.getMessage());
      throw e;
    } catch (Exception e) {
      LOGGER.error("commit: {} FAILED at step '{}' ({}ms into that step) for v{} -> v{} "
          + "(metadataLocation={}): {}", location, step,
          (System.nanoTime() - stepStartNanos) / 1_000_000, previous, next, metadataLocation,
          e.toString());
      throw new CommitFailedException(e,
          "Failed to commit Iceberg metadata v%d for table at %s (step: %s)", next, location, step);
    }
  }

  /** Logs a completed commit sub-step at DEBUG, or WARN if it took longer than
   * {@link #SLOW_STEP_MS} — an unusually slow step is the most plausible precursor to a
   * client-observed "succeeded" that wasn't actually durable yet. */
  private static void logStep(String location, String step, long stepStartNanos, String detail) {
    long ms = (System.nanoTime() - stepStartNanos) / 1_000_000;
    if (ms >= SLOW_STEP_MS) {
      LOGGER.warn("commit: {} step '{}' took {}ms (>= {}ms threshold) — {}",
          location, step, ms, SLOW_STEP_MS, detail);
    } else {
      LOGGER.debug("commit: {} step '{}' took {}ms — {}", location, step, ms, detail);
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
    // fallback-guard: allow catches only the SDK's canonical "definitely absent" signal (S3
    // key not found) as "no table yet"; other failures below are surfaced instead of being
    // silently treated as absence, which would make commit() think it's creating a brand
    // new table (version 0) and overwrite an existing one's metadata history.
    } catch (software.amazon.awssdk.services.s3.model.NoSuchKeyException notFound) {
      LOGGER.debug("readVersionHint: {} absent (NoSuchKeyException) — treating as no table yet",
          hintPath);
      return null;
    } catch (IOException e) {
      LOGGER.error("readVersionHint: {} unreadable (not a not-found — surfacing, not treating "
          + "as absent): {}", hintPath, e.toString());
      throw new java.io.UncheckedIOException(
          "Failed to read version hint at " + hintPath, e);
    }
  }

  private static String stripTrailingSlash(String path) {
    return path.endsWith("/") ? path.substring(0, path.length() - 1) : path;
  }
}
