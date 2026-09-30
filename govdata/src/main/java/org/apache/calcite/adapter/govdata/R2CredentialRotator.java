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
package org.apache.calcite.adapter.govdata;
// storage-provider-guard:ignore-file - audited: the only file written is the local credential-expiry stamp the launching engine reads, never an object-store URI.

import org.apache.calcite.adapter.file.storage.RotatingS3Credentials;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Keeps a long-running process's short-lived R2 credentials current, in place.
 *
 * <p>A server spawned with R2 session credentials holds them in every S3 consumer for its whole
 * life; they expire after an hour. When a schema's {@code s3Config} sets
 * {@code "credentialRotation": "r2"} and carries a {@code credentialExpiresAtMillis}, this
 * refreshes the set before it expires and replaces it through {@link RotatingS3Credentials},
 * which every consumer reads (or re-applies) — no reconnect, no dropped session.
 *
 * <p>When {@code credentialExpiryFile} is set, each rotation rewrites it with the new expiry, so
 * the launching engine, which replaces a server whose credentials have expired, sees the
 * rotated expiry rather than the one it spawned the server with.
 */
final class R2CredentialRotator {
  private static final Logger LOGGER = LoggerFactory.getLogger(R2CredentialRotator.class);

  static final String ROTATION_KEY = "credentialRotation";
  static final String EXPIRES_AT_KEY = "credentialExpiresAtMillis";
  static final String EXPIRY_FILE_KEY = "credentialExpiryFile";

  /** How often the rotator checks the remaining lifetime. */
  private static final long CHECK_INTERVAL_MS = 5L * 60_000L;
  /** Refresh once less than this remains — several checks' worth, so one failed fetch is
   *  retried well before the set expires. */
  static final long MIN_REMAINING_MS = 20L * 60_000L;

  /** Original access keys a rotator has been started for; one per credential lineage. */
  private static final Set<String> STARTED = ConcurrentHashMap.newKeySet();

  private final RotatingS3Credentials credentials;
  private final @Nullable Path expiryFile;
  private final String apiKey;

  private R2CredentialRotator(RotatingS3Credentials credentials, @Nullable Path expiryFile,
      String apiKey) {
    this.credentials = credentials;
    this.expiryFile = expiryFile;
    this.apiKey = apiKey;
  }

  /**
   * Starts the rotator for this {@code s3Config} when it asks for R2 rotation and its
   * credentials expire; a no-op for every other config, and for a lineage already rotating.
   *
   * @throws IllegalStateException when rotation is requested for expiring credentials but no
   *     API key is available to fetch replacements
   */
  static void startIfRequested(Map<String, Object> s3Config) {
    Object mode = s3Config.get(ROTATION_KEY);
    if (mode == null || mode.toString().isEmpty()) {
      return;
    }
    if (!"r2".equals(mode.toString())) {
      throw new IllegalArgumentException("Unknown " + ROTATION_KEY + " '" + mode
          + "'; the only supported value is 'r2'");
    }
    String expiresAt = string(s3Config, EXPIRES_AT_KEY);
    if (expiresAt == null) {
      // Credentials without an expiry (long-lived keys) never need replacing.
      return;
    }
    String accessKeyId = string(s3Config, "accessKeyId");
    String secretAccessKey = string(s3Config, "secretAccessKey");
    if (accessKeyId == null || secretAccessKey == null) {
      throw new IllegalArgumentException(ROTATION_KEY + " requires accessKeyId and "
          + "secretAccessKey in the same s3Config");
    }
    if (!STARTED.add(accessKeyId)) {
      return;
    }
    String apiKey = R2CredentialProvider.credentialApiKey();
    if (apiKey == null || apiKey.isEmpty()) {
      STARTED.remove(accessKeyId);
      throw new IllegalStateException(ROTATION_KEY + " is 'r2' but neither FREE_ASKAMERICA_KEY "
          + "nor ASKAMERICA_API_KEY is set, so the credentials expiring at " + expiresAt
          + " cannot be replaced");
    }
    String expiryFile = string(s3Config, EXPIRY_FILE_KEY);
    R2CredentialRotator rotator = new R2CredentialRotator(
        RotatingS3Credentials.of(accessKeyId, secretAccessKey, string(s3Config, "sessionToken")),
        expiryFile == null ? null : Paths.get(expiryFile), apiKey);
    ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
      Thread t = new Thread(r, "r2-credential-rotator");
      t.setDaemon(true);
      return t;
    });
    scheduler.scheduleWithFixedDelay(rotator::checkAndLog, CHECK_INTERVAL_MS, CHECK_INTERVAL_MS,
        TimeUnit.MILLISECONDS);
    LOGGER.info("R2 credential rotation started; credentials expire at {}", expiresAt);
  }

  /** One scheduled check. A failure is logged and retried at the next check — the scheduler
   *  cancels a task that throws, which would end rotation for the life of the process. */
  private void checkAndLog() {
    try {
      check();
    } catch (Exception e) {
      LOGGER.error("R2 credential rotation failed; retrying in {} min", CHECK_INTERVAL_MS / 60_000L,
          e);
    }
  }

  /** Fetches replacements when the current set is within {@link #MIN_REMAINING_MS} of expiring
   *  and applies them. Package-private for tests. */
  void check() throws IOException {
    Map<String, String> fresh = R2CredentialProvider.resolveOrFetch(apiKey, MIN_REMAINING_MS);
    String currentId = credentials.accessKeyId();
    String freshId = fresh.get("accessKeyId");
    if (currentId.equals(freshId)) {
      return;
    }
    RotatingS3Credentials.rotate(currentId, freshId, fresh.get("secretAccessKey"),
        fresh.get("sessionToken"));
    String expiresAt = fresh.get("expiresAtMillis");
    LOGGER.info("R2 credentials rotated in place; new set expires at {}", expiresAt);
    if (expiryFile != null) {
      writeExpiryFile(expiryFile, expiresAt);
    }
  }

  /** Atomically replaces the stamp; an unstamped set removes it, meaning "never expires". */
  static void writeExpiryFile(Path file, @Nullable String expiresAtMillis) throws IOException {
    if (expiresAtMillis == null || expiresAtMillis.isEmpty()) {
      Files.deleteIfExists(file);
      return;
    }
    Path tmp = Files.createTempFile(file.toAbsolutePath().getParent(), "creds-expiry-", ".tmp");
    try {
      Files.write(tmp, expiresAtMillis.getBytes(StandardCharsets.UTF_8));
      Files.move(tmp, file, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
    } finally {
      Files.deleteIfExists(tmp);
    }
  }

  private static @Nullable String string(Map<String, Object> map, String key) {
    Object v = map.get(key);
    return v == null || v.toString().isEmpty() ? null : v.toString();
  }
}
