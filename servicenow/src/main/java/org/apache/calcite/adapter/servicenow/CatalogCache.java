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
package org.apache.calcite.adapter.servicenow;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.function.Supplier;

/**
 * The instance's catalog snapshot, kept on disk so that it outlives the process.
 *
 * <p>Unlike a per-object describe, ServiceNow's metadata is bulk table data, so the cache holds
 * one snapshot per scope ({@code <directory>/<scope>/catalog.json}), not one file per table. The
 * scope is instance URL plus user, because the tables and columns visible depend on the user's
 * roles and ACLs. A snapshot is used until it is older than the time to live. The write goes to a
 * temporary file that is renamed into place, so a reader never sees a partial file.
 */
class CatalogCache {

  private static final Logger LOGGER = LoggerFactory.getLogger(CatalogCache.class);

  private final Path directory;
  private final Duration timeToLive;

  /**
   * Creates a cache.
   *
   * @param directory  root directory, shared by every scope
   * @param scope      what the snapshot is specific to: instance URL and user
   * @param timeToLive how long a snapshot is used for; zero turns the disk cache off
   */
  CatalogCache(Path directory, String scope, Duration timeToLive) {
    if (timeToLive.isNegative()) {
      throw new IllegalArgumentException("Catalog cache time to live is negative: " + timeToLive);
    }
    this.directory = directory.resolve(sha256(scope).substring(0, 16));
    this.timeToLive = timeToLive;
  }

  /** Returns the snapshot from disk if a fresh one is there, otherwise loads and stores one. */
  ServiceNowCatalog.Snapshot get(Supplier<ServiceNowCatalog.Snapshot> loader) {
    if (timeToLive.isZero()) {
      return loader.get();
    }
    final Path file = directory.resolve("catalog.json");
    try {
      if (Files.exists(file)) {
        final long age = System.currentTimeMillis() - Files.getLastModifiedTime(file).toMillis();
        if (age < timeToLive.toMillis()) {
          LOGGER.debug("Catalog read from {}", file);
          try {
            return ServiceNowCatalog.Snapshot.fromJson(
                new String(Files.readAllBytes(file), StandardCharsets.UTF_8));
          } catch (ServiceNowException e) {
            throw new ServiceNowException("The catalog cache file " + file + " is unusable; "
                + "delete it to reload the catalog. " + e.getMessage(), e);
          }
        }
      }
      final ServiceNowCatalog.Snapshot snapshot = loader.get();
      Files.createDirectories(directory);
      final Path temp = Files.createTempFile(directory, "catalog", ".tmp");
      Files.write(temp, snapshot.toJson().getBytes(StandardCharsets.UTF_8));
      Files.move(temp, file, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
      return snapshot;
    } catch (IOException e) {
      throw new UncheckedIOException("Catalog cache at " + file + " could not be used", e);
    }
  }

  private static String sha256(String value) {
    final MessageDigest digest;
    try {
      digest = MessageDigest.getInstance("SHA-256");
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException(e);
    }
    final StringBuilder hex = new StringBuilder();
    for (byte b : digest.digest(value.getBytes(StandardCharsets.UTF_8))) {
      hex.append(Character.forDigit((b >> 4) & 0xF, 16)).append(Character.forDigit(b & 0xF, 16));
    }
    return hex.toString();
  }
}
