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
package org.apache.calcite.adapter.file.duckdb;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongSupplier;

/**
 * Process-wide, short-lived pins of an Iceberg table to the metadata version that
 * {@code version-hint.text} named when the pin was taken.
 *
 * <p>{@code iceberg_scan('<table>')} re-reads {@code version-hint.text} and the metadata file it
 * names on every scan. Scanning the table with an explicit {@code version} argument
 * instead skips the pointer read, and a metadata file is immutable for its version, so holding a
 * pin for a bounded time trades at most that long of read staleness for the saved requests.
 * Nothing here is persisted: pins live only in this map and are applied by rewriting the pushed-down
 * SQL (see {@link EtagRetryConnection}), never to a catalog view definition.
 *
 * <p>Version numbers are reused by drop/recreate/purge, so a pin must never outlive the table it
 * names: those paths and this process's own commits call {@link #invalidate} and
 * {@link #invalidateAll}, and a statement that fails against a pin drops it.
 */
public final class IcebergMetadataPins {

  /** Resolves the current metadata version number, as text, for a table location. */
  public interface PointerReader {
    String currentVersion(String tableLocation) throws java.io.IOException;
  }

  private static final class Pin {
    final String version;
    final long expiresAtNanos;

    Pin(String version, long expiresAtNanos) {
      this.version = version;
      this.expiresAtNanos = expiresAtNanos;
    }
  }

  private static final Map<String, Pin> PINS = new ConcurrentHashMap<>();

  private IcebergMetadataPins() {
  }

  /**
   * The pinned metadata version for {@code tableLocation}, reading the pointer through
   * {@code reader} when there is no live pin.
   *
   * @param ttlMillis pin lifetime; 0 or less disables pinning and returns null
   * @param nanoClock monotonic clock in nanoseconds
   */
  static String resolve(String tableLocation, long ttlMillis, PointerReader reader,
      LongSupplier nanoClock) throws java.io.IOException {
    if (ttlMillis <= 0) {
      return null;
    }
    String key = key(tableLocation);
    long now = nanoClock.getAsLong();
    Pin pin = PINS.get(key);
    if (pin != null && now - pin.expiresAtNanos < 0) {
      return pin.version;
    }
    String version = reader.currentVersion(tableLocation);
    PINS.put(key, new Pin(version, now + ttlMillis * 1_000_000L));
    return version;
  }

  /** Drops the pin for one table location (this process committed to it). */
  public static void invalidate(String tableLocation) {
    PINS.remove(key(tableLocation));
  }

  private static String key(String tableLocation) {
    return tableLocation.endsWith("/")
        ? tableLocation.substring(0, tableLocation.length() - 1) : tableLocation;
  }

  /** Drops every pin (a table was dropped, recreated or purged). */
  public static void invalidateAll() {
    PINS.clear();
  }

  /** True when a pin (live or expired) is held for {@code tableLocation}; for tests. */
  static boolean contains(String tableLocation) {
    return PINS.containsKey(key(tableLocation));
  }
}
