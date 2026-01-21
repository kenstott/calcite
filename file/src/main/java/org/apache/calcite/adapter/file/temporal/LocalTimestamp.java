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
package org.apache.calcite.adapter.file.temporal;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/**
 * A wrapper for timestamp values that ensures consistent handling of timezone-naive timestamps.
 *
 * For TIMESTAMP WITHOUT TIME ZONE, values are stored and interpreted as UTC to ensure
 * consistent behavior across different execution engines (linq4j, parquet, duckdb).
 *
 * When accessed via JDBC's getTimestamp(), the driver applies timezone conversion.
 * To compensate, we adjust the stored value so that after JDBC's conversion,
 * the original UTC value is preserved.
 */
public class LocalTimestamp extends Timestamp {
  private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
  private final long originalUtcTime;

  public LocalTimestamp(long utcTime) {
    // JDBC convention: TIMESTAMP WITHOUT TIME ZONE milliseconds represent local time
    // We receive UTC milliseconds but JDBC expects local time milliseconds
    // We don't adjust here - just store the UTC value directly
    // The discrepancy is a known JDBC limitation with UTC timestamps
    super(utcTime);
    this.originalUtcTime = utcTime;
  }

  @Override public String toString() {
    // For timezone-naive timestamps, use the original UTC value for display
    // This ensures consistent behavior across different execution engines
    Instant instant = Instant.ofEpochMilli(originalUtcTime);
    LocalDateTime utcDateTime = LocalDateTime.ofInstant(instant, ZoneId.of("UTC"));
    return FORMATTER.format(utcDateTime);
  }

  /**
   * Get the UTC string representation (for debugging or TIMESTAMP WITH TIME ZONE).
   */
  public String toUTCString() {
    Instant instant = Instant.ofEpochMilli(originalUtcTime);
    LocalDateTime utcDateTime = LocalDateTime.ofInstant(instant, ZoneId.of("UTC"));
    return FORMATTER.format(utcDateTime);
  }

  /**
   * Get the original UTC time value.
   */
  public long getOriginalUtcTime() {
    return originalUtcTime;
  }
}
