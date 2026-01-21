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
 * A wrapper for timestamp values that ensures UTC string representation.
 *
 * This matches Parquet's behavior where TIMESTAMP(isAdjustedToUTC=true)
 * displays timestamps as UTC rather than local time.
 */
public class UtcTimestamp extends Timestamp {
  private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

  public UtcTimestamp(long time) {
    super(time);
  }

  @Override public String toString() {
    // Convert to UTC string representation
    // This matches Parquet's TIMESTAMP(isAdjustedToUTC=true) behavior
    Instant instant = Instant.ofEpochMilli(getTime());
    LocalDateTime utcDateTime = LocalDateTime.ofInstant(instant, ZoneId.of("UTC"));
    return FORMATTER.format(utcDateTime);
  }


  @Override public boolean equals(Object obj) {
    if (this == obj) return true;
    if (!(obj instanceof Timestamp)) return false;
    return super.equals(obj);
  }

  @Override public int hashCode() {
    return super.hashCode();
  }
}
