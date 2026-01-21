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
package org.apache.calcite.adapter.file.refresh;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.time.Duration;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Utility class for parsing refresh interval strings.
 * Supports formats like:
 * - "5 minutes"
 * - "1 hour"
 * - "30 seconds"
 * - "2 days"
 */
public class RefreshInterval {
  private static final Pattern INTERVAL_PATTERN =
      Pattern.compile("(\\d+)\\s+(second|minute|hour|day)s?", Pattern.CASE_INSENSITIVE);

  private RefreshInterval() {
    // Utility class should not be instantiated
  }

  /**
   * Parses a refresh interval string into a Duration.
   * Supports both human-readable formats like "5 minutes" and ISO 8601 formats like "PT1S".
   *
   * @param intervalStr Interval string like "5 minutes" or "PT1S"
   * @return Parsed duration, or null if invalid format
   */
  public static @Nullable Duration parse(@Nullable String intervalStr) {
    if (intervalStr == null || intervalStr.trim().isEmpty()) {
      return null;
    }

    String trimmed = intervalStr.trim();

    // First try ISO 8601 format (PT1S, PT5M, PT1H, etc.)
    if (trimmed.startsWith("PT") || trimmed.startsWith("P")) {
      try {
        return Duration.parse(trimmed);
      } catch (Exception e) {
        // Fall through to try human-readable format
      }
    }

    // Try human-readable format
    Matcher matcher = INTERVAL_PATTERN.matcher(trimmed);
    if (!matcher.matches()) {
      return null;
    }

    long value = Long.parseLong(matcher.group(1));
    String unit = matcher.group(2).toLowerCase(Locale.ROOT);

    switch (unit) {
    case "second":
      return Duration.ofSeconds(value);
    case "minute":
      return Duration.ofMinutes(value);
    case "hour":
      return Duration.ofHours(value);
    case "day":
      return Duration.ofDays(value);
    default:
      return null;
    }
  }

  /**
   * Gets the effective refresh interval, considering inheritance.
   *
   * @param tableInterval Table-specific interval (may be null)
   * @param schemaInterval Schema-level default interval (may be null)
   * @return Effective interval, or null if no refresh configured
   */
  public static @Nullable Duration getEffectiveInterval(
      @Nullable String tableInterval,
      @Nullable String schemaInterval) {
    // Table level takes precedence
    Duration interval = parse(tableInterval);
    if (interval != null) {
      return interval;
    }
    // Fall back to schema level
    return parse(schemaInterval);
  }
}
