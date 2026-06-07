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
package org.apache.calcite.adapter.file.util;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;

/**
 * Utility class for handling null equivalent strings in CSV and other text formats.
 */
public final class NullEquivalents {

  /**
   * Default set of strings that are considered equivalent to NULL.
   * These are checked case-insensitively.
   */
  public static final Set<String> DEFAULT_NULL_EQUIVALENTS =
      new HashSet<>(
          Arrays.asList("NULL",
      "NA",
      "N/A",
      "NONE",
      "NIL",
      ""));

  private NullEquivalents() {
    // Utility class
  }

  /**
   * Check if a string value represents a null value using the default null equivalents.
   * @param value The string value to check
   * @return true if the value matches a null equivalent (case-insensitive)
   */
  public static boolean isNullRepresentation(String value) {
    return isNullRepresentation(value, DEFAULT_NULL_EQUIVALENTS);
  }

  /**
   * Check if a string value represents a null value using a custom set of null equivalents.
   * @param value The string value to check
   * @param nullEquivalents Set of strings (case-insensitive) that represent null
   * @return true if the value matches a null equivalent (case-insensitive)
   */
  public static boolean isNullRepresentation(String value, Set<String> nullEquivalents) {
    if (value == null) {
      return false; // null is already null, not a "representation" of null
    }

    // Trim the value
    String trimmed = value.trim();

    // Empty or blank strings are considered null representations for type inference
    // (since they can't be parsed as numbers, dates, etc.)
    if (trimmed.isEmpty()) {
      return true;
    }

    // Check if it matches any explicit null markers
    String upper = trimmed.toUpperCase(Locale.ROOT);
    return nullEquivalents.contains(upper);
  }
}
