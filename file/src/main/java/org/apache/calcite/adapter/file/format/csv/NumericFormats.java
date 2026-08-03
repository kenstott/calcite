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
package org.apache.calcite.adapter.file.format.csv;

import java.util.regex.Pattern;

/**
 * Strips common human-readable numeric formatting — thousands separators, a leading/trailing
 * currency symbol, a trailing percent sign, and accounting-style parenthetical negatives — so
 * the result can be handed to {@code Long.parseLong}/{@code Double.parseDouble}.
 *
 * <p>Used by both {@link CsvTypeInferrer} (to decide whether a column is numeric) and
 * {@link CsvTypeConverter} (to actually parse it), so the two stay consistent: a value the
 * inferrer counted as numeric during sampling must also be parseable at query time.
 *
 * <p>The percent sign is only stripped, not converted to a fraction — {@code "12%"} becomes
 * {@code 12}, not {@code 0.12}. Treating "%" as a display affix to remove (like the currency
 * symbol and thousands commas) keeps this a pure formatting strip; dividing by 100 would be a
 * unit conversion this class has no way to know is wanted.
 *
 * <p>Because the strict numeric-pattern match still has to succeed afterward, stripping these
 * characters from a non-numeric string is normally harmless (e.g. stripping the comma from
 * {@code "Smith, John"} still leaves letters that fail the numeric pattern). The one accepted
 * false-positive case is a parenthesized non-numeric code that happens to contain only digits
 * (e.g. a phone extension written as {@code "(212)"}), which this will read as {@code -212}.
 */
final class NumericFormats {
  private static final Pattern CURRENCY_SYMBOLS = Pattern.compile("[$€£¥]");

  private NumericFormats() {
  }

  /**
   * Strips thousands separators, a currency symbol, a trailing percent sign, and converts an
   * accounting-style parenthetical negative into a leading minus sign.
   *
   * @param value the raw string value; must not be null
   * @return the value with formatting characters removed, ready for numeric parsing
   */
  static String stripFormatting(String value) {
    String result = value.trim();

    boolean parenNegative = result.length() >= 2
        && result.charAt(0) == '(' && result.charAt(result.length() - 1) == ')';
    if (parenNegative) {
      result = result.substring(1, result.length() - 1).trim();
    }

    if (result.endsWith("%")) {
      result = result.substring(0, result.length() - 1).trim();
    }

    result = CURRENCY_SYMBOLS.matcher(result).replaceAll("");
    result = result.replace(",", "");

    if (parenNegative) {
      result = "-" + result;
    }

    return result;
  }
}
