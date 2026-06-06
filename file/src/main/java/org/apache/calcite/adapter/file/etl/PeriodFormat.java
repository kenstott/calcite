/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.adapter.file.etl;

import java.time.Month;
import java.time.format.TextStyle;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Renders a period dimension's canonical value into the form a source API/URL
 * wants. This is the {@code format:} concern only — completion markers and
 * partition keys always use the canonical (sortable) value, never this.
 *
 * <p>A format spec is either a printf-style template (contains {@code %}) or a
 * shorthand directive:
 * <ul>
 *   <li>{@code %02d}, {@code 02d} → zero-padded integer ({@code 3} → {@code "03"})</li>
 *   <li>{@code %d}, {@code d} → bare integer</li>
 *   <li>{@code %B}, {@code B} → full month name ({@code 3} → {@code "March"})</li>
 *   <li>{@code %b}, {@code b} → short month name ({@code 3} → {@code "Mar"})</li>
 *   <li>{@code Q%d}, {@code Q} → {@code "Q3"} (literal prefix)</li>
 *   <li>{@code W%02d}, {@code W} → {@code "W03"}</li>
 * </ul>
 *
 * <p>Non-numeric values, or unrecognized specs, are returned unchanged.
 */
final class PeriodFormat {

  private static final Pattern DIRECTIVE = Pattern.compile("%(0?\\d*)([dBb])");

  private PeriodFormat() {
  }

  /**
   * Renders {@code value} with {@code spec}. Returns {@code value} unchanged when
   * the spec is empty, the value is non-numeric, or the spec is unrecognized.
   */
  static String render(String value, String spec) {
    if (spec == null || spec.isEmpty() || value == null) {
      return value;
    }
    Integer n = parseInt(value);
    if (n == null) {
      return value;
    }
    if (spec.indexOf('%') >= 0) {
      return applyTemplate(spec, n);
    }
    // Shorthand (no '%')
    if ("Q".equals(spec)) {
      return "Q" + n;
    }
    if ("W".equals(spec)) {
      return "W" + pad(n, 2);
    }
    if ("B".equals(spec)) {
      return monthName(n, true);
    }
    if ("b".equals(spec)) {
      return monthName(n, false);
    }
    if (spec.endsWith("d")) {
      return pad(n, width(spec.substring(0, spec.length() - 1)));
    }
    return value;
  }

  /** Replaces every {@code %[0]Nd}/{@code %B}/{@code %b} directive in a printf-style template. */
  private static String applyTemplate(String spec, int n) {
    Matcher m = DIRECTIVE.matcher(spec);
    StringBuffer out = new StringBuffer();
    while (m.find()) {
      String flagsAndWidth = m.group(1);
      String conv = m.group(2);
      String rendered;
      if ("d".equals(conv)) {
        rendered = pad(n, width(flagsAndWidth));
      } else if ("B".equals(conv)) {
        rendered = monthName(n, true);
      } else {
        rendered = monthName(n, false);
      }
      m.appendReplacement(out, Matcher.quoteReplacement(rendered));
    }
    m.appendTail(out);
    return out.toString();
  }

  /** Width from a {@code %} flags/width fragment such as {@code "02"}, {@code "2"}, or {@code ""}. */
  private static int width(String flagsAndWidth) {
    if (flagsAndWidth == null || flagsAndWidth.isEmpty()) {
      return 1;
    }
    String digits = flagsAndWidth.startsWith("0")
        ? flagsAndWidth.substring(1) : flagsAndWidth;
    if (digits.isEmpty()) {
      return 1;
    }
    try {
      return Integer.parseInt(digits);
    } catch (NumberFormatException e) {
      return 1;
    }
  }

  private static String pad(int value, int width) {
    String s = Integer.toString(value);
    if (s.length() >= width) {
      return s;
    }
    StringBuilder sb = new StringBuilder();
    for (int i = s.length(); i < width; i++) {
      sb.append('0');
    }
    return sb.append(s).toString();
  }

  private static String monthName(int month, boolean full) {
    if (month < 1 || month > 12) {
      return Integer.toString(month);
    }
    return Month.of(month).getDisplayName(full ? TextStyle.FULL : TextStyle.SHORT, Locale.ENGLISH);
  }

  private static Integer parseInt(String value) {
    try {
      return Integer.valueOf(value.trim());
    } catch (NumberFormatException e) {
      return null;
    }
  }
}
