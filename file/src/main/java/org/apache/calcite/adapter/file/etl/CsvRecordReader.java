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
package org.apache.calcite.adapter.file.etl;

import java.io.BufferedReader;
import java.io.IOException;

/**
 * Reads complete CSV/TSV records from a {@link BufferedReader}, correctly
 * handling RFC4180-style quoted fields that contain embedded newlines.
 *
 * <p>Plain {@link BufferedReader#readLine()} terminates on any of {@code \n},
 * {@code \r}, or {@code \r\n} — which truncates quoted multi-line fields and
 * silently corrupts records downstream. This reader continues consuming lines
 * (joined with {@code '\n'}) until the running double-quote count is even,
 * which is the RFC4180 invariant for a balanced record.
 *
 * <p>{@code ""} (escaped quote inside a quoted field) counts as two quotes and
 * therefore preserves quote parity — no special-case handling needed.
 */
public final class CsvRecordReader {

  private CsvRecordReader() { }

  /**
   * Reads the next complete CSV/TSV record. Returns {@code null} at end of stream.
   *
   * @param reader source reader
   * @return a single record (possibly spanning multiple physical lines joined by
   *     {@code '\n'}), or {@code null} if EOF reached before any data
   */
  public static String readRecord(BufferedReader reader) throws IOException {
    String line = reader.readLine();
    if (line == null) {
      return null;
    }
    if (!hasOddQuotes(line)) {
      return line;
    }
    StringBuilder sb = new StringBuilder(line);
    while (true) {
      String next = reader.readLine();
      if (next == null) {
        // EOF mid-quoted-field — return what we have rather than blocking forever
        return sb.toString();
      }
      sb.append('\n').append(next);
      if (!hasOddQuotes(sb)) {
        return sb.toString();
      }
    }
  }

  private static boolean hasOddQuotes(CharSequence s) {
    int count = 0;
    for (int i = 0; i < s.length(); i++) {
      if (s.charAt(i) == '"') {
        count++;
      }
    }
    return (count & 1) != 0;
  }

  /**
   * Splits a complete CSV/TSV record into fields, honoring RFC4180-style quoted
   * fields that may contain the delimiter, newlines, or escaped quotes ({@code ""}).
   *
   * <p>Fields surrounded by {@code "..."} have outer quotes stripped and any
   * embedded {@code ""} replaced with a single {@code "}. Unquoted fields are
   * returned as-is.
   *
   * @param record    a complete record (from {@link #readRecord})
   * @param delimiter the field delimiter character (e.g. {@code ','} for CSV,
   *                  {@code '\t'} for TSV)
   * @return list of fields in order
   */
  public static java.util.List<String> splitFields(String record, char delimiter) {
    java.util.List<String> out = new java.util.ArrayList<>();
    StringBuilder field = new StringBuilder();
    boolean inQuotes = false;
    int len = record.length();
    for (int i = 0; i < len; i++) {
      char c = record.charAt(i);
      if (inQuotes) {
        if (c == '"') {
          if (i + 1 < len && record.charAt(i + 1) == '"') {
            field.append('"');  // escaped quote
            i++;
          } else {
            inQuotes = false;  // closing quote
          }
        } else {
          field.append(c);
        }
      } else {
        if (c == delimiter) {
          out.add(field.toString());
          field.setLength(0);
        } else if (c == '"' && field.length() == 0) {
          inQuotes = true;  // opening quote at start of field
        } else {
          field.append(c);
        }
      }
    }
    out.add(field.toString());
    return out;
  }
}
