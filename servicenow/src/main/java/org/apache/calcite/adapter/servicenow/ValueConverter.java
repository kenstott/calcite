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

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.format.ResolverStyle;

/**
 * Turns the text of a ServiceNow field into the value Calcite stores for the column's SQL type.
 *
 * <p>ServiceNow sends every value as a string, and an empty value as {@code ""}; the wire format
 * does not distinguish an empty string from an absent value. Owner decision, kept in this one
 * place so that it is easy to change: an empty value is SQL NULL for every type, strings
 * included, and the adapter never returns {@code ""}. A non-empty value that does not parse as
 * the column's type is an error naming the table, column, record and value.
 *
 * <p>Calcite's internal forms: TIMESTAMP is milliseconds since the epoch, DATE is days since the
 * epoch, TIME is milliseconds since midnight. Timestamps are read with display values off and are
 * UTC.
 */
final class ValueConverter {
  private ValueConverter() {}

  private static final DateTimeFormatter DATE_TIME =
      DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss").withResolverStyle(ResolverStyle.STRICT);
  private static final DateTimeFormatter DATE =
      DateTimeFormatter.ofPattern("uuuu-MM-dd").withResolverStyle(ResolverStyle.STRICT);
  private static final DateTimeFormatter TIME =
      DateTimeFormatter.ofPattern("HH:mm:ss").withResolverStyle(ResolverStyle.STRICT);

  /** ServiceNow writes a time of day as a date-time on this day. */
  private static final String TIME_DAY_PREFIX = "1970-01-01 ";

  /**
   * Converts the text of one field.
   *
   * @param table  table name, for the error message
   * @param sysId  sys_id of the record, for the error message
   */
  static Object convert(ServiceNowColumn column, String text, String table, String sysId) {
    if (text.isEmpty()) {
      return null;
    }
    try {
      switch (column.kind) {
      case GUID:
      case TEXT:
      case REFERENCE:
        return text;
      case INTEGER:
        return Integer.valueOf(text);
      case LONG:
        return Long.valueOf(text);
      case DECIMAL:
      case CURRENCY:
        return new BigDecimal(text);
      case DOUBLE:
        return Double.valueOf(text);
      case BOOLEAN:
        if ("true".equals(text)) {
          return Boolean.TRUE;
        }
        if ("false".equals(text)) {
          return Boolean.FALSE;
        }
        throw new IllegalArgumentException("expected true or false");
      case TIMESTAMP:
        return LocalDateTime.parse(text, DATE_TIME).toInstant(ZoneOffset.UTC).toEpochMilli();
      case DATE:
        return (int) LocalDate.parse(text, DATE).toEpochDay();
      case TIME:
        if (!text.startsWith(TIME_DAY_PREFIX)) {
          throw new IllegalArgumentException("expected '" + TIME_DAY_PREFIX + "HH:mm:ss'");
        }
        return (int) (LocalTime.parse(text.substring(TIME_DAY_PREFIX.length()), TIME)
            .toNanoOfDay() / 1_000_000L);
      default:
        throw new IllegalStateException("Unhandled column kind " + column.kind);
      }
    } catch (IllegalArgumentException | DateTimeParseException e) {
      throw new ServiceNowException("Cannot read " + table + "." + column.field + " of record "
          + sysId + " as " + column.kind + " (field type " + column.glideType + "): value '"
          + text + "' is not valid: " + e.getMessage(), e);
    }
  }
}
