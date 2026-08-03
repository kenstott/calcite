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

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoField;
import java.util.Locale;
import java.util.function.Function;

/**
 * Library of null-safe date-parse expressions for use in schema YAML {@code dateFormat:}
 * fields, with two independent implementations kept in lockstep per format:
 *
 * <ul>
 *   <li>{@link #toExpression(String)} — a DuckDB SQL fragment (never throws — uses
 *       {@code TRY_STRPTIME}), the preferred/primary evaluation path when a batch of rows is
 *       processed through DuckDB.</li>
 *   <li>{@link #parse(String)} — a pure-Java equivalent, used as the fallback when the DuckDB
 *       path isn't available (single-row evaluation, or the DuckDB batch call itself failed).
 *       Unlike {@code toExpression}, this throws {@link DateTimeParseException} on a genuine
 *       mismatch — the caller (a {@code coerceValue}-style type coercion step) is responsible
 *       for deciding what a parse failure means (fail loud, null, or drop), not this class.</li>
 * </ul>
 *
 * <p>Both forms are null-safe the same way: {@code null} or blank input produces a {@code null}
 * result (not a thrown exception) — only a non-blank value that fails to match the declared
 * format is a genuine parse failure.
 *
 * <p>Usage in YAML column definition:
 * <pre>
 *   columns:
 *     - name: transaction_date
 *       type: date
 *       dateFormat: MMDDYYYY
 * </pre>
 *
 * <p>Usage in Java:
 * <pre>
 *   String expr = DateParseFormat.MMDDYYYY.toExpression("transaction_date");
 *   LocalDate date = DateParseFormat.MMDDYYYY.parse(rawValue);
 * </pre>
 */
public enum DateParseFormat {

  // ── US civil formats ────────────────────────────────────────────────────────

  /** MM/DD/YYYY — e.g. 12/31/2024 */
  SLASH {
    @Override public String toExpression(String col) {
      return nullSafe(col, "TRY_STRPTIME(" + col + ", '%m/%d/%Y')::DATE");
    }
    @Override public LocalDate parse(String raw) {
      return parseOrNull(raw, s -> LocalDate.parse(s, FMT_SLASH));
    }
  },

  /** M/D/YYYY or MM/DD/YYYY (DuckDB %m/%d/%Y handles both) — e.g. 1/5/2024 */
  SLASH_SHORT {
    @Override public String toExpression(String col) {
      return nullSafe(col, "TRY_STRPTIME(" + col + ", '%-m/%-d/%Y')::DATE");
    }
    @Override public LocalDate parse(String raw) {
      return parseOrNull(raw, s -> LocalDate.parse(s, FMT_SLASH_SHORT));
    }
  },

  /** MM/DD/YY — e.g. 12/31/24 */
  SLASH_SHORT_YEAR {
    @Override public String toExpression(String col) {
      return nullSafe(col, "TRY_STRPTIME(" + col + ", '%m/%d/%y')::DATE");
    }
    @Override public LocalDate parse(String raw) {
      return parseOrNull(raw, s -> LocalDate.parse(s, FMT_SLASH_SHORT_YEAR));
    }
  },

  /** MMDDYYYY (no separator, left-zero-padded to 8 chars) — e.g. 12312024 or 1312024 */
  MMDDYYYY {
    @Override public String toExpression(String col) {
      return nullSafe(col, "TRY_STRPTIME(LPAD(" + col + ", 8, '0'), '%m%d%Y')::DATE");
    }
    @Override public LocalDate parse(String raw) {
      return parseOrNull(raw, s -> LocalDate.parse(padLeftZero(s, 8), FMT_MMDDYYYY));
    }
  },

  /** YYYYMMDD (ISO compact) — e.g. 20241231 */
  YYYYMMDD {
    @Override public String toExpression(String col) {
      return nullSafe(col, "TRY_STRPTIME(" + col + ", '%Y%m%d')::DATE");
    }
    @Override public LocalDate parse(String raw) {
      return parseOrNull(raw, s -> LocalDate.parse(s, FMT_YYYYMMDD));
    }
  },

  /** YYYY-MM-DD (ISO 8601) — e.g. 2024-12-31 */
  ISO {
    @Override public String toExpression(String col) {
      return nullSafe(col, "TRY_CAST(" + col + " AS DATE)");
    }
    @Override public LocalDate parse(String raw) {
      return parseOrNull(raw, LocalDate::parse);
    }
  },

  /** YYYY/MM/DD — e.g. 2024/12/31 */
  YYYY_SLASH_MM_SLASH_DD {
    @Override public String toExpression(String col) {
      return nullSafe(col, "TRY_STRPTIME(" + col + ", '%Y/%m/%d')::DATE");
    }
    @Override public LocalDate parse(String raw) {
      return parseOrNull(raw, s -> LocalDate.parse(s, FMT_YYYY_SLASH_MM_SLASH_DD));
    }
  },

  // ── Day-first formats ───────────────────────────────────────────────────────

  /** DD/MM/YYYY (European) — e.g. 31/12/2024 */
  DD_SLASH_MM_SLASH_YYYY {
    @Override public String toExpression(String col) {
      return nullSafe(col, "TRY_STRPTIME(" + col + ", '%d/%m/%Y')::DATE");
    }
    @Override public LocalDate parse(String raw) {
      return parseOrNull(raw, s -> LocalDate.parse(s, FMT_DD_SLASH_MM_SLASH_YYYY));
    }
  },

  /** DD-MM-YYYY — e.g. 31-12-2024 */
  DD_DASH_MM_DASH_YYYY {
    @Override public String toExpression(String col) {
      return nullSafe(col, "TRY_STRPTIME(" + col + ", '%d-%m-%Y')::DATE");
    }
    @Override public LocalDate parse(String raw) {
      return parseOrNull(raw, s -> LocalDate.parse(s, FMT_DD_DASH_MM_DASH_YYYY));
    }
  },

  // ── Month-name formats ──────────────────────────────────────────────────────

  /**
   * DD-MON-YY (Oracle-style abbreviated month, 2-digit year) — e.g. 27-SEP-24.
   *
   * <p>2-digit year pivot (matches DuckDB/POSIX {@code strptime} {@code %y} convention,
   * not a Java default): 00-68 → 2000-2068, 69-99 → 1969-1999.
   */
  DD_MON_YY {
    @Override public String toExpression(String col) {
      return nullSafe(col, "TRY_STRPTIME(" + col + ", '%d-%b-%y')::DATE");
    }
    @Override public LocalDate parse(String raw) {
      return parseOrNull(raw, s -> LocalDate.parse(s, FMT_DD_MON_YY));
    }
  },

  /** DD-MON-YYYY (Oracle-style abbreviated month, 4-digit year) — e.g. 27-SEP-2024 */
  DD_MON_YYYY {
    @Override public String toExpression(String col) {
      return nullSafe(col, "TRY_STRPTIME(" + col + ", '%d-%b-%Y')::DATE");
    }
    @Override public LocalDate parse(String raw) {
      return parseOrNull(raw, s -> LocalDate.parse(s, FMT_DD_MON_YYYY));
    }
  },

  /** DD-Month-YYYY (full month name) — e.g. 27-September-2024 */
  DD_MONTH_YYYY {
    @Override public String toExpression(String col) {
      return nullSafe(col, "TRY_STRPTIME(" + col + ", '%d-%B-%Y')::DATE");
    }
    @Override public LocalDate parse(String raw) {
      return parseOrNull(raw, s -> LocalDate.parse(s, FMT_DD_MONTH_YYYY));
    }
  },

  /** MON DD YYYY (space-separated, abbreviated month) — e.g. Sep 27 2024 */
  MON_DD_YYYY {
    @Override public String toExpression(String col) {
      return nullSafe(col, "TRY_STRPTIME(" + col + ", '%b %d %Y')::DATE");
    }
    @Override public LocalDate parse(String raw) {
      return parseOrNull(raw, s -> LocalDate.parse(s, FMT_MON_DD_YYYY));
    }
  },

  /** Month DD YYYY (space-separated, full month name) — e.g. September 27 2024 */
  MONTH_DD_YYYY {
    @Override public String toExpression(String col) {
      return nullSafe(col, "TRY_STRPTIME(" + col + ", '%B %d %Y')::DATE");
    }
    @Override public LocalDate parse(String raw) {
      return parseOrNull(raw, s -> LocalDate.parse(s, FMT_MONTH_DD_YYYY));
    }
  },

  // ── Unix time ───────────────────────────────────────────────────────────────

  /**
   * Unix epoch seconds (integer) — e.g. 1735603200.
   *
   * <p>Epoch is inherently UTC (no offset to interpret), so the resulting DATE is the true
   * UTC calendar date of that instant — verified against DuckDB directly, not assumed.
   */
  EPOCH_SECONDS {
    @Override public String toExpression(String col) {
      return nullSafe(col, "epoch_ms(CAST(" + col + " AS BIGINT) * 1000)::DATE");
    }
    @Override public LocalDate parse(String raw) {
      return parseOrNull(raw,
          s -> Instant.ofEpochSecond(Long.parseLong(s)).atZone(ZoneOffset.UTC).toLocalDate());
    }
  },

  /**
   * Unix epoch milliseconds (integer) — e.g. 1735603200000.
   *
   * <p>Same UTC-calendar-date semantics as {@link #EPOCH_SECONDS} — see that entry.
   */
  EPOCH_MILLIS {
    @Override public String toExpression(String col) {
      return nullSafe(col, "epoch_ms(CAST(" + col + " AS BIGINT))::DATE");
    }
    @Override public LocalDate parse(String raw) {
      return parseOrNull(raw,
          s -> Instant.ofEpochMilli(Long.parseLong(s)).atZone(ZoneOffset.UTC).toLocalDate());
    }
  },

  // ── Fiscal / quarter formats ────────────────────────────────────────────────

  /** YYYY-Qn fiscal quarter start — e.g. 2024-Q3 → 2024-07-01 */
  FISCAL_QUARTER {
    @Override public String toExpression(String col) {
      // Extract year and quarter number, compute start month
      return nullSafe(col,
          "MAKE_DATE(CAST(SPLIT_PART(" + col + ", '-', 1) AS INTEGER),"
          + " (CAST(REPLACE(SPLIT_PART(" + col + ", 'Q', 2), '-', '') AS INTEGER) - 1) * 3 + 1,"
          + " 1)");
    }
    @Override public LocalDate parse(String raw) {
      return parseOrNull(raw, s -> {
        int qIdx = s.indexOf('Q');
        if (qIdx < 0) {
          throw new DateTimeParseException("No 'Q' in fiscal quarter value", s, 0);
        }
        int year = Integer.parseInt(s.substring(0, qIdx - 1));
        int quarter = Integer.parseInt(s.substring(qIdx + 1));
        int startMonth = (quarter - 1) * 3 + 1;
        return LocalDate.of(year, startMonth, 1);
      });
    }
  },

  // ── Ambiguous / auto-detect ─────────────────────────────────────────────────

  /**
   * MM/DD/YYYY or MMDDYYYY (no separator) — FEC individual_contributions style.
   * Branches on presence of '/' to pick format.
   */
  MMDDYYYY_OR_SLASH {
    @Override public String toExpression(String col) {
      return "CASE WHEN " + col + " IS NULL OR TRIM(" + col + ") = '' THEN NULL "
          + "WHEN " + col + " LIKE '%/%' "
          +   "THEN TRY_STRPTIME(" + col + ", '%m/%d/%Y')::DATE "
          + "ELSE TRY_STRPTIME(LPAD(" + col + ", 8, '0'), '%m%d%Y')::DATE END";
    }
    @Override public LocalDate parse(String raw) {
      return parseOrNull(raw, s -> s.indexOf('/') >= 0
          ? LocalDate.parse(s, FMT_SLASH)
          : LocalDate.parse(padLeftZero(s, 8), FMT_MMDDYYYY));
    }
  },

  /**
   * ISO 8601 with time component — strips time, parses date portion.
   * Handles: 2024-12-31T00:00:00Z, 2024-12-31 00:00:00, 2024-12-31T12:34:56+00:00
   *
   * <p><b>Timezone caveat (verified against DuckDB directly):</b> any offset or {@code Z}
   * suffix is discarded, not converted — the DATE is the literal date digits as written in
   * the source string, e.g. {@code "2024-12-31T23:30:00+05:00"} yields {@code 2024-12-31}
   * even though that instant is already {@code 2024-12-31T18:30:00Z}. If the source values
   * carry a non-UTC offset and the true UTC calendar date matters, this format silently gives
   * the wrong answer for boundary timestamps — convert to epoch and use {@link #EPOCH_MILLIS}
   * instead, or confirm the source's offset is always {@code 00:00}/{@code Z}.
   */
  ISO_DATETIME {
    @Override public String toExpression(String col) {
      return nullSafe(col,
          "TRY_CAST(SPLIT_PART(" + col + ", 'T', 1) AS DATE)");
    }
    @Override public LocalDate parse(String raw) {
      return parseOrNull(raw, s -> {
        int tIdx = s.indexOf('T');
        // No 'T': a trailing " HH:mm:ss" (or any other suffix) is discarded, matching the
        // verified DuckDB behavior of taking just the leading 10-char date, not requiring the
        // whole remaining string to be a bare date.
        String datePart = tIdx >= 0 ? s.substring(0, tIdx)
            : s.length() > 10 ? s.substring(0, 10) : s;
        return LocalDate.parse(datePart, FMT_YYYYMMDD_ISO_PREFIX);
      });
    }
  },

  /**
   * Timestamp string — delegate to DuckDB TRY_CAST via TIMESTAMP then DATE.
   * Handles most common timestamp strings regardless of time zone suffix.
   *
   * <p><b>Timezone caveat (verified against DuckDB directly):</b> {@code CAST(... AS
   * TIMESTAMP)} parses to a timezone-naive value and silently drops any offset — identical
   * literal-date-as-written behavior to {@link #ISO_DATETIME}, not a true UTC conversion.
   * See that entry's caveat; it applies here too.
   *
   * <p>The Java fallback covers the common shapes DuckDB accepts (space or {@code T}
   * separator, optional fractional seconds, optional {@code Z}/numeric offset which is
   * discarded to match the DuckDB behavior above) but is not a complete reimplementation of
   * DuckDB's timestamp grammar — an unusual shape that DuckDB tolerates may still throw here.
   */
  TIMESTAMP_TO_DATE {
    @Override public String toExpression(String col) {
      return nullSafe(col,
          "TRY_CAST(TRY_CAST(" + col + " AS TIMESTAMP) AS DATE)");
    }
    @Override public LocalDate parse(String raw) {
      return parseOrNull(raw, s -> {
        // Discard any trailing Z or numeric offset — matches the verified DuckDB behavior of
        // taking the literal wall-clock date/time as written, not converting by offset.
        String noOffset = OFFSET_SUFFIX.matcher(s).replaceFirst("");
        String normalized = noOffset.indexOf('T') >= 0
            ? noOffset : noOffset.replaceFirst(" ", "T");
        int tIdx = normalized.indexOf('T');
        if (tIdx < 0) {
          // Date-only value with no time component at all.
          return LocalDate.parse(normalized, FMT_YYYYMMDD_ISO_PREFIX);
        }
        return LocalDateTime.parse(normalized).toLocalDate();
      });
    }
  };

  // ── Abstract ─────────────────────────────────────────────────────────────────

  /**
   * Returns a DuckDB SQL expression that converts {@code col} to DATE. Preferred/primary
   * evaluation path for batch row processing.
   * The expression is null-safe: NULL or blank input → NULL output.
   *
   * @param col the column reference (bare name or qualified, no quoting added)
   * @return DuckDB SQL fragment evaluating to DATE
   */
  public abstract String toExpression(String col);

  /**
   * Parses {@code raw} into a {@link LocalDate} in pure Java — the fallback path for when the
   * DuckDB batch path isn't available. Null or blank input returns {@code null}; a non-blank
   * value that doesn't match this format's shape throws {@link DateTimeParseException}
   * (or another {@link RuntimeException} for the numeric/composite formats) rather than
   * silently returning null — the caller decides what a genuine parse failure means.
   *
   * @param raw the raw source string (or {@code null})
   * @return the parsed date, or {@code null} for null/blank input
   */
  public abstract LocalDate parse(String raw);

  // ── Helpers ──────────────────────────────────────────────────────────────────

  private static String nullSafe(String col, String inner) {
    return "CASE WHEN " + col + " IS NULL OR TRIM(" + col + ") = '' THEN NULL "
        + "ELSE " + inner + " END";
  }

  private static LocalDate parseOrNull(String raw, Function<String, LocalDate> fn) {
    if (raw == null) {
      return null;
    }
    String trimmed = raw.trim();
    if (trimmed.isEmpty()) {
      return null;
    }
    return fn.apply(trimmed);
  }

  private static String padLeftZero(String s, int width) {
    if (s.length() >= width) {
      return s;
    }
    StringBuilder sb = new StringBuilder(width);
    for (int i = s.length(); i < width; i++) {
      sb.append('0');
    }
    sb.append(s);
    return sb.toString();
  }

  private static DateTimeFormatterBuilder caseInsensitive(String pattern) {
    return new DateTimeFormatterBuilder().parseCaseInsensitive().appendPattern(pattern);
  }

  /** 2-digit year, POSIX/strptime {@code %y} pivot: 00-68 -> 2000-2068, 69-99 -> 1969-1999. */
  private static DateTimeFormatterBuilder appendReducedYear(DateTimeFormatterBuilder builder) {
    return builder.appendValueReduced(ChronoField.YEAR, 2, 2, 1969);
  }

  private static final java.util.regex.Pattern OFFSET_SUFFIX =
      java.util.regex.Pattern.compile("(Z|[+-]\\d{2}:?\\d{2})$");

  private static final DateTimeFormatter FMT_YYYYMMDD_ISO_PREFIX =
      DateTimeFormatter.ofPattern("uuuu-MM-dd", Locale.ENGLISH);

  private static final DateTimeFormatter FMT_SLASH =
      DateTimeFormatter.ofPattern("MM/dd/uuuu", Locale.ENGLISH);

  private static final DateTimeFormatter FMT_SLASH_SHORT =
      DateTimeFormatter.ofPattern("M/d/uuuu", Locale.ENGLISH);

  private static final DateTimeFormatter FMT_SLASH_SHORT_YEAR =
      appendReducedYear(new DateTimeFormatterBuilder().appendPattern("MM/dd/"))
          .toFormatter(Locale.ENGLISH);

  private static final DateTimeFormatter FMT_MMDDYYYY =
      DateTimeFormatter.ofPattern("MMdduuuu", Locale.ENGLISH);

  private static final DateTimeFormatter FMT_YYYYMMDD =
      DateTimeFormatter.ofPattern("uuuuMMdd", Locale.ENGLISH);

  private static final DateTimeFormatter FMT_YYYY_SLASH_MM_SLASH_DD =
      DateTimeFormatter.ofPattern("uuuu/MM/dd", Locale.ENGLISH);

  private static final DateTimeFormatter FMT_DD_SLASH_MM_SLASH_YYYY =
      DateTimeFormatter.ofPattern("dd/MM/uuuu", Locale.ENGLISH);

  private static final DateTimeFormatter FMT_DD_DASH_MM_DASH_YYYY =
      DateTimeFormatter.ofPattern("dd-MM-uuuu", Locale.ENGLISH);

  private static final DateTimeFormatter FMT_DD_MON_YY =
      appendReducedYear(caseInsensitive("dd-MMM-")).toFormatter(Locale.ENGLISH);

  private static final DateTimeFormatter FMT_DD_MON_YYYY =
      caseInsensitive("dd-MMM-uuuu").toFormatter(Locale.ENGLISH);

  private static final DateTimeFormatter FMT_DD_MONTH_YYYY =
      caseInsensitive("dd-MMMM-uuuu").toFormatter(Locale.ENGLISH);

  private static final DateTimeFormatter FMT_MON_DD_YYYY =
      caseInsensitive("MMM dd uuuu").toFormatter(Locale.ENGLISH);

  private static final DateTimeFormatter FMT_MONTH_DD_YYYY =
      caseInsensitive("MMMM dd uuuu").toFormatter(Locale.ENGLISH);
}
