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
package org.apache.calcite.adapter.govdata;

/**
 * Library of null-safe DuckDB date-parse expressions for use in schema YAML
 * {@code dateFormat:} fields and {@code FecDataRepair}.
 *
 * <p>Each value's {@link #toExpression(String)} returns a DuckDB SQL fragment
 * that evaluates to a DATE (never throws — uses TRY_STRPTIME).
 * NULL and blank inputs always produce NULL.
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
 * </pre>
 */
public enum DateParseFormat {

  // ── US civil formats ────────────────────────────────────────────────────────

  /** MM/DD/YYYY — e.g. 12/31/2024 */
  SLASH {
    @Override public String toExpression(String col) {
      return nullSafe(col, "TRY_STRPTIME(" + col + ", '%m/%d/%Y')::DATE");
    }
  },

  /** M/D/YYYY or MM/DD/YYYY (DuckDB %m/%d/%Y handles both) — e.g. 1/5/2024 */
  SLASH_SHORT {
    @Override public String toExpression(String col) {
      return nullSafe(col, "TRY_STRPTIME(" + col + ", '%-m/%-d/%Y')::DATE");
    }
  },

  /** MM/DD/YY — e.g. 12/31/24 */
  SLASH_SHORT_YEAR {
    @Override public String toExpression(String col) {
      return nullSafe(col, "TRY_STRPTIME(" + col + ", '%m/%d/%y')::DATE");
    }
  },

  /** MMDDYYYY (no separator, left-zero-padded to 8 chars) — e.g. 12312024 or 1312024 */
  MMDDYYYY {
    @Override public String toExpression(String col) {
      return nullSafe(col, "TRY_STRPTIME(LPAD(" + col + ", 8, '0'), '%m%d%Y')::DATE");
    }
  },

  /** YYYYMMDD (ISO compact) — e.g. 20241231 */
  YYYYMMDD {
    @Override public String toExpression(String col) {
      return nullSafe(col, "TRY_STRPTIME(" + col + ", '%Y%m%d')::DATE");
    }
  },

  /** YYYY-MM-DD (ISO 8601) — e.g. 2024-12-31 */
  ISO {
    @Override public String toExpression(String col) {
      return nullSafe(col, "TRY_CAST(" + col + " AS DATE)");
    }
  },

  /** YYYY/MM/DD — e.g. 2024/12/31 */
  YYYY_SLASH_MM_SLASH_DD {
    @Override public String toExpression(String col) {
      return nullSafe(col, "TRY_STRPTIME(" + col + ", '%Y/%m/%d')::DATE");
    }
  },

  // ── Day-first formats ───────────────────────────────────────────────────────

  /** DD/MM/YYYY (European) — e.g. 31/12/2024 */
  DD_SLASH_MM_SLASH_YYYY {
    @Override public String toExpression(String col) {
      return nullSafe(col, "TRY_STRPTIME(" + col + ", '%d/%m/%Y')::DATE");
    }
  },

  /** DD-MM-YYYY — e.g. 31-12-2024 */
  DD_DASH_MM_DASH_YYYY {
    @Override public String toExpression(String col) {
      return nullSafe(col, "TRY_STRPTIME(" + col + ", '%d-%m-%Y')::DATE");
    }
  },

  // ── Month-name formats ──────────────────────────────────────────────────────

  /** DD-MON-YY (Oracle-style abbreviated month, 2-digit year) — e.g. 27-SEP-24 */
  DD_MON_YY {
    @Override public String toExpression(String col) {
      return nullSafe(col, "TRY_STRPTIME(" + col + ", '%d-%b-%y')::DATE");
    }
  },

  /** DD-MON-YYYY (Oracle-style abbreviated month, 4-digit year) — e.g. 27-SEP-2024 */
  DD_MON_YYYY {
    @Override public String toExpression(String col) {
      return nullSafe(col, "TRY_STRPTIME(" + col + ", '%d-%b-%Y')::DATE");
    }
  },

  /** DD-Month-YYYY (full month name) — e.g. 27-September-2024 */
  DD_MONTH_YYYY {
    @Override public String toExpression(String col) {
      return nullSafe(col, "TRY_STRPTIME(" + col + ", '%d-%B-%Y')::DATE");
    }
  },

  /** MON DD YYYY (space-separated, abbreviated month) — e.g. Sep 27 2024 */
  MON_DD_YYYY {
    @Override public String toExpression(String col) {
      return nullSafe(col, "TRY_STRPTIME(" + col + ", '%b %d %Y')::DATE");
    }
  },

  /** Month DD YYYY (space-separated, full month name) — e.g. September 27 2024 */
  MONTH_DD_YYYY {
    @Override public String toExpression(String col) {
      return nullSafe(col, "TRY_STRPTIME(" + col + ", '%B %d %Y')::DATE");
    }
  },

  // ── Unix time ───────────────────────────────────────────────────────────────

  /** Unix epoch seconds (integer) — e.g. 1735603200 */
  EPOCH_SECONDS {
    @Override public String toExpression(String col) {
      return nullSafe(col, "epoch_ms(CAST(" + col + " AS BIGINT) * 1000)::DATE");
    }
  },

  /** Unix epoch milliseconds (integer) — e.g. 1735603200000 */
  EPOCH_MILLIS {
    @Override public String toExpression(String col) {
      return nullSafe(col, "epoch_ms(CAST(" + col + " AS BIGINT))::DATE");
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
  },

  /**
   * ISO 8601 with time component — strips time, parses date portion.
   * Handles: 2024-12-31T00:00:00Z, 2024-12-31 00:00:00, 2024-12-31T12:34:56+00:00
   */
  ISO_DATETIME {
    @Override public String toExpression(String col) {
      return nullSafe(col,
          "TRY_CAST(SPLIT_PART(" + col + ", 'T', 1) AS DATE)");
    }
  },

  /**
   * Timestamp string — delegate to DuckDB TRY_CAST via TIMESTAMP then DATE.
   * Handles most common timestamp strings regardless of time zone suffix.
   */
  TIMESTAMP_TO_DATE {
    @Override public String toExpression(String col) {
      return nullSafe(col,
          "TRY_CAST(TRY_CAST(" + col + " AS TIMESTAMP) AS DATE)");
    }
  };

  // ── Abstract ─────────────────────────────────────────────────────────────────

  /**
   * Returns a DuckDB SQL expression that converts {@code col} to DATE.
   * The expression is null-safe: NULL or blank input → NULL output.
   *
   * @param col the column reference (bare name or qualified, no quoting added)
   * @return DuckDB SQL fragment evaluating to DATE
   */
  public abstract String toExpression(String col);

  // ── Helper ───────────────────────────────────────────────────────────────────

  private static String nullSafe(String col, String inner) {
    return "CASE WHEN " + col + " IS NULL OR TRIM(" + col + ") = '' THEN NULL "
        + "ELSE " + inner + " END";
  }
}
