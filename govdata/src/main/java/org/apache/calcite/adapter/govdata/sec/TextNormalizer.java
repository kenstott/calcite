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
package org.apache.calcite.adapter.govdata.sec;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Normalizes text for consistent embedding generation.
 *
 * <p>Ensures that semantically equivalent expressions produce similar embeddings by
 * standardizing:
 * <ul>
 *   <li>Temporal references: "Q1 2024", "first quarter 2024" → "2024-Q1"</li>
 *   <li>Period expressions: "three months ended March 31" → "2024-Q1"</li>
 *   <li>Relative dates: "prior year", "year-over-year" → resolved to specific years</li>
 *   <li>Monetary values: "$5.2 million", "5.2M" → "$5,200,000"</li>
 * </ul>
 *
 * <p>Applied after chunking so that chunk_text and enriched_text remain comparable
 * in structure while enriched_text has normalized values for better embedding quality.
 */
public class TextNormalizer {

  // Filing context for resolving relative dates
  private final int filingYear;
  private final int filingQuarter;
  private final String periodEnd;

  // Quarter patterns
  private static final Pattern QUARTER_PATTERN = Pattern.compile(
      "\\b[Qq]([1-4])\\s*(\\d{4}|'?\\d{2})\\b");
  private static final Pattern QUARTER_YEAR_FIRST = Pattern.compile(
      "\\b(\\d{4})\\s*[Qq]([1-4])\\b");
  private static final Pattern ORDINAL_QUARTER = Pattern.compile(
      "\\b(first|second|third|fourth)\\s+quarter\\s+(of\\s+)?(\\d{4}|'?\\d{2})\\b",
      Pattern.CASE_INSENSITIVE);
  private static final Pattern ORDINAL_QUARTER_NO_YEAR = Pattern.compile(
      "(?<!\\d-)\\b(the\\s+)?(first|second|third|fourth)\\s+quarter\\b",
      Pattern.CASE_INSENSITIVE);

  // Period patterns
  private static final Pattern THREE_MONTHS_ENDED = Pattern.compile(
      "\\b(three|3)\\s+months?\\s+ended\\s+(\\w+\\s+\\d{1,2},?\\s+)?(\\d{4})\\b",
      Pattern.CASE_INSENSITIVE);
  private static final Pattern SIX_MONTHS_ENDED = Pattern.compile(
      "\\b(six|6)\\s+months?\\s+ended\\s+(\\w+\\s+\\d{1,2},?\\s+)?(\\d{4})\\b",
      Pattern.CASE_INSENSITIVE);
  private static final Pattern NINE_MONTHS_ENDED = Pattern.compile(
      "\\b(nine|9)\\s+months?\\s+ended\\s+(\\w+\\s+\\d{1,2},?\\s+)?(\\d{4})\\b",
      Pattern.CASE_INSENSITIVE);
  private static final Pattern TWELVE_MONTHS_ENDED = Pattern.compile(
      "\\b(twelve|12)\\s+months?\\s+ended\\s+(\\w+\\s+\\d{1,2},?\\s+)?(\\d{4})\\b",
      Pattern.CASE_INSENSITIVE);
  private static final Pattern FISCAL_YEAR_PATTERN = Pattern.compile(
      "\\b[Ff]iscal\\s+(year\\s+)?(\\d{4}|'?\\d{2})\\b");
  private static final Pattern FY_PATTERN = Pattern.compile(
      "\\bFY\\s*'?(\\d{4}|\\d{2})\\b");

  // Relative date patterns
  private static final Pattern PRIOR_YEAR = Pattern.compile(
      "\\b(prior|previous|last)\\s+(year|fiscal\\s+year)('s)?\\b",
      Pattern.CASE_INSENSITIVE);
  private static final Pattern PRIOR_YEAR_PERIOD = Pattern.compile(
      "\\b(prior|previous|last)\\s+(year|fiscal\\s+year)\\s+(period|quarter)\\b",
      Pattern.CASE_INSENSITIVE);
  private static final Pattern YEAR_AGO = Pattern.compile(
      "\\b(a\\s+)?year\\s+ago\\b",
      Pattern.CASE_INSENSITIVE);
  private static final Pattern YEAR_OVER_YEAR = Pattern.compile(
      "\\byear[- ]over[- ]year\\b",
      Pattern.CASE_INSENSITIVE);
  // YoY pattern - negative lookahead to avoid matching already-expanded "YoY (FY..."
  private static final Pattern YOY_PATTERN = Pattern.compile(
      "\\b[Yy][Oo][Yy]\\b(?!\\s*\\(FY)");
  // QoQ pattern - negative lookahead to avoid matching already-expanded "QoQ (..."
  private static final Pattern QOQ_PATTERN = Pattern.compile(
      "\\b[Qq][Oo][Qq]\\b(?!\\s*\\()");
  private static final Pattern CURRENT_QUARTER = Pattern.compile(
      "\\b(the\\s+)?(current|this)\\s+quarter\\b",
      Pattern.CASE_INSENSITIVE);
  private static final Pattern CURRENT_YEAR = Pattern.compile(
      "\\b(the\\s+)?(current|this)\\s+(fiscal\\s+)?year\\b",
      Pattern.CASE_INSENSITIVE);

  // Monetary patterns
  private static final Pattern MONEY_MILLIONS = Pattern.compile(
      "\\$\\s*(\\d+(?:\\.\\d+)?)\\s*(?:million|mil\\.?|mm)\\b",
      Pattern.CASE_INSENSITIVE);
  private static final Pattern MONEY_BILLIONS = Pattern.compile(
      "\\$\\s*(\\d+(?:\\.\\d+)?)\\s*(?:billion|bil\\.?|bn)\\b",
      Pattern.CASE_INSENSITIVE);
  private static final Pattern MONEY_M_SUFFIX = Pattern.compile(
      "\\$\\s*(\\d+(?:\\.\\d+)?)\\s*[Mm]\\b");
  private static final Pattern MONEY_B_SUFFIX = Pattern.compile(
      "\\$\\s*(\\d+(?:\\.\\d+)?)\\s*[Bb]\\b");
  private static final Pattern NUMBER_MILLIONS = Pattern.compile(
      "\\b(\\d+(?:\\.\\d+)?)\\s*(?:million|mil\\.?)\\b",
      Pattern.CASE_INSENSITIVE);
  private static final Pattern NUMBER_BILLIONS = Pattern.compile(
      "\\b(\\d+(?:\\.\\d+)?)\\s*(?:billion|bil\\.?)\\b",
      Pattern.CASE_INSENSITIVE);

  // Month name to number mapping
  private static final java.util.Map<String, Integer> MONTH_MAP = new java.util.HashMap<>();
  static {
    MONTH_MAP.put("january", 1);
    MONTH_MAP.put("february", 2);
    MONTH_MAP.put("march", 3);
    MONTH_MAP.put("april", 4);
    MONTH_MAP.put("may", 5);
    MONTH_MAP.put("june", 6);
    MONTH_MAP.put("july", 7);
    MONTH_MAP.put("august", 8);
    MONTH_MAP.put("september", 9);
    MONTH_MAP.put("october", 10);
    MONTH_MAP.put("november", 11);
    MONTH_MAP.put("december", 12);
    MONTH_MAP.put("jan", 1);
    MONTH_MAP.put("feb", 2);
    MONTH_MAP.put("mar", 3);
    MONTH_MAP.put("apr", 4);
    MONTH_MAP.put("jun", 6);
    MONTH_MAP.put("jul", 7);
    MONTH_MAP.put("aug", 8);
    MONTH_MAP.put("sep", 9);
    MONTH_MAP.put("sept", 9);
    MONTH_MAP.put("oct", 10);
    MONTH_MAP.put("nov", 11);
    MONTH_MAP.put("dec", 12);
  }

  /**
   * Creates a TextNormalizer with filing context for resolving relative dates.
   *
   * @param filingDate  Filing date in YYYY-MM-DD format
   * @param periodEnd   Period end date in YYYY-MM-DD format (optional, defaults to filingDate)
   */
  public TextNormalizer(String filingDate, String periodEnd) {
    int[] parsed = parseDate(filingDate);
    this.filingYear = parsed[0];
    int month = parsed[1];
    this.filingQuarter = monthToQuarter(month);
    this.periodEnd = periodEnd != null ? periodEnd : filingDate;
  }

  /**
   * Creates a TextNormalizer with explicit year and quarter context.
   *
   * @param year    The filing year
   * @param quarter The filing quarter (1-4)
   */
  public TextNormalizer(int year, int quarter) {
    this.filingYear = year;
    this.filingQuarter = quarter;
    this.periodEnd = String.format("%d-%02d-01", year, quarterToMonth(quarter));
  }

  /**
   * Creates a TextNormalizer with default context (current date).
   */
  public TextNormalizer() {
    java.time.LocalDate now = java.time.LocalDate.now();
    this.filingYear = now.getYear();
    this.filingQuarter = monthToQuarter(now.getMonthValue());
    this.periodEnd = now.toString();
  }

  /**
   * Normalizes text for consistent embedding generation.
   *
   * @param text The text to normalize
   * @return Normalized text with standardized temporal and monetary expressions
   */
  public String normalize(String text) {
    if (text == null || text.isEmpty()) {
      return text;
    }

    String result = text;

    // Normalize temporal expressions (order matters - specific patterns before general)
    result = normalizePeriodsEnded(result);
    result = normalizeQuarters(result);
    result = normalizeFiscalYears(result);
    result = normalizeRelativeDates(result);

    // Normalize monetary values
    result = normalizeMonetaryValues(result);

    return result;
  }

  /**
   * Normalizes quarter expressions to YYYY-QN format.
   */
  private String normalizeQuarters(String text) {
    String result = text;

    // "Q1 2024" or "Q1 '24" → "2024-Q1"
    Matcher m = QUARTER_PATTERN.matcher(result);
    StringBuffer sb = new StringBuffer();
    while (m.find()) {
      int quarter = Integer.parseInt(m.group(1));
      int year = parseYear(m.group(2));
      m.appendReplacement(sb, year + "-Q" + quarter);
    }
    m.appendTail(sb);
    result = sb.toString();

    // "2024 Q1" → "2024-Q1"
    m = QUARTER_YEAR_FIRST.matcher(result);
    sb = new StringBuffer();
    while (m.find()) {
      int year = Integer.parseInt(m.group(1));
      int quarter = Integer.parseInt(m.group(2));
      m.appendReplacement(sb, year + "-Q" + quarter);
    }
    m.appendTail(sb);
    result = sb.toString();

    // "first quarter 2024" → "2024-Q1"
    m = ORDINAL_QUARTER.matcher(result);
    sb = new StringBuffer();
    while (m.find()) {
      int quarter = ordinalToQuarter(m.group(1));
      int year = parseYear(m.group(3));
      m.appendReplacement(sb, year + "-Q" + quarter);
    }
    m.appendTail(sb);
    result = sb.toString();

    // "the first quarter" (no year) → "the YYYY-Q1" using context
    m = ORDINAL_QUARTER_NO_YEAR.matcher(result);
    sb = new StringBuffer();
    while (m.find()) {
      String prefix = m.group(1) != null ? m.group(1) : "";
      int quarter = ordinalToQuarter(m.group(2));
      m.appendReplacement(sb, prefix + filingYear + "-Q" + quarter);
    }
    m.appendTail(sb);
    result = sb.toString();

    // "the current quarter" → "the YYYY-QN"
    m = CURRENT_QUARTER.matcher(result);
    sb = new StringBuffer();
    while (m.find()) {
      String prefix = m.group(1) != null ? m.group(1) : "";
      m.appendReplacement(sb, prefix + filingYear + "-Q" + filingQuarter);
    }
    m.appendTail(sb);
    result = sb.toString();

    return result;
  }

  /**
   * Normalizes "N months ended" expressions.
   */
  private String normalizePeriodsEnded(String text) {
    String result = text;

    // "three months ended March 31, 2024" → "2024-Q1"
    result = normalizeMonthsEnded(result, THREE_MONTHS_ENDED, 3);

    // "six months ended June 30, 2024" → "2024-H1 (Q1-Q2)"
    result = normalizeMonthsEnded(result, SIX_MONTHS_ENDED, 6);

    // "nine months ended September 30, 2024" → "2024-9M (Q1-Q3)"
    result = normalizeMonthsEnded(result, NINE_MONTHS_ENDED, 9);

    // "twelve months ended December 31, 2024" → "2024-FY"
    result = normalizeMonthsEnded(result, TWELVE_MONTHS_ENDED, 12);

    return result;
  }

  private String normalizeMonthsEnded(String text, Pattern pattern, int months) {
    Matcher m = pattern.matcher(text);
    StringBuffer sb = new StringBuffer();
    while (m.find()) {
      int year = Integer.parseInt(m.group(3));
      String monthDatePart = m.group(2);
      int endQuarter = inferQuarterFromMonthDate(monthDatePart, months);

      String replacement;
      switch (months) {
        case 3:
          replacement = year + "-Q" + endQuarter;
          break;
        case 6:
          replacement = year + "-H" + (endQuarter <= 2 ? "1" : "2") + " (Q1-Q" + endQuarter + ")";
          break;
        case 9:
          replacement = year + "-9M (Q1-Q" + endQuarter + ")";
          break;
        case 12:
          replacement = year + "-FY";
          break;
        default:
          replacement = year + "-" + months + "M";
      }
      m.appendReplacement(sb, replacement);
    }
    m.appendTail(sb);
    return sb.toString();
  }

  /**
   * Normalizes fiscal year expressions.
   */
  private String normalizeFiscalYears(String text) {
    String result = text;

    // "fiscal year 2024" or "fiscal 2024" → "FY2024"
    Matcher m = FISCAL_YEAR_PATTERN.matcher(result);
    StringBuffer sb = new StringBuffer();
    while (m.find()) {
      int year = parseYear(m.group(2));
      m.appendReplacement(sb, "FY" + year);
    }
    m.appendTail(sb);
    result = sb.toString();

    // "FY'24" or "FY 24" → "FY2024"
    m = FY_PATTERN.matcher(result);
    sb = new StringBuffer();
    while (m.find()) {
      int year = parseYear(m.group(1));
      m.appendReplacement(sb, "FY" + year);
    }
    m.appendTail(sb);
    result = sb.toString();

    // "current year" → "FY2024"
    m = CURRENT_YEAR.matcher(result);
    sb = new StringBuffer();
    while (m.find()) {
      m.appendReplacement(sb, "FY" + filingYear);
    }
    m.appendTail(sb);
    result = sb.toString();

    return result;
  }

  /**
   * Normalizes relative date expressions to absolute dates.
   */
  private String normalizeRelativeDates(String text) {
    String result = text;
    int priorYear = filingYear - 1;
    int priorQuarter = filingQuarter;

    // "prior year period" → "2023-Q1" (same quarter, prior year)
    Matcher m = PRIOR_YEAR_PERIOD.matcher(result);
    StringBuffer sb = new StringBuffer();
    while (m.find()) {
      m.appendReplacement(sb, priorYear + "-Q" + priorQuarter);
    }
    m.appendTail(sb);
    result = sb.toString();

    // "prior year" → "FY2023"
    m = PRIOR_YEAR.matcher(result);
    sb = new StringBuffer();
    while (m.find()) {
      m.appendReplacement(sb, "FY" + priorYear);
    }
    m.appendTail(sb);
    result = sb.toString();

    // "a year ago" → "FY2023"
    m = YEAR_AGO.matcher(result);
    sb = new StringBuffer();
    while (m.find()) {
      m.appendReplacement(sb, "FY" + priorYear);
    }
    m.appendTail(sb);
    result = sb.toString();

    // "year-over-year" → "YoY (FY2024 vs FY2023)"
    m = YEAR_OVER_YEAR.matcher(result);
    sb = new StringBuffer();
    while (m.find()) {
      m.appendReplacement(sb, "YoY (FY" + filingYear + " vs FY" + priorYear + ")");
    }
    m.appendTail(sb);
    result = sb.toString();

    // "YoY" → "YoY (FY2024 vs FY2023)"
    m = YOY_PATTERN.matcher(result);
    sb = new StringBuffer();
    while (m.find()) {
      m.appendReplacement(sb, "YoY (FY" + filingYear + " vs FY" + priorYear + ")");
    }
    m.appendTail(sb);
    result = sb.toString();

    // "QoQ" → "QoQ (2024-Q1 vs 2024-Q0/2023-Q4)"
    m = QOQ_PATTERN.matcher(result);
    sb = new StringBuffer();
    while (m.find()) {
      int prevQ = filingQuarter - 1;
      int prevY = filingYear;
      if (prevQ < 1) {
        prevQ = 4;
        prevY = filingYear - 1;
      }
      m.appendReplacement(sb, "QoQ (" + filingYear + "-Q" + filingQuarter
          + " vs " + prevY + "-Q" + prevQ + ")");
    }
    m.appendTail(sb);
    result = sb.toString();

    return result;
  }

  /**
   * Normalizes monetary values to consistent format.
   */
  private String normalizeMonetaryValues(String text) {
    String result = text;

    // "$5.2 million" → "$5,200,000"
    result = normalizeMoney(result, MONEY_MILLIONS, 1_000_000);
    result = normalizeMoney(result, MONEY_BILLIONS, 1_000_000_000);
    result = normalizeMoney(result, MONEY_M_SUFFIX, 1_000_000);
    result = normalizeMoney(result, MONEY_B_SUFFIX, 1_000_000_000);

    // "5.2 million" (no $) → "5,200,000"
    result = normalizeNumber(result, NUMBER_MILLIONS, 1_000_000);
    result = normalizeNumber(result, NUMBER_BILLIONS, 1_000_000_000);

    return result;
  }

  private String normalizeMoney(String text, Pattern pattern, long multiplier) {
    Matcher m = pattern.matcher(text);
    StringBuffer sb = new StringBuffer();
    while (m.find()) {
      double value = Double.parseDouble(m.group(1)) * multiplier;
      String formatted = "$" + formatWithCommas((long) value);
      m.appendReplacement(sb, Matcher.quoteReplacement(formatted));
    }
    m.appendTail(sb);
    return sb.toString();
  }

  private String normalizeNumber(String text, Pattern pattern, long multiplier) {
    Matcher m = pattern.matcher(text);
    StringBuffer sb = new StringBuffer();
    while (m.find()) {
      double value = Double.parseDouble(m.group(1)) * multiplier;
      String formatted = formatWithCommas((long) value);
      m.appendReplacement(sb, Matcher.quoteReplacement(formatted));
    }
    m.appendTail(sb);
    return sb.toString();
  }

  // ========== Helper Methods ==========

  private int[] parseDate(String date) {
    if (date == null || date.isEmpty()) {
      return new int[]{java.time.LocalDate.now().getYear(), 1};
    }
    try {
      String[] parts = date.split("-");
      int year = Integer.parseInt(parts[0]);
      int month = parts.length > 1 ? Integer.parseInt(parts[1]) : 1;
      return new int[]{year, month};
    } catch (Exception e) {
      return new int[]{java.time.LocalDate.now().getYear(), 1};
    }
  }

  private int parseYear(String yearStr) {
    if (yearStr == null) {
      return filingYear;
    }
    yearStr = yearStr.replace("'", "").trim();
    int year = Integer.parseInt(yearStr);
    // Handle 2-digit years
    if (year < 100) {
      year += (year > 50) ? 1900 : 2000;
    }
    return year;
  }

  private int monthToQuarter(int month) {
    return ((month - 1) / 3) + 1;
  }

  private int quarterToMonth(int quarter) {
    return (quarter - 1) * 3 + 1;
  }

  private int ordinalToQuarter(String ordinal) {
    switch (ordinal.toLowerCase()) {
      case "first":
        return 1;
      case "second":
        return 2;
      case "third":
        return 3;
      case "fourth":
        return 4;
      default:
        return 1;
    }
  }

  private int inferQuarterFromMonthDate(String monthDatePart, int monthsInPeriod) {
    if (monthDatePart == null) {
      // Default based on period length
      return monthsInPeriod / 3;
    }

    String lower = monthDatePart.toLowerCase().trim();
    for (java.util.Map.Entry<String, Integer> entry : MONTH_MAP.entrySet()) {
      if (lower.startsWith(entry.getKey())) {
        return monthToQuarter(entry.getValue());
      }
    }

    // Default to filing quarter
    return filingQuarter;
  }

  private String formatWithCommas(long value) {
    return String.format("%,d", value);
  }

  /**
   * Creates a normalizer for a specific SEC filing.
   *
   * @param filingDate Filing date (YYYY-MM-DD)
   * @param periodEnd  Period end date (YYYY-MM-DD)
   * @return TextNormalizer configured for the filing
   */
  public static TextNormalizer forFiling(String filingDate, String periodEnd) {
    return new TextNormalizer(filingDate, periodEnd);
  }

  /**
   * Creates a normalizer for a specific year and quarter.
   *
   * @param year    Filing year
   * @param quarter Filing quarter (1-4)
   * @return TextNormalizer configured for the period
   */
  public static TextNormalizer forPeriod(int year, int quarter) {
    return new TextNormalizer(year, quarter);
  }
}
