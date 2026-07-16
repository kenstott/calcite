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
package org.apache.calcite.adapter.govdata.ref;

import org.apache.calcite.adapter.file.etl.DataProvider;
import org.apache.calcite.adapter.file.etl.EtlPipelineConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.DayOfWeek;
import java.time.LocalDate;
import java.time.Month;
import java.time.ZoneOffset;
import java.time.format.TextStyle;
import java.time.temporal.IsoFields;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Generates the {@code ref.calendar} date-spine table: one row per calendar day from
 * {@link #START_YEAR} (Jan 1) through the end of {@code currentYear + FORWARD_YEARS} (Dec 31).
 *
 * <p>The table is a pure computation — there is no external source. Every row carries the
 * same day in every full-date representation (each of which is declared {@code unique} in the
 * schema so it is a valid FK target for heterogeneously-formatted date columns elsewhere):
 * <ul>
 *   <li>{@code date_key} / {@code iso_date} — ISO-8601 calendar date {@code YYYY-MM-DD}</li>
 *   <li>{@code basic_iso} — ISO-8601 basic form {@code YYYYMMDD}</li>
 *   <li>{@code iso_ordinal} — ISO ordinal date {@code YYYY-DDD}</li>
 *   <li>{@code iso_week_date} — ISO week date {@code YYYY-Www-D}</li>
 *   <li>{@code epoch_day} — days since 1970-01-01 (numeric full-date form)</li>
 * </ul>
 * plus every date part in numeric and named forms, calendar flags, and US-federal fiscal
 * attributes ({@code fiscal_year} is the FK into {@code ref.fiscal_year}).
 *
 * <p>Wired as {@code dataset_type: computed_delta} keyed on {@code iso_date}: the provider emits
 * the full span on every run and the pipeline appends only days past the stored high-water mark,
 * so each daily ETL run extends the spine by exactly the newly-reached day(s). Mirrors the
 * {@code gleif_entities} append pattern.
 */
public class CalendarDataProvider implements DataProvider {

  private static final Logger LOGGER = LoggerFactory.getLogger(CalendarDataProvider.class);

  /** First calendar year in the spine. Fixed at 2010 per the schema contract. */
  private static final int START_YEAR = 2010;
  /** Number of whole years to extend the spine past the current year. */
  private static final int FORWARD_YEARS = 2;

  public CalendarDataProvider() {
  }

  @Override public Iterator<Map<String, Object>> fetch(EtlPipelineConfig config,
      Map<String, String> variables) {
    int endYear = resolveCurrentYear() + FORWARD_YEARS;
    LocalDate start = LocalDate.of(START_YEAR, 1, 1);
    LocalDate end = LocalDate.of(endYear, 12, 31);

    List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
    for (LocalDate d = start; !d.isAfter(end); d = d.plusDays(1)) {
      rows.add(buildRow(d));
    }
    LOGGER.info("CalendarDataProvider: generated {} days ({}..{})", rows.size(), start, end);
    return rows.iterator();
  }

  /**
   * The current year drives the forward horizon. Honours the {@code GOVDATA_CURRENT_YEAR}
   * override the schema factory sets for reproducible runs; otherwise it is the UTC clock's
   * year (the definition of "current", not a fallback for missing config).
   */
  private int resolveCurrentYear() {
    String override = System.getProperty("GOVDATA_CURRENT_YEAR");
    if (override != null && !override.isEmpty()) {
      return Integer.parseInt(override.trim());
    }
    return LocalDate.now(ZoneOffset.UTC).getYear();
  }

  private Map<String, Object> buildRow(LocalDate d) {
    int year = d.getYear();
    int month = d.getMonthValue();
    int dayOfMonth = d.getDayOfMonth();
    int dayOfYear = d.getDayOfYear();
    int quarter = d.get(IsoFields.QUARTER_OF_YEAR);
    int dayOfQuarter = d.get(IsoFields.DAY_OF_QUARTER);
    int isoWeek = d.get(IsoFields.WEEK_OF_WEEK_BASED_YEAR);
    int isoWeekYear = d.get(IsoFields.WEEK_BASED_YEAR);
    DayOfWeek dow = d.getDayOfWeek();
    int isoDow = dow.getValue();                 // 1=Mon .. 7=Sun
    int usDow = (isoDow % 7) + 1;                 // 1=Sun .. 7=Sat
    int daysInMonth = d.lengthOfMonth();
    boolean weekend = dow == DayOfWeek.SATURDAY || dow == DayOfWeek.SUNDAY;

    Map<String, Object> r = new HashMap<String, Object>();

    // --- Full-date representations (each unique → FK target) ---
    String isoDate = d.toString();               // YYYY-MM-DD
    r.put("date_key", isoDate);                  // type date; ISO string coerces to DATE
    r.put("iso_date", isoDate);
    r.put("basic_iso", String.format("%04d%02d%02d", year, month, dayOfMonth));
    r.put("iso_ordinal", String.format("%04d-%03d", year, dayOfYear));
    r.put("iso_week_date", String.format("%04d-W%02d-%d", isoWeekYear, isoWeek, isoDow));
    r.put("epoch_day", d.toEpochDay());

    // --- Numeric date parts ---
    r.put("year", year);
    r.put("quarter", quarter);
    r.put("month", month);
    r.put("day_of_month", dayOfMonth);
    r.put("day_of_week_iso", isoDow);
    r.put("day_of_week_us", usDow);
    r.put("day_of_year", dayOfYear);
    r.put("day_of_quarter", dayOfQuarter);
    r.put("iso_week", isoWeek);
    r.put("iso_week_year", isoWeekYear);
    r.put("days_in_month", daysInMonth);

    // --- Named / string date parts ---
    r.put("month_name", d.getMonth().getDisplayName(TextStyle.FULL, Locale.US));
    r.put("month_abbr", d.getMonth().getDisplayName(TextStyle.SHORT, Locale.US));
    r.put("day_name", dow.getDisplayName(TextStyle.FULL, Locale.US));
    r.put("day_abbr", dow.getDisplayName(TextStyle.SHORT, Locale.US));
    r.put("year_month", String.format("%04d-%02d", year, month));
    r.put("year_quarter", String.format("%04d-Q%d", year, quarter));
    r.put("year_iso_week", String.format("%04d-W%02d", isoWeekYear, isoWeek));

    // --- Calendar flags ---
    r.put("is_weekend", weekend);
    r.put("is_weekday", !weekend);
    r.put("is_month_start", dayOfMonth == 1);
    r.put("is_month_end", dayOfMonth == daysInMonth);
    r.put("is_quarter_start", dayOfQuarter == 1);
    r.put("is_quarter_end", d.plusDays(1).get(IsoFields.QUARTER_OF_YEAR) != quarter);
    r.put("is_year_start", dayOfYear == 1);
    r.put("is_year_end", month == 12 && dayOfMonth == 31);
    r.put("is_leap_year", d.isLeapYear());

    // --- US federal fiscal attributes (FY starts Oct 1; FY labelled by the year it ends) ---
    boolean lastQuarter = month >= Month.OCTOBER.getValue();
    int fiscalYear = lastQuarter ? year + 1 : year;
    int fiscalMonth = lastQuarter ? month - 9 : month + 3;   // Oct=1 .. Sep=12
    int fiscalQuarter = ((fiscalMonth - 1) / 3) + 1;
    LocalDate fiscalStart = LocalDate.of(fiscalYear - 1, 10, 1);
    int dayOfFiscalYear = (int) (d.toEpochDay() - fiscalStart.toEpochDay()) + 1;
    r.put("fiscal_year", fiscalYear);
    r.put("fiscal_quarter", fiscalQuarter);
    r.put("fiscal_month", fiscalMonth);
    r.put("fiscal_year_label", "FY" + fiscalYear);
    r.put("day_of_fiscal_year", dayOfFiscalYear);

    return r;
  }
}
