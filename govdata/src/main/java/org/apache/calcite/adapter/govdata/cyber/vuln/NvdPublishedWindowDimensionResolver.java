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
package org.apache.calcite.adapter.govdata.cyber.vuln;

import org.apache.calcite.adapter.file.etl.DimensionConfig;
import org.apache.calcite.adapter.file.etl.DimensionResolver;
import org.apache.calcite.adapter.file.storage.StorageProvider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Dimension resolver for NVD CVE published-date windows.
 *
 * <p>The NVD API enforces a 120-day maximum window when date filters are used.
 * This resolver generates asymmetric windows:
 * <ul>
 *   <li><b>History</b> (years before the current calendar year): quarterly windows,
 *       one per year+quarter (≤92 days each), refreshed yearly.</li>
 *   <li><b>Tip</b> (the trailing days up to today): daily windows for freshness,
 *       in chunks of at most {@code tipChunkDays} days.</li>
 * </ul>
 *
 * <p>Windows are expressed as {@code pubStartDate} / {@code pubEndDate} parameters
 * in ISO-8601 datetime form ({@code YYYY-MM-DDTHH:mm:ss.000}) compatible with
 * the NVD CVE 2.0 API.
 *
 * <h3>Configuration</h3>
 * <pre>{@code
 * hooks:
 *   dimensionResolver: "...NvdPublishedWindowDimensionResolver"
 *
 * dimensions:
 *   pubStartDate:
 *     type: custom
 *     properties:
 *       startYear: "2002"       # earliest NVD data; default 2002
 *       tipDays: "90"           # rolling tip period in days (≤120); default 90
 *       tipChunkDays: "1"       # tip window chunk size in days; default 1 (daily)
 *   pubEndDate:
 *     type: custom              # resolved per pubStartDate using context
 * }</pre>
 *
 * <h3>Wiring</h3>
 * <p>The {@code pubStartDate} dimension is resolved first to produce all window
 * start dates. The {@code pubEndDate} dimension is resolved with context containing
 * the current {@code pubStartDate} value, and returns the corresponding end date.
 *
 * <p>The YAML {@code source.parameters} map references both:
 * <pre>{@code
 * source:
 *   parameters:
 *     pubStartDate: "{pubStartDate}"
 *     pubEndDate: "{pubEndDate}"
 * }</pre>
 *
 * <h3>Partition routing</h3>
 * <p>Because a single published-date window fetch may return CVEs published across
 * multiple years/quarters (e.g., a lastMod fetch spanning a large range), the schema
 * uses {@code effectiveYearField} / {@code effectiveMonthField} on the dimension
 * config to fan rows into their correct Iceberg {@code year}/{@code quarter}
 * partitions by the CVE's own {@code published} date.
 */
public class NvdPublishedWindowDimensionResolver implements DimensionResolver {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(NvdPublishedWindowDimensionResolver.class);

  /** ISO-8601 datetime format required by the NVD API. */
  private static final DateTimeFormatter NVD_DT_FMT =
      DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS");

  /** Default earliest NVD published year. The NVD CVE list starts in 1999 but
   *  meaningful CVSS-scored entries begin around 2002. */
  public static final int DEFAULT_START_YEAR = 2002;

  /** Default tip period (days before today that are treated as the rolling tip). */
  public static final int DEFAULT_TIP_DAYS = 90;

  /** Default tip chunk size in days (daily by default for maximum freshness). */
  public static final int DEFAULT_TIP_CHUNK_DAYS = 1;

  /** Maximum NVD API window in days. Enforced as hard cap on all generated windows. */
  public static final int NVD_MAX_WINDOW_DAYS = 120;

  /**
   * Internal map from pubStartDate ISO string → pubEndDate ISO string.
   * Built once during the first {@code pubStartDate} resolve call and reused
   * for all subsequent {@code pubEndDate} context lookups.
   */
  private final Map<String, String> startToEnd = new LinkedHashMap<String, String>();

  /** Ordered list of all pubStartDate values (iteration order = API call order). */
  private final List<String> startDates = new ArrayList<String>();

  /** Whether the window map has been built for the current resolve cycle. */
  private boolean windowsBuilt = false;

  /** The effective "today" used when building windows; fixed per resolve cycle. */
  private LocalDate resolveDate = null;

  /**
   * Default constructor required for reflection-based instantiation by
   * {@link org.apache.calcite.adapter.file.etl.DimensionIterator}.
   */
  public NvdPublishedWindowDimensionResolver() {
  }

  @Override public List<String> resolve(String dimensionName, DimensionConfig config,
      Map<String, String> context, StorageProvider storageProvider) {

    if ("pubStartDate".equals(dimensionName)) {
      return resolvePubStartDate(config);
    }

    if ("pubEndDate".equals(dimensionName)) {
      return resolvePubEndDate(config, context);
    }

    LOGGER.debug("NvdPublishedWindowDimensionResolver: dimension '{}' not handled, returning empty",
        dimensionName);
    return Collections.emptyList();
  }

  /**
   * Returns the ordered list of all window start dates.
   * Builds the full window map if not yet built.
   */
  private List<String> resolvePubStartDate(DimensionConfig config) {
    ensureWindowsBuilt(config);
    LOGGER.info("NvdPublishedWindow: {} total windows (start dates)", startDates.size());
    return Collections.unmodifiableList(startDates);
  }

  /**
   * Returns the single end date corresponding to the {@code pubStartDate} in context.
   */
  private List<String> resolvePubEndDate(DimensionConfig config, Map<String, String> context) {
    ensureWindowsBuilt(config);

    String start = context.get("pubStartDate");
    if (start == null || start.isEmpty()) {
      throw new IllegalStateException(
          "NvdPublishedWindowDimensionResolver: no pubStartDate in context for pubEndDate "
          + "resolution. Ensure pubStartDate dimension is defined before pubEndDate.");
    }

    String end = startToEnd.get(start);
    if (end == null) {
      throw new IllegalStateException(
          "NvdPublishedWindowDimensionResolver: unknown pubStartDate '"
          + start + "' — not in built window map. "
          + "Ensure pubEndDate dimension uses the same resolver instance as pubStartDate.");
    }

    LOGGER.debug("NvdPublishedWindow: pubStartDate={} -> pubEndDate={}", start, end);
    return Collections.singletonList(end);
  }

  /**
   * Builds the full window map (history quarterly + tip daily) if not yet done.
   * Thread-safe via synchronized — the resolver is instantiated once per schema
   * and may be called concurrently in multi-threaded expansion (though typically
   * called single-threaded by {@link org.apache.calcite.adapter.file.etl.DimensionIterator}).
   */
  private synchronized void ensureWindowsBuilt(DimensionConfig config) {
    if (windowsBuilt) {
      return;
    }

    resolveDate = LocalDate.now();
    // Precedence: GOVDATA_START_YEAR env/sysprop (set by dq-rebuild) > YAML startYear > default.
    // This ensures the dq-rebuild's 2-year DQ window (GOVDATA_START_YEAR=2025) limits history
    // to 2025+, while a full production backfill sets a lower start year.
    int startYear = resolveStartYear(config);
    int tipDays = parseIntProperty(config, "tipDays", DEFAULT_TIP_DAYS);
    int tipChunkDays = parseIntProperty(config, "tipChunkDays", DEFAULT_TIP_CHUNK_DAYS);

    // Clamp to NVD 120-day cap
    tipDays = Math.min(tipDays, NVD_MAX_WINDOW_DAYS);
    tipChunkDays = Math.min(tipChunkDays, NVD_MAX_WINDOW_DAYS);

    // Tip boundary: the rolling window of recent days
    LocalDate tipStart = resolveDate.minusDays(tipDays);
    // History covers [startYear-Jan-01, tipStart - 1 day]
    LocalDate historyEnd = tipStart.minusDays(1);

    LOGGER.info("NvdPublishedWindow: resolveDate={} startYear={} tipDays={} "
        + "tipChunkDays={} tipStart={} historyEnd={}",
        resolveDate, startYear, tipDays, tipChunkDays, tipStart, historyEnd);

    buildHistoryWindows(startYear, historyEnd);
    buildTipWindows(tipStart, resolveDate, tipChunkDays);

    LOGGER.info("NvdPublishedWindow: built {} history windows + {} tip windows = {} total",
        countHistoryWindows(startYear, historyEnd),
        countTipWindows(tipStart, resolveDate, tipChunkDays),
        startDates.size());

    windowsBuilt = true;
  }

  /**
   * Builds quarterly windows from {@code startYear}-Q1 through {@code historyEnd}.
   * Each quarter window is at most 92 days (well within the NVD 120-day cap).
   * Windows are added in chronological order (oldest first) so the tracker
   * progresses from earliest data forward.
   */
  private void buildHistoryWindows(int startYear, LocalDate historyEnd) {
    int currentYear = resolveDate.getYear();

    for (int year = startYear; year <= currentYear; year++) {
      for (int quarter = 1; quarter <= 4; quarter++) {
        LocalDate qStart = quarterStart(year, quarter);
        LocalDate qEnd = quarterEnd(year, quarter);

        // Skip quarters that are entirely after the history cutoff
        if (qStart.isAfter(historyEnd)) {
          break;
        }

        // Clamp the quarter end to historyEnd
        if (qEnd.isAfter(historyEnd)) {
          qEnd = historyEnd;
        }

        addWindow(qStart, qEnd);
      }
    }
  }

  /**
   * Builds daily (or multi-day chunk) windows from {@code tipStart} through {@code today}.
   * Windows are added chronologically (oldest tip first, so most recent is processed last).
   */
  private void buildTipWindows(LocalDate tipStart, LocalDate today, int chunkDays) {
    LocalDate cursor = tipStart;
    while (!cursor.isAfter(today)) {
      LocalDate windowEnd = cursor.plusDays(chunkDays - 1);
      if (windowEnd.isAfter(today)) {
        windowEnd = today;
      }
      addWindow(cursor, windowEnd);
      cursor = cursor.plusDays(chunkDays);
    }
  }

  /**
   * Adds a window to both the ordered list and the lookup map.
   */
  private void addWindow(LocalDate start, LocalDate end) {
    String startStr = start.atStartOfDay().format(NVD_DT_FMT);
    // End of day for inclusive end date
    String endStr = end.atTime(23, 59, 59, 0).format(NVD_DT_FMT);
    startToEnd.put(startStr, endStr);
    startDates.add(startStr);
    LOGGER.debug("NvdPublishedWindow: added window {} -> {}", startStr, endStr);
  }

  /**
   * Returns the first day of the given quarter.
   */
  public static LocalDate quarterStart(int year, int quarter) {
    int month = (quarter - 1) * 3 + 1;
    return LocalDate.of(year, month, 1);
  }

  /**
   * Returns the last day of the given quarter.
   */
  public static LocalDate quarterEnd(int year, int quarter) {
    int lastMonth = quarter * 3;
    LocalDate start = LocalDate.of(year, lastMonth, 1);
    return start.withDayOfMonth(start.lengthOfMonth());
  }

  /** Counts history windows for logging. */
  private int countHistoryWindows(int startYear, LocalDate historyEnd) {
    int count = 0;
    int currentYear = resolveDate.getYear();
    for (int year = startYear; year <= currentYear; year++) {
      for (int quarter = 1; quarter <= 4; quarter++) {
        LocalDate qStart = quarterStart(year, quarter);
        if (qStart.isAfter(historyEnd)) {
          break;
        }
        count++;
      }
    }
    return count;
  }

  /** Counts tip windows for logging. */
  private int countTipWindows(LocalDate tipStart, LocalDate today, int chunkDays) {
    int count = 0;
    LocalDate cursor = tipStart;
    while (!cursor.isAfter(today)) {
      count++;
      cursor = cursor.plusDays(chunkDays);
    }
    return count;
  }

  /**
   * Resolves the history start year with the following precedence:
   * <ol>
   *   <li>{@code GOVDATA_START_YEAR} environment variable or system property — set by
   *       the dq-rebuild path to scope history to the DQ window (e.g. 2025 for N−1).</li>
   *   <li>YAML dimension property {@code startYear} — the schema author's data floor.</li>
   *   <li>{@link #DEFAULT_START_YEAR} — hard-coded fallback of 2002.</li>
   * </ol>
   *
   * <p>The env/sysprop value acts as a <em>lower bound</em> (takes the maximum of the
   * env value and the YAML property) so that a YAML {@code startYear: 2010} combined
   * with a dq-rebuild {@code GOVDATA_START_YEAR=2025} correctly produces 2025, while a
   * full production backfill with {@code GOVDATA_START_YEAR=1999} and YAML
   * {@code startYear: 2002} produces 2002 (the tighter data floor wins).
   */
  private static int resolveStartYear(DimensionConfig config) {
    int yamlStart = parseIntProperty(config, "startYear", DEFAULT_START_YEAR);

    // GOVDATA_START_YEAR is set as a system property from the model operand by the factory.
    String govdataStartYear = System.getProperty("GOVDATA_START_YEAR");

    if (govdataStartYear != null && !govdataStartYear.isEmpty()) {
      try {
        int envYear = Integer.parseInt(govdataStartYear.trim());
        // Use the later of the env year and the YAML floor (tighter bound wins)
        int effective = Math.max(envYear, yamlStart);
        LOGGER.info("NvdPublishedWindow: startYear={} (GOVDATA_START_YEAR={}, yamlStartYear={})",
            effective, envYear, yamlStart);
        return effective;
      } catch (NumberFormatException e) {
        LOGGER.warn("NvdPublishedWindow: invalid GOVDATA_START_YEAR='{}', using YAML/default {}",
            govdataStartYear, yamlStart);
      }
    }

    LOGGER.info("NvdPublishedWindow: startYear={} (from YAML property or default; "
        + "GOVDATA_START_YEAR not set)", yamlStart);
    return yamlStart;
  }

  /**
   * Parses an integer property from the dimension config, with a default.
   */
  private static int parseIntProperty(DimensionConfig config, String key, int defaultValue) {
    String val = config.getProperty(key);
    if (val == null || val.isEmpty()) {
      return defaultValue;
    }
    try {
      return Integer.parseInt(val.trim());
    } catch (NumberFormatException e) {
      LOGGER.warn("NvdPublishedWindow: invalid integer property '{}={}', using default {}",
          key, val, defaultValue);
      return defaultValue;
    }
  }

  /**
   * Returns a human-readable summary of windows for a given config.
   * Used by tests for assertion.
   *
   * @param startYear  earliest history year
   * @param tipDays    rolling tip period in days
   * @param chunkDays  tip chunk size in days
   * @param today      the reference date (typically LocalDate.now())
   * @return list of (startDate, endDate) pairs
   */
  public static List<String[]> buildWindowsForTesting(
      int startYear, int tipDays, int chunkDays, LocalDate today) {
    NvdPublishedWindowDimensionResolver r = new NvdPublishedWindowDimensionResolver();
    r.resolveDate = today;

    int cappedTipDays = Math.min(tipDays, NVD_MAX_WINDOW_DAYS);
    int cappedChunkDays = Math.min(chunkDays, NVD_MAX_WINDOW_DAYS);
    LocalDate tipStart = today.minusDays(cappedTipDays);
    LocalDate historyEnd = tipStart.minusDays(1);

    r.buildHistoryWindows(startYear, historyEnd);
    r.buildTipWindows(tipStart, today, cappedChunkDays);

    List<String[]> result = new ArrayList<String[]>();
    for (String start : r.startDates) {
      result.add(new String[]{start, r.startToEnd.get(start)});
    }
    return result;
  }
}
