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
package org.apache.calcite.adapter.govdata.econ;

import org.apache.calcite.adapter.file.etl.DimensionConfig;
import org.apache.calcite.adapter.file.etl.HttpSource;
import org.apache.calcite.adapter.file.etl.TableContext;
import org.apache.calcite.adapter.file.etl.TableLifecycleListener;
import org.apache.calcite.adapter.file.etl.VariableResolver;
import org.apache.calcite.adapter.file.storage.StorageProvider;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Splits already-downloaded whole-year {@code fred_indicators} raw-cache responses into 12
 * per-month raw-cache files, so historical (closed) years never need a per-month HTTP fetch —
 * only the still-open current year does.
 *
 * <p>Runs once per schema pass via {@link #beforeSource}, before the normal per-(series, year,
 * month) fetch loop starts. For every whole-year cache file it finds — whether left over from
 * before {@code fred_indicators} moved to a monthly partition layout, or freshly written by the
 * {@code fred_indicators_yearly_bulk} bulk download — it buckets that year's {@code observations}
 * by month and writes each bucket to the exact per-month cache path {@link HttpSource} will look
 * for. {@code HttpSource} itself is untouched: this only ever pre-populates cache files it would
 * otherwise have downloaded one month at a time.
 *
 * <p>The current (still-updating) year is deliberately never split — see
 * {@code fred_indicators_yearly_bulk}'s comment in the schema YAML for why seeding every month's
 * cache from one stale whole-year snapshot would silently reproduce the original staleness bug.
 *
 * <p>Idempotent: a year already fully split (all 12 monthly files present) is skipped, and
 * splitting a year never overwrites a monthly file that already exists.
 *
 * <p>Before splitting, {@link #beforeSource} also ensures every historical (closed) year has a
 * whole-year raw-cache file to split in the first place: for each {@code (series, year)} pair
 * strictly before the current calendar year that has no cached response yet, it issues ONE
 * whole-year fetch (mirroring the pre-migration year-wide request) and caches it, rather than
 * letting the normal per-month path make 12 separate requests for a year that will only ever be
 * fetched once. The still-open current year is never fetched this way — see the class javadoc.
 */
public class FredYearlyRawCacheSplitter implements TableLifecycleListener {

  private static final Logger LOGGER = LoggerFactory.getLogger(FredYearlyRawCacheSplitter.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String[] MONTHS =
      {"01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"};
  private static final String FRED_OBSERVATIONS_URL =
      "https://api.stlouisfed.org/fred/series/observations";
  /** Matches FRED's own per-request pace (see the {@code fred_indicators} table's rateLimit). */
  private static final long MIN_INTERVAL_MS = 500;

  /**
   * Matches a whole-year (pre-split) raw-cache file path: {@code .../series=<S>/.../year=<Y>/
   * response.json}, with no {@code month=} segment (that guard is applied separately since a
   * regex negative-lookahead across path segments is harder to read than a plain
   * {@code contains} check).
   */
  private static final Pattern YEARLY_CACHE_PATH =
      Pattern.compile(".*series=([^/]+)/.*year=([0-9]{4})/response\\.json$");

  @Override public void beforeTable(TableContext context) {
    // No-op: this listener only needs the beforeSource hook.
  }

  @Override public void afterTable(TableContext context,
      org.apache.calcite.adapter.file.etl.EtlResult result) {
    // No-op.
  }

  @Override public boolean onTableError(TableContext context, Exception error) {
    return true; // Continue with other tables.
  }

  @Override public void beforeSource(TableContext context) {
    if (!"fred_indicators".equals(context.getTableName())) {
      return;
    }
    try {
      ensureHistoricalYearlyFilesCached(context);
      splitAllYearlyFiles(context);
    } catch (IOException e) {
      // Non-fatal: worst case, HttpSource falls through to its normal per-month fetch for
      // whatever this pass would have downloaded/split.
      LOGGER.error("FredYearlyRawCacheSplitter: failed: {}", e.getMessage());
    }
  }

  /**
   * Downloads one whole-year response per (series, historical year) not already in the raw
   * cache, so the split step below has something to split instead of falling through to 12
   * per-month HTTP fetches for a year that will only ever be fetched once.
   */
  private void ensureHistoricalYearlyFilesCached(TableContext context) throws IOException {
    Map<String, DimensionConfig> dimensions = context.getDimensions();
    DimensionConfig seriesDim = dimensions == null ? null : dimensions.get("series");
    DimensionConfig yearDim = dimensions == null ? null : dimensions.get("year");
    List<String> seriesList = seriesDim == null ? null : seriesDim.getValues();
    Integer start = yearDim == null ? null : yearDim.getStart();
    if (seriesList == null || seriesList.isEmpty() || start == null) {
      LOGGER.warn("FredYearlyRawCacheSplitter: table config missing series/year dimensions, "
          + "skipping historical bulk-download step");
      return;
    }

    String apiKey = VariableResolver.substitute("{env:FRED_API_KEY}",
        Collections.<String, String>emptyMap());
    if (apiKey == null || apiKey.isEmpty() || apiKey.startsWith("{env:")) {
      LOGGER.warn("FredYearlyRawCacheSplitter: FRED_API_KEY not resolved, skipping "
          + "historical bulk-download step (per-month fetch will still work)");
      return;
    }

    StorageProvider storage = context.getSourceStorageProvider();
    String rawCacheRoot = context.getTableName();
    int currentYear = Calendar.getInstance().get(Calendar.YEAR);
    int downloaded = 0;
    long nextAllowedAtMs = 0;

    for (String series : seriesList) {
      for (int year = start; year < currentYear; year++) {
        String yearlyPath = yearlyCachePath(rawCacheRoot, series, year);
        if (storage.exists(yearlyPath)) {
          continue; // already in raw cache — nothing to download
        }
        long waitMs = nextAllowedAtMs - System.currentTimeMillis();
        if (waitMs > 0) {
          sleepUninterruptibly(waitMs);
        }
        nextAllowedAtMs = System.currentTimeMillis() + MIN_INTERVAL_MS;
        downloadWholeYear(storage, apiKey, series, year, yearlyPath);
        downloaded++;
      }
    }

    if (downloaded > 0) {
      LOGGER.info("FredYearlyRawCacheSplitter: downloaded {} whole-year response(s) for "
          + "previously-missing historical (series, year) combinations", downloaded);
    }
  }

  private void downloadWholeYear(StorageProvider storage, String apiKey, String series,
      int year, String cachePath) throws IOException {
    String url = FRED_OBSERVATIONS_URL
        + "?api_key=" + apiKey
        + "&file_type=json"
        + "&series_id=" + series
        + "&observation_start=" + year + "-01-01"
        + "&observation_end=" + year + "-12-31"
        + "&limit=100000";

    final int maxAttempts = 4;
    IOException last = null;
    for (int attempt = 1; attempt <= maxAttempts; attempt++) {
      try {
        fetchOnce(url, cachePath, storage);
        return;
      } catch (IOException e) {
        last = e;
        if (attempt < maxAttempts) {
          long backoffMs = 2000L * attempt;
          LOGGER.warn("FredYearlyRawCacheSplitter: attempt {}/{} failed for {}/{} ({}); "
              + "retrying in {}ms", attempt, maxAttempts, series, year, e.getMessage(), backoffMs);
          sleepUninterruptibly(backoffMs);
        }
      }
    }
    throw new IOException("FRED whole-year download failed after " + maxAttempts
        + " attempts for " + series + "/" + year, last);
  }

  private void fetchOnce(String url, String cachePath, StorageProvider storage)
      throws IOException {
    URL urlObj = URI.create(url).toURL();
    HttpURLConnection conn = (HttpURLConnection) urlObj.openConnection();
    conn.setRequestMethod("GET");
    conn.setConnectTimeout(30000);
    conn.setReadTimeout(120000);
    int responseCode = conn.getResponseCode();
    if (responseCode != 200) {
      throw new IOException("HTTP " + responseCode + " for " + url);
    }
    try (InputStream in = conn.getInputStream()) {
      storage.writeFile(cachePath, in);
    }
  }

  private static void sleepUninterruptibly(long ms) {
    try {
      Thread.sleep(ms);
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
    }
  }

  /** The exact whole-year raw-cache path {@link HttpSource} used pre-migration (no month). */
  private String yearlyCachePath(String rawCacheRoot, String series, int year) {
    Map<String, String> variables = new HashMap<>();
    variables.put("type", "fred_indicators");
    variables.put("series", series);
    variables.put("year", String.valueOf(year));
    variables.put("effective_year", String.valueOf(year));
    return HttpSource.buildRawCachePath(rawCacheRoot, variables, null, 0, false);
  }

  private void splitAllYearlyFiles(TableContext context) throws IOException {
    StorageProvider storage = context.getSourceStorageProvider();
    // Matches HttpSource's own rawCachePath base for this table (EtlPipeline.createDataSource
    // uses the table name as the raw-cache root when no rawCache.sharedKey is configured).
    String rawCacheRoot = context.getTableName();
    if (!storage.exists(rawCacheRoot)) {
      return;
    }

    int currentYear = Calendar.getInstance().get(Calendar.YEAR);
    int split = 0;
    int alreadySplit = 0;
    int currentYearSkipped = 0;

    for (StorageProvider.FileEntry entry : storage.listFiles(rawCacheRoot, true)) {
      if (entry.isDirectory()) {
        continue;
      }
      String path = entry.getPath();
      if (path.contains("month=")) {
        continue; // already a per-month file, not a whole-year source
      }
      Matcher m = YEARLY_CACHE_PATH.matcher(path);
      if (!m.matches()) {
        continue;
      }
      String series = m.group(1);
      int year = Integer.parseInt(m.group(2));
      if (year >= currentYear) {
        currentYearSkipped++;
        continue; // never seed monthly caches from the still-open current year
      }
      if (splitOneYear(storage, rawCacheRoot, series, year, path)) {
        split++;
      } else {
        alreadySplit++;
      }
    }

    if (split > 0) {
      LOGGER.info("FredYearlyRawCacheSplitter: split {} yearly cache file(s) into monthly files "
          + "({} already split, {} current-year file(s) left unused)",
          split, alreadySplit, currentYearSkipped);
    }
  }

  /**
   * Splits one (series, year) whole-year cache file into 12 monthly cache files.
   *
   * @return true if this call actually wrote new monthly files, false if every month was
   *         already present (nothing to do)
   */
  private boolean splitOneYear(StorageProvider storage, String rawCacheRoot, String series,
      int year, String yearlyPath) throws IOException {
    Map<String, String> monthPaths = new HashMap<>();
    boolean allPresent = true;
    for (String month : MONTHS) {
      String path = monthlyCachePath(rawCacheRoot, series, year, month);
      monthPaths.put(month, path);
      if (!storage.exists(path)) {
        allPresent = false;
      }
    }
    if (allPresent) {
      return false;
    }

    JsonNode root = readJson(storage, yearlyPath);
    JsonNode observations = root.path("observations");
    if (!observations.isArray()) {
      LOGGER.warn("FredYearlyRawCacheSplitter: {} has no observations array, skipping",
          yearlyPath);
      return false;
    }

    Map<String, ArrayNode> byMonth = new HashMap<>();
    for (String month : MONTHS) {
      byMonth.put(month, MAPPER.createArrayNode());
    }
    for (JsonNode obs : observations) {
      String date = obs.path("date").asText(null);
      if (date == null || date.length() < 7) {
        continue;
      }
      ArrayNode bucket = byMonth.get(date.substring(5, 7));
      if (bucket != null) {
        bucket.add(obs);
      }
    }

    for (String month : MONTHS) {
      String path = monthPaths.get(month);
      if (storage.exists(path)) {
        continue; // this month already split from an earlier pass
      }
      ObjectNode envelope = MAPPER.createObjectNode();
      envelope.set("observations", byMonth.get(month));
      storage.writeFile(path, MAPPER.writeValueAsBytes(envelope));
    }
    LOGGER.debug("FredYearlyRawCacheSplitter: split {}/{} ({} observations) into monthly files",
        series, year, observations.size());
    return true;
  }

  /** The exact per-month raw-cache path {@link HttpSource} will look for this combination. */
  private String monthlyCachePath(String rawCacheRoot, String series, int year, String month) {
    Map<String, String> variables = new HashMap<>();
    variables.put("type", "fred_indicators");
    variables.put("series", series);
    variables.put("year", String.valueOf(year));
    variables.put("effective_year", String.valueOf(year));
    variables.put("month", month);
    return HttpSource.buildRawCachePath(rawCacheRoot, variables, null, 0, false);
  }

  private JsonNode readJson(StorageProvider storage, String path) throws IOException {
    try (InputStream in = storage.openInputStream(path)) {
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      byte[] buffer = new byte[8192];
      int n;
      while ((n = in.read(buffer)) != -1) {
        out.write(buffer, 0, n);
      }
      return MAPPER.readTree(new String(out.toByteArray(), StandardCharsets.UTF_8));
    }
  }
}
