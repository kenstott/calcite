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
package org.apache.calcite.adapter.govdata.health;

import org.apache.calcite.adapter.file.etl.CachingDataProvider;
import org.apache.calcite.adapter.file.etl.CsvRecordReader;
import org.apache.calcite.adapter.file.etl.EtlPipelineConfig;
import org.apache.calcite.adapter.file.etl.RawCache;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * DataProvider for {@code health.cms_pbj_nurse_staffing} — CMS Payroll-Based Journal (PBJ)
 * "Daily Nurse Staffing", one row per facility (PROVNUM) per calendar day.
 *
 * <p>Each quarterly release is its own independently-hosted CSV distribution in CMS's
 * {@code data.json} catalog under the dataset titled "Payroll Based Journal Daily Nurse
 * Staffing" — the download path includes an opaque per-quarter UUID with no derivable pattern
 * (confirmed live 2026-09-27: e.g. quarter CY2025Q4 is hosted at
 * {@code .../2026-04/8f85c7d4-a1f6-4b36-ad20-17abc8aa57d2/PBJ_dailynursestaffing_CY2025Q4.csv}),
 * so this table is built by catalog discovery — reading {@code distribution[].downloadURL} and
 * {@code distribution[].temporal} off the dataset entry — rather than a templated URL. Releases
 * are streamed newest-quarter-first so a {@code dqRowLimit}-capped DQ sample exercises the
 * current CSV shape rather than the oldest one.
 *
 * <p>Column names drift across the file's ~9-year history even though the row shape does not:
 * 2017Q1-Q4 name the RN-director-of-nursing and LPN-admin total columns
 * {@code hrs_rn_donadmin}/{@code hrs_lpn_admin}; every release from 2018Q1 on uses
 * {@code hrs_rndon}/{@code hrs_lpnadmin} — both aliased here to the same output column. Header
 * lookup is by name (case-insensitive, alias-normalized), never position, because at least one
 * release has a genuinely malformed header: the 2017Q2 file (verified live) is missing
 * {@code hrs_medaide_ctr} entirely, with a stray duplicate {@code hrs_rn_donadmin} token in its
 * place. On an alias collision the first occurrence wins and the rest is dropped — never
 * overwritten — so a mislabeled duplicate can only produce a null column for that one release,
 * never silently corrupt an unrelated column's values.
 */
public class CmsPbjNurseStaffingProvider implements CachingDataProvider {

  private static final Logger LOGGER = LoggerFactory.getLogger(CmsPbjNurseStaffingProvider.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String DEFAULT_UA = "Apache-Calcite-GovData/1.0";
  private static final String DATASET_TITLE = "Payroll Based Journal Daily Nurse Staffing";
  private static final DateTimeFormatter WORKDATE_FMT = DateTimeFormatter.BASIC_ISO_DATE;

  /** Raw-header (lowercase) -> canonical key, for names that changed across vintages. */
  private static final Map<String, String> HEADER_ALIASES = new HashMap<>();
  static {
    HEADER_ALIASES.put("hrs_rn_donadmin", "hrs_rndon");
    HEADER_ALIASES.put("hrs_lpn_admin", "hrs_lpnadmin");
    HEADER_ALIASES.put("hrs_na_trn", "hrs_natrn");
  }

  private static final String[] REQUIRED_HEADERS = {"provnum", "workdate", "mdscensus"};

  @Override public Iterator<Map<String, Object>> fetch(EtlPipelineConfig config,
      Map<String, String> variables, RawCache rawCache) throws IOException {
    String catalogUrl = config.getSource() != null ? config.getSource().getUrl() : null;
    if (catalogUrl == null || catalogUrl.isEmpty()) {
      throw new IOException("CMS PBJ nurse staffing: source.url (data.json catalog) is required");
    }
    String userAgent = headerOrDefault(config, "User-Agent", DEFAULT_UA);

    List<QuarterFile> quarters = resolveQuarters(catalogUrl, userAgent);
    if (quarters.isEmpty()) {
      throw new IOException("CMS PBJ nurse staffing: no quarterly CSV distributions resolved "
          + "from " + catalogUrl + " for dataset \"" + DATASET_TITLE + "\"");
    }
    LOGGER.info("CMS PBJ nurse staffing: resolved {} quarterly distribution(s)", quarters.size());

    return new MultiQuarterRowIterator(quarters, userAgent, rawCache);
  }

  // ---------------------------------------------------------------------
  // Discovery: data.json catalog -> matching dataset -> per-quarter CSVs
  // ---------------------------------------------------------------------

  private static final class QuarterFile {
    final int year;
    final int quarter;
    final String csvUrl;

    QuarterFile(int year, int quarter, String csvUrl) {
      this.year = year;
      this.quarter = quarter;
      this.csvUrl = csvUrl;
    }
  }

  private List<QuarterFile> resolveQuarters(String catalogUrl, String userAgent)
      throws IOException {
    JsonNode datasets = MAPPER.readTree(getText(catalogUrl, userAgent)).path("dataset");
    List<QuarterFile> quarters = new ArrayList<>();
    if (!datasets.isArray()) {
      return quarters;
    }
    for (JsonNode dataset : datasets) {
      if (!DATASET_TITLE.equals(dataset.path("title").asText(""))) {
        continue;
      }
      for (JsonNode dist : dataset.path("distribution")) {
        String downloadUrl = dist.path("downloadURL").asText(null);
        String temporal = dist.path("temporal").asText(null);
        if (downloadUrl == null || temporal == null || !temporal.contains("/")) {
          continue;
        }
        String startDate = temporal.substring(0, temporal.indexOf('/'));
        LocalDate start;
        try {
          start = LocalDate.parse(startDate);
        } catch (RuntimeException e) {
          LOGGER.info("CMS PBJ nurse staffing: unparseable temporal '{}' on distribution {} "
              + "— skipping", temporal, downloadUrl);
          continue;
        }
        int quarter = ((start.getMonthValue() - 1) / 3) + 1;
        quarters.add(new QuarterFile(start.getYear(), quarter, downloadUrl));
      }
      break;
    }
    // Newest first: a dqRowLimit-capped DQ sample then exercises the current CSV shape,
    // not the earliest (aliased-header) vintage.
    quarters.sort(Comparator.<QuarterFile>comparingInt(q -> q.year)
        .thenComparingInt(q -> q.quarter).reversed());
    return quarters;
  }

  // ---------------------------------------------------------------------
  // Row streaming: one quarter's CSV at a time, one row at a time
  // ---------------------------------------------------------------------

  private static final class MultiQuarterRowIterator implements Iterator<Map<String, Object>> {
    private final List<QuarterFile> quarters;
    private final String userAgent;
    private final RawCache rawCache;
    private int quarterIndex;
    private BufferedReader reader;
    private Map<String, Integer> headerIndex;
    private Map<String, Object> nextRow;
    private boolean done;

    MultiQuarterRowIterator(List<QuarterFile> quarters, String userAgent, RawCache rawCache) {
      this.quarters = quarters;
      this.userAgent = userAgent;
      this.rawCache = rawCache;
    }

    private void advance() throws IOException {
      while (nextRow == null && !done) {
        if (reader == null) {
          if (!openNextQuarter()) {
            done = true;
            return;
          }
        }
        String record = CsvRecordReader.readRecord(reader);
        if (record == null) {
          reader.close();
          reader = null;
          continue;
        }
        if (record.trim().isEmpty()) {
          continue;
        }
        nextRow = toRow(CsvRecordReader.splitFields(record, ','), headerIndex);
      }
    }

    private boolean openNextQuarter() throws IOException {
      if (quarterIndex >= quarters.size()) {
        return false;
      }
      QuarterFile quarter = quarters.get(quarterIndex++);
      LOGGER.info("CMS PBJ nurse staffing {}Q{}: streaming {}", quarter.year, quarter.quarter,
          quarter.csvUrl);
      final String csvUrl = quarter.csvUrl;
      final String ua = userAgent;
      reader = new BufferedReader(new InputStreamReader(
          rawCache.openStream(csvUrl, () -> openWithUserAgent(csvUrl, ua)),
          StandardCharsets.UTF_8));
      String header = CsvRecordReader.readRecord(reader);
      if (header == null) {
        throw new IOException("CMS PBJ nurse staffing " + quarter.year + "Q" + quarter.quarter
            + ": empty file " + csvUrl);
      }
      if (header.startsWith("﻿")) {
        header = header.substring(1);
      }
      headerIndex = headerIndex(CsvRecordReader.splitFields(header, ','), csvUrl);
      return true;
    }

    @Override public boolean hasNext() {
      try {
        advance();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      return nextRow != null;
    }

    @Override public Map<String, Object> next() {
      try {
        advance();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      if (nextRow == null) {
        throw new NoSuchElementException();
      }
      Map<String, Object> row = nextRow;
      nextRow = null;
      return row;
    }
  }

  private static Map<String, Integer> headerIndex(List<String> cols, String url)
      throws IOException {
    Map<String, Integer> pos = new LinkedHashMap<>();
    for (int i = 0; i < cols.size(); i++) {
      String raw = cols.get(i).trim().toLowerCase(Locale.ROOT);
      String canonical = HEADER_ALIASES.getOrDefault(raw, raw);
      if (pos.containsKey(canonical)) {
        // A genuinely malformed header (e.g. 2017Q2's duplicate hrs_rn_donadmin token in place
        // of the missing hrs_medaide_ctr column) — keep the first, real occurrence and drop the
        // rest rather than silently reassigning an already-resolved column to the wrong data.
        LOGGER.warn("CMS PBJ nurse staffing: duplicate header '{}' (raw '{}') in {} — dropping "
            + "the extra column, keeping the first occurrence", canonical, raw, url);
        continue;
      }
      pos.put(canonical, i);
    }
    for (String required : REQUIRED_HEADERS) {
      if (!pos.containsKey(required)) {
        throw new IOException("CMS PBJ nurse staffing: expected column '" + required
            + "' not found in " + url + " — header=" + cols);
      }
    }
    return pos;
  }

  private static Map<String, Object> toRow(List<String> fields, Map<String, Integer> headerIndex) {
    String workDateText = field(fields, headerIndex, "workdate");
    LocalDate workDate = workDateText == null ? null
        : LocalDate.parse(padLeftZero(workDateText, 8), WORKDATE_FMT);

    Map<String, Object> row = new LinkedHashMap<>();
    row.put("ccn", field(fields, headerIndex, "provnum"));
    row.put("provider_name", field(fields, headerIndex, "provname"));
    row.put("city", field(fields, headerIndex, "city"));
    row.put("state", field(fields, headerIndex, "state"));
    row.put("county_name", field(fields, headerIndex, "county_name"));
    row.put("county_fips", field(fields, headerIndex, "county_fips"));
    row.put("report_quarter", field(fields, headerIndex, "cy_qtr"));
    row.put("work_date", workDate == null ? null : workDate.toString());
    row.put("resident_census", parseInt(field(fields, headerIndex, "mdscensus")));

    row.put("rn_don_hours", parseDouble(field(fields, headerIndex, "hrs_rndon")));
    row.put("rn_don_hours_employee", parseDouble(field(fields, headerIndex, "hrs_rndon_emp")));
    row.put("rn_don_hours_contract", parseDouble(field(fields, headerIndex, "hrs_rndon_ctr")));

    row.put("rn_admin_hours", parseDouble(field(fields, headerIndex, "hrs_rnadmin")));
    row.put("rn_admin_hours_employee", parseDouble(field(fields, headerIndex, "hrs_rnadmin_emp")));
    row.put("rn_admin_hours_contract", parseDouble(field(fields, headerIndex, "hrs_rnadmin_ctr")));

    row.put("rn_hours", parseDouble(field(fields, headerIndex, "hrs_rn")));
    row.put("rn_hours_employee", parseDouble(field(fields, headerIndex, "hrs_rn_emp")));
    row.put("rn_hours_contract", parseDouble(field(fields, headerIndex, "hrs_rn_ctr")));

    row.put("lpn_admin_hours", parseDouble(field(fields, headerIndex, "hrs_lpnadmin")));
    row.put("lpn_admin_hours_employee",
        parseDouble(field(fields, headerIndex, "hrs_lpnadmin_emp")));
    row.put("lpn_admin_hours_contract",
        parseDouble(field(fields, headerIndex, "hrs_lpnadmin_ctr")));

    row.put("lpn_hours", parseDouble(field(fields, headerIndex, "hrs_lpn")));
    row.put("lpn_hours_employee", parseDouble(field(fields, headerIndex, "hrs_lpn_emp")));
    row.put("lpn_hours_contract", parseDouble(field(fields, headerIndex, "hrs_lpn_ctr")));

    row.put("cna_hours", parseDouble(field(fields, headerIndex, "hrs_cna")));
    row.put("cna_hours_employee", parseDouble(field(fields, headerIndex, "hrs_cna_emp")));
    row.put("cna_hours_contract", parseDouble(field(fields, headerIndex, "hrs_cna_ctr")));

    row.put("nurse_aide_trainee_hours", parseDouble(field(fields, headerIndex, "hrs_natrn")));
    row.put("nurse_aide_trainee_hours_employee",
        parseDouble(field(fields, headerIndex, "hrs_natrn_emp")));
    row.put("nurse_aide_trainee_hours_contract",
        parseDouble(field(fields, headerIndex, "hrs_natrn_ctr")));

    row.put("medication_aide_hours", parseDouble(field(fields, headerIndex, "hrs_medaide")));
    row.put("medication_aide_hours_employee",
        parseDouble(field(fields, headerIndex, "hrs_medaide_emp")));
    row.put("medication_aide_hours_contract",
        parseDouble(field(fields, headerIndex, "hrs_medaide_ctr")));

    row.put("type", "cms_pbj_nurse_staffing");
    row.put("year", workDate == null ? null : Integer.valueOf(workDate.getYear()));
    return row;
  }

  private static String field(List<String> fields, Map<String, Integer> headerIndex,
      String name) {
    Integer at = headerIndex.get(name);
    if (at == null || at.intValue() >= fields.size()) {
      return null;
    }
    String raw = fields.get(at.intValue());
    if (raw == null) {
      return null;
    }
    String trimmed = raw.trim();
    return trimmed.isEmpty() ? null : trimmed;
  }

  private static String padLeftZero(String s, int width) {
    StringBuilder sb = new StringBuilder();
    for (int i = s.length(); i < width; i++) {
      sb.append('0');
    }
    return sb.append(s).toString();
  }

  private static Integer parseInt(String text) {
    if (text == null) {
      return null;
    }
    try {
      return Integer.valueOf((int) Double.parseDouble(text.replace(",", "").trim()));
    } catch (NumberFormatException e) {
      return null;
    }
  }

  private static Double parseDouble(String text) {
    if (text == null) {
      return null;
    }
    try {
      return Double.valueOf(Double.parseDouble(text.replace(",", "").trim()));
    } catch (NumberFormatException e) {
      return null;
    }
  }

  // ---------------------------------------------------------------------
  // HTTP helpers
  // ---------------------------------------------------------------------

  private static InputStream openWithUserAgent(String url, String userAgent) throws IOException {
    HttpURLConnection conn = (HttpURLConnection) URI.create(url).toURL().openConnection();
    conn.setRequestProperty("User-Agent", userAgent);
    conn.setConnectTimeout(30000);
    conn.setReadTimeout(300000);
    conn.setInstanceFollowRedirects(true);
    int code = conn.getResponseCode();
    if (code < 200 || code >= 300) {
      throw new IOException("CMS PBJ nurse staffing: HTTP " + code + " for " + url);
    }
    return conn.getInputStream();
  }

  private static String headerOrDefault(EtlPipelineConfig config, String name, String dflt) {
    Map<String, String> headers = config.getSource() != null
        ? config.getSource().getHeaders() : null;
    if (headers != null) {
      String v = headers.get(name);
      if (v != null && !v.isEmpty()) {
        return v;
      }
    }
    return dflt;
  }

  private static String getText(String url, String userAgent) throws IOException {
    HttpURLConnection conn = (HttpURLConnection) URI.create(url).toURL().openConnection();
    conn.setRequestProperty("User-Agent", userAgent);
    conn.setConnectTimeout(30000);
    conn.setReadTimeout(60000);
    conn.setInstanceFollowRedirects(true);
    int code = conn.getResponseCode();
    if (code < 200 || code >= 300) {
      throw new IOException("CMS PBJ nurse staffing: HTTP " + code + " for " + url);
    }
    ByteArrayOutputStream buf = new ByteArrayOutputStream();
    try (InputStream in = conn.getInputStream()) {
      byte[] chunk = new byte[65536];
      int n;
      while ((n = in.read(chunk)) != -1) {
        buf.write(chunk, 0, n);
      }
    }
    return new String(buf.toByteArray(), StandardCharsets.UTF_8);
  }
}
