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
package org.apache.calcite.adapter.govdata.fiscal;

import org.apache.calcite.adapter.file.etl.CachingDataProvider;
import org.apache.calcite.adapter.file.etl.EtlPipelineConfig;
import org.apache.calcite.adapter.file.etl.RawCache;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * DataProvider for {@code govt_finance_by_unit} — Census's annual "Individual Unit File" from the
 * State &amp; Local Government Finance survey, one amount per finance item code per government
 * unit per year.
 *
 * <p>Three steps, all here because the download filename is neither year-templatable nor stable
 * across years (confirmed live: {@code 2023_Individual_Unit_Files.zip},
 * {@code 2021_Individual_Unit_File.zip}, {@code 2013_Individual_Unit_file.zip},
 * {@code 2014-individual-unit-file.zip} — singular/plural, casing and underscore-vs-hyphen all
 * vary by year, and the year-templated
 * {@code https://www2.census.gov/programs-surveys/gov-finances/tables/{year}/{year}_Individual_Unit_Files.zip}
 * guess 404s for most years): (1) read the per-year "Public Use Datasets" landing page and scan
 * every anchor href for {@link #ZIP_LINK_PATTERN}; (2) open that ZIP; (3) advance to the
 * {@code *FinEstDAT*.txt} entry and stream its fixed-width records.
 *
 * <p>Both the landing page and the ZIP are read through {@link RawCache}, so a re-run does not
 * re-download either. The records stream straight from the open ZIP entry: the data file reaches
 * 62 MB uncompressed (2012) and 1.7 million records, so holding the parsed result in memory is
 * what this provider exists to avoid.
 *
 * <p>Record layout per the technical documentation: positions 1-12 = ID (1-2 state FIPS, 3 =
 * government type, 4-6 county/county-type FIPS, 7-12 unit identifier), then item code (3), amount
 * in thousands of dollars (12), data year (4) and imputation flag (1). The ID field comes in two
 * widths, so records are 32 or 34 characters: survey years 2017 and later use a 12-character ID;
 * 2012-2016 use 14 — the same 12-character ID followed by a literal {@code "00"} pad — which
 * shifts every later field right by two. Confirmed live against the 2012, 2013 and 2016 files (all
 * records 34 characters, positions 13-14 always {@code "00"}) and the 2017 and 2019 files (all
 * records 32).
 */
public class GovtFinanceProvider implements CachingDataProvider {

  private static final Logger LOGGER = LoggerFactory.getLogger(GovtFinanceProvider.class);

  /**
   * Matches an anchor href naming the Individual Unit File, independent of case and of
   * underscore/hyphen choice — confirmed live to vary by year: {@code Individual_Unit_File}
   * (2012, 2017+), {@code Individual_Unit_file} (2013, 2016), {@code individual-unit-file}
   * (2014, 2015).
   */
  private static final Pattern ZIP_LINK_PATTERN =
      Pattern.compile("individual[-_]unit[-_]fil", Pattern.CASE_INSENSITIVE);

  /**
   * Names the fixed-width data entry inside the ZIP. Every year's ZIP also carries a
   * {@code NNstatetypepu.txt} summary and a {@code Fin_GID_*}/{@code Fin_PID_*} identifier file;
   * only the estimates file carries per-unit amounts. Confirmed live for 2012, 2013, 2016, 2017,
   * 2019 and 2023.
   */
  private static final String DATA_ENTRY_MARKER = "finestdat";

  private static final Map<String, String> GOV_TYPE_NAMES = buildGovTypeNameMap();

  @Override public Iterator<Map<String, Object>> fetch(EtlPipelineConfig config,
      Map<String, String> variables, RawCache rawCache) throws IOException {
    String year = variables.get("effective_year");
    if (year == null || year.isEmpty()) {
      year = variables.get("year");
    }
    if (year == null || year.isEmpty()) {
      LOGGER.warn("govt_finance_by_unit: no year in dimension variables {}", variables);
      return Collections.emptyIterator();
    }
    final String yearStr = year.trim();

    final String landingUrl = "https://www.census.gov/data/datasets/" + yearStr
        + "/econ/local/public-use-datasets.html";
    String landingHtml = readFully(rawCache.openStream(landingUrl));
    String zipUrl = findZipUrl(landingHtml);
    if (zipUrl == null) {
      throw new IOException("govt_finance_by_unit: no Individual Unit File link found for year="
          + yearStr + " on " + landingUrl);
    }
    LOGGER.info("govt_finance_by_unit: reading {}", zipUrl);

    // Left open deliberately: the iterator streams records straight off this entry and closes the
    // stream when the entry is exhausted.
    final ZipInputStream zis = new ZipInputStream(rawCache.openStream(zipUrl));
    if (!advanceToDataEntry(zis, zipUrl, yearStr)) {
      zis.close();
      throw new IOException("govt_finance_by_unit: no *" + DATA_ENTRY_MARKER + "*.txt entry in "
          + zipUrl + " for year=" + yearStr);
    }
    final BufferedReader reader =
        new BufferedReader(new InputStreamReader(zis, StandardCharsets.US_ASCII));

    return new Iterator<Map<String, Object>>() {
      private Map<String, Object> nextRow;
      private boolean done;
      private int skipped;

      private void advance() {
        if (nextRow != null || done) {
          return;
        }
        try {
          String line;
          while ((line = reader.readLine()) != null) {
            Map<String, Object> row = toRow(line, yearStr);
            if (row == null) {
              skipped++;
              continue;
            }
            nextRow = row;
            return;
          }
          done = true;
          reader.close();
          if (skipped > 0) {
            LOGGER.warn("govt_finance_by_unit: skipped {} records for year={} whose length is "
                + "neither 32 (2017+ layout) nor 34 (2012-2016 layout)", skipped, yearStr);
          }
        } catch (IOException e) {
          throw new RuntimeException("govt_finance_by_unit: streaming failed for year="
              + yearStr, e);
        }
      }

      @Override public boolean hasNext() {
        advance();
        return nextRow != null;
      }

      @Override public Map<String, Object> next() {
        advance();
        if (nextRow == null) {
          throw new NoSuchElementException();
        }
        Map<String, Object> row = nextRow;
        nextRow = null;
        return row;
      }
    };
  }

  /** Returns null for a record whose length matches neither layout. */
  private static Map<String, Object> toRow(String line, String yearStr) {
    // Deciding the ID width from the record length rather than the year makes a future layout
    // change a loud skip rather than a silent two-character shift through item code, amount, year
    // and flag.
    final int idWidth;
    if (line.length() == 32) {
      idWidth = 12;
    } else if (line.length() == 34) {
      idWidth = 14;
    } else {
      return null;
    }

    String itemCode = line.substring(idWidth, idWidth + 3).trim();
    String amountRaw = line.substring(idWidth + 3, idWidth + 15).trim();
    String dataYearRaw = line.substring(idWidth + 15, idWidth + 19).trim();
    String flag = line.substring(idWidth + 19, idWidth + 20).trim();

    Integer dataYear = dataYearRaw.isEmpty() ? parseIntOrNull(yearStr) : parseIntOrNull(dataYearRaw);
    String govTypeCode = line.substring(2, 3);

    Map<String, Object> row = new LinkedHashMap<String, Object>();
    row.put("year", dataYear != null ? dataYear : parseIntOrNull(yearStr));
    row.put("state_fips", line.substring(0, 2));
    row.put("gov_type_code", govTypeCode);
    row.put("gov_type_name", GOV_TYPE_NAMES.get(govTypeCode));
    row.put("county_fips", line.substring(3, 6));
    row.put("unit_id", line.substring(6, 12));
    row.put("item_code", itemCode);
    row.put("amount_thousands", amountRaw.isEmpty() ? null : parseLongOrNull(amountRaw));
    row.put("imputation_flag", flag);
    return row;
  }

  /** Positions {@code zis} at the fixed-width data entry, or returns false if the ZIP has none. */
  private static boolean advanceToDataEntry(ZipInputStream zis, String zipUrl, String yearStr)
      throws IOException {
    ZipEntry entry;
    while ((entry = zis.getNextEntry()) != null) {
      String name = entry.getName().toLowerCase(java.util.Locale.ROOT);
      if (name.endsWith(".txt") && name.contains(DATA_ENTRY_MARKER)) {
        LOGGER.debug("govt_finance_by_unit: streaming entry {} for year={}", entry.getName(),
            yearStr);
        return true;
      }
    }
    return false;
  }

  private static String findZipUrl(String landingHtml) {
    Document doc = Jsoup.parse(landingHtml);
    for (Element link : doc.select("a[href]")) {
      String href = link.attr("href");
      if (ZIP_LINK_PATTERN.matcher(href).find()) {
        return resolveHref(href);
      }
    }
    return null;
  }

  /**
   * Resolves an anchor href to an absolute URL. Confirmed live to appear in three shapes across
   * years: absolute, protocol-relative, and site-root-relative.
   */
  private static String resolveHref(String href) {
    if (href.startsWith("http")) {
      return href;
    }
    if (href.startsWith("//")) {
      return "https:" + href;
    }
    return "https://www.census.gov" + href;
  }

  private static String readFully(InputStream in) throws IOException {
    StringBuilder sb = new StringBuilder();
    try (BufferedReader r =
             new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
      char[] buf = new char[8192];
      int len;
      while ((len = r.read(buf)) > 0) {
        sb.append(buf, 0, len);
      }
    }
    return sb.toString();
  }

  private static Long parseLongOrNull(String s) {
    try {
      return Long.parseLong(s);
    // fallback-guard: allow a non-numeric amount to become NULL rather than fail the record; the
    // Census file leaves the amount blank or dashed for a non-reporting unit.
    } catch (NumberFormatException e) {
      return null;
    }
  }

  private static Integer parseIntOrNull(String s) {
    if (s == null) {
      return null;
    }
    try {
      return Integer.parseInt(s.trim());
    // fallback-guard: allow an unparseable data-year to fall back to the batch's year, which the
    // caller supplies for exactly this case.
    } catch (NumberFormatException e) {
      return null;
    }
  }

  private static Map<String, String> buildGovTypeNameMap() {
    Map<String, String> m = new HashMap<>();
    m.put("0", "State");
    m.put("1", "County");
    m.put("2", "City");
    m.put("3", "Township");
    m.put("4", "Special District");
    m.put("5", "Independent School District");
    return m;
  }
}
