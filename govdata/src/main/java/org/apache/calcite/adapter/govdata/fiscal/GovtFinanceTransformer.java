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

import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.ResponseTransformer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * Discovers and parses Census's annual "Individual Unit File" from the State &amp; Local
 * Government Finance survey — one amount per finance item code per government unit per year.
 *
 * <p>Three-step process, all driven by this single transformer since the download filename is
 * neither year-templatable nor stable across years (confirmed live: {@code
 * 2023_Individual_Unit_Files.zip}, {@code 2021_Individual_Unit_File.zip}, {@code
 * 2013_Individual_Unit_file.zip}, {@code 2014-individual-unit-file.zip} — singular/plural,
 * casing, and underscore-vs-hyphen all vary by year, and the year-templated
 * {@code https://www2.census.gov/programs-surveys/gov-finances/tables/{year}/{year}_Individual_Unit_Files.zip}
 * guess 404s for most years): (1) the framework fetches Census's per-year "Public Use Datasets"
 * landing page ({@code https://www.census.gov/data/datasets/{year}/econ/local/public-use-datasets.html})
 * and hands its HTML to {@link #transform}; this method scans every anchor href for
 * {@link #ZIP_LINK_PATTERN} (matches {@code individual[-_]unit[-_]fil}, case-insensitive — covers
 * every filename variant seen above, unlike a plain case-sensitive substring check, which missed
 * 2013-2016); (2) downloads that ZIP; (3) inside the ZIP, picks the largest {@code .txt} entry
 * that is not the {@code Fin_PID_*} identifier file (the ZIP always contains exactly one 32-char
 * fixed-width data file, one PID lookup file, and technical-documentation PDFs — the data file is
 * unambiguously the largest .txt), and parses its fixed-width records per the technical
 * documentation's record layout: positions 1-12 = ID (1-2 state FIPS, 3 = government type, 4-6
 * county/county-type FIPS, 7-12 unit identifier), 13-15 = item code, 16-27 = amount in thousands
 * of dollars, 28-31 = data year, 32 = imputation flag.
 *
 * <p>Verified live against the real 2023 file (downloaded 2026-08-02,
 * {@code 2023FinEstDAT_06052025modp_pu.txt}, 509,080 records): record layout, item codes E12/F12
 * (elementary-secondary education current/construction), E36/F36 (hospitals current/construction)
 * and E61/F61 (parks &amp; recreation current/construction) all confirmed present with the
 * technical documentation's stated descriptions. Landing-page discovery confirmed live for 2012,
 * 2013, 2014, 2015, 2016, 2017, 2020, 2021, 2022, 2023, and 2024 (2013-2016 confirmed only after
 * the case-insensitive/hyphen-agnostic pattern replaced the original case-sensitive selector).
 */
public class GovtFinanceTransformer implements ResponseTransformer {

  private static final Logger LOGGER = LoggerFactory.getLogger(GovtFinanceTransformer.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  /**
   * Matches an anchor href naming the Individual Unit File, independent of case and of
   * underscore/hyphen choice — confirmed live to vary by year: {@code Individual_Unit_File}
   * (2012, 2017+), {@code Individual_Unit_file} (2013, 2016), {@code individual-unit-file}
   * (2014, 2015).
   */
  private static final Pattern ZIP_LINK_PATTERN =
      Pattern.compile("individual[-_]unit[-_]fil", Pattern.CASE_INSENSITIVE);

  private static final Map<String, String> GOV_TYPE_NAMES = buildGovTypeNameMap();

  @Override public String transform(String response, RequestContext context) {
    String yearStr = context.getDimensionValues().get("year");
    if (response == null || response.isEmpty()) {
      LOGGER.warn("Govt Finance: Empty landing page response for year={}", yearStr);
      return "[]";
    }

    try {
      String zipUrl = findZipUrl(response);
      if (zipUrl == null) {
        LOGGER.warn("Govt Finance: no Individual Unit File link found for year={} on {}",
            yearStr, context.getUrl());
        return "[]";
      }

      byte[] zipBytes = downloadBytes(zipUrl);
      byte[] dataFileBytes = extractDataFile(zipBytes);
      if (dataFileBytes == null) {
        LOGGER.warn("Govt Finance: no data .txt entry found in {} for year={}", zipUrl, yearStr);
        return "[]";
      }

      return parseFixedWidth(dataFileBytes, yearStr);

    } catch (Exception e) {
      throw new RuntimeException("Govt Finance: Failed to parse response for "
          + context.getUrl() + " (year=" + yearStr + ")", e);
    }
  }

  private String findZipUrl(String landingHtml) {
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
   * years: absolute ({@code https://www2.census.gov/...}, most years), site-root-relative
   * ({@code /data/...}), and protocol-relative ({@code //www2.census.gov/...}, 2013/2016 — a
   * different host than the www.census.gov landing page, so it cannot be treated as a relative
   * path or the result is the malformed https://www.census.gov//www2.census.gov/...).
   */
  private String resolveHref(String href) {
    if (href.startsWith("http")) {
      return href;
    }
    if (href.startsWith("//")) {
      return "https:" + href;
    }
    return "https://www.census.gov" + href;
  }

  private byte[] extractDataFile(byte[] zipBytes) throws IOException {
    byte[] best = null;
    long bestSize = -1;
    try (ZipInputStream zis = new ZipInputStream(new ByteArrayInputStream(zipBytes))) {
      ZipEntry entry;
      while ((entry = zis.getNextEntry()) != null) {
        String name = entry.getName();
        if (!name.toLowerCase(java.util.Locale.ROOT).endsWith(".txt")
            || name.toLowerCase(java.util.Locale.ROOT).contains("fin_pid")) {
          continue;
        }
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        byte[] buf = new byte[65536];
        int len;
        while ((len = zis.read(buf)) > 0) {
          baos.write(buf, 0, len);
        }
        byte[] content = baos.toByteArray();
        if (content.length > bestSize) {
          bestSize = content.length;
          best = content;
        }
      }
    }
    return best;
  }

  private String parseFixedWidth(byte[] dataFileBytes, String yearStr) throws IOException {
    Integer year = parseIntOrNull(yearStr);
    ArrayNode result = MAPPER.createArrayNode();

    try (BufferedReader reader = new BufferedReader(new InputStreamReader(
        new ByteArrayInputStream(dataFileBytes), StandardCharsets.US_ASCII))) {
      String line;
      int skipped = 0;
      while ((line = reader.readLine()) != null) {
        if (line.length() < 32) {
          skipped++;
          continue;
        }
        String stateFips = line.substring(0, 2);
        String govTypeCode = line.substring(2, 3);
        String countyFips = line.substring(3, 6);
        String unitId = line.substring(6, 12);
        String itemCode = line.substring(12, 15).trim();
        String amountRaw = line.substring(15, 27).trim();
        String dataYearRaw = line.substring(27, 31).trim();
        String flag = line.substring(31, 32).trim();

        Long amountThousands = amountRaw.isEmpty() ? null : parseLongOrNull(amountRaw);
        Integer dataYear = dataYearRaw.isEmpty() ? year : parseIntOrNull(dataYearRaw);

        ObjectNode out = MAPPER.createObjectNode();
        out.put("year", dataYear != null ? dataYear : year);
        out.put("state_fips", stateFips);
        out.put("gov_type_code", govTypeCode);
        String govTypeName = GOV_TYPE_NAMES.get(govTypeCode);
        if (govTypeName != null) {
          out.put("gov_type_name", govTypeName);
        } else {
          out.putNull("gov_type_name");
        }
        out.put("county_fips", countyFips);
        out.put("unit_id", unitId);
        out.put("item_code", itemCode);
        if (amountThousands != null) {
          out.put("amount_thousands", amountThousands);
        } else {
          out.putNull("amount_thousands");
        }
        out.put("imputation_flag", flag);
        result.add(out);
      }
      if (skipped > 0) {
        LOGGER.warn("Govt Finance: skipped {} malformed (< 32 char) records for year={}",
            skipped, yearStr);
      }
    }

    LOGGER.debug("Govt Finance: Parsed {} rows for year={}", result.size(), yearStr);
    return result.toString();
  }

  private byte[] downloadBytes(String url) throws IOException {
    HttpURLConnection conn = (HttpURLConnection) URI.create(url).toURL().openConnection();
    conn.setConnectTimeout(30000);
    conn.setReadTimeout(120000);
    conn.setRequestProperty("User-Agent", "GovData/1.0");
    int status = conn.getResponseCode();
    if (status != 200) {
      throw new IOException("HTTP " + status + " from " + url);
    }
    try (InputStream is = conn.getInputStream();
         ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
      byte[] buf = new byte[65536];
      int len;
      while ((len = is.read(buf)) > 0) {
        baos.write(buf, 0, len);
      }
      return baos.toByteArray();
    }
  }

  private Long parseLongOrNull(String s) {
    try {
      return Long.parseLong(s);
    } catch (NumberFormatException e) {
      return null;
    }
  }

  private Integer parseIntOrNull(String s) {
    if (s == null) {
      return null;
    }
    try {
      return Integer.parseInt(s.trim());
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
