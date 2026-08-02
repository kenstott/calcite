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
package org.apache.calcite.adapter.govdata.ag;

import org.apache.calcite.adapter.file.etl.DataProvider;
import org.apache.calcite.adapter.file.etl.EtlPipelineConfig;

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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * DataProvider for the USGS Pesticide National Synthesis Project (NAWQA) county-level
 * "Estimated Annual Agricultural Pesticide Use" data.
 *
 * <p>Live-verified 2026-08-02 against
 * {@code https://water.usgs.gov/nawqa/pnsp/usage/maps/county-level/}: this is not one
 * bulk file per year on a predictable URL. Final estimates for 1992-2012 are direct
 * per-year {@code .txt} files; the 2013-17 final (ver 2.0) release ships as ONE combined
 * multi-year {@code .txt}; subsequent preliminary years (2018 and 2019 confirmed live at
 * verification time) are each a separate ScienceBase data-release item reachable only via
 * a {@code doi.org} link whose target ScienceBase item id cannot be derived from the year.
 * USGS's own stated plan is to publish a consolidated final 2018-22 release "in 2026" and
 * then resume annual preliminary releases — this provider re-scrapes the landing page on
 * every run, so a newly added link of either shape (a new per-year {@code .txt} or a new
 * {@code doi.org} entry) is picked up with no code change.
 *
 * <p>Every file found shares one schema: tab-delimited {@code COMPOUND / YEAR /
 * STATE_FIPS_CODE / COUNTY_FIPS_CODE / EPEST_LOW_KG / EPEST_HIGH_KG} (verified identical
 * across the 2012 final file, the 2013-17 combined file, and the 2019 preliminary file).
 * No row-shape transformation is needed — the reason this is a {@link DataProvider} rather
 * than the generic HTTP+CSV pipeline is purely the discovery step (resolving a
 * {@code doi.org} redirect to a ScienceBase item, then reading that item's file list),
 * which URL templating cannot express.
 *
 * <p>Discovery is scoped to the landing page's "County-level pesticide use estimates" and
 * "Preliminary county-level pesticide-use estimates" sections, bounded by the "State-level
 * pesticide use estimates..." heading that follows them on the same page — the
 * state-level-by-crop-group series (wrong geographic grain) and the explicitly-superseded
 * "Archived data" section (stale preliminary duplicates of data covered by the sections
 * above) are deliberately excluded.
 *
 * <p>The table has no {@code year} dimension: one run fetches every year the source
 * currently publishes and partitions by each row's own {@code year} column, the same
 * pattern {@code ers_farm_income} uses for the same reason — a multi-vintage source with
 * no single per-year URL to template.
 */
public class UsgsPesticideCountyUseProvider implements DataProvider {

  private static final Logger LOGGER = LoggerFactory.getLogger(UsgsPesticideCountyUseProvider.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static final String DEFAULT_UA = "Mozilla/5.0 (compatible; govdata-etl/1.0)";

  /** Bounds of the county-level (as opposed to state-level) section of the landing page. */
  private static final String SECTION_START_MARKER = "county-level pesticide use estimates";
  private static final String SECTION_END_MARKER = "state-level pesticide use estimates";

  private static final Pattern HREF = Pattern.compile("href=\"([^\"]+)\"", Pattern.CASE_INSENSITIVE);
  private static final Pattern SCIENCEBASE_ITEM_ID = Pattern.compile("catalog/item/([a-fA-F0-9]+)");

  private static final String[] REQUIRED_HEADERS = {
      "COMPOUND", "YEAR", "STATE_FIPS_CODE", "COUNTY_FIPS_CODE", "EPEST_LOW_KG", "EPEST_HIGH_KG"
  };

  @Override public Iterator<Map<String, Object>> fetch(EtlPipelineConfig config,
      Map<String, String> variables) throws IOException {
    String landingUrl = config.getSource() != null ? config.getSource().getUrl() : null;
    if (landingUrl == null || landingUrl.isEmpty()) {
      throw new IOException("USGS pesticide use: source.url (landing page) is required");
    }
    String userAgent = headerOrDefault(config, "User-Agent", DEFAULT_UA);

    List<String> dataFileUrls = resolveDataFileUrls(landingUrl, userAgent);
    if (dataFileUrls.isEmpty()) {
      throw new IOException("USGS pesticide use: no county-level data files resolved from "
          + landingUrl);
    }
    LOGGER.info("USGS pesticide use: resolved {} county-level data file(s) from {}",
        dataFileUrls.size(), landingUrl);

    return new MultiFileRowIterator(dataFileUrls, userAgent);
  }

  // ---------------------------------------------------------------------
  // Discovery: landing page -> ordered list of tab-delimited data file URLs
  // ---------------------------------------------------------------------

  private List<String> resolveDataFileUrls(String landingUrl, String userAgent) throws IOException {
    String html = getText(landingUrl, userAgent);
    String lower = html.toLowerCase(Locale.ROOT);
    int start = lower.indexOf(SECTION_START_MARKER);
    if (start < 0) {
      throw new IOException("USGS pesticide use: '" + SECTION_START_MARKER
          + "' section not found on " + landingUrl + " (page structure changed)");
    }
    int end = lower.indexOf(SECTION_END_MARKER, start);
    if (end < 0) {
      throw new IOException("USGS pesticide use: '" + SECTION_END_MARKER
          + "' boundary not found on " + landingUrl + " (page structure changed)");
    }
    String section = html.substring(start, end);

    List<String> fileUrls = new ArrayList<String>();
    Matcher m = HREF.matcher(section);
    while (m.find()) {
      String href = m.group(1);
      String lowerHref = href.toLowerCase(Locale.ROOT);
      if (lowerHref.endsWith(".txt")) {
        fileUrls.add(URI.create(landingUrl).resolve(href).toString());
      } else if (lowerHref.startsWith("https://doi.org/") || lowerHref.startsWith("http://doi.org/")) {
        fileUrls.addAll(resolveDoiToDataFiles(href, userAgent));
      } else {
        LOGGER.debug("USGS pesticide use: ignoring unrecognized link in county-level section: {}",
            href);
      }
    }
    return fileUrls;
  }

  /** Follows a doi.org redirect to its ScienceBase item, then returns that item's data file URL(s). */
  private List<String> resolveDoiToDataFiles(String doiUrl, String userAgent) throws IOException {
    String resolvedUrl = followRedirect(doiUrl, userAgent);
    Matcher idMatch = SCIENCEBASE_ITEM_ID.matcher(resolvedUrl);
    if (!idMatch.find()) {
      throw new IOException("USGS pesticide use: DOI " + doiUrl
          + " did not resolve to a ScienceBase catalog item (got " + resolvedUrl + ")");
    }
    String itemId = idMatch.group(1);
    String apiUrl = "https://www.sciencebase.gov/catalog/item/" + itemId + "?format=json&fields=files";
    String json = getText(apiUrl, userAgent);
    JsonNode files = MAPPER.readTree(json).path("files");
    List<String> dataFiles = new ArrayList<String>();
    if (files.isArray()) {
      for (JsonNode f : files) {
        String name = f.path("name").asText("");
        String lowerName = name.toLowerCase(Locale.ROOT);
        // Skip metadata/version-history sidecar files; keep only the data table(s).
        if (lowerName.endsWith(".txt") && !lowerName.startsWith("metadata")
            && !lowerName.startsWith("version_history")) {
          String downloadUri = f.path("downloadUri").asText(null);
          if (downloadUri != null) {
            dataFiles.add(downloadUri);
          }
        }
      }
    }
    if (dataFiles.isEmpty()) {
      throw new IOException("USGS pesticide use: ScienceBase item " + itemId
          + " (from " + doiUrl + ") has no data .txt file");
    }
    return dataFiles;
  }

  /** Manually follows the redirect chain (HttpURLConnection.getURL() does not update on redirect). */
  private String followRedirect(String url, String userAgent) throws IOException {
    String current = url;
    for (int hop = 0; hop < 5; hop++) {
      HttpURLConnection conn = (HttpURLConnection) URI.create(current).toURL().openConnection();
      conn.setRequestProperty("User-Agent", userAgent);
      conn.setConnectTimeout(30000);
      conn.setReadTimeout(60000);
      conn.setInstanceFollowRedirects(false);
      conn.setRequestMethod("HEAD");
      int code = conn.getResponseCode();
      if (code >= 300 && code < 400) {
        String location = conn.getHeaderField("Location");
        conn.disconnect();
        if (location == null || location.isEmpty()) {
          throw new IOException("USGS pesticide use: HTTP " + code
              + " with no Location header for " + current);
        }
        current = URI.create(current).resolve(location).toString();
        continue;
      }
      conn.disconnect();
      if (code < 200 || code >= 300) {
        throw new IOException("USGS pesticide use: HTTP " + code + " resolving " + current);
      }
      return current;
    }
    throw new IOException("USGS pesticide use: too many redirects resolving " + url);
  }

  // ---------------------------------------------------------------------
  // Row parsing: one shared tab-delimited schema across every file
  // ---------------------------------------------------------------------

  /** Lazily concatenates rows from each file in {@code fileUrls}, one open reader at a time. */
  private static final class MultiFileRowIterator implements Iterator<Map<String, Object>> {
    private final List<String> fileUrls;
    private final String userAgent;
    private int fileIndex;
    private BufferedReader reader;
    private int[] columnIndex;
    private String currentUrl;
    private Map<String, Object> nextRow;
    private boolean done;

    MultiFileRowIterator(List<String> fileUrls, String userAgent) {
      this.fileUrls = fileUrls;
      this.userAgent = userAgent;
    }

    private void advance() throws IOException {
      while (nextRow == null && !done) {
        if (reader == null) {
          if (!openNextFile()) {
            done = true;
            return;
          }
        }
        String line = reader.readLine();
        if (line == null) {
          reader.close();
          reader = null;
          continue;
        }
        if (line.isEmpty()) {
          continue;
        }
        nextRow = toRow(line, columnIndex);
      }
    }

    private boolean openNextFile() throws IOException {
      if (fileIndex >= fileUrls.size()) {
        return false;
      }
      currentUrl = fileUrls.get(fileIndex++);
      LOGGER.info("USGS pesticide use: streaming {}", currentUrl);
      HttpURLConnection conn = (HttpURLConnection) URI.create(currentUrl).toURL().openConnection();
      conn.setRequestProperty("User-Agent", userAgent);
      conn.setConnectTimeout(30000);
      conn.setReadTimeout(300000);
      conn.setInstanceFollowRedirects(true);
      int code = conn.getResponseCode();
      if (code < 200 || code >= 300) {
        throw new IOException("USGS pesticide use: HTTP " + code + " for " + currentUrl);
      }
      reader = new BufferedReader(new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8));
      String header = reader.readLine();
      if (header == null) {
        throw new IOException("USGS pesticide use: empty file " + currentUrl);
      }
      columnIndex = headerIndex(header, currentUrl);
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

  private static int[] headerIndex(String header, String fileUrl) throws IOException {
    String[] cols = header.split("\t", -1);
    Map<String, Integer> pos = new HashMap<String, Integer>();
    for (int i = 0; i < cols.length; i++) {
      pos.put(cols[i].trim().toUpperCase(Locale.ROOT), Integer.valueOf(i));
    }
    int[] index = new int[REQUIRED_HEADERS.length];
    for (int i = 0; i < REQUIRED_HEADERS.length; i++) {
      Integer at = pos.get(REQUIRED_HEADERS[i]);
      if (at == null) {
        throw new IOException("USGS pesticide use: expected column '" + REQUIRED_HEADERS[i]
            + "' not found in " + fileUrl + " — header=" + header);
      }
      index[i] = at.intValue();
    }
    return index;
  }

  private static Map<String, Object> toRow(String line, int[] index) {
    String[] fields = line.split("\t", -1);
    String compound = field(fields, index[0]);
    String year = field(fields, index[1]);
    String stateFips = pad(field(fields, index[2]), 2);
    String countyFips3 = pad(field(fields, index[3]), 3);
    String epestLow = field(fields, index[4]);
    String epestHigh = field(fields, index[5]);

    Map<String, Object> row = new LinkedHashMap<String, Object>();
    row.put("compound", compound);
    row.put("year", year == null ? null : Integer.valueOf(Integer.parseInt(year)));
    row.put("state_fips_code", stateFips);
    row.put("county_fips_code", countyFips3);
    row.put("county_fips", countyFips(stateFips, countyFips3));
    row.put("epest_low_kg", parseDouble(epestLow));
    row.put("epest_high_kg", parseDouble(epestHigh));
    return row;
  }

  private static String field(String[] fields, int at) {
    if (at >= fields.length) {
      return null;
    }
    String raw = fields[at].trim();
    return raw.isEmpty() ? null : raw;
  }

  private static Double parseDouble(String raw) {
    if (raw == null) {
      return null;
    }
    try {
      return Double.valueOf(Double.parseDouble(raw));
    } catch (NumberFormatException e) {
      throw new RuntimeException("USGS pesticide use: non-numeric value '" + raw + "'", e);
    }
  }

  private static String pad(String v, int width) {
    if (v == null) {
      return null;
    }
    String t = v.trim();
    if (t.isEmpty()) {
      return null;
    }
    StringBuilder sb = new StringBuilder(t);
    while (sb.length() < width) {
      sb.insert(0, '0');
    }
    return sb.toString();
  }

  private static String countyFips(String stateFips, String countyFips3) {
    if (stateFips == null || countyFips3 == null) {
      return null;
    }
    return stateFips + countyFips3;
  }

  private static String headerOrDefault(EtlPipelineConfig config, String name, String dflt) {
    Map<String, String> headers = config.getSource() != null ? config.getSource().getHeaders() : null;
    if (headers != null) {
      String v = headers.get(name);
      if (v != null && !v.isEmpty()) {
        return v;
      }
    }
    return dflt;
  }

  private static String getText(String url, String userAgent) throws IOException {
    return new String(getBytes(url, userAgent), StandardCharsets.UTF_8);
  }

  private static byte[] getBytes(String url, String userAgent) throws IOException {
    HttpURLConnection conn = (HttpURLConnection) URI.create(url).toURL().openConnection();
    conn.setRequestProperty("User-Agent", userAgent);
    conn.setConnectTimeout(30000);
    conn.setReadTimeout(60000);
    conn.setInstanceFollowRedirects(true);
    int code = conn.getResponseCode();
    if (code < 200 || code >= 300) {
      throw new IOException("USGS pesticide use: HTTP " + code + " for " + url);
    }
    ByteArrayOutputStream buf = new ByteArrayOutputStream();
    try (InputStream in = conn.getInputStream()) {
      byte[] chunk = new byte[65536];
      int n;
      while ((n = in.read(chunk)) != -1) {
        buf.write(chunk, 0, n);
      }
    }
    return buf.toByteArray();
  }
}
