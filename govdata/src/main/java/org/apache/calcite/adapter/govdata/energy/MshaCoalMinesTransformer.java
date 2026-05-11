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
package org.apache.calcite.adapter.govdata.energy;

import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.ResponseTransformer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class MshaCoalMinesTransformer implements ResponseTransformer {

  private static final Logger LOGGER = LoggerFactory.getLogger(MshaCoalMinesTransformer.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String MINES_ZIP_URL =
      "https://arlweb.msha.gov/OpenGovernmentData/DataSets/Mines.zip";
  private static final String MINES_CACHE_FILE =
      System.getProperty("java.io.tmpdir") + File.separator + "msha_mines_latest.csv";
  private static final long CACHE_TTL_MS = 24L * 60 * 60 * 1000; // 24 hours

  @Override
  public String transform(String response, RequestContext context) {
    if (response == null || response.isEmpty()) {
      LOGGER.warn("MSHA Coal Mines: empty response");
      return "[]";
    }

    Map<String, String> dims = context.getDimensionValues();
    String yearStr = dims != null ? dims.get("year") : null;
    int filterYear = 0;
    if (yearStr != null && !yearStr.isEmpty()) {
      try {
        filterYear = Integer.parseInt(yearStr);
      } catch (NumberFormatException e) {
        LOGGER.warn("MSHA Coal Mines: invalid year dimension: {}", yearStr);
      }
    }

    try {
      // The HTTP pipeline delivers a binary ZIP as a string (corrupted by TEXT mode).
      // Download the ZIP directly and extract the pipe-delimited production file.
      String url = context.getUrl();
      String prodText;
      try {
        byte[] zipBytes = downloadBytes(url);
        // Try exact names first, then fall back to any .txt/.csv with "prod" in the name.
        prodText = extractCsvFromZip(zipBytes, "MinesProdYearly.txt");
        if (prodText == null) {
          prodText = extractCsvFromZip(zipBytes, "MinesProdYearly.csv");
        }
        if (prodText == null) {
          prodText = extractCsvFromZipByPattern(zipBytes, "prod");
        }
        if (prodText == null) {
          logZipContents(zipBytes, url);
          return "[]";
        }
      } catch (Exception zipEx) {
        LOGGER.warn("MSHA Coal Mines: failed to download/extract ZIP from {}: {}", url, zipEx.getMessage());
        return "[]";
      }
      // Parse the pipe-delimited production CSV
      List<Map<String, String>> prodRows = parsePipeDelimited(prodText);

      // Load mine reference data (lat/lon/controller_name) from cached Mines.csv
      Map<String, Map<String, String>> mineRef = loadMinesReference();

      ArrayNode result = MAPPER.createArrayNode();

      for (Map<String, String> row : prodRows) {
        // Filter to coal only — production file uses C_M_IND, reference file uses COAL_METAL_IND
        String coalMetal = row.get("C_M_IND");
        if (!"C".equals(coalMetal)) {
          continue;
        }

        // Filter by year if specified
        if (filterYear > 0) {
          String calYr = row.get("CALENDAR_YR");
          if (calYr == null || !yearStr.equals(calYr.trim())) {
            continue;
          }
        }

        ObjectNode out = MAPPER.createObjectNode();

        String mineId = row.get("MINE_ID");
        putStringVal(out, "mine_id", mineId);
        putStringVal(out, "mine_name", row.get("CURR_MINE_NM"));
        putStringVal(out, "state_abbr", row.get("STATE_ABBR"));
        putIntVal(out, "report_year", row.get("CALENDAR_YR"));
        putStringVal(out, "subunit_code", row.get("SUBUNIT_CD"));
        putStringVal(out, "subunit", row.get("SUBUNIT_DESC"));
        putDoubleVal(out, "production_short_tons", row.get("ANNUAL_COAL_PROD"));
        putDoubleVal(out, "annual_hours", row.get("ANNUAL_HRS"));
        putDoubleVal(out, "avg_employee_count", row.get("AVG_ANNUAL_EMPL"));

        // Compute labor productivity (short tons per employee hour)
        Double prod = parseDouble(row.get("ANNUAL_COAL_PROD"));
        Double hours = parseDouble(row.get("ANNUAL_HRS"));
        if (prod != null && hours != null && hours > 0.0) {
          out.put("labor_productivity", prod / hours);
        } else {
          out.putNull("labor_productivity");
        }

        // Enrich from mine reference data (Mines.txt columns)
        if (mineId != null && mineRef.containsKey(mineId.trim())) {
          Map<String, String> ref = mineRef.get(mineId.trim());
          putDoubleVal(out, "latitude", ref.get("LATITUDE"));
          putDoubleVal(out, "longitude", ref.get("LONGITUDE"));
          putStringVal(out, "controller_name", ref.get("CURRENT_CONTROLLER_NAME"));
          putStringVal(out, "operator_name", ref.get("CURRENT_OPERATOR_NAME"));
          putStringVal(out, "county_fips", ref.get("FIPS_CNTY_CD"));
          putStringVal(out, "county_name", ref.get("FIPS_CNTY_NM"));
          putStringVal(out, "mine_type", ref.get("CURRENT_MINE_TYPE"));
          putStringVal(out, "mine_status", ref.get("CURRENT_MINE_STATUS"));
          putDoubleVal(out, "avg_mine_height_inches", ref.get("AVG_MINE_HEIGHT"));
        } else {
          out.putNull("latitude");
          out.putNull("longitude");
          out.putNull("controller_name");
          out.putNull("operator_name");
          out.putNull("county_fips");
          out.putNull("county_name");
          out.putNull("mine_type");
          out.putNull("mine_status");
          out.putNull("avg_mine_height_inches");
        }
        out.putNull("coal_type"); // not directly available in MSHA files

        result.add(out);
      }

      LOGGER.debug("MSHA Coal Mines: transformed {} records for year {}", result.size(), yearStr);
      return result.toString();

    } catch (Exception e) {
      LOGGER.error("MSHA Coal Mines: failed to parse response: {}", e.getMessage());
      return "[]";
    }
  }

  private List<Map<String, String>> parsePipeDelimited(String content) {
    List<Map<String, String>> rows = new ArrayList<>();
    String[] lines = content.split("\n");
    if (lines.length == 0) {
      return rows;
    }

    String[] headers = lines[0].split("\\|", -1);
    for (int i = 0; i < headers.length; i++) {
      headers[i] = headers[i].trim().replace("\"", "");
    }

    for (int i = 1; i < lines.length; i++) {
      String line = lines[i].trim();
      if (line.isEmpty()) {
        continue;
      }
      String[] parts = line.split("\\|", -1);
      Map<String, String> row = new HashMap<>();
      for (int c = 0; c < headers.length; c++) {
        String val = c < parts.length ? parts[c].trim().replace("\"", "") : "";
        row.put(headers[c], val);
      }
      rows.add(row);
    }
    return rows;
  }

  private Map<String, Map<String, String>> loadMinesReference() {
    File cacheFile = new File(MINES_CACHE_FILE);
    boolean needDownload = !cacheFile.exists()
        || (System.currentTimeMillis() - cacheFile.lastModified()) > CACHE_TTL_MS;

    if (needDownload) {
      try {
        byte[] zipBytes = downloadBytes(MINES_ZIP_URL);
        String csvContent = extractCsvFromZip(zipBytes, "Mines.txt");
        if (csvContent == null) {
          csvContent = extractCsvFromZip(zipBytes, "Mines.csv");
        }
        if (csvContent != null) {
          FileOutputStream fos = new FileOutputStream(cacheFile);
          try {
            fos.write(csvContent.getBytes("UTF-8"));
          } finally {
            fos.close();
          }
        } else {
          LOGGER.warn("MSHA Mines: could not find Mines.txt or Mines.csv in ZIP");
          return new HashMap<>();
        }
      } catch (Exception e) {
        LOGGER.warn("MSHA Mines: failed to download reference data: {}", e.getMessage());
        if (!cacheFile.exists()) {
          return new HashMap<>();
        }
      }
    }

    try {
      FileInputStream fis = new FileInputStream(cacheFile);
      byte[] bytes;
      try {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        byte[] buf = new byte[8192];
        int len;
        while ((len = fis.read(buf)) > 0) {
          baos.write(buf, 0, len);
        }
        bytes = baos.toByteArray();
      } finally {
        fis.close();
      }
      String content = new String(bytes, "UTF-8");
      List<Map<String, String>> rows = parsePipeDelimited(content);
      Map<String, Map<String, String>> result = new HashMap<>();
      for (Map<String, String> row : rows) {
        String mineId = row.get("MINE_ID");
        if (mineId != null && !mineId.isEmpty()) {
          result.put(mineId.trim(), row);
        }
      }
      return result;
    } catch (Exception e) {
      LOGGER.warn("MSHA Mines: failed to read cached reference file: {}", e.getMessage());
      return new HashMap<>();
    }
  }

  private String extractCsvFromZip(byte[] zipBytes, String targetName) throws Exception {
    ZipInputStream zis = new ZipInputStream(new ByteArrayInputStream(zipBytes));
    try {
      ZipEntry entry;
      while ((entry = zis.getNextEntry()) != null) {
        String name = entry.getName();
        if (name.equalsIgnoreCase(targetName) || name.toLowerCase().endsWith(
            targetName.toLowerCase())) {
          ByteArrayOutputStream baos = new ByteArrayOutputStream();
          byte[] buf = new byte[8192];
          int len;
          while ((len = zis.read(buf)) > 0) {
            baos.write(buf, 0, len);
          }
          return baos.toString("UTF-8");
        }
        zis.closeEntry();
      }
    } finally {
      zis.close();
    }
    return null;
  }

  private void logZipContents(byte[] zipBytes, String url) {
    LOGGER.warn("MSHA Coal Mines: no production data file found in ZIP from {}. ZIP entries:", url);
    try {
      ZipInputStream zis = new ZipInputStream(new ByteArrayInputStream(zipBytes));
      try {
        ZipEntry entry;
        while ((entry = zis.getNextEntry()) != null) {
          LOGGER.warn("  - {}", entry.getName());
          zis.closeEntry();
        }
      } finally {
        zis.close();
      }
    } catch (Exception e) {
      LOGGER.warn("  (failed to list ZIP entries: {})", e.getMessage());
    }
  }

  /** Extract the first text/csv file whose name (lowercased) contains {@code pattern}. */
  private String extractCsvFromZipByPattern(byte[] zipBytes, String pattern) throws Exception {
    ZipInputStream zis = new ZipInputStream(new ByteArrayInputStream(zipBytes));
    try {
      ZipEntry entry;
      while ((entry = zis.getNextEntry()) != null) {
        String name = entry.getName().toLowerCase();
        LOGGER.debug("MSHA ZIP entry: {}", entry.getName());
        if ((name.endsWith(".txt") || name.endsWith(".csv")) && name.contains(pattern)) {
          LOGGER.info("MSHA Coal Mines: matched ZIP entry '{}' for pattern '{}'",
              entry.getName(), pattern);
          ByteArrayOutputStream baos = new ByteArrayOutputStream();
          byte[] buf = new byte[8192];
          int len;
          while ((len = zis.read(buf)) > 0) {
            baos.write(buf, 0, len);
          }
          return baos.toString("UTF-8");
        }
        zis.closeEntry();
      }
    } finally {
      zis.close();
    }
    return null;
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
    InputStream is = conn.getInputStream();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try {
      byte[] buf = new byte[65536];
      int len;
      while ((len = is.read(buf)) > 0) {
        baos.write(buf, 0, len);
      }
    } finally {
      is.close();
    }
    return baos.toByteArray();
  }

  private void putStringVal(ObjectNode out, String key, String val) {
    if (val != null && !val.isEmpty()) {
      out.put(key, val);
    } else {
      out.putNull(key);
    }
  }

  private void putIntVal(ObjectNode out, String key, String val) {
    if (val == null || val.isEmpty()) {
      out.putNull(key);
      return;
    }
    try {
      out.put(key, Integer.parseInt(val.trim()));
    } catch (NumberFormatException e) {
      out.putNull(key);
    }
  }

  private void putDoubleVal(ObjectNode out, String key, String val) {
    Double d = parseDouble(val);
    if (d != null) {
      out.put(key, d);
    } else {
      out.putNull(key);
    }
  }

  private Double parseDouble(String val) {
    if (val == null || val.trim().isEmpty()) {
      return null;
    }
    try {
      return Double.parseDouble(val.trim().replace(",", ""));
    } catch (NumberFormatException e) {
      return null;
    }
  }
}
