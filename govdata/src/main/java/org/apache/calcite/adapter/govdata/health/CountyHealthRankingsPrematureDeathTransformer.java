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

import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.ResponseTransformer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * Parses County Health Rankings' national analytic-data CSV into
 * {@code cms_nursing_home}-style flat rows carrying the Premature Death (years-of-potential-
 * life-lost, YPLL, per 100,000) measure at nation/state/county grain.
 *
 * <p>The source file has two header lines — a human-readable display-name row, then a row of
 * CHR's own stable machine codes (e.g. {@code v001_rawvalue}). This transformer builds its
 * column map from the SECOND row, not the first: those codes are CHR's own permanent measure
 * identifiers, documented as stable across annual releases, unlike the (very long, frequently
 * reordered) display-name row. Only the Premature Death (v001) columns are extracted; the file
 * carries hundreds of unrelated measures (smoking, obesity, screening rates, ...) out of scope
 * for this table.
 *
 * <p>The file is plain unquoted CSV (verified against the live download: zero {@code "}
 * characters in 13MB) so a manual comma split is correct here, unlike a general-purpose CSV
 * source that would need real quote handling.
 */
public class CountyHealthRankingsPrematureDeathTransformer implements ResponseTransformer {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Override
  public String transform(String response, RequestContext context) {
    String url = context.getUrl();
    try {
      byte[] bytes = downloadBytes(url);
      ArrayNode rows = MAPPER.createArrayNode();
      try (BufferedReader reader = new BufferedReader(
          new InputStreamReader(new java.io.ByteArrayInputStream(bytes), StandardCharsets.UTF_8))) {
        reader.readLine(); // display-name header — not used
        String machineHeader = reader.readLine();
        if (machineHeader == null) {
          throw new IllegalStateException("CHR CSV has no machine-code header row");
        }
        Map<String, Integer> col = indexHeader(machineHeader.split(",", -1));
        String line;
        while ((line = reader.readLine()) != null) {
          if (line.isEmpty()) {
            continue;
          }
          rows.add(parseRow(line.split(",", -1), col));
        }
      }
      return MAPPER.writeValueAsString(rows);
    } catch (Exception e) {
      throw new RuntimeException("County Health Rankings: failed to parse from " + url, e);
    }
  }

  private Map<String, Integer> indexHeader(String[] headerFields) {
    Map<String, Integer> col = new HashMap<>();
    for (int i = 0; i < headerFields.length; i++) {
      col.put(headerFields[i], i);
    }
    return col;
  }

  private ObjectNode parseRow(String[] fields, Map<String, Integer> col) {
    String stateFips = field(fields, col, "statecode");
    String countyFips = field(fields, col, "countycode");
    String geoLevel = "00".equals(stateFips) ? "nation" : "000".equals(countyFips) ? "state" : "county";

    ObjectNode row = MAPPER.createObjectNode();
    row.put("geo_level", geoLevel);
    row.put("state_fips", stateFips);
    row.put("county_fips", countyFips);
    row.put("fips_code", field(fields, col, "fipscode"));
    row.put("state_abbr", field(fields, col, "state"));
    row.put("name", field(fields, col, "county"));
    putDouble(row, "release_year", field(fields, col, "year"));
    putDouble(row, "premature_death_rate", field(fields, col, "v001_rawvalue"));
    putDouble(row, "premature_death_numerator", field(fields, col, "v001_numerator"));
    putDouble(row, "premature_death_denominator", field(fields, col, "v001_denominator"));
    putDouble(row, "ci_low", field(fields, col, "v001_cilow"));
    putDouble(row, "ci_high", field(fields, col, "v001_cihigh"));
    row.put("data_flag", field(fields, col, "v001_flag"));
    putDouble(row, "rate_aian", field(fields, col, "v001_race_aian"));
    putDouble(row, "rate_asian", field(fields, col, "v001_race_asian"));
    putDouble(row, "rate_black", field(fields, col, "v001_race_black"));
    putDouble(row, "rate_hispanic", field(fields, col, "v001_race_hispanic"));
    putDouble(row, "rate_white", field(fields, col, "v001_race_white"));
    putDouble(row, "rate_nhopi", field(fields, col, "v001_race_nhopi"));
    row.put("type", "chr_premature_death");
    return row;
  }

  private String field(String[] fields, Map<String, Integer> col, String name) {
    Integer idx = col.get(name);
    if (idx == null || idx >= fields.length) {
      return null;
    }
    String v = fields[idx].trim();
    return v.isEmpty() ? null : v;
  }

  private void putDouble(ObjectNode row, String key, String value) {
    if (value == null) {
      row.putNull(key);
      return;
    }
    try {
      row.put(key, Double.parseDouble(value));
    } catch (NumberFormatException e) {
      row.putNull(key);
    }
  }

  private byte[] downloadBytes(String url) throws IOException {
    HttpURLConnection conn = (HttpURLConnection) URI.create(url).toURL().openConnection();
    conn.setConnectTimeout(30000);
    conn.setReadTimeout(120000);
    conn.setRequestProperty("User-Agent", "Mozilla/5.0 GovData/1.0");
    int status = conn.getResponseCode();
    if (status != 200) {
      throw new IOException("HTTP " + status + " from " + url);
    }
    InputStream is = conn.getInputStream();
    java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
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
}
