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

import org.apache.calcite.adapter.file.etl.CsvRecordReader;
import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.ResponseTransformer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

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
import java.util.List;

/**
 * Discovers and parses CMS's current-quarter "Provider of Services File - Quality Improvement
 * and Evaluation System" (POS) CSV — provider identity, location, and bed-count fields for every
 * certified hospital and non-hospital facility.
 *
 * <p>Two-step process, driven by this single transformer since the CSV download URL contains an
 * opaque per-release UUID/hash with no year-templatable pattern (confirmed live: {@code
 * .../2026-07/7780b4e3-4c4b-4811-8884-65ca23b7a4e8/Hospital_and_other.DATA.Q2_2026.csv}): (1) the
 * framework fetches CMS's site-wide {@code data.json} DCAT catalog and hands its body to
 * {@link #transform}; this method finds the dataset titled "Provider of Services File - Quality
 * Improvement and Evaluation System" and, within its {@code distribution} array, the entry with
 * {@code format=CSV} (skipping the sibling {@code format=API} "latest" entry, which has no
 * {@code downloadURL}) whose {@code temporal} window is most recent; (2) downloads and parses
 * that CSV with {@link CsvRecordReader} (RFC4180-aware — a naive comma split misaligns roughly
 * 1 in 350 rows here, since several address/name fields carry quoted embedded commas, confirmed
 * against the live file).
 *
 * <p>Verified live against the real Q2 2026 file (downloaded 2026-08-02,
 * {@code Hospital_and_other.DATA.Q2_2026.csv}, 44,707 records, 150 columns): PRVDR_NUM is unique
 * across all rows under correct CSV parsing (0 duplicates); all 6 PRVDR_CTGRY_CD facility types
 * (hospital, SNF/nursing, clinic, mental health, other) and every curated bed-count column present
 * with real nonzero values.
 */
public class CmsPosTransformer implements ResponseTransformer {

  private static final Logger LOGGER = LoggerFactory.getLogger(CmsPosTransformer.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String DATASET_TITLE =
      "Provider of Services File - Quality Improvement and Evaluation System";

  @Override public String transform(String response, RequestContext context) {
    if (response == null || response.isEmpty()) {
      LOGGER.warn("CMS POS: Empty data.json response");
      return "[]";
    }

    try {
      String csvUrl = findCsvUrl(response);
      if (csvUrl == null) {
        LOGGER.warn("CMS POS: no CSV distribution found for dataset '{}' on {}",
            DATASET_TITLE, context.getUrl());
        return "[]";
      }

      byte[] csvBytes = downloadBytes(csvUrl);
      return parseCsv(csvBytes);

    } catch (Exception e) {
      throw new RuntimeException("CMS POS: Failed to parse response for " + context.getUrl(), e);
    }
  }

  private String findCsvUrl(String dataJson) throws IOException {
    JsonNode root = MAPPER.readTree(dataJson);
    JsonNode datasets = root.get("dataset");
    if (datasets == null || !datasets.isArray()) {
      return null;
    }

    String bestUrl = null;
    String bestTemporal = null;
    for (JsonNode dataset : datasets) {
      JsonNode titleNode = dataset.get("title");
      if (titleNode == null || !DATASET_TITLE.equals(titleNode.asText())) {
        continue;
      }
      JsonNode distributions = dataset.get("distribution");
      if (distributions == null || !distributions.isArray()) {
        continue;
      }
      for (JsonNode dist : distributions) {
        JsonNode formatNode = dist.get("format");
        JsonNode urlNode = dist.get("downloadURL");
        if (formatNode == null || urlNode == null
            || !"CSV".equalsIgnoreCase(formatNode.asText())) {
          continue;
        }
        String temporal = dist.has("temporal") ? dist.get("temporal").asText() : "";
        if (bestUrl == null || temporal.compareTo(bestTemporal) > 0) {
          bestUrl = urlNode.asText();
          bestTemporal = temporal;
        }
      }
      break;
    }
    return bestUrl;
  }

  private String parseCsv(byte[] csvBytes) throws IOException {
    ArrayNode out = MAPPER.createArrayNode();
    try (BufferedReader reader = new BufferedReader(new InputStreamReader(
        new ByteArrayInputStream(csvBytes), StandardCharsets.UTF_8))) {
      String headerLine = CsvRecordReader.readRecord(reader);
      if (headerLine == null) {
        return "[]";
      }
      List<String> headers = CsvRecordReader.splitFields(headerLine, ',');

      String line;
      while ((line = CsvRecordReader.readRecord(reader)) != null) {
        if (line.trim().isEmpty()) {
          continue;
        }
        List<String> values = CsvRecordReader.splitFields(line, ',');
        ObjectNode row = MAPPER.createObjectNode();
        mapRow(headers, values, row);
        out.add(row);
      }
    }
    LOGGER.debug("CMS POS: Parsed {} rows", out.size());
    return out.toString();
  }

  private void mapRow(List<String> headers, List<String> values, ObjectNode row) {
    String providerNumber = col(headers, values, "PRVDR_NUM");
    String categoryCode = col(headers, values, "PRVDR_CTGRY_CD");
    String subtypeCode = col(headers, values, "PRVDR_CTGRY_SBTYP_CD");
    String facilityName = col(headers, values, "FAC_NAME");
    String city = col(headers, values, "CITY_NAME");
    String state = col(headers, values, "STATE_CD");
    String zipCode = col(headers, values, "ZIP_CD");
    String stateFips = col(headers, values, "FIPS_STATE_CD");
    String countyCd = col(headers, values, "FIPS_CNTY_CD");
    String ownershipCode = col(headers, values, "GNRL_CNTL_TYPE_CD");
    String certificationDate = col(headers, values, "CRTFCTN_DT");

    put(row, "provider_number", providerNumber);
    put(row, "provider_category_code", categoryCode);
    put(row, "provider_subtype_code", subtypeCode);
    put(row, "facility_name", facilityName);
    put(row, "city", city);
    put(row, "state", state);
    put(row, "zip_code", zipCode);
    put(row, "state_fips", stateFips);

    String countyFips = null;
    if (stateFips != null && countyCd != null
        && stateFips.length() == 2 && countyCd.length() == 3) {
      countyFips = stateFips + countyCd;
    }
    put(row, "county_fips", countyFips);

    put(row, "ownership_code", ownershipCode);
    put(row, "certification_date", certificationDate);
    putLong(row, "bed_count", col(headers, values, "BED_CNT"));
    putLong(row, "certified_bed_count", col(headers, values, "CRTFD_BED_CNT"));
    putLong(row, "icf_iid_bed_count", col(headers, values, "ICFIID_BED_CNT"));
    putLong(row, "medicaid_nf_bed_count", col(headers, values, "MDCD_NF_BED_CNT"));
    putLong(row, "medicare_snf_bed_count", col(headers, values, "MDCR_SNF_BED_CNT"));
    putLong(row, "medicare_medicaid_snf_bed_count", col(headers, values, "MDCR_MDCD_SNF_BED_CNT"));
    putLong(row, "hospice_bed_count", col(headers, values, "HOSPC_BED_CNT"));
    putLong(row, "rehab_bed_count", col(headers, values, "REHAB_BED_CNT"));
    putLong(row, "psych_unit_bed_count", col(headers, values, "PSYCH_UNIT_BED_CNT"));
    putLong(row, "rehab_unit_bed_count", col(headers, values, "REHAB_UNIT_BED_CNT"));
  }

  private String col(List<String> headers, List<String> values, String name) {
    for (int i = 0; i < headers.size(); i++) {
      if (headers.get(i).equalsIgnoreCase(name)) {
        if (i < values.size()) {
          String v = values.get(i).trim();
          return v.isEmpty() ? null : v;
        }
        return null;
      }
    }
    return null;
  }

  private void put(ObjectNode row, String key, String value) {
    if (value == null) {
      row.putNull(key);
    } else {
      row.put(key, value);
    }
  }

  private void putLong(ObjectNode row, String key, String value) {
    if (value == null) {
      row.putNull(key);
      return;
    }
    try {
      row.put(key, Long.parseLong(value));
    } catch (NumberFormatException e) {
      row.putNull(key);
    }
  }

  private byte[] downloadBytes(String url) throws IOException {
    HttpURLConnection conn = (HttpURLConnection) URI.create(url).toURL().openConnection();
    conn.setConnectTimeout(30000);
    conn.setReadTimeout(180000);
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
}
