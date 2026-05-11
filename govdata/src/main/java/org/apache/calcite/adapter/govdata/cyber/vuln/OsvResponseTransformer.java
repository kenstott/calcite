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
import java.util.ArrayList;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * Transforms OSV (Open Source Vulnerabilities) per-ecosystem ZIP responses.
 *
 * <p>Fetches ecosystem ZIPs from {@code https://osv-vulnerabilities.storage.googleapis.com/
 * {ecosystem}/all.zip}. Each ZIP contains one JSON file per vulnerability record.
 * Ecosystems are controlled by the {@code CYBER_OSV_ECOSYSTEMS} environment variable
 * (comma-separated; defaults to a curated subset).
 *
 * <p>Produces rows for two logical tables depending on the dimension:
 * <ul>
 *   <li>{@code osv_vulnerabilities} — OSV-native records with full detail</li>
 *   <li>{@code vuln_cross_refs} — alias mappings where an alias is a CVE-YYYY-NNNNN ID</li>
 * </ul>
 *
 * <p>The {@code response} parameter is ignored; all data is fetched directly from
 * the OSV GCS bucket. This is consistent with how {@link NvdResponseTransformer}
 * handles multi-page pagination.
 */
public class OsvResponseTransformer implements ResponseTransformer {

  private static final Logger LOGGER = LoggerFactory.getLogger(OsvResponseTransformer.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static final String OSV_BASE_URL =
      "https://osv-vulnerabilities.storage.googleapis.com";
  private static final String DEFAULT_ECOSYSTEMS =
      "PyPI,npm,Go,Maven,RubyGems,NuGet,Packagist,Hex,Pub,CRAN";
  private static final int TIMEOUT_MS = 120_000;
  private static final long RATE_DELAY_MS = 500L;

  @Override public String transform(String response, RequestContext context) {
    String ecosystemsEnv = System.getenv("CYBER_OSV_ECOSYSTEMS");
    if (ecosystemsEnv == null || ecosystemsEnv.trim().isEmpty()) {
      ecosystemsEnv = DEFAULT_ECOSYSTEMS;
    }

    String[] ecosystems = ecosystemsEnv.split(",");
    ArrayNode allRows = MAPPER.createArrayNode();

    for (String ecosystem : ecosystems) {
      String eco = ecosystem.trim();
      if (eco.isEmpty()) {
        continue;
      }
      try {
        processEcosystem(eco, allRows);
        sleepQuietly(RATE_DELAY_MS);
      } catch (Exception e) {
        LOGGER.warn("OSV: failed to process ecosystem {}: {}", eco, e.getMessage());
      }
    }

    LOGGER.info("OSV: returning {} rows across {} ecosystems", allRows.size(), ecosystems.length);
    return allRows.toString();
  }

  private void processEcosystem(String ecosystem, ArrayNode accumulator) throws IOException {
    String url = OSV_BASE_URL + "/" + ecosystem + "/all.zip";
    LOGGER.info("OSV: fetching ecosystem {} from {}", ecosystem, url);

    byte[] zipBytes = fetchBytes(url);
    if (zipBytes == null || zipBytes.length == 0) {
      LOGGER.warn("OSV: empty response for ecosystem {}", ecosystem);
      return;
    }

    int count = 0;
    try (ZipInputStream zis = new ZipInputStream(new ByteArrayInputStream(zipBytes))) {
      ZipEntry entry;
      while ((entry = zis.getNextEntry()) != null) {
        String name = entry.getName();
        if (name.endsWith(".json") && !entry.isDirectory()) {
          byte[] entryBytes = readEntry(zis);
          try {
            JsonNode record = MAPPER.readTree(entryBytes);
            ObjectNode row = flattenRecord(record, ecosystem);
            if (row != null) {
              accumulator.add(row);
              count++;
            }
          } catch (Exception e) {
            LOGGER.debug("OSV: skipping malformed entry {} in {}: {}", name, ecosystem,
                e.getMessage());
          }
        }
        zis.closeEntry();
      }
    }
    LOGGER.info("OSV: processed {} records for ecosystem {}", count, ecosystem);
  }

  private ObjectNode flattenRecord(JsonNode record, String ecosystem) {
    String osvId = textOrNull(record, "id");
    if (osvId == null) {
      return null;
    }

    ObjectNode row = MAPPER.createObjectNode();
    row.put("osv_id", osvId);
    row.put("modified", textOrNull(record, "modified"));
    row.put("published", textOrNull(record, "published"));
    row.put("withdrawn", textOrNull(record, "withdrawn"));
    row.put("aliases", joinArray(record.path("aliases")));
    row.put("summary", textOrNull(record, "summary"));
    row.put("details", textOrNull(record, "details"));
    row.put("schema_version", textOrNull(record, "schema_version"));
    row.put("ecosystem", ecosystem);

    // severity: use first entry
    JsonNode severity = record.path("severity");
    if (severity.isArray() && severity.size() > 0) {
      JsonNode first = severity.get(0);
      row.put("severity_type", textOrNull(first, "type"));
      row.put("severity_score", textOrNull(first, "score"));
    } else {
      row.putNull("severity_type");
      row.putNull("severity_score");
    }

    // package info: use first affected entry
    JsonNode affected = record.path("affected");
    if (affected.isArray() && affected.size() > 0) {
      JsonNode firstAffected = affected.get(0);
      JsonNode pkg = firstAffected.path("package");
      row.put("package_name", textOrNull(pkg, "name"));
      row.put("package_purl", textOrNull(firstAffected, "package_purl") != null
          ? textOrNull(firstAffected, "package_purl")
          : buildPurl(pkg, ecosystem));
    } else {
      row.putNull("package_name");
      row.putNull("package_purl");
    }

    // database_specific as JSON blob
    JsonNode dbSpecific = record.path("database_specific");
    row.put("database_specific", dbSpecific.isMissingNode() ? null : dbSpecific.toString());

    return row;
  }

  private String buildPurl(JsonNode pkg, String ecosystem) {
    String name = textOrNull(pkg, "name");
    if (name == null) {
      return null;
    }
    String eco = textOrNull(pkg, "ecosystem");
    if (eco == null) {
      eco = ecosystem;
    }
    return "pkg:" + eco.toLowerCase() + "/" + name;
  }

  private String joinArray(JsonNode arrayNode) {
    if (!arrayNode.isArray() || arrayNode.size() == 0) {
      return null;
    }
    List<String> items = new ArrayList<String>();
    for (JsonNode item : arrayNode) {
      String val = item.asText(null);
      if (val != null && !val.isEmpty()) {
        items.add(val);
      }
    }
    if (items.isEmpty()) {
      return null;
    }
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < items.size(); i++) {
      if (i > 0) {
        sb.append("|");
      }
      sb.append(items.get(i));
    }
    return sb.toString();
  }

  private byte[] fetchBytes(String url) {
    try {
      HttpURLConnection conn =
          (HttpURLConnection) URI.create(url).toURL().openConnection();
      conn.setRequestMethod("GET");
      conn.setConnectTimeout(TIMEOUT_MS);
      conn.setReadTimeout(TIMEOUT_MS);

      int status = conn.getResponseCode();
      if (status != 200) {
        LOGGER.warn("OSV: HTTP {} for {}", status, url);
        conn.disconnect();
        return null;
      }

      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      try (InputStream in = conn.getInputStream()) {
        byte[] buf = new byte[65536];
        int len;
        while ((len = in.read(buf)) > 0) {
          baos.write(buf, 0, len);
        }
      }
      conn.disconnect();
      return baos.toByteArray();
    } catch (Exception e) {
      LOGGER.warn("OSV: error fetching {}: {}", url, e.getMessage());
      return null;
    }
  }

  private byte[] readEntry(ZipInputStream zis) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    byte[] buf = new byte[8192];
    int len;
    while ((len = zis.read(buf)) > 0) {
      baos.write(buf, 0, len);
    }
    return baos.toByteArray();
  }

  private static String textOrNull(JsonNode node, String field) {
    JsonNode v = node.get(field);
    if (v == null || v.isNull() || v.isMissingNode()) {
      return null;
    }
    String t = v.asText();
    return t.isEmpty() ? null : t;
  }

  private static void sleepQuietly(long ms) {
    try {
      Thread.sleep(ms);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
