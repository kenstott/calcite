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
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * Transforms MITRE CVE daily delta ZIP responses from the CVEProject GitHub releases.
 *
 * <p>Downloads the latest delta ZIP from
 * {@code https://github.com/CVEProject/cvelistV5/releases/latest/download/delta.zip}.
 * Each entry in the ZIP is a JSON file following the CVE JSON 5.0 schema. Only records
 * where the CVE ID is present and maps to at least one external reference are emitted.
 *
 * <p>Produces rows for the {@code vuln_cross_refs} table:
 * {@code cve_id}, {@code external_id}, {@code external_source}, {@code url}.
 *
 * <p>The {@code response} parameter is ignored; the ZIP is fetched directly from GitHub.
 * This ensures the transformer always operates on the current delta, not a cached copy.
 */
public class MitreCveResponseTransformer implements ResponseTransformer {

  private static final Logger LOGGER = LoggerFactory.getLogger(MitreCveResponseTransformer.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  /** Queries the GitHub releases API to find the date-stamped delta ZIP asset URL. */
  private String resolveDeltaZipUrl() {
    try {
      HttpURLConnection conn =
          (HttpURLConnection) URI.create(RELEASES_API_URL).toURL().openConnection();
      conn.setRequestMethod("GET");
      conn.setConnectTimeout(TIMEOUT_MS);
      conn.setReadTimeout(TIMEOUT_MS);
      conn.setRequestProperty("Accept", "application/vnd.github+json");
      conn.setRequestProperty("User-Agent", "calcite-govdata/1.0");

      // Optional: use GitHub token if provided to avoid rate limits
      String token = System.getenv("CYBER_GITHUB_TOKEN");
      if (token != null && !token.isEmpty()) {
        conn.setRequestProperty("Authorization", "bearer " + token);
      }

      if (conn.getResponseCode() != 200) {
        conn.disconnect();
        return null;
      }

      BufferedReader reader = new BufferedReader(
          new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8));
      StringBuilder sb = new StringBuilder();
      String line;
      while ((line = reader.readLine()) != null) {
        sb.append(line);
      }
      reader.close();
      conn.disconnect();

      JsonNode release = MAPPER.readTree(sb.toString());
      for (JsonNode asset : release.path("assets")) {
        String name = asset.path("name").asText("");
        if (name.contains("delta") && name.endsWith(".zip")) {
          return asset.path("browser_download_url").asText(null);
        }
      }
      LOGGER.warn("MitreCve: no delta ZIP asset found in latest release");
      return null;
    } catch (Exception e) {
      LOGGER.warn("MitreCve: error resolving delta ZIP URL: {}", e.getMessage());
      return null;
    }
  }

  private static final String RELEASES_API_URL =
      "https://api.github.com/repos/CVEProject/cvelistV5/releases/latest";
  private static final int TIMEOUT_MS = 120_000;

  @Override public String transform(String response, RequestContext context) {
    String deltaZipUrl = resolveDeltaZipUrl();
    if (deltaZipUrl == null) {
      LOGGER.warn("MitreCve: could not resolve delta ZIP URL from GitHub releases API");
      return "[]";
    }
    LOGGER.info("MitreCve: fetching delta ZIP from {}", deltaZipUrl);

    byte[] zipBytes = fetchBytes(deltaZipUrl);
    if (zipBytes == null || zipBytes.length == 0) {
      LOGGER.warn("MitreCve: empty or failed delta ZIP fetch");
      return "[]";
    }

    ArrayNode allRows = MAPPER.createArrayNode();
    try {
      processZip(zipBytes, allRows);
    } catch (Exception e) {
      LOGGER.error("MitreCve: failed to process delta ZIP: {}", e.getMessage());
      throw new RuntimeException("Failed to process MITRE CVE delta ZIP: " + e.getMessage(), e);
    }

    LOGGER.info("MitreCve: returning {} vuln_cross_refs rows", allRows.size());
    return allRows.toString();
  }

  private void processZip(byte[] zipBytes, ArrayNode accumulator) throws IOException {
    int count = 0;
    try (ZipInputStream zis = new ZipInputStream(new ByteArrayInputStream(zipBytes))) {
      ZipEntry entry;
      while ((entry = zis.getNextEntry()) != null) {
        String name = entry.getName();
        if (!entry.isDirectory() && name.endsWith(".json")) {
          byte[] entryBytes = readEntry(zis);
          try {
            JsonNode record = MAPPER.readTree(entryBytes);
            addCrossRefs(record, accumulator);
            count++;
          } catch (Exception e) {
            LOGGER.debug("MitreCve: skipping malformed entry {}: {}", name, e.getMessage());
          }
        }
        zis.closeEntry();
      }
    }
    LOGGER.info("MitreCve: processed {} CVE JSON files from delta ZIP", count);
  }

  private void addCrossRefs(JsonNode record, ArrayNode accumulator) {
    // CVE JSON 5.0: root has cveMetadata.cveId and containers.cna.references
    JsonNode meta = record.path("cveMetadata");
    String cveId = textOrNull(meta, "cveId");
    if (cveId == null || !cveId.startsWith("CVE-")) {
      return;
    }

    // cna references
    JsonNode cna = record.path("containers").path("cna");
    addRefsFromNode(cveId, cna.path("references"), accumulator);

    // adp references (additional data providers)
    JsonNode adpArray = record.path("containers").path("adp");
    if (adpArray.isArray()) {
      for (JsonNode adp : adpArray) {
        addRefsFromNode(cveId, adp.path("references"), accumulator);
      }
    }
  }

  private void addRefsFromNode(String cveId, JsonNode references, ArrayNode accumulator) {
    if (!references.isArray()) {
      return;
    }
    for (JsonNode ref : references) {
      String url = textOrNull(ref, "url");
      if (url == null) {
        continue;
      }
      String source = inferSource(url);
      ObjectNode row = MAPPER.createObjectNode();
      row.put("cve_id", cveId);
      row.put("external_id", url);
      row.put("external_source", source);
      row.put("url", url);
      accumulator.add(row);
    }
  }

  private String inferSource(String url) {
    if (url.contains("github.com") || url.contains("github.advisory")) {
      return "github";
    }
    if (url.contains("nvd.nist.gov")) {
      return "nvd";
    }
    if (url.contains("cisa.gov")) {
      return "cisa";
    }
    if (url.contains("mitre.org")) {
      return "mitre";
    }
    return "cve";
  }

  private byte[] fetchBytes(String url) {
    try {
      // Follow redirects (GitHub releases redirect to CDN)
      HttpURLConnection conn =
          (HttpURLConnection) URI.create(url).toURL().openConnection();
      conn.setInstanceFollowRedirects(true);
      conn.setRequestMethod("GET");
      conn.setConnectTimeout(TIMEOUT_MS);
      conn.setReadTimeout(TIMEOUT_MS);
      conn.setRequestProperty("User-Agent", "calcite-govdata/1.0");

      int status = conn.getResponseCode();
      if (status != 200) {
        LOGGER.warn("MitreCve: HTTP {} fetching delta ZIP", status);
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
      LOGGER.warn("MitreCve: error fetching delta ZIP: {}", e.getMessage());
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
}
