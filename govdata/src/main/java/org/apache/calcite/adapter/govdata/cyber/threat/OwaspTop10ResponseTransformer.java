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
package org.apache.calcite.adapter.govdata.cyber.threat;

import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.ResponseTransformer;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Fetches and parses the OWASP Top 10 2021 Markdown files from GitHub into
 * flat {@code owasp_top10} rows — one row per entry.
 *
 * <p>There is no canonical JSON for OWASP Top 10; data is parsed from 10 Markdown files at:
 * {@code https://raw.githubusercontent.com/OWASP/Top10/master/2021/docs/en/A{NN}_2021-*.md}
 *
 * <p>Parsed per entry:
 * <ul>
 *   <li>{@code rank} — NN (01–10) from filename</li>
 *   <li>{@code entry_id} — "A{NN}:2021" from H1 heading</li>
 *   <li>{@code name} — title after the em-dash in the H1</li>
 *   <li>{@code overview} — prose from the Overview section</li>
 *   <li>{@code cwe_ids} — pipe-delimited CWE numbers found inline (e.g., CWE-200)</li>
 *   <li>{@code year} — 2021</li>
 * </ul>
 *
 * <p>The {@code response} parameter is ignored; all 10 files are fetched directly.
 */
public class OwaspTop10ResponseTransformer implements ResponseTransformer {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(OwaspTop10ResponseTransformer.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static final String BASE_URL =
      "https://raw.githubusercontent.com/OWASP/Top10/master/2021/docs/en/";
  private static final int TIMEOUT_MS = 30_000;

  private static final String[] ENTRY_FILES = {
      "A01_2021-Broken_Access_Control.md",
      "A02_2021-Cryptographic_Failures.md",
      "A03_2021-Injection.md",
      "A04_2021-Insecure_Design.md",
      "A05_2021-Security_Misconfiguration.md",
      "A06_2021-Vulnerable_and_Outdated_Components.md",
      "A07_2021-Identification_and_Authentication_Failures.md",
      "A08_2021-Software_and_Data_Integrity_Failures.md",
      "A09_2021-Security_Logging_and_Monitoring_Failures.md",
      "A10_2021-Server-Side_Request_Forgery_(SSRF).md"
  };

  private static final Pattern RANK_PATTERN = Pattern.compile("^A(\\d{2})_");
  private static final Pattern H1_PATTERN =
      Pattern.compile("^#\\s+A(\\d{2}):2021\\s+[–\\-]\\s+(.+)$", Pattern.MULTILINE);
  private static final Pattern SECTION_PATTERN =
      Pattern.compile("^##\\s+(.+)$", Pattern.MULTILINE);
  private static final Pattern CWE_PATTERN =
      Pattern.compile("CWE-?(\\d+)", Pattern.CASE_INSENSITIVE);

  @Override public String transform(String response, RequestContext context) {
    try {
      ByteArrayOutputStream baos = new ByteArrayOutputStream(256 * 1024);
      JsonGenerator gen = MAPPER.getFactory().createGenerator(baos);
      gen.writeStartArray();

      int count = 0;
      for (String filename : ENTRY_FILES) {
        String url = BASE_URL + filename;
        String markdown = fetchText(url);
        if (markdown == null) {
          LOGGER.warn("OWASP: failed to fetch {}", filename);
          continue;
        }

        ObjectNode row = parseEntry(filename, markdown);
        if (row != null) {
          gen.writeTree(row);
          count++;
        }
      }

      gen.writeEndArray();
      gen.close();

      LOGGER.info("OWASP Top 10: returning {} entries", count);
      return baos.toString("UTF-8");

    } catch (Exception e) {
      LOGGER.error("OWASP: failed: {}", e.getMessage());
      throw new RuntimeException("Failed to process OWASP Top 10: " + e.getMessage(), e);
    }
  }

  private ObjectNode parseEntry(String filename, String markdown) {
    // Extract rank from filename (A01_, A02_, ...)
    Matcher rankMatcher = RANK_PATTERN.matcher(filename);
    int rank = rankMatcher.find() ? Integer.parseInt(rankMatcher.group(1)) : 0;

    // Extract entry_id and name from H1 heading
    Matcher h1 = H1_PATTERN.matcher(markdown);
    String entryId = null;
    String name = null;
    if (h1.find()) {
      entryId = "A" + h1.group(1) + ":2021";
      name = h1.group(2).trim();
    }
    if (entryId == null) {
      LOGGER.warn("OWASP: could not parse H1 from {}", filename);
      return null;
    }

    String overview = extractSection(markdown, "Overview");
    String cweIds = extractCweIds(markdown);

    ObjectNode row = MAPPER.createObjectNode();
    row.put("entry_id", entryId);
    row.put("rank", rank);
    row.put("name", name);
    row.put("overview", truncate(overview, 2000));
    row.put("cwe_ids", cweIds);
    row.put("year", 2021);
    row.put("source", "owasp");
    return row;
  }

  /**
   * Extracts prose from a markdown section by heading name, stopping at the next ## heading.
   */
  private static String extractSection(String markdown, String sectionName) {
    // Find the section heading
    Pattern sectionStart = Pattern.compile(
        "^##\\s+" + Pattern.quote(sectionName) + "\\s*$", Pattern.MULTILINE);
    Matcher startMatcher = sectionStart.matcher(markdown);
    if (!startMatcher.find()) {
      return null;
    }
    int contentStart = startMatcher.end();

    // Find the next ## heading
    Matcher nextSection = SECTION_PATTERN.matcher(markdown);
    int contentEnd = markdown.length();
    while (nextSection.find(contentStart)) {
      contentEnd = nextSection.start();
      break;
    }

    String section = markdown.substring(contentStart, contentEnd).trim();
    // Remove markdown formatting: image tags, links, bold, italic
    section = section.replaceAll("!\\[.*?\\]\\(.*?\\)", "");
    section = section.replaceAll("\\[([^\\]]+)\\]\\([^)]+\\)", "$1");
    section = section.replaceAll("[*_]{1,2}([^*_]+)[*_]{1,2}", "$1");
    section = section.replaceAll("\\s+", " ").trim();
    return section.isEmpty() ? null : section;
  }

  /** Extracts unique CWE IDs from the entire markdown, pipe-delimited. */
  private static String extractCweIds(String markdown) {
    Matcher m = CWE_PATTERN.matcher(markdown);
    List<String> ids = new ArrayList<String>();
    while (m.find()) {
      String cweId = "CWE-" + m.group(1);
      if (!ids.contains(cweId)) {
        ids.add(cweId);
      }
    }
    if (ids.isEmpty()) {
      return null;
    }
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < ids.size(); i++) {
      if (i > 0) {
        sb.append("|");
      }
      sb.append(ids.get(i));
    }
    return sb.toString();
  }

  private static String truncate(String text, int maxLen) {
    if (text == null) {
      return null;
    }
    return text.length() > maxLen ? text.substring(0, maxLen) : text;
  }

  private String fetchText(String url) {
    try {
      HttpURLConnection conn =
          (HttpURLConnection) URI.create(url).toURL().openConnection();
      conn.setRequestMethod("GET");
      conn.setConnectTimeout(TIMEOUT_MS);
      conn.setReadTimeout(TIMEOUT_MS);
      conn.setRequestProperty("User-Agent", "calcite-govdata/1.0");

      if (conn.getResponseCode() != 200) {
        conn.disconnect();
        return null;
      }

      BufferedReader reader = new BufferedReader(
          new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8));
      StringBuilder sb = new StringBuilder();
      String line;
      while ((line = reader.readLine()) != null) {
        sb.append(line).append("\n");
      }
      reader.close();
      conn.disconnect();
      return sb.toString();
    } catch (Exception e) {
      LOGGER.warn("OWASP: error fetching {}: {}", url, e.getMessage());
      return null;
    }
  }
}
