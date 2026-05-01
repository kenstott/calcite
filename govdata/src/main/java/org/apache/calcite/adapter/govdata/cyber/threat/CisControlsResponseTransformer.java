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
 * Fetches and parses CIS Controls v8 safeguard RST files from the
 * CISecurity/ControlsAssessmentSpecification GitHub repository.
 *
 * <p>Discovery: for each of the 18 controls, fetches the {@code index.rst}
 * and parses the toctree to enumerate safeguard file names. Then fetches each
 * safeguard RST and parses:
 * <ul>
 *   <li>{@code safeguard_id} — "N.M" from title</li>
 *   <li>{@code title} — name after "N.M: "</li>
 *   <li>{@code description} — first prose paragraph</li>
 *   <li>{@code asset_type} — from list-table data row column 1</li>
 *   <li>{@code security_function} — from list-table data row column 2</li>
 *   <li>{@code ig_group} — implementation groups from list-table column 3</li>
 * </ul>
 *
 * <p>The {@code response} parameter is ignored; all data is fetched from GitHub.
 * CIS Controls v8 has 18 controls and ~153 safeguards total.
 */
public class CisControlsResponseTransformer implements ResponseTransformer {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(CisControlsResponseTransformer.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static final String BASE_URL =
      "https://raw.githubusercontent.com/CISecurity/ControlsAssessmentSpecification/master/";
  private static final int NUM_CONTROLS = 18;
  private static final int TIMEOUT_MS = 30_000;
  private static final long RATE_DELAY_MS = 200L;

  // Matches toctree entries like "   control-1.1" or "   control-1.10"
  private static final Pattern TOCTREE_ENTRY =
      Pattern.compile("^\\s+(control-\\d+\\.\\d+)\\s*$", Pattern.MULTILINE);

  // RST title: "N.M: Safeguard Name" followed by === underline
  private static final Pattern TITLE_PATTERN =
      Pattern.compile("^(\\d+\\.\\d+):\\s+(.+?)\\s*\\n[=]+", Pattern.MULTILINE);

  // list-table data row: "   * - Value" after the header row
  // We look for the second "   * - " block (data row vs header row)
  private static final Pattern LIST_TABLE_PATTERN =
      Pattern.compile(
          "\\.\\. list-table::[^\\n]*\\n(?:[ \\t]+:[^\\n]*\\n)*[ \\t]*\\n"
          + "[ \\t]+\\*[ \\t]+-[ \\t]+[^\\n]*\\n"  // header row start
          + "(?:[ \\t]+-[ \\t]+[^\\n]*\\n)*"         // remaining header cells
          + "[ \\t]+\\*[ \\t]+-[ \\t]+([^\\n]*?)\\s*\\n"  // data row col 1
          + "[ \\t]+-[ \\t]+([^\\n]*?)\\s*\\n"             // data row col 2
          + "[ \\t]+-[ \\t]+([^\\n]*?)\\s*\\n",            // data row col 3
          Pattern.DOTALL);

  // Control-level title index.rst: "CIS Control N: Title" on first heading
  private static final Pattern CONTROL_TITLE_PATTERN =
      Pattern.compile("^CIS Control \\d+:\\s+(.+?)\\s*\\n[=]+", Pattern.MULTILINE);

  @Override public String transform(String response, RequestContext context) {
    try {
      ByteArrayOutputStream baos = new ByteArrayOutputStream(512 * 1024);
      JsonGenerator gen = MAPPER.getFactory().createGenerator(baos);
      gen.writeStartArray();

      int[] count = {0};
      for (int controlNum = 1; controlNum <= NUM_CONTROLS; controlNum++) {
        processControl(controlNum, gen, count);
        sleepQuietly(RATE_DELAY_MS);
      }

      gen.writeEndArray();
      gen.close();

      LOGGER.info("CisControls: returning {} safeguard rows", count[0]);
      return baos.toString("UTF-8");

    } catch (Exception e) {
      LOGGER.error("CisControls: failed: {}", e.getMessage());
      throw new RuntimeException("Failed to process CIS Controls: " + e.getMessage(), e);
    }
  }

  private void processControl(int controlNum, JsonGenerator gen, int[] count) throws Exception {
    String indexUrl = BASE_URL + "control-" + controlNum + "/index.rst";
    String indexRst = fetchText(indexUrl);
    if (indexRst == null) {
      LOGGER.warn("CisControls: failed to fetch index for control {}", controlNum);
      return;
    }

    String controlTitle = extractControlTitle(indexRst);

    // Discover safeguards from toctree
    List<String> safeguardFiles = new ArrayList<String>();
    Matcher toc = TOCTREE_ENTRY.matcher(indexRst);
    while (toc.find()) {
      safeguardFiles.add(toc.group(1));
    }

    if (safeguardFiles.isEmpty()) {
      LOGGER.warn("CisControls: no safeguards found in toctree for control {}", controlNum);
      return;
    }

    for (String safeguardFile : safeguardFiles) {
      String safeguardUrl = BASE_URL + "control-" + controlNum + "/" + safeguardFile + ".rst";
      String safeguardRst = fetchText(safeguardUrl);
      if (safeguardRst == null) {
        LOGGER.debug("CisControls: failed to fetch {}", safeguardUrl);
        continue;
      }

      ObjectNode row = parseSafeguard(safeguardRst, controlNum, controlTitle);
      if (row != null) {
        gen.writeTree(row);
        count[0]++;
      }
      sleepQuietly(RATE_DELAY_MS);
    }
  }

  private ObjectNode parseSafeguard(String rst, int controlNum, String controlTitle) {
    Matcher titleMatcher = TITLE_PATTERN.matcher(rst);
    if (!titleMatcher.find()) {
      return null;
    }

    String safeguardId = titleMatcher.group(1);
    String safeguardTitle = titleMatcher.group(2).trim();

    // Description: first non-empty paragraph after the title underline
    String afterTitle = rst.substring(titleMatcher.end()).trim();
    String description = extractFirstParagraph(afterTitle);

    // Asset type, security function, IG from list-table
    String assetType = null;
    String securityFunction = null;
    String igGroup = null;

    Matcher tableMatcher = LIST_TABLE_PATTERN.matcher(rst);
    if (tableMatcher.find()) {
      assetType = tableMatcher.group(1).trim();
      securityFunction = tableMatcher.group(2).trim();
      igGroup = tableMatcher.group(3).trim();
    }

    ObjectNode row = MAPPER.createObjectNode();
    row.put("control_id", String.valueOf(controlNum));
    row.put("control_title", controlTitle);
    row.put("safeguard_id", safeguardId);
    row.put("title", safeguardTitle);
    row.put("description", truncate(description, 2000));
    row.put("asset_type", emptyToNull(assetType));
    row.put("security_function", emptyToNull(securityFunction));
    row.put("ig_group", emptyToNull(igGroup));
    row.put("version", "v8");
    row.put("source", "cis-controls");
    return row;
  }

  /**
   * Extracts the control-level title from the index.rst heading.
   * Looks for "CIS Control N: Title" as the first RST heading.
   */
  private static String extractControlTitle(String rst) {
    Matcher m = CONTROL_TITLE_PATTERN.matcher(rst);
    return m.find() ? m.group(1).trim() : null;
  }

  /**
   * Extracts the first non-empty paragraph (lines up to the first blank line,
   * skipping RST directives that start with "..").
   */
  private static String extractFirstParagraph(String text) {
    String[] lines = text.split("\\n");
    StringBuilder sb = new StringBuilder();
    boolean inParagraph = false;

    for (String line : lines) {
      String trimmed = line.trim();
      if (trimmed.isEmpty()) {
        if (inParagraph) {
          break;
        }
        continue;
      }
      if (trimmed.startsWith("..") || trimmed.startsWith(":")) {
        if (inParagraph) {
          break;
        }
        continue;
      }
      // Skip RST underline lines (all same char)
      if (trimmed.matches("^[=\\-~^]+$")) {
        continue;
      }
      inParagraph = true;
      if (sb.length() > 0) {
        sb.append(" ");
      }
      sb.append(trimmed);
    }

    String result = sb.toString().trim();
    return result.isEmpty() ? null : result;
  }

  private static String truncate(String text, int maxLen) {
    if (text == null) {
      return null;
    }
    return text.length() > maxLen ? text.substring(0, maxLen) : text;
  }

  private static String emptyToNull(String s) {
    if (s == null || s.isEmpty()) {
      return null;
    }
    return s;
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
      LOGGER.debug("CisControls: error fetching {}: {}", url, e.getMessage());
      return null;
    }
  }

  private static void sleepQuietly(long ms) {
    try {
      Thread.sleep(ms);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
