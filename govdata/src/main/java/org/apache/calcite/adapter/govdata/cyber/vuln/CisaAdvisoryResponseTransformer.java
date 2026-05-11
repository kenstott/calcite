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

import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

/**
 * Transforms the CISA cybersecurity advisory RSS feed into structured rows,
 * enriched with severity and affected-product data from CSAF JSON files.
 *
 * <p>Parses the RSS XML from {@code https://www.cisa.gov/cybersecurity-advisories/all.xml}.
 * CVE IDs are extracted from the HTML-encoded {@code description} element of each RSS item.
 * For each item, the CSAF JSON URL is extracted from the description's "View CSAF" href,
 * then fetched from {@code raw.githubusercontent.com/cisagov/CSAF/develop/...} to populate
 * severity (from {@code vulnerabilities[].scores[].cvss_v3.baseSeverity}) and
 * affected_systems (from the vendor/product {@code product_tree} hierarchy).
 *
 * <p>CSAF fetch is best-effort: if the URL cannot be resolved or fetched, severity and
 * affected_systems remain null and processing continues for the remaining advisories.
 *
 * <p>Produces rows for the {@code advisories} table:
 * {@code advisory_id}, {@code source}, {@code title}, {@code published_date},
 * {@code severity}, {@code cve_ids}, {@code affected_systems}, {@code url}.
 */
public class CisaAdvisoryResponseTransformer implements ResponseTransformer {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(CisaAdvisoryResponseTransformer.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final int TIMEOUT_MS = 30_000;

  private static final Pattern CVE_PATTERN =
      Pattern.compile("CVE-\\d{4}-\\d{4,7}", Pattern.CASE_INSENSITIVE);

  // Extracts the CSAF blob URL from the HTML-encoded description href
  private static final Pattern CSAF_BLOB_PATTERN =
      Pattern.compile("href=[\"'](https://github\\.com/cisagov/CSAF/blob/[^\"']+\\.json)[\"']",
          Pattern.CASE_INSENSITIVE);

  // RSS pubDate format: "Thu, 30 Apr 26 12:00:00 +0000" or "Thu, 30 Apr 2026 12:00:00 +0000"
  private static final Pattern PUB_DATE_PATTERN =
      Pattern.compile("(\\d{1,2})\\s+(\\w{3})\\s+(\\d{2,4})");

  private static final String[] MONTH_NAMES =
      {"Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"};

  @Override public String transform(String response, RequestContext context) {
    if (response == null || response.trim().isEmpty()) {
      LOGGER.warn("CisaAdvisory: empty response from {}", context.getUrl());
      return "[]";
    }

    ArrayNode rows = MAPPER.createArrayNode();
    try {
      DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
      factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
      DocumentBuilder builder = factory.newDocumentBuilder();
      Document doc = builder.parse(
          new ByteArrayInputStream(response.getBytes(StandardCharsets.UTF_8)));

      NodeList items = doc.getElementsByTagName("item");
      for (int i = 0; i < items.getLength(); i++) {
        Element item = (Element) items.item(i);
        ObjectNode row = parseItem(item);
        if (row != null) {
          rows.add(row);
        }
      }
    } catch (Exception e) {
      LOGGER.error("CisaAdvisory: failed to parse RSS XML: {}", e.getMessage());
      return "[]";
    }

    LOGGER.info("CisaAdvisory: returning {} advisory rows", rows.size());
    return rows.toString();
  }

  private ObjectNode parseItem(Element item) {
    String title = getText(item, "title");
    String link = getText(item, "link");
    String description = getText(item, "description");
    String pubDate = getText(item, "pubDate");

    if (link == null || link.isEmpty()) {
      return null;
    }

    String advisoryId = extractAdvisoryId(link);
    String publishedDate = parseDate(pubDate);
    String decodedDesc = description != null ? htmlDecode(description) : "";

    String severity = null;
    String affectedSystems = null;

    String csafBlobUrl = extractCsafBlobUrl(decodedDesc);
    if (csafBlobUrl != null) {
      String csafRawUrl = blobToRaw(csafBlobUrl);
      JsonNode csaf = fetchCsaf(csafRawUrl);
      if (csaf != null) {
        severity = parseCsafSeverity(csaf);
        affectedSystems = parseCsafAffectedSystems(csaf);
      }
    }

    ObjectNode row = MAPPER.createObjectNode();
    row.put("advisory_id", advisoryId);
    row.put("source", "cisa");
    row.put("title", title);
    row.put("published_date", publishedDate);
    row.put("url", link);
    row.put("cve_ids", extractCveIds(decodedDesc));
    row.put("severity", severity);
    row.put("affected_systems", affectedSystems);

    return row;
  }

  private String extractCsafBlobUrl(String decodedDesc) {
    Matcher m = CSAF_BLOB_PATTERN.matcher(decodedDesc);
    return m.find() ? m.group(1) : null;
  }

  /** Converts a GitHub blob URL to a raw.githubusercontent.com URL. */
  private String blobToRaw(String blobUrl) {
    // https://github.com/cisagov/CSAF/blob/develop/... →
    // https://raw.githubusercontent.com/cisagov/CSAF/develop/...
    return blobUrl
        .replace("https://github.com/", "https://raw.githubusercontent.com/")
        .replace("/blob/", "/");
  }

  private JsonNode fetchCsaf(String url) {
    if (url == null) {
      return null;
    }
    try {
      HttpURLConnection conn = (HttpURLConnection) URI.create(url).toURL().openConnection();
      conn.setInstanceFollowRedirects(true);
      conn.setRequestMethod("GET");
      conn.setConnectTimeout(TIMEOUT_MS);
      conn.setReadTimeout(TIMEOUT_MS);
      conn.setRequestProperty("User-Agent", "calcite-govdata/1.0");
      conn.setRequestProperty("Accept", "application/json");

      int status = conn.getResponseCode();
      if (status != 200) {
        LOGGER.debug("CisaAdvisory: CSAF HTTP {} for {}", status, url);
        conn.disconnect();
        return null;
      }

      StringBuilder sb = new StringBuilder();
      try (BufferedReader reader = new BufferedReader(
          new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8))) {
        String line;
        while ((line = reader.readLine()) != null) {
          sb.append(line);
        }
      }
      conn.disconnect();
      return MAPPER.readTree(sb.toString());
    } catch (Exception e) {
      LOGGER.debug("CisaAdvisory: failed to fetch/parse CSAF from {}: {}", url, e.getMessage());
      return null;
    }
  }

  /**
   * Returns the highest severity found across all vulnerabilities in the CSAF document.
   * Priority: CRITICAL > HIGH > MEDIUM > LOW.
   */
  private String parseCsafSeverity(JsonNode csaf) {
    String best = null;
    JsonNode vulns = csaf.path("vulnerabilities");
    if (!vulns.isArray()) {
      return null;
    }
    for (JsonNode vuln : vulns) {
      JsonNode scores = vuln.path("scores");
      if (!scores.isArray()) {
        continue;
      }
      for (JsonNode score : scores) {
        String sev = textOrNull(score.path("cvss_v3"), "baseSeverity");
        if (sev == null) {
          sev = textOrNull(score.path("cvss_v31"), "baseSeverity");
        }
        if (sev == null) {
          sev = textOrNull(score.path("cvss_v40"), "baseSeverity");
        }
        if (sev != null) {
          best = pickHigher(best, sev.toUpperCase());
        }
      }
    }
    return best;
  }

  private String pickHigher(String current, String candidate) {
    int[] rank = severityRank(current);
    int[] rankC = severityRank(candidate);
    return rankC[0] > rank[0] ? candidate : current;
  }

  private int[] severityRank(String sev) {
    if (sev == null) return new int[]{-1};
    switch (sev) {
      case "CRITICAL": return new int[]{4};
      case "HIGH":     return new int[]{3};
      case "MEDIUM":   return new int[]{2};
      case "LOW":      return new int[]{1};
      default:         return new int[]{0};
    }
  }

  /**
   * Extracts a human-readable affected_systems string from the CSAF product_tree.
   * Walks the top-level vendor branch and collects unique "vendor product" pairs,
   * joined by "; " and truncated to 200 characters.
   */
  private String parseCsafAffectedSystems(JsonNode csaf) {
    JsonNode tree = csaf.path("product_tree");
    if (tree.isMissingNode()) {
      return null;
    }
    JsonNode branches = tree.path("branches");
    if (!branches.isArray() || branches.size() == 0) {
      return null;
    }

    List<String> pairs = new ArrayList<String>();
    for (JsonNode vendorBranch : branches) {
      String vendor = textOrNull(vendorBranch, "name");
      if (vendor == null) {
        continue;
      }
      JsonNode products = vendorBranch.path("branches");
      if (products.isArray() && products.size() > 0) {
        for (JsonNode productBranch : products) {
          String product = textOrNull(productBranch, "name");
          if (product != null) {
            String pair = vendor + " " + product;
            if (!pairs.contains(pair)) {
              pairs.add(pair);
            }
          }
        }
      } else {
        if (!pairs.contains(vendor)) {
          pairs.add(vendor);
        }
      }
    }

    if (pairs.isEmpty()) {
      return null;
    }

    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < pairs.size(); i++) {
      if (i > 0) {
        sb.append("; ");
      }
      sb.append(pairs.get(i));
      if (sb.length() >= 200) {
        break;
      }
    }
    String result = sb.toString();
    return result.length() > 200 ? result.substring(0, 200) : result;
  }

  private String extractAdvisoryId(String url) {
    int lastSlash = url.lastIndexOf('/');
    if (lastSlash >= 0 && lastSlash < url.length() - 1) {
      return url.substring(lastSlash + 1).toUpperCase();
    }
    return url.toUpperCase();
  }

  private String parseDate(String pubDate) {
    if (pubDate == null) {
      return null;
    }
    Matcher m = PUB_DATE_PATTERN.matcher(pubDate);
    if (!m.find()) {
      return null;
    }
    int day = Integer.parseInt(m.group(1));
    String monthStr = m.group(2);
    String yearStr = m.group(3);
    int year = Integer.parseInt(yearStr);
    if (year < 100) {
      year += 2000;
    }
    int month = 0;
    for (int i = 0; i < MONTH_NAMES.length; i++) {
      if (MONTH_NAMES[i].equalsIgnoreCase(monthStr)) {
        month = i + 1;
        break;
      }
    }
    if (month == 0) {
      return null;
    }
    return String.format("%04d-%02d-%02d", year, month, day);
  }

  private String extractCveIds(String text) {
    List<String> ids = new ArrayList<String>();
    Matcher m = CVE_PATTERN.matcher(text);
    while (m.find()) {
      String id = m.group().toUpperCase();
      if (!ids.contains(id)) {
        ids.add(id);
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

  private String getText(Element parent, String tagName) {
    NodeList nl = parent.getElementsByTagName(tagName);
    if (nl.getLength() == 0) {
      return null;
    }
    String text = nl.item(0).getTextContent();
    return text == null || text.isEmpty() ? null : text.trim();
  }

  private String htmlDecode(String s) {
    return s.replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&amp;", "&")
        .replace("&quot;", "\"")
        .replace("&#39;", "'")
        .replace("&apos;", "'");
  }

  private static String textOrNull(JsonNode node, String field) {
    if (node == null || node.isMissingNode()) {
      return null;
    }
    JsonNode v = node.get(field);
    if (v == null || v.isNull() || v.isMissingNode()) {
      return null;
    }
    String t = v.asText();
    return t.isEmpty() ? null : t;
  }
}
