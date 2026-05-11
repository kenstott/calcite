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

import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.text.PDFTextStripper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Utility for extracting text and IOC patterns from PDF security advisories.
 *
 * <p>Uses Apache PDFBox 2.x to strip text from PDF files, then applies regex
 * patterns to identify common IOC types:
 * <ul>
 *   <li>CVE identifiers (e.g. {@code CVE-2021-44228})</li>
 *   <li>IPv4 addresses</li>
 *   <li>Domain names (basic pattern)</li>
 * </ul>
 *
 * <p>This class is shared across later Phase implementations that process
 * PDF-format advisories (CISA AA advisories, ICS-CERT, vendor bulletins).
 */
public class PdfAdvisoryExtractor {

  private static final Logger LOGGER = LoggerFactory.getLogger(PdfAdvisoryExtractor.class);

  private static final Pattern CVE_PATTERN =
      Pattern.compile("CVE-\\d{4}-\\d{4,7}", Pattern.CASE_INSENSITIVE);

  private static final Pattern IPV4_PATTERN =
      Pattern.compile(
          "\\b(?:(?:25[0-5]|2[0-4]\\d|[01]?\\d\\d?)\\.){3}"
              + "(?:25[0-5]|2[0-4]\\d|[01]?\\d\\d?)\\b");

  // Minimal domain pattern: at least one label.tld, excluding IP-like strings
  private static final Pattern DOMAIN_PATTERN =
      Pattern.compile(
          "\\b(?!(?:\\d+\\.){3}\\d+\\b)"
              + "(?:[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?\\.)"
              + "+(?:com|net|org|gov|edu|io|info|biz|co|uk|de|ru|cn|jp|br|au|fr)"
              + "\\b",
          Pattern.CASE_INSENSITIVE);

  /**
   * Extracts all text from a PDF input stream.
   *
   * @param pdfStream PDF byte stream (caller is responsible for closing)
   * @return Full extracted text, or empty string on failure
   */
  public String extractText(InputStream pdfStream) {
    PDDocument doc = null;
    try {
      doc = PDDocument.load(pdfStream);
      PDFTextStripper stripper = new PDFTextStripper();
      return stripper.getText(doc);
    } catch (IOException e) {
      LOGGER.error("PdfAdvisoryExtractor: failed to extract text: {}", e.getMessage());
      return "";
    } finally {
      closeSilently(doc);
    }
  }

  /**
   * Extracts CVE identifiers found in the given text.
   *
   * @param text Text to scan
   * @return Deduplicated list of CVE IDs in upper case, in order of first appearance
   */
  public List<String> extractCveIds(String text) {
    Set<String> seen = new LinkedHashSet<String>();
    Matcher m = CVE_PATTERN.matcher(text);
    while (m.find()) {
      seen.add(m.group().toUpperCase());
    }
    return new ArrayList<String>(seen);
  }

  /**
   * Extracts IPv4 addresses found in the given text.
   *
   * @param text Text to scan
   * @return Deduplicated list of IP addresses in order of first appearance
   */
  public List<String> extractIpAddresses(String text) {
    Set<String> seen = new LinkedHashSet<String>();
    Matcher m = IPV4_PATTERN.matcher(text);
    while (m.find()) {
      seen.add(m.group());
    }
    return new ArrayList<String>(seen);
  }

  /**
   * Extracts domain names found in the given text.
   *
   * <p>Uses a conservative pattern that only matches common TLDs to reduce
   * false positives from prose text.
   *
   * @param text Text to scan
   * @return Deduplicated list of domain names in order of first appearance
   */
  public List<String> extractDomains(String text) {
    Set<String> seen = new LinkedHashSet<String>();
    Matcher m = DOMAIN_PATTERN.matcher(text);
    while (m.find()) {
      seen.add(m.group().toLowerCase());
    }
    return new ArrayList<String>(seen);
  }

  /**
   * Joins a list of strings with the given delimiter, returning null for empty lists.
   */
  public static String joinOrNull(List<String> items, String delimiter) {
    if (items == null || items.isEmpty()) {
      return null;
    }
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < items.size(); i++) {
      if (i > 0) {
        sb.append(delimiter);
      }
      sb.append(items.get(i));
    }
    return sb.toString();
  }

  private static void closeSilently(PDDocument doc) {
    if (doc != null) {
      try {
        doc.close();
      } catch (IOException e) {
        LOGGER.debug("PdfAdvisoryExtractor: error closing PDDocument: {}", e.getMessage());
      }
    }
  }
}
