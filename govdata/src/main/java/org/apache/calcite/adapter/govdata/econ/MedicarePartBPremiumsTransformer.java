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
package org.apache.calcite.adapter.govdata.econ;

import org.apache.calcite.adapter.file.etl.NetworkOutageWait;
import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.StreamingResponseTransformer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Builds the Medicare Part B standard monthly premium and annual deductible history from the
 * annual CMS notice "Medicare Program; Medicare Part B Monthly Actuarial Rates, Premium Rate(s),
 * and Annual Deductible" in the Federal Register. The configured source URL is the keyless
 * Federal Register documents API search for that notice; the transformer reads each notice's
 * plain text and extracts the premium and deductible sentences. Correction notices and the
 * quarterly program-issuance listings that mention the same phrase are skipped. A notice whose
 * premium or deductible cannot be extracted, or a year published twice, fails the run.
 */
public class MedicarePartBPremiumsTransformer implements StreamingResponseTransformer {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final int TIMEOUT_MS = 120_000;
  /** A notice is about 1 MB of text; anything beyond this cap is not the notice we expect. */
  private static final int MAX_BYTES = 16 * 1024 * 1024;

  private static final Pattern TITLE_YEAR =
      Pattern.compile("(?:Beginning January 1,|Calendar Year) (\\d{4})");
  private static final String MONEY = "\\$([0-9][0-9,]*\\.\\d\\d)";
  /** "standard monthly premium rate for [all enrollees for] 2026 is $202.90" (2008 onward). */
  private static final Pattern PREMIUM_STANDARD = Pattern.compile(
      "standard monthly premium rate for (?:all enrollees for )?(\\d{4}) is " + MONEY);
  /** "monthly Part B premium rate for 2005 is $78.20" / "Part B monthly premium rate for 2006 is". */
  private static final Pattern PREMIUM_PLAIN = Pattern.compile(
      "(?:monthly Part B|Part B monthly) premium rate for (\\d{4}) is " + MONEY);
  /** 2007 notice: "premium rates to be paid in 2007, including ..., are $93.50 (the standard premium)". */
  private static final Pattern PREMIUM_LIST = Pattern.compile(
      "premium rates to be paid in (\\d{4}), including the income-related monthly adjustment "
          + "amounts, are " + MONEY + " \\(the standard premium\\)");
  private static final Pattern DEDUCTIBLE = Pattern.compile(
      "deductible for (?:calendar year )?(\\d{4}) is " + MONEY);

  @Override public String transform(String response, RequestContext context) {
    throw new UnsupportedOperationException(
        "medicare_part_b_premiums is a StreamingResponseTransformer; call fetchAndTransform");
  }

  @Override public Iterator<Map<String, Object>> fetchAndTransform(RequestContext context)
      throws IOException {
    String url = context.getUrl();
    JsonNode results = MAPPER.readTree(get(url)).path("results");
    if (!results.isArray() || results.size() == 0) {
      throw new IllegalStateException("medicare_part_b_premiums: no results from " + url);
    }
    Map<Integer, Map<String, Object>> byYear = new LinkedHashMap<Integer, Map<String, Object>>();
    for (JsonNode doc : results) {
      String title = doc.path("title").asText();
      if (!isAnnualNotice(title)) {
        continue;
      }
      String docNumber = doc.path("document_number").asText();
      Map<String, Object> row =
          parseNotice(title, docNumber, doc.path("publication_date").asText(),
              get(doc.path("raw_text_url").asText()));
      Integer year = (Integer) row.get("year");
      if (byYear.put(year, row) != null) {
        throw new IllegalStateException("medicare_part_b_premiums: more than one annual notice "
            + "for " + year + " (second: " + docNumber + ")");
      }
    }
    if (byYear.isEmpty()) {
      throw new IllegalStateException("medicare_part_b_premiums: no annual notices in " + url);
    }
    return new ArrayList<Map<String, Object>>(byYear.values()).iterator();
  }

  static boolean isAnnualNotice(String title) {
    return title.contains("Medicare Part B Monthly Actuarial Rates")
        && !title.contains("Correction") && !title.contains("Hospice")
        && !title.contains("Quarterly Listing");
  }

  static Map<String, Object> parseNotice(String title, String docNumber, String published,
      String text) {
    Matcher ty = TITLE_YEAR.matcher(title);
    if (!ty.find()) {
      throw new IllegalStateException("medicare_part_b_premiums: no year in title of " + docNumber
          + ": " + title);
    }
    int year = Integer.parseInt(ty.group(1));
    String flat = text.replaceAll("\\s+", " ");
    Double premium = firstFor(year, flat, PREMIUM_STANDARD, PREMIUM_PLAIN, PREMIUM_LIST);
    Double deductible = firstFor(year, flat, DEDUCTIBLE);
    if (premium == null || deductible == null) {
      throw new IllegalStateException("medicare_part_b_premiums: notice " + docNumber + " for "
          + year + " has no extractable " + (premium == null ? "standard premium" : "deductible"));
    }
    Map<String, Object> row = new LinkedHashMap<String, Object>();
    row.put("year", year);
    row.put("standard_premium", premium);
    row.put("annual_deductible", deductible);
    row.put("notice_document_number", docNumber);
    row.put("notice_publication_date", published);
    return row;
  }

  private static Double firstFor(int year, String flat, Pattern... patterns) {
    for (Pattern p : patterns) {
      Matcher m = p.matcher(flat);
      while (m.find()) {
        if (Integer.parseInt(m.group(1)) == year) {
          return Double.valueOf(m.group(2).replace(",", ""));
        }
      }
    }
    return null;
  }

  private static String get(String url) throws IOException {
    HttpURLConnection conn = (HttpURLConnection) URI.create(url).toURL().openConnection();
    try {
      conn.setConnectTimeout(TIMEOUT_MS);
      conn.setReadTimeout(TIMEOUT_MS);
      conn.setRequestProperty("User-Agent", "GovData/1.0");
      NetworkOutageWait.connect(conn);
      int status = conn.getResponseCode();
      if (status != HttpURLConnection.HTTP_OK) {
        throw new IOException("HTTP " + status + " from " + url);
      }
      try (InputStream in = conn.getInputStream()) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        byte[] buf = new byte[8192];
        int n;
        while ((n = in.read(buf)) > 0) {
          if (out.size() + n > MAX_BYTES) {
            throw new IOException("response from " + url + " exceeds " + MAX_BYTES + " bytes");
          }
          out.write(buf, 0, n);
        }
        return new String(out.toByteArray(), StandardCharsets.UTF_8);
      }
    } finally {
      conn.disconnect();
    }
  }
}
