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
package org.apache.calcite.adapter.govdata.lands;

import org.apache.calcite.adapter.file.etl.NetworkOutageWait;
import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.ResponseTransformer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.text.PDFTextStripper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Builds {@code nps_deferred_maintenance} rows from the National Park Service
 * "Infrastructure by the Numbers" park fact sheets.
 *
 * <p>NPS publishes park-level deferred maintenance and repairs (DM&amp;R) only as one PDF fact
 * sheet per park unit. The index of those PDFs is a CSV (park name, state list, link) that the
 * table's source URL points at. This transformer reads the index, downloads each fact sheet,
 * extracts page one with PDFBox, and parses the inventory counts, total DM&amp;R, annual routine
 * maintenance and the DM&amp;R split by asset category.
 *
 * <p>Fact sheets print dollar amounts rounded to two significant figures ("$1.3 B", "$149 K").
 * Every parsed row is cross-checked: the nine category amounts must sum to the printed total
 * within rounding tolerance, otherwise parsing throws, so a layout change fails loudly rather
 * than emitting partial rows.
 */
public class NpsDeferredMaintenanceTransformer implements ResponseTransformer {

  private static final Logger LOGGER = LoggerFactory.getLogger(NpsDeferredMaintenanceTransformer.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final int MAX_BYTES = 25 * 1024 * 1024;

  private static final Pattern INDEX_LINK = Pattern.compile("href=\"\"([^\"]+)\"\"");
  private static final Pattern FISCAL_YEAR = Pattern.compile("Fiscal Year \\(FY\\) (\\d{4})");
  private static final Pattern DMR_TOTAL = Pattern.compile(
      "estimated (\\$[\\d.,]+ ?[KMB]?) in Deferred Maintenance and Repairs");
  private static final Pattern ROUTINE = Pattern.compile(
      "estimated (\\$[\\d.,]+ ?[KMB]?) in Annual Routine Maintenance");
  private static final Pattern OTHER_ASSETS = Pattern.compile("also has ([\\d,]+) other assets?");
  private static final Pattern CONDITION_START = Pattern.compile("Park Infrastructure Condition");

  /** Asset category label in the condition paragraph, in output-column order. */
  private static final String[][] CATEGORIES = {
      {"Buildings", "dmr_buildings"},
      {"Trails", "dmr_trails"},
      {"Paved Roads", "dmr_paved_roads"},
      {"Unpaved Roads", "dmr_unpaved_roads"},
      {"Waste Water Systems", "dmr_waste_water_systems"},
      {"Water Systems", "dmr_water_systems"},
      {"Housing", "dmr_housing"},
      {"Campgrounds", "dmr_campgrounds"},
      {"All Other Assets", "dmr_other_assets"},
  };

  /** Inventory line regexes: count, then the output column. */
  private static final String[][] INVENTORY = {
      {"([\\d,]+) Buildings?", "buildings"},
      {"([\\d,.]+) Miles? of Trails?", "trail_miles"},
      {"([\\d,.]+) Miles? of Unpaved Roads?", "unpaved_road_miles"},
      {"([\\d,]+) Water Systems?", "water_systems"},
      {"([\\d,]+) Campgrounds?", "campgrounds"},
      {"([\\d,]+) Housing Units?", "housing_units"},
      {"([\\d,]+) Waste Water Systems?", "waste_water_systems"},
      {"([\\d,.]+) Miles? of Paved Roads?", "paved_road_miles"},
  };

  @Override public String transform(String response, RequestContext context) {
    String indexUrl = context.getUrl();
    try {
      String index = new String(download(indexUrl), StandardCharsets.ISO_8859_1);
      ArrayNode rows = MAPPER.createArrayNode();
      String[] lines = index.split("\r?\n");
      for (int i = 1; i < lines.length; i++) {
        String line = lines[i];
        if (line.trim().isEmpty()) {
          continue;
        }
        Matcher link = INDEX_LINK.matcher(line);
        if (!link.find()) {
          throw new IllegalStateException("nps_deferred_maintenance: no fact-sheet link in index line: "
              + line);
        }
        String[] head = line.split(",", 3);
        String parkName = head[0].trim();
        String states = unquote(head[1]);
        String pdfUrl = link.group(1).replace(" ", "%20");
        byte[] pdf;
        try {
          pdf = download(pdfUrl);
        } catch (NotFoundException e) {
          // The index itself links a fact sheet that NPS never published (verified 2026-11-16:
          // Kaloko-Honokohau NHP and National Park of American Samoa return 404 at every URL
          // variant). The park is absent from the table rather than given invented figures;
          // it is logged here and called out in the table comment.
          LOGGER.warn("nps_deferred_maintenance: index lists {} but its fact sheet is HTTP 404 at {};"
              + " no row emitted", parkName, pdfUrl);
          continue;
        }
        String text;
        try (PDDocument doc = PDDocument.load(new ByteArrayInputStream(pdf))) {
          PDFTextStripper stripper = new PDFTextStripper();
          stripper.setStartPage(1);
          stripper.setEndPage(1);
          text = stripper.getText(doc);
        }
        rows.add(parseFactSheet(parkName, states, pdfUrl, text));
      }
      if (rows.size() == 0) {
        throw new IllegalStateException("nps_deferred_maintenance: index at " + indexUrl
            + " listed no parks");
      }
      return MAPPER.writeValueAsString(rows);
    } catch (IOException e) {
      throw new IllegalStateException("nps_deferred_maintenance: failed reading " + indexUrl, e);
    }
  }

  /** Parses page-one text of one fact sheet into an output row. Package-visible for tests. */
  static ObjectNode parseFactSheet(String parkName, String states, String pdfUrl, String rawText) {
    String text = rawText.replaceAll("\\s+", " ");
    String where = parkName + " (" + pdfUrl + ")";
    ObjectNode row = MAPPER.createObjectNode();
    row.put("park_name", parkName);
    row.put("states", states);
    row.put("fiscal_year", Integer.parseInt(require(FISCAL_YEAR, text, "fiscal year", where)));
    for (String[] inv : INVENTORY) {
      String num = require(Pattern.compile(inv[0]), text, inv[1], where);
      double value = Double.parseDouble(num.replace(",", ""));
      if (inv[1].endsWith("_miles")) {
        row.put(inv[1], value);
      } else {
        row.put(inv[1], (long) value);
      }
    }
    row.put("other_assets", Long.parseLong(
        require(OTHER_ASSETS, text, "other_assets", where).replace(",", "")));
    String totalText = require(DMR_TOTAL, text, "dmr_total", where);
    long total = dollars(totalText);
    row.put("dmr_total_usd", total);
    row.put("annual_routine_maintenance_usd", dollars(require(ROUTINE, text, "routine", where)));

    Matcher cond = CONDITION_START.matcher(text);
    if (!cond.find()) {
      throw new IllegalStateException("nps_deferred_maintenance: no condition paragraph in " + where);
    }
    String condition = text.substring(cond.end());
    String remaining = condition;
    // Printed values carry two significant figures, so each can be off by up to 5%; allow 7%
    // of the total, $2K for the smallest figures, and half a unit of the total's last printed
    // digit (a total printed "$1 M" is only known to +/- $0.5M).
    long tolerance = Math.round(total * 0.07) + 2000 + halfUnit(totalText);
    // The "have $0" tail sentence lists the categories with no backlog.
    Matcher zeroSentence = Pattern.compile("(?:The )?([^.]*?) asset categor(?:y|ies) (?:have|has) \\$0")
        .matcher(condition);
    String zeroList = zeroSentence.find() ? zeroSentence.group(1) : "";
    long sum = 0;
    Map<String, Long> cats = new LinkedHashMap<String, Long>();
    String unaccounted = null;
    for (String[] cat : CATEGORIES) {
      // "<Category> asset category currently accounts for the largest portion of DM&R at $X",
      // "<Category> asset category has $X in DM&R", "<Category> has $X" — the label may follow
      // "and"/"." and the "asset category" words are optional.
      Pattern p = Pattern.compile("(?:^|[ .,]|and )"
          + Pattern.quote(cat[0])
          + "(?: asset categor(?:y|ies))?(?: currently)?(?: accounts for the largest portion of"
          + " DM&R at| has) (\\$[\\d.,]+ ?[KMB]?)");
      Matcher m = p.matcher(remaining);
      long v = 0L;
      if (m.find()) {
        v = dollars(m.group(1));
        // Blank the match so "Waste Water Systems" is not re-read as "Water Systems".
        remaining = remaining.substring(0, m.start()) + " " + remaining.substring(m.end());
      } else if (zeroList.contains(cat[0])) {
        // Listed as having $0. Blank it so "Waste Water Systems" does not also claim "Water Systems".
        zeroList = zeroList.replace(cat[0], " ");
      } else if (unaccounted == null) {
        unaccounted = cat[1];
      } else {
        throw new IllegalStateException("nps_deferred_maintenance: categories " + unaccounted
            + " and " + cat[1] + " are neither given an amount nor listed at $0 for " + where
            + "; condition text: " + condition);
      }
      cats.put(cat[1], v);
      sum += v;
    }
    if (unaccounted != null) {
      // Exactly one category is not named anywhere in the paragraph (the fact sheet names the
      // positive categories and the $0 ones; "All Other Assets" is left out when it holds the
      // remainder). Its amount is the printed total minus the named ones.
      long remainder = total - sum;
      if (remainder < -tolerance) {
        throw new IllegalStateException("nps_deferred_maintenance: named categories sum to " + sum
            + ", above total " + total + ", leaving " + unaccounted + " negative for " + where);
      }
      remainder = Math.max(0L, remainder);
      cats.put(unaccounted, remainder);
      sum += remainder;
    }
    if (Math.abs(sum - total) > tolerance) {
      throw new IllegalStateException("nps_deferred_maintenance: category DM&R sums to " + sum
          + " but total is " + total + " for " + where + "; condition text: " + condition);
    }
    for (Map.Entry<String, Long> e : cats.entrySet()) {
      row.put(e.getKey(), e.getValue());
    }
    row.put("fact_sheet_url", pdfUrl);
    row.put("type", "nps_deferred_maintenance");
    return row;
  }

  private static String require(Pattern p, String text, String what, String where) {
    Matcher m = p.matcher(text);
    if (!m.find()) {
      throw new IllegalStateException("nps_deferred_maintenance: cannot find " + what + " in "
          + where);
    }
    return m.group(1);
  }

  /** Half of the last printed digit of an amount like "$1.3 B" ($0.05B), in dollars. */
  static long halfUnit(String s) {
    String t = s.replace("$", "").replace(",", "").trim();
    double mult = 1.0;
    char last = t.charAt(t.length() - 1);
    if (last == 'K' || last == 'M' || last == 'B') {
      mult = last == 'K' ? 1e3 : last == 'M' ? 1e6 : 1e9;
      t = t.substring(0, t.length() - 1).trim();
    }
    int dot = t.indexOf('.');
    int decimals = dot < 0 ? 0 : t.length() - dot - 1;
    return Math.round(0.5 * Math.pow(10, -decimals) * mult);
  }

  /** "$1.3 B" / "$149 K" / "$0" to whole dollars. */
  static long dollars(String s) {
    String t = s.replace("$", "").replace(",", "").trim();
    double mult = 1.0;
    char last = t.charAt(t.length() - 1);
    if (last == 'K' || last == 'M' || last == 'B') {
      mult = last == 'K' ? 1e3 : last == 'M' ? 1e6 : 1e9;
      t = t.substring(0, t.length() - 1).trim();
    }
    return Math.round(Double.parseDouble(t) * mult);
  }

  private static String unquote(String s) {
    String t = s.trim();
    if (t.length() >= 2 && t.startsWith("\"") && t.endsWith("\"")) {
      t = t.substring(1, t.length() - 1);
    }
    return t;
  }

  private static byte[] download(String url) throws IOException {
    HttpURLConnection conn = (HttpURLConnection) URI.create(url).toURL().openConnection();
    conn.setConnectTimeout(30000);
    conn.setReadTimeout(120000);
    conn.setRequestProperty("User-Agent", "GovData/1.0");
    NetworkOutageWait.connect(conn);
    int status = conn.getResponseCode();
    if (status != 200) {
      if (status == 404) {
        throw new NotFoundException("HTTP 404 from " + url);
      }
      throw new IOException("HTTP " + status + " from " + url);
    }
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (InputStream is = conn.getInputStream()) {
      byte[] buf = new byte[65536];
      int len;
      while ((len = is.read(buf)) > 0) {
        baos.write(buf, 0, len);
        if (baos.size() > MAX_BYTES) {
          throw new IOException("Response from " + url + " exceeds " + MAX_BYTES + " bytes");
        }
      }
    }
    return baos.toByteArray();
  }

  /** HTTP 404 on a fact-sheet download, distinct from every other failure (which is fatal). */
  private static final class NotFoundException extends IOException {
    NotFoundException(String message) {
      super(message);
    }
  }
}
