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
package org.apache.calcite.adapter.govdata.ag;

import org.apache.calcite.adapter.file.etl.ModelOperand;
import org.apache.calcite.adapter.file.etl.NetworkOutageWait;
import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.StreamingResponseTransformer;
import org.apache.calcite.adapter.govdata.econ.CensusTradeStreamingTransformer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * Streams Census HS-10 monthly trade rows and keeps only agricultural products, for
 * {@code ag.agricultural_exports} and {@code ag.agricultural_imports}.
 *
 * <p>The Census request and row parsing are {@link CensusTradeStreamingTransformer}'s. The
 * product definition is the USDA FAS GATS commodity catalog
 * ({@code /api/gats/commodities}), which flags every HS-10 code it lists; codes it does not
 * list are not agricultural.
 *
 * <p><b>Flag polarity.</b> The catalog's {@code isAgCommodity} field is {@code false} for
 * agricultural products (wheat, soybeans, cotton) and {@code true} for the related
 * non-agricultural ones (fish, forest products, rubber). The {@code false} set reproduces
 * USDA ERS's published agricultural totals (verified 2026-11-25: world 2021 exports and
 * imports, and Thailand 2020/2021 exports, match to the dollar). The load fails if grain
 * (HS chapter 10) is not uniformly {@code false} or mineral fuels (chapter 27) not uniformly
 * {@code true}, so a change of the field's meaning cannot silently invert the tables.
 */
public class FasAgriculturalTradeTransformer implements StreamingResponseTransformer {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(FasAgriculturalTradeTransformer.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  static final String DESCRIPTION_FIELD = "COMMODITY_DESCRIPTION";
  private static final String CONFIG_ROOT = "ag.fasGatsCatalog.";
  private static final String GRAIN_CHAPTER = "10";
  private static final String FUEL_CHAPTER = "27";
  private static final int HS10_LENGTH = 10;
  /** First data year of the Census timeseries API; codes retired earlier are never returned. */
  private static final String FIRST_DATA_MONTH = "201301";
  private static final int TIMEOUT_MS = 120_000;
  private static final int MAX_ATTEMPTS = 4;

  /** Loaded once per JVM per trade direction. */
  private static final Map<Boolean, Map<String, String>> CATALOGS =
      new HashMap<Boolean, Map<String, String>>();

  private final CensusTradeStreamingTransformer census = new CensusTradeStreamingTransformer();

  @Override public Iterator<Map<String, Object>> fetchAndTransform(RequestContext context)
      throws IOException {
    boolean exports = isExports(context.getUrl());
    Map<String, String> agricultural = catalog(exports);
    return filter(census.fetchAndTransform(context), exports ? "E_COMMODITY" : "I_COMMODITY",
        agricultural);
  }

  static boolean isExports(String url) {
    if (url != null && url.contains("/intltrade/exports/")) {
      return true;
    }
    if (url != null && url.contains("/intltrade/imports/")) {
      return false;
    }
    throw new IllegalStateException("Not a Census trade URL: " + url);
  }

  /** Keeps rows whose commodity code is agricultural, adding the catalog description. */
  static Iterator<Map<String, Object>> filter(final Iterator<Map<String, Object>> rows,
      final String codeField, final Map<String, String> agricultural) {
    return new Iterator<Map<String, Object>>() {
      private Map<String, Object> pending;

      @Override public boolean hasNext() {
        while (pending == null && rows.hasNext()) {
          Map<String, Object> row = rows.next();
          if (!row.containsKey(codeField)) {
            throw new IllegalStateException("Census trade row has no " + codeField + ": " + row);
          }
          String code = String.valueOf(row.get(codeField));
          if (agricultural.containsKey(code)) {
            row.put(DESCRIPTION_FIELD, agricultural.get(code));
            pending = row;
          }
        }
        return pending != null;
      }

      @Override public Map<String, Object> next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        Map<String, Object> row = pending;
        pending = null;
        return row;
      }
    };
  }

  private static synchronized Map<String, String> catalog(boolean exports) throws IOException {
    Map<String, String> loaded = CATALOGS.get(exports);
    if (loaded == null) {
      String url = required("url");
      String chaptersKey = exports ? "exportChapters" : "importChapters";
      loaded = agriculturalCodes(download(url, required("apiKey")), exports,
          requestedChapters(chaptersKey));
      LOGGER.info("FAS GATS catalog: {} agricultural HS-10 {} codes", loaded.size(),
          exports ? "export" : "import");
      CATALOGS.put(exports, loaded);
    }
    return loaded;
  }

  private static String required(String key) {
    String value = ModelOperand.getString(CONFIG_ROOT + key);
    if (value == null || value.isEmpty()) {
      throw new IllegalStateException("Missing model operand " + CONFIG_ROOT + key);
    }
    return value;
  }

  private static Set<String> requestedChapters(String key) {
    Set<String> chapters = new HashSet<String>();
    for (int i = 0; ModelOperand.has(CONFIG_ROOT + key + "." + i); i++) {
      chapters.add(ModelOperand.getString(CONFIG_ROOT + key + "." + i));
    }
    if (chapters.isEmpty()) {
      throw new IllegalStateException("Missing model operand " + CONFIG_ROOT + key);
    }
    return chapters;
  }

  /**
   * Returns {@code HS-10 code -> description} for the agricultural codes of one direction.
   *
   * @param catalog catalog response: an array of
   *     {@code {hS10Code, productType, isAgCommodity, commodityName, commodityDescription,
   *     startDate, endDtate}} ({@code endDtate} is the source's spelling). Product type
   *     {@code X} is an export code, {@code M} an import code, {@code T} and {@code B} apply
   *     to both.
   * @param exports whether to select export codes
   * @param chapters HS chapters the table requests
   */
  static Map<String, String> agriculturalCodes(JsonNode catalog, boolean exports,
      Set<String> chapters) {
    String excluded = exports ? "M" : "X";
    Map<String, String> result = new HashMap<String, String>();
    Map<String, Boolean> flags = new HashMap<String, Boolean>();
    Map<String, String> latestStart = new HashMap<String, String>();
    Set<String> unrequested = new HashSet<String>();
    int grain = 0;
    int fuel = 0;
    for (JsonNode entry : catalog) {
      String code = text(entry, "hS10Code");
      if (code == null || code.length() != HS10_LENGTH
          || excluded.equals(text(entry, "productType"))) {
        continue;
      }
      JsonNode flag = entry.get("isAgCommodity");
      if (flag == null || !flag.isBoolean()) {
        throw new IllegalStateException("FAS GATS catalog: no isAgCommodity flag for " + code);
      }
      Boolean previous = flags.put(code, flag.booleanValue());
      if (previous != null && previous.booleanValue() != flag.booleanValue()) {
        throw new IllegalStateException("FAS GATS catalog: conflicting flags for " + code);
      }
      String chapter = code.substring(0, 2);
      boolean agricultural = !flag.booleanValue();
      if (GRAIN_CHAPTER.equals(chapter)) {
        grain++;
        if (!agricultural) {
          throw polarityChanged(code);
        }
      } else if (FUEL_CHAPTER.equals(chapter)) {
        fuel++;
        if (agricultural) {
          throw polarityChanged(code);
        }
      }
      if (!agricultural) {
        continue;
      }
      String end = text(entry, "endDtate");
      if (!chapters.contains(chapter) && (end == null || end.compareTo(FIRST_DATA_MONTH) >= 0)) {
        unrequested.add(chapter);
      }
      String start = String.valueOf(text(entry, "startDate"));
      String seen = latestStart.get(code);
      if (seen == null || start.compareTo(seen) >= 0) {
        latestStart.put(code, start);
        String description = text(entry, "commodityDescription");
        result.put(code, description != null ? description : text(entry, "commodityName"));
      }
    }
    if (grain == 0 || fuel == 0) {
      throw new IllegalStateException("FAS GATS catalog is incomplete: " + grain
          + " grain and " + fuel + " mineral fuel codes");
    }
    if (!unrequested.isEmpty()) {
      throw new IllegalStateException("FAS GATS catalog marks agricultural codes in HS chapters "
          + unrequested + " that the table does not request; add them to " + CONFIG_ROOT
          + (exports ? "exportChapters" : "importChapters"));
    }
    return result;
  }

  private static IllegalStateException polarityChanged(String code) {
    return new IllegalStateException("FAS GATS catalog: unexpected isAgCommodity value for "
        + code + "; the flag's meaning has changed");
  }

  private static String text(JsonNode node, String field) {
    JsonNode value = node.get(field);
    if (value == null || value.isNull()) {
      return null;
    }
    String text = value.asText().trim();
    return text.isEmpty() ? null : text;
  }

  private static JsonNode download(String url, String apiKey) throws IOException {
    IOException last = null;
    for (int attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
      HttpURLConnection conn = (HttpURLConnection) URI.create(url).toURL().openConnection();
      conn.setConnectTimeout(TIMEOUT_MS);
      conn.setReadTimeout(TIMEOUT_MS);
      conn.setRequestProperty("X-Api-Key", apiKey);
      conn.setRequestProperty("User-Agent", "GovData/1.0");
      try {
        NetworkOutageWait.connect(conn);
        int status = conn.getResponseCode();
        if (status == HttpURLConnection.HTTP_OK) {
          try (InputStream in = conn.getInputStream()) {
            return MAPPER.readTree(in);
          }
        }
        last = new IOException("HTTP " + status + " from " + url);
        if (status != 429 && status < 500) {
          throw last;
        }
      } catch (IOException e) {
        if (e == last) {
          throw e;
        }
        last = e;
      } finally {
        conn.disconnect();
      }
      LOGGER.warn("FAS GATS catalog fetch failed (attempt {}/{}): {}", attempt, MAX_ATTEMPTS,
          last.toString());
      try {
        Thread.sleep(2000L * attempt);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException("Interrupted fetching " + url, e);
      }
    }
    throw new IOException("FAS GATS catalog: fetch failed after " + MAX_ATTEMPTS + " attempts",
        last);
  }
}
