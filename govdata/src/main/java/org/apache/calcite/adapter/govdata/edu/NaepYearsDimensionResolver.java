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
package org.apache.calcite.adapter.govdata.edu;

import org.apache.calcite.adapter.file.etl.DimensionConfig;
import org.apache.calcite.adapter.file.etl.DimensionResolver;
import org.apache.calcite.adapter.file.storage.StorageProvider;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Resolves available NAEP assessment years dynamically from the Nations Report Card API.
 *
 * <p>Calls the NAEP years discovery endpoint to obtain the actual list of administered
 * assessment years. This avoids hardcoding irregular assessment cycles (NAEP is biennial
 * but skipped 2021 due to COVID, breaking the even/odd cadence).
 *
 * <p>Applies {@code dataLag} and {@code minYear} from the dimension config so that
 * recently-administered years whose data has not yet been published are excluded.
 *
 * <p>Schema configuration:
 * <pre>{@code
 * hooks:
 *   dimensionResolver: "org.apache.calcite.adapter.govdata.edu.NaepYearsDimensionResolver"
 *
 * dimensions:
 *   year:
 *     type: custom
 *     minYear: 2013
 *     dataLag: 2
 *     properties:
 *       yearsUrl: "https://www.nationsreportcard.gov/Dataservice/GetAdhocData.aspx?type=Years&subject=MAT&grade=4&subscale=MRPCM"
 * }</pre>
 */
public class NaepYearsDimensionResolver implements DimensionResolver {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(NaepYearsDimensionResolver.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String DEFAULT_YEARS_URL =
      "https://www.nationsreportcard.gov/Dataservice/GetAdhocData.aspx"
      + "?type=Years&subject=MAT&grade=4&subscale=MRPCM";

  private List<String> cachedYears;

  @Override
  public List<String> resolve(String dimensionName, DimensionConfig config,
      Map<String, String> context, StorageProvider storageProvider) {
    if (!"year".equals(dimensionName)) {
      return Collections.emptyList();
    }
    if (cachedYears != null) {
      return cachedYears;
    }
    cachedYears = fetchAndFilter(config);
    return cachedYears;
  }

  private List<String> fetchAndFilter(DimensionConfig config) {
    String url = config.getProperty("yearsUrl", DEFAULT_YEARS_URL);
    Integer minYear = config.getMinYear();
    Integer dataLag = config.getDataLag();

    int effectiveMax = Integer.MAX_VALUE;
    if (dataLag != null && dataLag > 0) {
      effectiveMax = Calendar.getInstance().get(Calendar.YEAR) - dataLag;
    }

    LOGGER.debug("NAEP-YEARS: fetching available years from {}", url);

    try {
      HttpClient client = HttpClient.newBuilder()
          .connectTimeout(Duration.ofSeconds(30))
          .build();
      HttpRequest request = HttpRequest.newBuilder()
          .uri(URI.create(url))
          .timeout(Duration.ofSeconds(30))
          .GET()
          .build();

      HttpResponse<String> response =
          client.send(request, HttpResponse.BodyHandlers.ofString());

      if (response.statusCode() != 200) {
        throw new RuntimeException(
            "NAEP years API returned status " + response.statusCode());
      }

      List<String> years = parseYears(response.body(), minYear, effectiveMax);
      LOGGER.info("NAEP-YEARS: resolved {} assessment years (minYear={}, maxYear={})",
          years.size(), minYear, effectiveMax);
      return years;

    } catch (Exception e) {
      throw new RuntimeException("NAEP-YEARS: failed to fetch available years from "
          + url + ": " + e.getMessage(), e);
    }
  }

  private List<String> parseYears(String body, Integer minYear, int effectiveMax) {
    List<Integer> rawYears = new ArrayList<Integer>();

    try {
      JsonNode root = MAPPER.readTree(body);
      JsonNode arrayNode = root.isArray() ? root : root.path("result");

      if (!arrayNode.isArray()) {
        throw new RuntimeException("Expected JSON array in NAEP years response");
      }

      for (JsonNode node : arrayNode) {
        if (node.isInt() || node.isTextual()) {
          try {
            rawYears.add(node.isInt() ? node.intValue() : Integer.parseInt(node.asText()));
          } catch (NumberFormatException e) {
            LOGGER.warn("NAEP-YEARS: skipping non-integer year value '{}'", node.asText());
          }
        }
      }
    } catch (Exception e) {
      throw new RuntimeException("NAEP-YEARS: failed to parse years response: "
          + e.getMessage(), e);
    }

    Collections.sort(rawYears);

    List<String> filtered = new ArrayList<String>();
    for (int year : rawYears) {
      if (minYear != null && year < minYear) {
        LOGGER.debug("NAEP-YEARS: skipping year {} (below minYear {})", year, minYear);
        continue;
      }
      if (year > effectiveMax) {
        LOGGER.debug("NAEP-YEARS: skipping year {} (exceeds effectiveMax {})", year, effectiveMax);
        continue;
      }
      filtered.add(String.valueOf(year));
    }
    return filtered;
  }
}
