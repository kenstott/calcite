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

import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.ResponseTransformer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

/**
 * Transformer for the USDA FAS Export Sales Reporting (ESR) API
 * ({@code ag.fas_export_sales}).
 *
 * <p>The {@code allCountries/marketYear/{year}} endpoint returns a flat JSON array
 * of weekly rows for every destination in one call, e.g.:
 * <pre>{@code
 * {"commodityCode":401,"countryCode":1220,"weeklyExports":34818,
 *  "accumulatedExports":34818,"outstandingSales":712440,"grossNewSales":52762,
 *  "currentMYNetSales":52762,"currentMYTotalCommitment":747258,
 *  "nextMYOutstandingSales":0,"nextMYNetSales":0,"unitId":1,
 *  "weekEndingDate":"2023-09-07T00:00:00"}
 * }</pre>
 *
 * <p>Each row is enriched with a readable commodity/country/unit name and, where
 * FAS publishes one (gencCode), an ISO 3166-1 alpha-3 code — looked up from the
 * static catalogs bundled at {@code /ag/fas-commodities.json},
 * {@code /ag/fas-countries.json}, and {@code /ag/fas-units.json} rather than an
 * extra API call per fetch. marketYear is not present on the raw row (it is the
 * URL's own {@code effective_year}), so it is read from the request context.
 *
 * @see ResponseTransformer
 */
public class FasExportSalesTransformer implements ResponseTransformer {

  private static final Logger LOGGER = LoggerFactory.getLogger(FasExportSalesTransformer.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static final Map<Integer, String> COMMODITY_NAMES =
      loadCatalog("/ag/fas-commodities.json", "commodities", "commodityCode", "commodityName");
  private static final Map<Integer, String> COUNTRY_NAMES =
      loadCatalog("/ag/fas-countries.json", "countries", "countryCode", "countryName");
  private static final Map<Integer, String> COUNTRY_ISO3 =
      loadCatalog("/ag/fas-countries.json", "countries", "countryCode", "gencCode");
  private static final Map<Integer, String> COUNTRY_REGION_ID =
      loadCatalog("/ag/fas-countries.json", "countries", "countryCode", "regionId");
  private static final Map<Integer, String> UNIT_NAMES =
      loadCatalog("/ag/fas-units.json", "units", "unitId", "unitNames");

  @Override public String transform(String response, RequestContext context) {
    if (response == null || response.isEmpty()) {
      LOGGER.warn("FAS ESR: Empty response received for {}", context.getUrl());
      return "[]";
    }

    String marketYear = context.getDimensionValues().containsKey("effective_year")
        ? context.getDimensionValues().get("effective_year")
        : context.getDimensionValues().get("year");

    try {
      JsonNode root = MAPPER.readTree(response);

      JsonNode error = root.path("error");
      if (!error.isMissingNode() && !error.isNull()) {
        LOGGER.debug("FAS ESR: no data for {} ({})", context.getDimensionValues(), error);
        return "[]";
      }

      if (!root.isArray()) {
        LOGGER.debug("FAS ESR: non-array response for {}", context.getDimensionValues());
        return "[]";
      }

      ArrayNode out = MAPPER.createArrayNode();
      for (JsonNode record : root) {
        out.add(transformRecord(record, marketYear));
      }
      LOGGER.debug("FAS ESR: transformed {} records for {}", out.size(),
          context.getDimensionValues());
      return out.toString();

    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      LOGGER.error("FAS ESR: failed to transform response for {}: {}",
          context.getUrl(), e.getMessage());
      throw new RuntimeException("Failed to transform FAS ESR response: " + e.getMessage(), e);
    }
  }

  private ObjectNode transformRecord(JsonNode record, String marketYear) {
    ObjectNode result = MAPPER.createObjectNode();

    result.put("year", marketYear == null ? null : Integer.parseInt(marketYear));

    Integer commodityCode = getIntValue(record, "commodityCode");
    result.put("commodity_code", commodityCode);
    result.put("commodity_name", commodityCode == null ? null : COMMODITY_NAMES.get(commodityCode));

    Integer unitId = getIntValue(record, "unitId");
    result.put("unit_id", unitId);
    result.put("unit_name", unitId == null ? null : UNIT_NAMES.get(unitId));

    Integer countryCode = getIntValue(record, "countryCode");
    result.put("country_code", countryCode);
    result.put("country_name", countryCode == null ? null : COUNTRY_NAMES.get(countryCode));
    result.put("country_iso3", countryCode == null ? null : COUNTRY_ISO3.get(countryCode));
    String regionIdStr = countryCode == null ? null : COUNTRY_REGION_ID.get(countryCode);
    result.put("region_id", regionIdStr == null ? null : Integer.parseInt(regionIdStr));

    String weekEndingDate = getTextValue(record, "weekEndingDate");
    result.put("week_ending_date", weekEndingDate == null ? null : weekEndingDate.split("T")[0]);

    result.put("weekly_exports", getDoubleValue(record, "weeklyExports"));
    result.put("accumulated_exports", getDoubleValue(record, "accumulatedExports"));
    result.put("outstanding_sales", getDoubleValue(record, "outstandingSales"));
    result.put("gross_new_sales", getDoubleValue(record, "grossNewSales"));
    result.put("current_my_net_sales", getDoubleValue(record, "currentMYNetSales"));
    result.put("current_my_total_commitment", getDoubleValue(record, "currentMYTotalCommitment"));
    result.put("next_my_outstanding_sales", getDoubleValue(record, "nextMYOutstandingSales"));
    result.put("next_my_net_sales", getDoubleValue(record, "nextMYNetSales"));

    return result;
  }

  private Integer getIntValue(JsonNode node, String field) {
    JsonNode v = node.get(field);
    return (v == null || v.isNull()) ? null : v.asInt();
  }

  private Double getDoubleValue(JsonNode node, String field) {
    JsonNode v = node.get(field);
    return (v == null || v.isNull()) ? null : v.asDouble();
  }

  private String getTextValue(JsonNode node, String field) {
    JsonNode v = node.get(field);
    return (v == null || v.isNull()) ? null : v.asText();
  }

  /**
   * Loads a {@code code -> label} lookup from a bundled classpath catalog JSON
   * file shaped {@code {"<arrayField>": [{"<keyField>": ..., "<valueField>": ...}, ...]}}.
   * Keys and values are read as text so both numeric and string source fields work.
   */
  private static Map<Integer, String> loadCatalog(String resourcePath, String arrayField,
      String keyField, String valueField) {
    Map<Integer, String> catalog = new HashMap<>();
    try (InputStream is = FasExportSalesTransformer.class.getResourceAsStream(resourcePath)) {
      if (is == null) {
        throw new IllegalStateException("FAS ESR catalog resource not found: " + resourcePath);
      }
      JsonNode root = MAPPER.readTree(is);
      for (JsonNode record : root.path(arrayField)) {
        JsonNode key = record.get(keyField);
        JsonNode value = record.get(valueField);
        if (key == null || key.isNull()) {
          continue;
        }
        String text = (value == null || value.isNull()) ? null : value.asText().trim();
        catalog.put(key.asInt(), (text == null || text.isEmpty()) ? null : text);
      }
    } catch (IOException e) {
      throw new RuntimeException("Failed to load FAS ESR catalog: " + resourcePath, e);
    }
    return catalog;
  }
}
