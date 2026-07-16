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
package org.apache.calcite.adapter.govdata.fiscal;

import org.apache.calcite.adapter.file.etl.DataProvider;
import org.apache.calcite.adapter.file.etl.EtlPipelineConfig;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * DataProvider for {@code usaspending_by_agency} — federal obligations by
 * awarding agency for a fiscal year, from the USAspending Spending Explorer
 * ({@code POST /api/v2/spending/}, {@code type=agency}, {@code period=12}).
 * Amounts are cumulative obligations through the fiscal year (Oct-Sep).
 */
public class UsaSpendingAgencyProvider implements DataProvider {

  private static final Logger LOGGER = LoggerFactory.getLogger(UsaSpendingAgencyProvider.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String ENDPOINT = "https://api.usaspending.gov/api/v2/spending/";

  @Override public Iterator<Map<String, Object>> fetch(EtlPipelineConfig config,
      Map<String, String> variables) throws IOException {
    String year = variables.get("effective_year");
    if (year == null || year.isEmpty()) {
      year = variables.get("year");
    }
    if (year == null || year.isEmpty()) {
      LOGGER.warn("usaspending_by_agency: no year in dimension variables {}", variables);
      return Collections.emptyIterator();
    }
    String body = "{\"type\":\"agency\",\"filters\":{\"fy\":\"" + year.trim() + "\",\"period\":\"12\"}}";
    LOGGER.info("usaspending_by_agency: POST {} fy={}", ENDPOINT, year);

    JsonNode root;
    InputStream in = FiscalHttp.openPostJson(ENDPOINT, body).getInputStream();
    try {
      root = MAPPER.readTree(in);
    } finally {
      in.close();
    }
    JsonNode results = root.path("results");
    List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
    if (results.isArray()) {
      for (JsonNode r : results) {
        String code = text(r, "code");
        String name = text(r, "name");
        if (code == null && name == null) {
          continue;
        }
        Map<String, Object> row = new LinkedHashMap<String, Object>();
        row.put("agency_code", code);
        row.put("agency_name", name);
        row.put("obligated_amount", num(r, "amount"));
        rows.add(row);
      }
    }
    LOGGER.info("usaspending_by_agency: {} agency rows for fy {}", rows.size(), year);
    return rows.iterator();
  }

  private static String text(JsonNode node, String field) {
    JsonNode v = node.get(field);
    if (v == null || v.isNull()) {
      return null;
    }
    String s = v.asText();
    return (s == null || s.trim().isEmpty()) ? null : s;
  }

  private static Double num(JsonNode node, String field) {
    JsonNode v = node.get(field);
    if (v == null || v.isNull() || !v.isNumber() && !v.isTextual()) {
      return null;
    }
    return FiscalHttp.toDouble(v.asText());
  }
}
