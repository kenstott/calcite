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

import org.apache.calcite.adapter.file.etl.CachingDataProvider;
import org.apache.calcite.adapter.file.etl.CsvRecordReader;
import org.apache.calcite.adapter.file.etl.EtlPipelineConfig;
import org.apache.calcite.adapter.file.etl.RawCache;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * DataProvider for {@code cbo_budget_projections}: the Congressional Budget Office's
 * baseline budget projections and historical budget data, read from CBO's own open data
 * repository {@code github.com/US-CBO/cbo-data} (cbo.gov itself sits behind DataDome and
 * refuses non-browser clients).
 *
 * <p>The repository's {@code catalog.json} lists every vintage of the three budget datasets
 * ({@code ten_year_budget}, {@code long_term_budget}, {@code historical_budget}). The catalog
 * is read uncached so a new CBO release is picked up on the next run; each vintage CSV is
 * read through the raw cache, since a vintage file is a fixed release. A variable with no
 * entry in the dataset's {@code schema.json}, or a malformed row, fails the load.
 */
public class CboBaselineProvider implements CachingDataProvider {

  private static final Logger LOGGER = LoggerFactory.getLogger(CboBaselineProvider.class);

  static final String BASE = "https://raw.githubusercontent.com/US-CBO/cbo-data/main/";
  static final String CATALOG_URL = BASE + "catalog.json";

  static final List<String> DATASETS =
      Arrays.asList("ten_year_budget", "long_term_budget", "historical_budget");

  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Override public Iterator<Map<String, Object>> fetch(EtlPipelineConfig config,
      Map<String, String> variables, RawCache rawCache) throws IOException {
    JsonNode catalog = readJson(CATALOG_URL);
    List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
    for (String dataset : DATASETS) {
      JsonNode entry = findDataset(catalog, dataset);
      JsonNode fields = readJson(BASE + entry.path("schema_path").asText()).path("fields");
      if (!fields.isObject() || fields.size() == 0) {
        throw new IOException("cbo_budget_projections: no fields in schema of " + dataset);
      }
      int files = 0;
      for (JsonNode dist : entry.path("distribution")) {
        if (!"annual_fy".equals(dist.path("file_type").asText())) {
          continue;
        }
        String vintage = dist.path("vintage").asText();
        String url = BASE + dist.path("downloadURL").asText();
        if (vintage.isEmpty() || dist.path("downloadURL").asText().isEmpty()) {
          throw new IOException("cbo_budget_projections: catalog entry without vintage or URL in "
              + dataset + ": " + dist);
        }
        try (InputStream in = rawCache.openStream(url);
            BufferedReader reader =
                new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
          int before = rows.size();
          parseCsv(reader, dataset, vintage, fields, url, rows);
          LOGGER.info("cbo_budget_projections: {} {} -> {} rows", dataset, vintage,
              Integer.valueOf(rows.size() - before));
        }
        files++;
      }
      if (files == 0) {
        throw new IOException("cbo_budget_projections: catalog lists no annual_fy file for "
            + dataset);
      }
    }
    return rows.iterator();
  }

  /** Parses one long-format vintage file ({@code date,variable,value}) into {@code rows}. */
  static void parseCsv(BufferedReader reader, String dataset, String vintage, JsonNode fields,
      String source, List<Map<String, Object>> rows) throws IOException {
    String header = CsvRecordReader.readRecord(reader);
    if (header == null) {
      throw new IOException("cbo_budget_projections: empty CSV at " + source);
    }
    Map<String, Integer> idx = FiscalHttp.headerIndex(CsvRecordReader.splitFields(header, ','));
    int dateCol = FiscalHttp.required(idx, "date", source);
    int varCol = FiscalHttp.required(idx, "variable", source);
    int valCol = FiscalHttp.required(idx, "value", source);
    String record;
    while ((record = CsvRecordReader.readRecord(reader)) != null) {
      List<String> cols = CsvRecordReader.splitFields(record, ',');
      if (cols.isEmpty()) {
        continue;
      }
      String date = FiscalHttp.cell(cols, dateCol);
      String variable = FiscalHttp.cell(cols, varCol);
      Double value = FiscalHttp.toDouble(FiscalHttp.cell(cols, valCol));
      if (date == null || !date.matches("FY\\d{4}") || variable == null || value == null) {
        throw new IOException("cbo_budget_projections: malformed row '" + record + "' in "
            + source);
      }
      JsonNode meta = fields.get(variable);
      if (meta == null) {
        throw new IOException("cbo_budget_projections: variable " + variable + " in " + source
            + " has no entry in the dataset schema.json");
      }
      Map<String, Object> row = new LinkedHashMap<String, Object>();
      row.put("dataset", dataset);
      row.put("vintage", vintage);
      row.put("fiscal_year", Integer.valueOf(date.substring(2)));
      row.put("variable", variable);
      row.put("value", value);
      row.put("unit", meta.path("unit").asText(null));
      row.put("category", meta.path("category").asText(null));
      row.put("description", meta.path("description").asText(null));
      rows.add(row);
    }
  }

  private static JsonNode findDataset(JsonNode catalog, String id) throws IOException {
    for (JsonNode d : catalog.path("datasets")) {
      if (id.equals(d.path("identifier").asText())) {
        return d;
      }
    }
    throw new IOException("cbo_budget_projections: dataset " + id + " not in " + CATALOG_URL);
  }

  private static JsonNode readJson(String url) throws IOException {
    java.net.HttpURLConnection conn = FiscalHttp.openGetWithRetry(url);
    try (InputStream in = conn.getInputStream()) {
      return MAPPER.readTree(in);
    } finally {
      conn.disconnect();
    }
  }
}
