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
package org.apache.calcite.adapter.govdata.census;

import org.apache.calcite.adapter.file.etl.CachingDataProvider;
import org.apache.calcite.adapter.file.etl.CsvRecordReader;
import org.apache.calcite.adapter.file.etl.EtlPipelineConfig;
import org.apache.calcite.adapter.file.etl.RawCache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * DataProvider for {@code pep_population_estimates}: state and county resident population for
 * one data year, read from the Census PEP bulk file {@code co-est{V}-alldata.csv} of the latest
 * published vintage. The file holds one wide row per state (SUMLEV 040) and county (SUMLEV 050)
 * with a {@code POPESTIMATE<year>} column per data year 2020 to V. The Census API serves these
 * vintages late (api.census.gov pep/charv for 2024 returned 404), the bulk file does not.
 */
public class PepPopulationProvider implements CachingDataProvider {

  private static final Logger LOGGER = LoggerFactory.getLogger(PepPopulationProvider.class);

  private static final String BASE =
      "https://www2.census.gov/programs-surveys/popest/datasets/";
  private static final int FIRST_YEAR = 2020;
  private static final String STATE_SUMLEV = "040";
  private static final String COUNTY_SUMLEV = "050";

  @Override public Iterator<Map<String, Object>> fetch(EtlPipelineConfig config,
      Map<String, String> variables, RawCache rawCache) throws IOException {
    int year = requiredInt(variables, "effective_year");
    int vintage = requiredInt(variables, "pep_vintage");
    if (year < FIRST_YEAR || year > vintage) {
      throw new IOException("pep_population_estimates: data year " + year
          + " is outside the vintage " + vintage + " file (" + FIRST_YEAR + "-" + vintage + ")");
    }
    final String url = url(vintage);
    LOGGER.info("pep_population_estimates: year {} from {}", Integer.valueOf(year), url);

    List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
    // The PEP files are ISO-8859-1 ("Dona Ana County" with a tilde n is the single byte 0xF1).
    try (BufferedReader reader = new BufferedReader(
        new InputStreamReader(rawCache.openStream(url), StandardCharsets.ISO_8859_1))) {
      String headerRecord = CsvRecordReader.readRecord(reader);
      if (headerRecord == null) {
        throw new IOException("pep_population_estimates: empty CSV at " + url);
      }
      Map<String, Integer> idx = headerIndex(CsvRecordReader.splitFields(headerRecord, ','));
      int sumlev = require(idx, "SUMLEV", url);
      int state = require(idx, "STATE", url);
      int county = require(idx, "COUNTY", url);
      int stname = require(idx, "STNAME", url);
      int ctyname = require(idx, "CTYNAME", url);
      int pop = require(idx, "POPESTIMATE" + year, url);
      String record;
      while ((record = CsvRecordReader.readRecord(reader)) != null) {
        List<String> cols = CsvRecordReader.splitFields(record, ',');
        if (cols.isEmpty() || (cols.size() == 1 && cols.get(0).trim().isEmpty())) {
          continue;
        }
        String level = cols.get(sumlev).trim();
        boolean isState = STATE_SUMLEV.equals(level);
        if (!isState && !COUNTY_SUMLEV.equals(level)) {
          continue;
        }
        String stateFips = cols.get(state).trim();
        String countyCode = cols.get(county).trim();
        Map<String, Object> row = new LinkedHashMap<String, Object>();
        row.put("geography", isState ? "state" : "county");
        row.put("state", stateFips);
        row.put("county", isState ? null : countyCode);
        row.put("county_fips", isState ? null : stateFips + countyCode);
        row.put("geo_name", isState ? cols.get(stname).trim() : cols.get(ctyname).trim());
        row.put("population", num(cols, pop, "POPESTIMATE" + year));
        row.put("vintage", String.valueOf(vintage));
        rows.add(row);
      }
    }
    if (rows.isEmpty()) {
      throw new IOException("pep_population_estimates: no state or county rows in " + url);
    }
    return rows.iterator();
  }

  static String url(int vintage) {
    return BASE + FIRST_YEAR + "-" + vintage + "/counties/totals/co-est" + vintage
        + "-alldata.csv";
  }

  private static int requiredInt(Map<String, String> variables, String name)
      throws IOException {
    String v = variables.get(name);
    if (v == null || v.isEmpty()) {
      throw new IOException("pep_population_estimates: dimension variable '" + name
          + "' missing from " + variables);
    }
    try {
      return Integer.parseInt(v.trim());
    } catch (NumberFormatException e) {
      throw new IOException("pep_population_estimates: dimension variable '" + name
          + "' is not a number: " + v, e);
    }
  }

  private static Map<String, Integer> headerIndex(List<String> headers) {
    Map<String, Integer> m = new HashMap<String, Integer>();
    for (int i = 0; i < headers.size(); i++) {
      m.put(headers.get(i).trim().toUpperCase(Locale.ROOT), Integer.valueOf(i));
    }
    return m;
  }

  private static int require(Map<String, Integer> idx, String name, String url)
      throws IOException {
    Integer i = idx.get(name.toUpperCase(Locale.ROOT));
    if (i == null) {
      throw new IOException("pep_population_estimates: column '" + name + "' not found in "
          + url);
    }
    return i.intValue();
  }

  private static Long num(List<String> cols, int at, String name) throws IOException {
    String v = at < cols.size() ? cols.get(at).trim() : "";
    if (v.isEmpty()) {
      throw new IOException("pep_population_estimates: empty value in column " + name);
    }
    try {
      return Long.valueOf(Long.parseLong(v));
    } catch (NumberFormatException e) {
      throw new IOException("pep_population_estimates: non-integer value '" + v
          + "' in column " + name, e);
    }
  }
}
