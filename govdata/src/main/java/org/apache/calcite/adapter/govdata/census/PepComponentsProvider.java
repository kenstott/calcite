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
 * DataProvider for {@code pep_components_of_change}: the Census Population Estimates Program
 * county files {@code co-est{V}-alldata.csv}, which hold one wide row per county with a column per
 * data year for each component ({@code POPESTIMATE2021}, {@code BIRTHS2021}, ...). The Census
 * API serves components only for data year 2019, so the bulk files are the source for every other
 * year.
 *
 * <p>Two files are read: the final 2010-2019 vintage (data years 2010-2019) and the latest
 * published vintage ({@code pep_vintage} dimension, data years 2020 up to that vintage). One
 * fetch returns the county rows of one data year. State rows (SUMLEV 040) are not emitted; the
 * county rows sum to the state. Headers differ between the two files ({@code NATURALINC} versus
 * {@code NATURALCHG}); a header the provider does not know fails loudly.
 */
public class PepComponentsProvider implements CachingDataProvider {

  private static final Logger LOGGER = LoggerFactory.getLogger(PepComponentsProvider.class);

  private static final String BASE =
      "https://www2.census.gov/programs-surveys/popest/datasets/";
  private static final int FINAL_OLD_VINTAGE = 2019;
  private static final int FIRST_NEW_YEAR = 2020;
  private static final String COUNTY_SUMLEV = "050";

  @Override public Iterator<Map<String, Object>> fetch(EtlPipelineConfig config,
      Map<String, String> variables, RawCache rawCache) throws IOException {
    int year = requiredInt(variables, "effective_year", "year");
    int vintage;
    String naturalPrefix;
    if (year < FIRST_NEW_YEAR) {
      vintage = FINAL_OLD_VINTAGE;
      naturalPrefix = "NATURALINC";
    } else {
      vintage = requiredInt(variables, "pep_vintage", null);
      naturalPrefix = "NATURALCHG";
    }
    final String url = url(vintage);
    LOGGER.info("pep_components_of_change: year {} from {}", Integer.valueOf(year), url);

    List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
    // The PEP files are ISO-8859-1 ("Dona Ana County" with a tilde n is the single byte 0xF1).
    try (BufferedReader reader = new BufferedReader(
        new InputStreamReader(rawCache.openStream(url), StandardCharsets.ISO_8859_1))) {
      String headerRecord = CsvRecordReader.readRecord(reader);
      if (headerRecord == null) {
        throw new IOException("pep_components_of_change: empty CSV at " + url);
      }
      Map<String, Integer> idx = headerIndex(CsvRecordReader.splitFields(headerRecord, ','));
      int sumlev = require(idx, "SUMLEV", url);
      int state = require(idx, "STATE", url);
      int county = require(idx, "COUNTY", url);
      int stname = require(idx, "STNAME", url);
      int ctyname = require(idx, "CTYNAME", url);
      int pop = require(idx, "POPESTIMATE" + year, url);
      int births = require(idx, "BIRTHS" + year, url);
      int deaths = require(idx, "DEATHS" + year, url);
      int natural = require(idx, naturalPrefix + year, url);
      int intl = require(idx, "INTERNATIONALMIG" + year, url);
      int dom = require(idx, "DOMESTICMIG" + year, url);
      int net = require(idx, "NETMIG" + year, url);
      int residual = require(idx, "RESIDUAL" + year, url);
      String record;
      while ((record = CsvRecordReader.readRecord(reader)) != null) {
        List<String> cols = CsvRecordReader.splitFields(record, ',');
        if (cols.isEmpty() || (cols.size() == 1 && cols.get(0).trim().isEmpty())) {
          continue;
        }
        if (!COUNTY_SUMLEV.equals(cols.get(sumlev).trim())) {
          continue;
        }
        String stateFips = cols.get(state).trim();
        String countyCode = cols.get(county).trim();
        Map<String, Object> row = new LinkedHashMap<String, Object>();
        row.put("state_fips", stateFips);
        row.put("county_fips", stateFips + countyCode);
        row.put("state_name", cols.get(stname).trim());
        row.put("county_name", cols.get(ctyname).trim());
        row.put("population_estimate", num(cols, pop, "POPESTIMATE" + year));
        row.put("births", num(cols, births, "BIRTHS" + year));
        row.put("deaths", num(cols, deaths, "DEATHS" + year));
        row.put("natural_change", num(cols, natural, naturalPrefix + year));
        row.put("international_migration", num(cols, intl, "INTERNATIONALMIG" + year));
        row.put("domestic_migration", num(cols, dom, "DOMESTICMIG" + year));
        row.put("net_migration", num(cols, net, "NETMIG" + year));
        row.put("residual", num(cols, residual, "RESIDUAL" + year));
        row.put("vintage", String.valueOf(vintage));
        rows.add(row);
      }
    }
    if (rows.isEmpty()) {
      throw new IOException("pep_components_of_change: no county rows (SUMLEV 050) in " + url);
    }
    return rows.iterator();
  }

  static String url(int vintage) {
    int first = vintage == FINAL_OLD_VINTAGE ? 2010 : FIRST_NEW_YEAR;
    return BASE + first + "-" + vintage + "/counties/totals/co-est" + vintage + "-alldata.csv";
  }

  private static int requiredInt(Map<String, String> variables, String name, String alternate)
      throws IOException {
    String v = variables.get(name);
    if ((v == null || v.isEmpty()) && alternate != null) {
      v = variables.get(alternate);
    }
    if (v == null || v.isEmpty()) {
      throw new IOException("pep_components_of_change: dimension variable '" + name
          + "' missing from " + variables);
    }
    try {
      return Integer.parseInt(v.trim());
    } catch (NumberFormatException e) {
      throw new IOException("pep_components_of_change: dimension variable '" + name
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
      throw new IOException("pep_components_of_change: column '" + name + "' not found in "
          + url);
    }
    return i.intValue();
  }

  private static Long num(List<String> cols, int at, String name) throws IOException {
    String v = at < cols.size() ? cols.get(at).trim() : "";
    if (v.isEmpty()) {
      throw new IOException("pep_components_of_change: empty value in column " + name);
    }
    try {
      return Long.valueOf(Long.parseLong(v));
    } catch (NumberFormatException e) {
      throw new IOException("pep_components_of_change: non-integer value '" + v + "' in column "
          + name, e);
    }
  }
}
