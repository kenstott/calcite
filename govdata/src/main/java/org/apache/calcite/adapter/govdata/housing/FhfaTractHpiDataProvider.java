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
package org.apache.calcite.adapter.govdata.housing;

import org.apache.calcite.adapter.file.etl.DataProvider;
import org.apache.calcite.adapter.file.etl.EtlPipelineConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * FHFA annual all-transactions House Price Index at census-tract grain ({@code hpi_at_tract.csv},
 * about 2.2 million tract-year rows). The CSV is streamed line by line from a scratch file, which
 * is deleted once the iterator is exhausted or closed.
 */
public class FhfaTractHpiDataProvider implements DataProvider {

  private static final Logger LOGGER = LoggerFactory.getLogger(FhfaTractHpiDataProvider.class);

  private static final String URL = "https://www.fhfa.gov/hpi/download/annual/hpi_at_tract.csv";
  private static final String[] HEADER =
      {"tract", "state_abbr", "year", "annual_change", "hpi", "hpi1990", "hpi2000"};

  @Override public Iterator<Map<String, Object>> fetch(EtlPipelineConfig config,
      Map<String, String> variables) throws IOException {
    LOGGER.info("FhfaTractHpiDataProvider: fetching {} from {}", config.getName(), URL);
    File tmp = FhfaDownload.toTempFile(URL, ".csv");
    BufferedReader reader = null;
    try {
      reader = Files.newBufferedReader(tmp.toPath(), StandardCharsets.UTF_8);
      String header = reader.readLine();
      if (header == null || !Arrays.equals(HEADER, header.trim().split(",", -1))) {
        throw new IOException("fhfa tract hpi: unexpected header '" + header + "'");
      }
      return new TractRowIterator(reader, tmp);
    } catch (IOException | RuntimeException e) {
      if (reader != null) {
        reader.close();
      }
      Files.deleteIfExists(tmp.toPath());
      throw e;
    }
  }

  private static final class TractRowIterator implements Iterator<Map<String, Object>> {
    private final BufferedReader reader;
    private final File tmp;
    private Map<String, Object> next;
    private long lineNo = 1;
    private long count;
    private boolean done;

    TractRowIterator(BufferedReader reader, File tmp) {
      this.reader = reader;
      this.tmp = tmp;
    }

    @Override public boolean hasNext() {
      if (next == null && !done) {
        advance();
      }
      return next != null;
    }

    @Override public Map<String, Object> next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      Map<String, Object> r = next;
      next = null;
      return r;
    }

    private void advance() {
      try {
        String line = reader.readLine();
        if (line == null) {
          finish();
          if (count == 0) {
            throw new IOException("fhfa tract hpi: file parsed to zero rows");
          }
          LOGGER.info("FhfaTractHpiDataProvider: {} rows", count);
          return;
        }
        lineNo++;
        String[] f = line.split(",", -1);
        if (f.length != HEADER.length) {
          throw new IOException("fhfa tract hpi: line " + lineNo + " has " + f.length
              + " fields, expected " + HEADER.length);
        }
        String tract = f[0].trim();
        if (tract.length() != 11) {
          throw new IOException("fhfa tract hpi: line " + lineNo + " tract '" + tract
              + "' is not an 11-digit GEOID");
        }
        Map<String, Object> row = new LinkedHashMap<String, Object>();
        row.put("type", "tract_house_price_index");
        row.put("state_fips", tract.substring(0, 2));
        row.put("county_fips", tract.substring(0, 5));
        row.put("tract_geoid", tract);
        row.put("state_abbr", f[1].trim());
        Double yr = FhfaDownload.number(f[2], "year on line " + lineNo);
        if (yr == null) {
          throw new IOException("fhfa tract hpi: line " + lineNo + " has no year");
        }
        row.put("data_year", Integer.valueOf((int) Math.round(yr)));
        row.put("annual_change_pct", FhfaDownload.number(f[3], "annual_change on line " + lineNo));
        row.put("hpi", FhfaDownload.number(f[4], "hpi on line " + lineNo));
        row.put("hpi_1990_base", FhfaDownload.number(f[5], "hpi1990 on line " + lineNo));
        row.put("hpi_2000_base", FhfaDownload.number(f[6], "hpi2000 on line " + lineNo));
        count++;
        next = row;
      } catch (IOException e) {
        finish();
        throw new IllegalStateException(e.getMessage(), e);
      }
    }

    private void finish() {
      done = true;
      try {
        reader.close();
        Files.deleteIfExists(tmp.toPath());
      } catch (IOException e) {
        LOGGER.warn("FhfaTractHpiDataProvider: could not clean scratch file {}: {}", tmp,
            e.getMessage());
      }
    }
  }
}
