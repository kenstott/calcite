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

import org.apache.calcite.adapter.file.etl.CsvRecordReader;
import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.StreamingResponseTransformer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

/**
 * Streaming transformer for NCES IPEDS Completions bulk CSV ({@code C{year}_A.zip}).
 *
 * <p>Downloads the ZIP, locates the CSV entry (preferring the {@code _rv} revised file),
 * and streams rows lazily one at a time. Memory is O(one CSV row) regardless of file size.
 *
 * <p>Wide format: one row per (unitid, year, cipcode, majornum, award_level) with
 * race/ethnicity and sex counts as separate columns. {@code year} is injected from
 * the URL dimension context.
 */
public class IpedsCompletionsResponseTransformer implements StreamingResponseTransformer {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(IpedsCompletionsResponseTransformer.class);

  private static final Map<String, String> COLUMN_MAP;

  static {
    Map<String, String> m = new HashMap<String, String>();
    m.put("UNITID",  "unitid");
    m.put("CIPCODE", "cipcode");
    m.put("MAJORNUM", "majornum");
    m.put("AWLEVEL", "award_level");
    m.put("CTOTALT", "ctotalt");
    m.put("CTOTALM", "ctotalm");
    m.put("CTOTALW", "ctotalw");
    m.put("CAIANT",  "caiant");
    m.put("CAIANM",  "caianm");
    m.put("CAIANW",  "caianw");
    m.put("CASIAT",  "casiat");
    m.put("CASIAM",  "casiam");
    m.put("CASIAW",  "casiaw");
    m.put("CBKAAT",  "cbkaat");
    m.put("CBKAAM",  "cbkaam");
    m.put("CBKAAW",  "cbkaaw");
    m.put("CHISPT",  "chispt");
    m.put("CHISPM",  "chispm");
    m.put("CHISPW",  "chispw");
    m.put("CNHPIT",  "cnhpit");
    m.put("CNHPIM",  "cnhpim");
    m.put("CNHPIW",  "cnhpiw");
    m.put("CWHITT",  "cwhitt");
    m.put("CWHITM",  "cwhitm");
    m.put("CWHITW",  "cwhitw");
    m.put("C2MORT",  "c2mort");
    m.put("C2MORM",  "c2morm");
    m.put("C2MORW",  "c2morw");
    m.put("CUNKNT",  "cunknt");
    m.put("CUNKNM",  "cunknm");
    m.put("CUNKNW",  "cunknw");
    m.put("CNRALT",  "cnralt");
    m.put("CNRALM",  "cnralm");
    m.put("CNRALW",  "cnralw");
    COLUMN_MAP = Collections.unmodifiableMap(m);
  }

  @Override public Iterator<Map<String, Object>> fetchAndTransform(RequestContext context)
      throws IOException {
    String yearStr = context.getDimensionValues().get("year");

    File tempZip = IpedsFinancialsResponseTransformer.downloadToTemp(context.getUrl());
    ZipEntry entry = findCsvEntry(new ZipFile(tempZip), yearStr);
    if (entry == null) {
      tempZip.delete();
      LOGGER.warn("IpedsCompletions: no CSV in ZIP for year={}", yearStr);
      return Collections.emptyIterator();
    }

    ZipFile zf = new ZipFile(tempZip);
    BufferedReader reader = new BufferedReader(
        new InputStreamReader(zf.getInputStream(zf.getEntry(entry.getName())),
            StandardCharsets.UTF_8));
    return new LazyCompletionsIterator(reader, zf, tempZip, yearStr);
  }

  private ZipEntry findCsvEntry(ZipFile zf, String yearStr) throws IOException {
    String year = yearStr == null ? "" : yearStr.toLowerCase();
    ZipEntry preferred = null;
    ZipEntry fallback = null;
    Enumeration<? extends ZipEntry> entries = zf.entries();
    while (entries.hasMoreElements()) {
      ZipEntry e = entries.nextElement();
      String name = e.getName().toLowerCase();
      if (name.startsWith("c" + year + "_a") && name.endsWith(".csv")) {
        if (name.contains("_rv") || name.contains("_r")) {
          preferred = e;
        } else if (fallback == null) {
          fallback = e;
        }
      }
    }
    zf.close();
    return preferred != null ? preferred : fallback;
  }

  private static final class LazyCompletionsIterator implements Iterator<Map<String, Object>> {
    private final BufferedReader reader;
    private final ZipFile zipFile;
    private final File tempZip;
    private final String yearStr;
    private final String[] header;
    private Map<String, Object> next;
    private boolean closed;

    LazyCompletionsIterator(BufferedReader reader, ZipFile zipFile, File tempZip,
        String yearStr) throws IOException {
      this.reader = reader;
      this.zipFile = zipFile;
      this.tempZip = tempZip;
      this.yearStr = yearStr;

      String headerLine = CsvRecordReader.readRecord(reader);
      if (headerLine == null) {
        this.header = new String[0];
        close();
      } else {
        if (!headerLine.isEmpty() && headerLine.charAt(0) == '﻿') {
          headerLine = headerLine.substring(1);
        }
        String[] raw = IpedsFinancialsResponseTransformer.parseCsvLine(headerLine);
        for (int i = 0; i < raw.length; i++) {
          raw[i] = raw[i].trim().toUpperCase();
        }
        this.header = raw;
        advance();
      }
    }

    private void advance() {
      if (closed) {
        next = null;
        return;
      }
      try {
        String line = CsvRecordReader.readRecord(reader);
        if (line == null) {
          close();
          next = null;
          return;
        }
        String[] values = IpedsFinancialsResponseTransformer.parseCsvLine(line);
        Map<String, Object> row = new LinkedHashMap<String, Object>();
        if (yearStr != null) {
          try { row.put("year", Integer.parseInt(yearStr)); } catch (NumberFormatException e) { /**/ }
        }
        for (int c = 0; c < header.length && c < values.length; c++) {
          String col = header[c];
          if (col.startsWith("X")) continue;
          String canonical = COLUMN_MAP.get(col);
          if (canonical == null) continue;
          String raw = values[c].trim();
          if (raw.isEmpty()) {
            row.put(canonical, null);
          } else if ("cipcode".equals(canonical)) {
            row.put(canonical, raw);
          } else if ("unitid".equals(canonical) || "majornum".equals(canonical)
              || "award_level".equals(canonical)) {
            try { row.put(canonical, Integer.parseInt(raw)); } catch (NumberFormatException e) { row.put(canonical, null); }
          } else {
            try { row.put(canonical, Integer.parseInt(raw)); } catch (NumberFormatException e) { row.put(canonical, null); }
          }
        }
        if (row.get("unitid") != null) {
          next = row;
        } else {
          advance();
        }
      } catch (IOException e) {
        close();
        next = null;
      }
    }

    private void close() {
      if (!closed) {
        closed = true;
        try { reader.close(); } catch (IOException ignored) { /**/ }
        try { zipFile.close(); } catch (IOException ignored) { /**/ }
        tempZip.delete();
      }
    }

    @Override public boolean hasNext() { return next != null; }

    @Override public Map<String, Object> next() {
      if (next == null) throw new NoSuchElementException();
      Map<String, Object> result = next;
      advance();
      return result;
    }
  }
}
