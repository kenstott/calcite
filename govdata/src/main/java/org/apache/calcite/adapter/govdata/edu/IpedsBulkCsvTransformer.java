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

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

/**
 * Base for streaming transformers over NCES IPEDS bulk survey ZIPs whose CSV has one
 * row per UNITID (or UNITID x level) and purely numeric measure columns.
 *
 * <p>Subclasses name the CSV file prefix (for example {@code ef} + year + {@code a}),
 * the source-to-canonical column map and may derive extra columns per row. Revised
 * ({@code _rv}) files are preferred over the base file. Imputation-flag columns
 * (X-prefixed) are ignored. A non-empty value that is not an integer is a source
 * contract violation and fails the load.
 */
abstract class IpedsBulkCsvTransformer implements StreamingResponseTransformer {

  /** CSV base name for the effective year, lower case, e.g. {@code ef2023a}. */
  abstract String csvBaseName(String effectiveYear);

  /** Source column (upper case) to canonical column name; unmapped columns are dropped. */
  abstract Map<String, String> columnMap();

  /** Adds derived columns to a parsed row. */
  abstract void derive(Map<String, Object> row);

  @Override public Iterator<Map<String, Object>> fetchAndTransform(RequestContext context)
      throws IOException {
    String yearStr = context.getDimensionValues().get("effective_year");
    if (yearStr == null || yearStr.isEmpty()) {
      yearStr = context.getDimensionValues().get("year");
    }
    int year = Integer.parseInt(yearStr);
    String base = csvBaseName(yearStr);

    File tempZip = IpedsFinancialsResponseTransformer.downloadToTemp(context.getUrl());
    ZipFile zf = new ZipFile(tempZip);
    ZipEntry preferred = null;
    ZipEntry plain = null;
    Enumeration<? extends ZipEntry> entries = zf.entries();
    while (entries.hasMoreElements()) {
      ZipEntry e = entries.nextElement();
      String name = e.getName().toLowerCase();
      if (name.equals(base + "_rv.csv")) {
        preferred = e;
      } else if (name.equals(base + ".csv")) {
        plain = e;
      }
    }
    ZipEntry chosen = preferred != null ? preferred : plain;
    if (chosen == null) {
      zf.close();
      tempZip.delete();
      throw new IOException("No " + base + ".csv in ZIP from " + context.getUrl());
    }
    BufferedReader reader = new BufferedReader(
        new InputStreamReader(zf.getInputStream(chosen), StandardCharsets.ISO_8859_1));
    return new RowIterator(reader, zf, tempZip, year, columnMap(), this);
  }

  private static final class RowIterator implements Iterator<Map<String, Object>> {
    private final BufferedReader reader;
    private final ZipFile zipFile;
    private final File tempZip;
    private final int year;
    private final Map<String, String> columns;
    private final IpedsBulkCsvTransformer owner;
    private final String[] header;
    private Map<String, Object> next;
    private boolean closed;

    RowIterator(BufferedReader reader, ZipFile zipFile, File tempZip, int year,
        Map<String, String> columns, IpedsBulkCsvTransformer owner) throws IOException {
      this.reader = reader;
      this.zipFile = zipFile;
      this.tempZip = tempZip;
      this.year = year;
      this.columns = columns;
      this.owner = owner;
      String headerLine = CsvRecordReader.readRecord(reader);
      if (headerLine == null) {
        close();
        throw new IOException("Empty IPEDS CSV for year " + year);
      }
      if (!headerLine.isEmpty() && headerLine.charAt(0) == '\uFEFF') {
        headerLine = headerLine.substring(1);
      }
      String[] raw = IpedsFinancialsResponseTransformer.parseCsvLine(headerLine);
      for (int i = 0; i < raw.length; i++) {
        raw[i] = raw[i].trim().toUpperCase();
      }
      this.header = raw;
      advance();
    }

    private void advance() {
      try {
        String line = CsvRecordReader.readRecord(reader);
        if (line == null) {
          close();
          next = null;
          return;
        }
        String[] values = IpedsFinancialsResponseTransformer.parseCsvLine(line);
        Map<String, Object> row = new LinkedHashMap<String, Object>();
        row.put("year", year);
        for (int c = 0; c < header.length && c < values.length; c++) {
          String canonical = columns.get(header[c]);
          if (canonical == null) {
            continue;
          }
          String v = values[c].trim();
          row.put(canonical, v.isEmpty() ? null : Integer.valueOf(Integer.parseInt(v)));
        }
        if (row.get("unitid") == null) {
          throw new IllegalStateException("IPEDS row without UNITID in year " + year);
        }
        owner.derive(row);
        next = row;
      } catch (IOException e) {
        close();
        throw new IllegalStateException("IPEDS CSV read failed for year " + year, e);
      }
    }

    private void close() {
      if (!closed) {
        closed = true;
        try {
          reader.close();
          zipFile.close();
        } catch (IOException e) {
          throw new IllegalStateException("IPEDS ZIP close failed", e);
        } finally {
          tempZip.delete();
        }
      }
    }

    @Override public boolean hasNext() {
      return next != null;
    }

    @Override public Map<String, Object> next() {
      if (next == null) {
        throw new NoSuchElementException();
      }
      Map<String, Object> result = next;
      advance();
      return result;
    }
  }
}
