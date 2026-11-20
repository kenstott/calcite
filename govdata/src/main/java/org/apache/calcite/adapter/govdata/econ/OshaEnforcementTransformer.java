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
package org.apache.calcite.adapter.govdata.econ;
// storage-provider-guard:ignore-file - audited: the DOL bulk zip is downloaded to and read from a genuinely-local staging temp file (java.util.zip.ZipFile needs a real path).

import org.apache.calcite.adapter.file.etl.CsvRecordReader;
import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.StreamingResponseTransformer;
import org.apache.calcite.adapter.govdata.ZipDownloadUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

/**
 * Base class that streams a U.S. Department of Labor OSHA enforcement bulk file
 * ({@code data.dol.gov/data-catalog/OSHA/<dataset>/OSHA_<dataset>.zip}) into rows.
 *
 * <p>Each zip holds the whole history as a series of {@code <date>_chunk_N.csv} entries (stored,
 * not deflated, UTF-8 with a BOM, one shared header per chunk). The zip is downloaded once to a
 * local staging file, then every CSV entry is read lazily, record by record, so memory stays
 * O(one record) however many millions of rows the file holds. The staging file is removed when
 * the iterator is exhausted.
 *
 * <p>Source encodings of "no value" are {@code NaT} (dates), {@code nan} (numbers) and the empty
 * string; all three become {@code null}. Any other value that does not parse as its declared type
 * fails the load rather than being dropped.
 */
abstract class OshaEnforcementTransformer implements StreamingResponseTransformer {

  private static final Logger LOGGER = LoggerFactory.getLogger(OshaEnforcementTransformer.class);

  /** Column kinds a {@link Col} can declare. */
  enum Kind { STRING, INT, DOUBLE, DATE, YEAR }

  /** One output column: its name, the CSV header it comes from and its type. */
  static final class Col {
    final String out;
    final String src;
    final Kind kind;

    Col(String out, String src, Kind kind) {
      this.out = out;
      this.src = src;
      this.kind = kind;
    }
  }

  /** Dataset name used in logs and in the thrown errors. */
  protected abstract String datasetName();

  /** Output columns, in order. */
  protected abstract List<Col> columns();

  static Col str(String out, String src) {
    return new Col(out, src, Kind.STRING);
  }

  static Col intCol(String out, String src) {
    return new Col(out, src, Kind.INT);
  }

  static Col dbl(String out, String src) {
    return new Col(out, src, Kind.DOUBLE);
  }

  static Col date(String out, String src) {
    return new Col(out, src, Kind.DATE);
  }

  /** Calendar year of the date in {@code src}. */
  static Col year(String out, String src) {
    return new Col(out, src, Kind.YEAR);
  }

  @Override public String transform(String response, RequestContext context) {
    throw new UnsupportedOperationException(
        getClass().getSimpleName() + " is a StreamingResponseTransformer; use fetchAndTransform");
  }

  @Override public Iterator<Map<String, Object>> fetchAndTransform(RequestContext context)
      throws IOException {
    final String url = context.getUrl();
    final File zipFile = org.apache.calcite.adapter.file.StagingDirectory.newTempFile(
        "osha-" + datasetName() + "-", ".zip").toPath().toFile();
    boolean ok = false;
    try {
      Map<String, String> headers = new HashMap<String, String>();
      headers.put("User-Agent", "govdata-etl/1.0 (+https://github.com/kenstott/calcite)");
      ZipDownloadUtils.downloadToFile(url, headers, zipFile);
      Iterator<Map<String, Object>> it = stream(zipFile, url, datasetName(), columns());
      ok = true;
      return it;
    } finally {
      if (!ok && zipFile.exists() && !zipFile.delete()) {
        zipFile.deleteOnExit();
      }
    }
  }

  /** Streams every CSV chunk of {@code zipFile}; deletes the file once exhausted. */
  static Iterator<Map<String, Object>> stream(File zipFile, String url, String dataset,
      List<Col> cols) throws IOException {
    return new RowIterator(zipFile, url, dataset, cols);
  }

  /** Lazily walks every CSV entry of the zip. */
  private static final class RowIterator implements Iterator<Map<String, Object>> {
    private final File zipFile;
    private final String url;
    private final String dataset;
    private final List<Col> cols;
    private final ZipFile zip;
    private final List<ZipEntry> entries = new ArrayList<ZipEntry>();
    private int nextEntry;
    private BufferedReader reader;
    private int[] srcIdx;
    private int headerWidth;
    private String currentEntry;
    private Map<String, Object> pending;
    private boolean done;
    private long rows;

    RowIterator(File zipFile, String url, String dataset, List<Col> cols) throws IOException {
      this.zipFile = zipFile;
      this.url = url;
      this.dataset = dataset;
      this.cols = cols;
      this.zip = new ZipFile(zipFile);
      Enumeration<? extends ZipEntry> en = zip.entries();
      while (en.hasMoreElements()) {
        ZipEntry e = en.nextElement();
        if (!e.isDirectory() && e.getName().toLowerCase().endsWith(".csv")) {
          entries.add(e);
        }
      }
      if (entries.isEmpty()) {
        close();
        throw new IOException("osha_" + dataset + ": no .csv entry in zip from " + url);
      }
      Collections.sort(entries, new Comparator<ZipEntry>() {
        @Override public int compare(ZipEntry a, ZipEntry b) {
          return a.getName().compareTo(b.getName());
        }
      });
      LOGGER.info("osha_{}: {} csv chunk(s) in {}", dataset, entries.size(), url);
    }

    private void openNextEntry() throws IOException {
      ZipEntry e = entries.get(nextEntry++);
      currentEntry = e.getName();
      reader = new BufferedReader(
          new InputStreamReader(zip.getInputStream(e), StandardCharsets.UTF_8), 1 << 16);
      String headerRecord = CsvRecordReader.readRecord(reader);
      if (headerRecord == null) {
        throw new IOException("osha_" + dataset + ": empty chunk " + currentEntry);
      }
      if (headerRecord.length() > 0 && headerRecord.charAt(0) == '﻿') {
        headerRecord = headerRecord.substring(1);
      }
      List<String> header = CsvRecordReader.splitFields(headerRecord, ',');
      headerWidth = header.size();
      Map<String, Integer> pos = new HashMap<String, Integer>();
      for (int i = 0; i < header.size(); i++) {
        pos.put(header.get(i).trim().toUpperCase(), i);
      }
      srcIdx = new int[cols.size()];
      for (int i = 0; i < cols.size(); i++) {
        Integer p = pos.get(cols.get(i).src);
        if (p == null) {
          throw new IOException("osha_" + dataset + ": column " + cols.get(i).src
              + " missing from header of " + currentEntry);
        }
        srcIdx[i] = p;
      }
    }

    private void fill() {
      if (pending != null || done) {
        return;
      }
      try {
        while (true) {
          if (reader == null) {
            if (nextEntry >= entries.size()) {
              close();
              return;
            }
            openNextEntry();
          }
          String record = CsvRecordReader.readRecord(reader);
          if (record == null) {
            reader.close();
            reader = null;
            continue;
          }
          if (record.isEmpty()) {
            continue;
          }
          List<String> f = CsvRecordReader.splitFields(record, ',');
          if (f.size() != headerWidth) {
            throw new IllegalStateException("osha_" + dataset + ": row has " + f.size()
                + " fields, header has " + headerWidth + " in " + currentEntry + ": " + record);
          }
          pending = toRow(f);
          rows++;
          return;
        }
      } catch (IOException e) {
        throw new IllegalStateException("osha_" + dataset + ": failed reading " + url, e);
      }
    }

    private Map<String, Object> toRow(List<String> f) {
      Map<String, Object> row = new LinkedHashMap<String, Object>(cols.size() * 2);
      for (int i = 0; i < cols.size(); i++) {
        Col c = cols.get(i);
        String raw = f.get(srcIdx[i]).trim();
        row.put(c.out, convert(c, raw));
      }
      return row;
    }

    private Object convert(Col c, String raw) {
      if (raw.isEmpty() || "NaT".equals(raw) || "nan".equals(raw)) {
        return null;
      }
      switch (c.kind) {
      case STRING:
        return raw;
      case INT:
        return Integer.valueOf(parseDouble(c, raw).intValue());
      case DOUBLE:
        return parseDouble(c, raw);
      case DATE:
        return isoDate(c, raw);
      case YEAR:
        return Integer.valueOf(isoDate(c, raw).substring(0, 4));
      default:
        throw new IllegalStateException("unhandled kind " + c.kind);
      }
    }

    private Double parseDouble(Col c, String raw) {
      try {
        return Double.valueOf(raw);
      } catch (NumberFormatException e) {
        throw new IllegalStateException("osha_" + dataset + ": column " + c.src
            + " is not numeric in " + currentEntry + ": '" + raw + "'", e);
      }
    }

    /** Source dates are {@code 1978-08-18 00:00:00+00:00}; the date part is already ISO. */
    private String isoDate(Col c, String raw) {
      if (raw.length() < 10 || raw.charAt(4) != '-' || raw.charAt(7) != '-') {
        throw new IllegalStateException("osha_" + dataset + ": column " + c.src
            + " is not a date in " + currentEntry + ": '" + raw + "'");
      }
      return raw.substring(0, 10);
    }

    private void close() {
      done = true;
      try {
        if (reader != null) {
          reader.close();
        }
        zip.close();
      } catch (IOException e) {
        LOGGER.warn("osha_{}: close failed: {}", dataset, e.getMessage());
      }
      if (zipFile.exists() && !zipFile.delete()) {
        zipFile.deleteOnExit();
      }
      LOGGER.info("osha_{}: streamed {} rows", dataset, rows);
    }

    @Override public boolean hasNext() {
      fill();
      return pending != null;
    }

    @Override public Map<String, Object> next() {
      fill();
      if (pending == null) {
        throw new NoSuchElementException();
      }
      Map<String, Object> r = pending;
      pending = null;
      return r;
    }
  }
}
