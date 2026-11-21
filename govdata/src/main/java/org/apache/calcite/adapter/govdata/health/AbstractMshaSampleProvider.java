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
package org.apache.calcite.adapter.govdata.health;

import org.apache.calcite.adapter.file.etl.CachingDataProvider;
import org.apache.calcite.adapter.file.etl.CsvRecordReader;
import org.apache.calcite.adapter.file.etl.EtlPipelineConfig;
import org.apache.calcite.adapter.file.etl.RawCache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.HashMap;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * Base for the MSHA Open Government Data sample tables: one pipe-delimited {@code .txt} inside a
 * ZIP at {@code arlweb.msha.gov/OpenGovernmentData/DataSets}, streamed row by row through the raw
 * cache so a 1 GB file is never held in memory.
 *
 * <p>Every field is double-quoted and padded with spaces to the column width by the source;
 * values are trimmed and empty strings become null. Header lookup is by column name, never by
 * position, and a missing required column throws.
 */
abstract class AbstractMshaSampleProvider implements CachingDataProvider {

  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractMshaSampleProvider.class);
  static final DateTimeFormatter US_DATE = DateTimeFormatter.ofPattern("MM/dd/yyyy", Locale.ROOT);

  /** Source label used in messages. */
  abstract String label();

  /** Builds the output row for one source record, or null to skip it. */
  abstract Map<String, Object> toRow(Map<String, String> rec);

  /** Source columns that must be present in the header. */
  abstract String[] requiredColumns();

  /** Called once all rows are consumed, for a summary log of any rows skipped. */
  void finished() {
  }

  /** Opens the zip at {@code url} and returns a reader over its first {@code .txt} entry. */
  static BufferedReader openFirstTxt(RawCache rawCache, String url) throws IOException {
    InputStream raw = rawCache.openStream(url);
    ZipInputStream zis = new ZipInputStream(raw);
    ZipEntry entry;
    while ((entry = zis.getNextEntry()) != null) {
      if (entry.getName().toLowerCase(Locale.ROOT).endsWith(".txt")) {
        return new BufferedReader(new InputStreamReader(zis, StandardCharsets.ISO_8859_1),
            1 << 16);
      }
    }
    zis.close();
    throw new IOException("MSHA zip " + url + " contains no .txt entry");
  }

  /** Iterates the records of a pipe-delimited file as column-name to value maps. */
  static final class RecordIterator implements Iterator<Map<String, String>> {
    private final BufferedReader reader;
    private final String label;
    private final String[] header;
    private Map<String, String> next;

    RecordIterator(BufferedReader reader, String label, String[] required) throws IOException {
      this.reader = reader;
      this.label = label;
      String head = CsvRecordReader.readRecord(reader);
      if (head == null) {
        throw new IOException(label + ": empty file");
      }
      List<String> cols = CsvRecordReader.splitFields(head, '|');
      this.header = new String[cols.size()];
      Map<String, Boolean> present = new HashMap<>();
      for (int i = 0; i < header.length; i++) {
        header[i] = cols.get(i).trim().toUpperCase(Locale.ROOT);
        present.put(header[i], Boolean.TRUE);
      }
      for (String r : required) {
        if (!present.containsKey(r)) {
          throw new IOException(label + ": expected column " + r + " not in header " + cols);
        }
      }
    }

    private void advance() throws IOException {
      while (next == null) {
        String rec = CsvRecordReader.readRecord(reader);
        if (rec == null) {
          reader.close();
          return;
        }
        if (rec.trim().isEmpty()) {
          continue;
        }
        List<String> f = CsvRecordReader.splitFields(rec, '|');
        Map<String, String> m = new HashMap<>(header.length * 2);
        for (int i = 0; i < header.length && i < f.size(); i++) {
          String v = f.get(i).trim();
          if (!v.isEmpty()) {
            m.put(header[i], v);
          }
        }
        next = m;
      }
    }

    @Override public boolean hasNext() {
      try {
        advance();
      } catch (IOException e) {
        throw new RuntimeException(label + ": read failed", e);
      }
      return next != null;
    }

    @Override public Map<String, String> next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      Map<String, String> r = next;
      next = null;
      return r;
    }
  }

  @Override public Iterator<Map<String, Object>> fetch(EtlPipelineConfig config,
      Map<String, String> variables, RawCache rawCache) throws IOException {
    String url = config.getSource() != null ? config.getSource().getUrl() : null;
    if (url == null || url.isEmpty()) {
      throw new IOException(label() + ": source.url is required");
    }
    prepare(config, rawCache);
    final RecordIterator records =
        new RecordIterator(openFirstTxt(rawCache, url), label(), requiredColumns());
    return new Iterator<Map<String, Object>>() {
      private Map<String, Object> pending;
      private boolean ended;

      private void advance() {
        while (pending == null && !ended) {
          if (!records.hasNext()) {
            ended = true;
            finished();
            return;
          }
          pending = toRow(records.next());
        }
      }

      @Override public boolean hasNext() {
        advance();
        return pending != null;
      }

      @Override public Map<String, Object> next() {
        advance();
        if (pending == null) {
          throw new NoSuchElementException();
        }
        Map<String, Object> r = pending;
        pending = null;
        return r;
      }
    };
  }

  /** Hook run before streaming, e.g. to load a lookup file. */
  void prepare(EtlPipelineConfig config, RawCache rawCache) throws IOException {
  }

  // ---- value helpers -------------------------------------------------

  static String str(Map<String, String> r, String col) {
    return r.get(col);
  }

  static Double dbl(Map<String, String> r, String col) {
    String v = r.get(col);
    if (v == null) {
      return null;
    }
    try {
      return Double.valueOf(v.replace(",", ""));
    } catch (NumberFormatException e) {
      throw new IllegalStateException("MSHA: non-numeric " + col + "='" + v + "'", e);
    }
  }

  static Integer intg(Map<String, String> r, String col) {
    Double d = dbl(r, col);
    return d == null ? null : Integer.valueOf((int) Math.round(d.doubleValue()));
  }

  static LocalDate date(Map<String, String> r, String col) {
    String v = r.get(col);
    if (v == null) {
      return null;
    }
    String[] p = v.split("/");
    if (p.length != 3) {
      throw new IllegalStateException("MSHA: bad date " + col + "='" + v + "'");
    }
    return LocalDate.of(Integer.parseInt(p[2]), Integer.parseInt(p[0]), Integer.parseInt(p[1]));
  }

  /** Whether {@code d} is later than today: the source carries ~40 typo dates out to 2099. */
  static boolean inFuture(LocalDate d) {
    return d.isAfter(LocalDate.now(ZoneOffset.UTC));
  }

  static void logSkipped(String label, long skipped) {
    if (skipped > 0) {
      LOGGER.warn("{}: skipped {} rows whose sample date is in the future (source typos)", label,
          skipped);
    }
  }
}
