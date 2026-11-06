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
import org.apache.calcite.adapter.file.etl.NetworkOutageWait;
import org.apache.calcite.adapter.file.etl.RawCache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.nio.charset.Charset;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * Streams the rows of a bulk CSV download (a plain CSV or every matching CSV entry of a zip)
 * through the pipeline's raw cache, one row at a time, so a multi-gigabyte archive never has to
 * fit in memory and a multi-part archive (USAspending splits a fiscal year across several CSVs)
 * loses no part.
 *
 * <p>A subclass names the source URL, the zip entries to read, the character set and the output
 * columns. Every declared source header must exist: a missing header throws, and so does a
 * numeric cell that does not parse, rather than yielding a null.
 */
abstract class AbstractBulkCsvProvider implements CachingDataProvider {

  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractBulkCsvProvider.class);

  /** Column kind: string, double, integer, long. */
  static final String S = "s";
  static final String D = "d";
  static final String I = "i";
  static final String L = "l";

  /** The table name, for log and error messages. */
  abstract String tableName();

  /** The URL of the file for this batch. */
  abstract String resolveUrl(Map<String, String> variables) throws IOException;

  /**
   * Cache identity for this batch's download. Defaults to the URL; a source whose URL never
   * changes while its content does overrides this to add the dimension that busts the cache.
   */
  String cacheKey(String url, Map<String, String> variables) {
    return url;
  }

  /** Zip entry name suffix to read (case-insensitive), or null when the download is a plain CSV. */
  abstract String entrySuffix();

  abstract Charset charset();

  /** {output column, source header, kind}. */
  abstract String[][] columns();

  /** Source cell text that means "no value" (for example {@code NULL}), or null for none. */
  String nullToken() {
    return null;
  }

  @Override public Iterator<Map<String, Object>> fetch(EtlPipelineConfig config,
      Map<String, String> variables, RawCache rawCache) throws IOException {
    final String url = resolveUrl(variables);
    LOGGER.info("{}: reading {}", tableName(), url);
    InputStream in = rawCache.openStream(cacheKey(url, variables), new RawCache.ContentSupplier() {
      @Override public InputStream open() throws IOException {
        return openUrl(url);
      }
    });
    return new RowIterator(in, url);
  }

  /** Consecutive resumes that make no progress before the download is abandoned. */
  private static final int MAX_STALLED_RESUMES = 20;

  /**
   * Opens a GET, following redirects, failing on any non-200 status. The returned stream checks
   * the bytes received against the declared {@code Content-Length} and, when the server closes
   * the connection early (files.usaspending.gov does, cleanly and without an error, partway
   * through multi-gigabyte files), reopens the URL with a {@code Range} request from the byte
   * reached. It throws if the file cannot be completed, so a truncated archive is never cached.
   */
  static InputStream openUrl(String url) throws IOException {
    return new ResumableStream(url);
  }

  private static final class ResumableStream extends InputStream {
    private final String url;
    private HttpURLConnection conn;
    private InputStream in;
    private long total = -1;
    private long pos;
    private int resumes;
    private int stalled;
    private long posAtResume = -1;

    ResumableStream(String url) throws IOException {
      this.url = url;
      open(0);
    }

    private void open(long from) throws IOException {
      if (conn != null) {
        conn.disconnect();
      }
      conn = (HttpURLConnection) java.net.URI.create(url).toURL().openConnection();
      conn.setRequestMethod("GET");
      conn.setConnectTimeout(30000);
      conn.setReadTimeout(300000);
      conn.setRequestProperty("User-Agent", FiscalHttp.USER_AGENT);
      if (from > 0) {
        conn.setRequestProperty("Range", "bytes=" + from + "-");
      }
      NetworkOutageWait.connect(conn);
      int status = conn.getResponseCode();
      if (from == 0) {
        if (status != 200) {
          throw new IOException("HTTP " + status + " for " + url);
        }
        total = conn.getContentLengthLong();
        if (total < 0) {
          throw new IOException("No Content-Length from " + url + "; completeness cannot be "
              + "verified");
        }
      } else if (status != 206) {
        throw new IOException("HTTP " + status + " resuming " + url + " at byte " + from);
      }
      in = conn.getInputStream();
    }

    @Override public int read() throws IOException {
      byte[] one = new byte[1];
      int n = read(one, 0, 1);
      return n < 0 ? -1 : one[0] & 0xff;
    }

    @Override public int read(byte[] b, int off, int len) throws IOException {
      while (true) {
        if (pos >= total) {
          return -1;
        }
        int n;
        try {
          n = in.read(b, off, len);
        } catch (java.io.IOException e) {
          n = -2;
          LOGGER.warn("Read from {} failed at byte {} of {}: {}", url, pos, total, e.toString());
        }
        if (n > 0) {
          pos += n;
          return n;
        }
        if (pos >= total) {
          return -1;
        }
        stalled = pos == posAtResume ? stalled + 1 : 0;
        if (stalled > MAX_STALLED_RESUMES) {
          throw new IOException("Download of " + url + " stopped at byte " + pos + " of " + total
              + " and made no progress in " + MAX_STALLED_RESUMES + " resumes");
        }
        posAtResume = pos;
        resumes++;
        if (resumes % 20 == 1) {
          LOGGER.info("Resuming {} at byte {} of {} (resume {})", url, pos, total, resumes);
        }
        open(pos);
      }
    }

    @Override public void close() throws IOException {
      if (conn != null) {
        conn.disconnect();
      }
    }
  }

  private final class RowIterator implements Iterator<Map<String, Object>> {
    private final String url;
    private final ZipInputStream zip;
    private final InputStream plain;
    private final String[][] cols = columns();
    private BufferedReader reader;
    private int[] idx;
    private String entryName;
    private boolean plainConsumed;
    private Map<String, Object> next;
    private boolean done;
    private long emitted;

    RowIterator(InputStream in, String url) {
      this.url = url;
      if (entrySuffix() == null) {
        this.zip = null;
        this.plain = in;
      } else {
        this.zip = new ZipInputStream(in);
        this.plain = null;
      }
    }

    private boolean openNext() throws IOException {
      if (zip == null) {
        if (plainConsumed) {
          return false;
        }
        plainConsumed = true;
        entryName = url;
        reader = new BufferedReader(new InputStreamReader(plain, charset()), 1 << 16);
      } else {
        ZipEntry e;
        String suffix = entrySuffix().toLowerCase(java.util.Locale.ROOT);
        while ((e = zip.getNextEntry()) != null) {
          if (!e.isDirectory() && e.getName().toLowerCase(java.util.Locale.ROOT).endsWith(suffix)) {
            break;
          }
        }
        if (e == null) {
          return false;
        }
        entryName = e.getName();
        reader = new BufferedReader(new InputStreamReader(zip, charset()), 1 << 16);
      }
      LOGGER.info("{}: streaming {}", tableName(), entryName);
      String header = CsvRecordReader.readRecord(reader);
      if (header == null) {
        throw new IOException(tableName() + ": empty CSV " + entryName + " in " + url);
      }
      Map<String, Integer> h = FiscalHttp.headerIndex(CsvRecordReader.splitFields(stripBom(header), ','));
      idx = new int[cols.length];
      for (int i = 0; i < cols.length; i++) {
        idx[i] = FiscalHttp.required(h, cols[i][1], entryName);
      }
      return true;
    }

    private String stripBom(String s) {
      return !s.isEmpty() && s.charAt(0) == '﻿' ? s.substring(1) : s;
    }

    private void advance() {
      if (next != null || done) {
        return;
      }
      try {
        while (true) {
          if (reader == null && !openNext()) {
            done = true;
            closeAll();
            return;
          }
          String record = CsvRecordReader.readRecord(reader);
          if (record == null) {
            // Close only the reader's view of a plain stream; a zip stays open for the next entry.
            if (zip == null) {
              reader.close();
            }
            reader = null;
            continue;
          }
          List<String> cells = CsvRecordReader.splitFields(record, ',');
          if (cells.isEmpty()) {
            continue;
          }
          next = toRow(cells);
          emitted++;
          return;
        }
      } catch (IOException e) {
        throw new RuntimeException(tableName() + ": streaming failed in " + entryName
            + " after " + emitted + " rows", e);
      }
    }

    private void closeAll() throws IOException {
      if (zip != null) {
        zip.close();
      }
    }

    private Map<String, Object> toRow(List<String> cells) {
      Map<String, Object> row = new LinkedHashMap<String, Object>();
      for (int i = 0; i < cols.length; i++) {
        if (idx[i] >= cells.size()) {
          throw new IllegalStateException(tableName() + ": row in " + entryName + " has "
              + cells.size() + " cells, header column '" + cols[i][1] + "' is at " + idx[i]);
        }
        String raw = cells.get(idx[i]).trim();
        String token = nullToken();
        if (raw.isEmpty() || (token != null && raw.equals(token))) {
          row.put(cols[i][0], null);
          continue;
        }
        String kind = cols[i][2];
        try {
          if (D.equals(kind)) {
            row.put(cols[i][0], Double.valueOf(raw));
          } else if (I.equals(kind)) {
            row.put(cols[i][0], Integer.valueOf(raw));
          } else if (L.equals(kind)) {
            row.put(cols[i][0], Long.valueOf(raw));
          } else {
            row.put(cols[i][0], raw);
          }
        } catch (NumberFormatException e) {
          throw new IllegalStateException(tableName() + ": column '" + cols[i][1] + "' value '"
              + raw + "' is not a number (" + kind + ") in " + entryName, e);
        }
      }
      return row;
    }

    @Override public boolean hasNext() {
      advance();
      return next != null;
    }

    @Override public Map<String, Object> next() {
      advance();
      if (next == null) {
        throw new NoSuchElementException();
      }
      Map<String, Object> r = next;
      next = null;
      return r;
    }
  }
}
