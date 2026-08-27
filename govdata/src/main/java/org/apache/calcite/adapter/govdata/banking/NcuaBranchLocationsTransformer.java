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
package org.apache.calcite.adapter.govdata.banking;

import org.apache.calcite.adapter.file.etl.CsvRecordReader;
import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.StreamingResponseTransformer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.Iterator;

/**
 * Streaming transformer for NCUA's quarterly "5300 Call Report" bulk zip's
 * {@code Credit Union Branch Information.txt} member — one row per federally-insured
 * credit union branch/office, comma-delimited with RFC4180 quoting.
 *
 * <p>The zip holds ~25 files; this pulls only the one named member rather than the
 * first entry ({@link org.apache.calcite.adapter.govdata.ag.RmaSummaryOfBusinessTransformer}'s
 * pattern, adapted for a multi-file archive). The credit-union equivalent of
 * {@code banking.locations} (FDIC branch/office locations) — joins to
 * {@code banking.institutions}-shaped analysis by treating {@code cu_number} as the
 * institution key, so a bank-vs-credit-union comparison is a UNION ALL of the two
 * tables rather than a schema change to either.
 */
public class NcuaBranchLocationsTransformer implements StreamingResponseTransformer {

  private static final Logger LOGGER = LoggerFactory.getLogger(NcuaBranchLocationsTransformer.class);
  private static final String ENTRY_NAME = "Credit Union Branch Information.txt";

  @Override public Iterator<Map<String, Object>> fetchAndTransform(RequestContext context)
      throws IOException {
    final String url = context.getUrl();
    final String cycle = context.getDimensionValues().get("cycle");
    final BufferedReader reader = new BufferedReader(
        new InputStreamReader(openNamedZipEntry(url, ENTRY_NAME), StandardCharsets.UTF_8));

    final String headerLine = CsvRecordReader.readRecord(reader);
    if (headerLine == null) {
      reader.close();
      return java.util.Collections.emptyIterator();
    }
    final String[] headers = CsvRecordReader.splitFields(headerLine, ',').toArray(new String[0]);

    return new Iterator<Map<String, Object>>() {
      private final ArrayDeque<Map<String, Object>> pending = new ArrayDeque<Map<String, Object>>();
      private boolean closed;

      private void fill() {
        try {
          String record;
          while (pending.isEmpty() && (record = CsvRecordReader.readRecord(reader)) != null) {
            if (!record.isEmpty()) {
              List<String> fields = CsvRecordReader.splitFields(record, ',');
              pending.add(mapRow(headers, fields.toArray(new String[0]), cycle, url));
            }
          }
        } catch (IOException e) {
          throw new RuntimeException("Failed streaming NCUA branch file: " + url, e);
        }
        if (pending.isEmpty() && !closed) {
          closed = true;
          try {
            reader.close();
          } catch (IOException ignored) {
            // best-effort
          }
        }
      }

      @Override public boolean hasNext() {
        fill();
        return !pending.isEmpty();
      }

      @Override public Map<String, Object> next() {
        fill();
        if (pending.isEmpty()) {
          throw new NoSuchElementException();
        }
        return pending.poll();
      }
    };
  }

  private Map<String, Object> mapRow(String[] headers, String[] values, String cycle, String url) {
    Map<String, Object> row = new LinkedHashMap<String, Object>();
    row.put("cycle", cycle);
    row.put("cu_number", longOrNull(col(headers, values, "CU_NUMBER")));
    row.put("site_id", longOrNull(col(headers, values, "SiteId")));
    row.put("cu_name", col(headers, values, "CU_NAME"));
    row.put("site_name", col(headers, values, "SiteName"));
    row.put("site_type_name", col(headers, values, "SiteTypeName"));
    row.put("is_main_office", yesNoOrNull(col(headers, values, "MainOffice")));
    row.put("address", col(headers, values, "PhysicalAddressLine1"));
    row.put("city", col(headers, values, "PhysicalAddressCity"));
    row.put("state_abbr", col(headers, values, "PhysicalAddressStateCode"));
    row.put("zip", col(headers, values, "PhysicalAddressPostalCode"));
    row.put("county_name", col(headers, values, "PhysicalAddressCountyName"));
    row.put("country", col(headers, values, "PhysicalAddressCountry"));
    row.put("phone", col(headers, values, "PhoneNumber"));
    row.put("has_atm", oneZeroOrNull(col(headers, values, "ATM")));
    row.put("has_drive_thru", oneZeroOrNull(col(headers, values, "DriveThru")));
    return row;
  }

  private static String col(String[] headers, String[] values, String name) {
    for (int i = 0; i < headers.length; i++) {
      if (headers[i].equalsIgnoreCase(name)) {
        if (i < values.length) {
          String v = values[i].trim();
          return v.isEmpty() ? null : v;
        }
        return null;
      }
    }
    return null;
  }

  private static Long longOrNull(String raw) {
    if (raw == null) {
      return null;
    }
    try {
      return Long.valueOf(raw);
    } catch (NumberFormatException e) {
      return null;
    }
  }

  private static Boolean yesNoOrNull(String raw) {
    if (raw == null) {
      return null;
    }
    return "Yes".equalsIgnoreCase(raw) ? Boolean.TRUE
        : "No".equalsIgnoreCase(raw) ? Boolean.FALSE : null;
  }

  private static Boolean oneZeroOrNull(String raw) {
    if (raw == null) {
      return null;
    }
    return "1".equals(raw) ? Boolean.TRUE : "0".equals(raw) ? Boolean.FALSE : null;
  }

  /** Downloads the zip and returns a stream positioned at the named entry. */
  private static InputStream openNamedZipEntry(String url, String entryName) throws IOException {
    HttpURLConnection conn = (HttpURLConnection) URI.create(url).toURL().openConnection();
    conn.setConnectTimeout(30000);
    conn.setReadTimeout(300000);
    conn.setInstanceFollowRedirects(true);
    int code = conn.getResponseCode();
    if (code < 200 || code >= 300) {
      throw new IOException("NCUA call report download HTTP " + code + ": " + url);
    }
    ZipInputStream zis = new ZipInputStream(conn.getInputStream());
    ZipEntry entry;
    while ((entry = zis.getNextEntry()) != null) {
      if (entryName.equals(entry.getName())) {
        LOGGER.debug("NCUA: streaming {} from {}", entryName, url);
        return zis;
      }
    }
    zis.close();
    throw new IOException("NCUA zip missing entry '" + entryName + "': " + url);
  }
}
