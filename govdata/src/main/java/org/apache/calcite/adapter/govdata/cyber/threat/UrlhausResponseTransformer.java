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
package org.apache.calcite.adapter.govdata.cyber.threat;

import org.apache.calcite.adapter.file.etl.CsvRecordReader;
import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.ResponseTransformer;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * Transforms URLhaus malicious URL feed (ZIP archive containing CSV) into
 * flat {@code ioc_urls} rows.
 *
 * <p>The ZIP from {@code https://urlhaus.abuse.ch/downloads/csv/} contains a
 * single {@code .txt} file. Lines starting with {@code #} are comments and are
 * skipped. Data lines are CSV with the format:
 * <pre>
 * id,dateadded,url,url_status,last_online,threat,tags,urlhaus_link,reporter
 * </pre>
 *
 * <p>The response bytes passed to this transformer may be corrupt when passed as a
 * String (ZIP is binary), so the ZIP is fetched directly from {@code context.getUrl()}.
 *
 * <p>{@code first_seen} (partition column) is the date portion of {@code dateadded}.
 * {@code source} is set to {@code "urlhaus"}.
 */
public class UrlhausResponseTransformer implements ResponseTransformer {

  private static final Logger LOGGER = LoggerFactory.getLogger(UrlhausResponseTransformer.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static final int TIMEOUT_MS = 120_000;

  // Column indices in the URLhaus CSV
  private static final int COL_ID = 0;
  private static final int COL_DATEADDED = 1;
  private static final int COL_URL = 2;
  private static final int COL_URL_STATUS = 3;
  private static final int COL_LAST_ONLINE = 4;
  private static final int COL_THREAT = 5;
  private static final int COL_TAGS = 6;
  private static final int COL_URLHAUS_LINK = 7;
  private static final int COL_REPORTER = 8;

  @Override public String transform(String response, RequestContext context) {
    String url = context.getUrl();
    LOGGER.info("URLhaus: fetching ZIP from {}", url);

    byte[] zipBytes = fetchBytes(url);
    if (zipBytes == null || zipBytes.length == 0) {
      LOGGER.warn("URLhaus: empty or failed ZIP fetch");
      return "[]";
    }

    try {
      ByteArrayOutputStream baos = new ByteArrayOutputStream(4 * 1024 * 1024);
      JsonGenerator gen = MAPPER.getFactory().createGenerator(baos);
      gen.writeStartArray();

      int[] count = {0};
      processCsvFromZip(zipBytes, gen, count);

      gen.writeEndArray();
      gen.close();

      LOGGER.info("URLhaus: returning {} ioc_urls rows", count[0]);
      return baos.toString("UTF-8");

    } catch (Exception e) {
      LOGGER.error("URLhaus: failed to process ZIP: {}", e.getMessage());
      throw new RuntimeException("Failed to process URLhaus ZIP: " + e.getMessage(), e);
    }
  }

  private void processCsvFromZip(byte[] zipBytes, JsonGenerator gen, int[] count)
      throws Exception {
    try (ZipInputStream zis = new ZipInputStream(new ByteArrayInputStream(zipBytes))) {
      ZipEntry entry;
      while ((entry = zis.getNextEntry()) != null) {
        String name = entry.getName();
        if (!entry.isDirectory() && (name.endsWith(".txt") || name.endsWith(".csv"))) {
          LOGGER.info("URLhaus: processing ZIP entry {}", name);
          parseCsv(zis, gen, count);
        }
        zis.closeEntry();
      }
    }
  }

  private void parseCsv(InputStream in, JsonGenerator gen, int[] count) throws Exception {
    BufferedReader reader = new BufferedReader(
        new InputStreamReader(in, StandardCharsets.UTF_8));
    String line;
    while ((line = CsvRecordReader.readRecord(reader)) != null) {
      if (line.startsWith("#") || line.trim().isEmpty()) {
        continue;
      }
      String[] cols = parseCsvLine(line);
      if (cols.length <= COL_ID) {
        continue;
      }
      // rowFilter: id must look like a number
      String id = col(cols, COL_ID);
      if (id == null || !id.matches("^[0-9]+$")) {
        continue;
      }

      String dateAdded = col(cols, COL_DATEADDED);
      ObjectNode row = MAPPER.createObjectNode();
      row.put("url_id", id);
      row.put("url", col(cols, COL_URL));
      row.put("url_status", col(cols, COL_URL_STATUS));
      row.put("date_added", dateAdded);
      row.put("last_online", col(cols, COL_LAST_ONLINE));
      row.put("threat", col(cols, COL_THREAT));
      row.put("tags", col(cols, COL_TAGS));
      row.put("urlhaus_link", col(cols, COL_URLHAUS_LINK));
      row.put("reporter", col(cols, COL_REPORTER));
      row.put("source", "urlhaus");
      row.put("first_seen", extractDate(dateAdded));

      gen.writeTree(row);
      count[0]++;
    }
  }

  /** Delegates to {@link CsvRecordReader#splitFields} — canonical CSV field split. */
  static String[] parseCsvLine(String line) {
    return CsvRecordReader.splitFields(line, ',').toArray(new String[0]);
  }

  private static String col(String[] cols, int idx) {
    if (idx >= cols.length) {
      return null;
    }
    String v = cols[idx].trim();
    return v.isEmpty() ? null : v;
  }

  private static String extractDate(String datetime) {
    if (datetime == null || datetime.length() < 10) {
      return datetime;
    }
    return datetime.substring(0, 10);
  }

  private byte[] fetchBytes(String url) {
    try {
      HttpURLConnection conn =
          (HttpURLConnection) URI.create(url).toURL().openConnection();
      conn.setInstanceFollowRedirects(true);
      conn.setRequestMethod("GET");
      conn.setConnectTimeout(TIMEOUT_MS);
      conn.setReadTimeout(TIMEOUT_MS);
      conn.setRequestProperty("User-Agent", "calcite-govdata/1.0");

      int status = conn.getResponseCode();
      if (status != 200) {
        LOGGER.warn("URLhaus: HTTP {} fetching ZIP", status);
        conn.disconnect();
        return null;
      }

      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      try (InputStream in = conn.getInputStream()) {
        byte[] buf = new byte[65536];
        int len;
        while ((len = in.read(buf)) > 0) {
          baos.write(buf, 0, len);
        }
      }
      conn.disconnect();
      return baos.toByteArray();
    } catch (Exception e) {
      LOGGER.warn("URLhaus: error fetching ZIP: {}", e.getMessage());
      return null;
    }
  }
}
