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
// storage-provider-guard:ignore-file -- the CMS downloads are staged in a local temp directory
package org.apache.calcite.adapter.govdata.health;

import org.apache.calcite.adapter.file.etl.CompleteBody;
import org.apache.calcite.adapter.file.etl.CsvRecordReader;
import org.apache.calcite.adapter.file.etl.NetworkOutageWait;
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
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * Streams CMS's Provider of Services (POS) providers for {@code cms_pos_facilities} and
 * {@code cms_pos_termination_history}: one row per CMS Certification Number across hospitals
 * (QIES category 01), psychiatric residential treatment facilities (06), rural health clinics
 * (12), community mental health centers (19) and federally qualified health centers (21).
 *
 * <p>The source URL is CMS's {@code data.json} catalog, from which three CSV files are resolved:
 * <ol>
 *   <li>the newest QIES release, which holds hospitals only;</li>
 *   <li>the iQIES release of the same quarter, which holds the other four categories;</li>
 *   <li>the QIES release of {@value #LAST_COMBINED_QUARTER}, the last one to hold all five
 *       categories. It supplies the terminated non-hospital providers that CMS did not carry
 *       into iQIES, and the cross-reference numbers iQIES does not publish.</li>
 * </ol>
 *
 * <p>A release that does not match this division of categories fails the fetch, so that a table is
 * never overwritten with part of its providers.
 */
public class CmsPosTransformer implements StreamingResponseTransformer {

  private static final Logger LOGGER = LoggerFactory.getLogger(CmsPosTransformer.class);

  private static final String QIES_SERIES =
      "Provider of Services File - Quality Improvement and Evaluation System";
  private static final String IQIES_SERIES =
      "Provider of Services File - Internet Quality Improvement and Evaluation System";
  static final String LAST_COMBINED_QUARTER = "2026-04-01";

  private static final String HOSPITAL = "01";
  private static final String ACTIVE = "00";
  private static final Charset QIES_CHARSET = Charset.forName("windows-1252");

  @Override public Iterator<Map<String, Object>> fetchAndTransform(RequestContext context)
      throws IOException {
    Path dir = Files.createTempDirectory("cms-pos");
    boolean handedOver = false;
    try {
      Path catalog = download(context.getUrl(), dir.resolve("data.json"));
      CmsPosCatalog[] series;
      try (InputStream in = Files.newInputStream(catalog)) {
        series = CmsPosCatalog.parse(in, QIES_SERIES, IQIES_SERIES);
      }
      String quarter = series[0].latestQuarter();
      if (quarter.compareTo(LAST_COMBINED_QUARTER) <= 0
          || !quarter.equals(series[1].latestQuarter())) {
        throw new IOException("CMS POS: need QIES and iQIES releases of one quarter after "
            + LAST_COMBINED_QUARTER + "; newest are QIES " + quarter + " and iQIES "
            + series[1].latestQuarter());
      }
      Source hospitals = new Source(
          download(series[0].csvUrl(quarter), dir.resolve("qies.csv")), QIES_CHARSET);
      Source others = new Source(
          download(series[1].csvUrl(quarter), dir.resolve("iqies.csv")), StandardCharsets.UTF_8);
      Source combined = new Source(
          download(series[0].csvUrl(LAST_COMBINED_QUARTER), dir.resolve("qies-combined.csv")),
          QIES_CHARSET);
      Iterator<Map<String, Object>> rows = rows(dir, hospitals, others, combined);
      handedOver = true;
      LOGGER.info("CMS POS: streaming quarter {}", quarter);
      return rows;
    } finally {
      if (!handedOver) {
        delete(dir);
      }
    }
  }

  /** Validates the three downloaded files and returns the lazy union over them. */
  static Iterator<Map<String, Object>> rows(Path dir, Source hospitals, Source others,
      Source combined) throws IOException {
    checkHospitalsOnly(hospitals);
    checkIqiesCategories(others);
    return new UnionIterator(dir, hospitals, others, combined, crossReferences(combined));
  }

  private static void checkHospitalsOnly(Source source) throws IOException {
    int rows = 0;
    try (Cursor cursor = new Cursor(source, false)) {
      List<String> values;
      while ((values = cursor.read()) != null) {
        String category = cursor.mapper.category(values);
        if (!HOSPITAL.equals(category)) {
          throw new IOException("CMS POS: newest QIES release holds category " + category
              + "; expected hospitals only");
        }
        rows++;
      }
    }
    if (rows == 0) {
      throw new IOException("CMS POS: newest QIES release holds no hospitals");
    }
  }

  private static void checkIqiesCategories(Source source) throws IOException {
    Set<String> missing = new HashSet<String>(CmsPosProviderMapper.IQIES_CATEGORIES.values());
    try (Cursor cursor = new Cursor(source, true)) {
      List<String> values;
      while (!missing.isEmpty() && (values = cursor.read()) != null) {
        missing.remove(cursor.mapper.category(values));
      }
    }
    if (!missing.isEmpty()) {
      throw new IOException("CMS POS: iQIES release holds no providers of categories " + missing);
    }
  }

  private static Map<String, String> crossReferences(Source combined) throws IOException {
    Map<String, String> references = new HashMap<String, String>();
    try (Cursor cursor = new Cursor(combined, false)) {
      List<String> values;
      while ((values = cursor.read()) != null) {
        String reference = cursor.mapper.crossReference(values);
        if (reference != null) {
          references.put(cursor.mapper.providerNumber(values), reference);
        }
      }
    }
    return references;
  }

  private static Path download(String url, Path target) throws IOException {
    HttpURLConnection conn = (HttpURLConnection) URI.create(url).toURL().openConnection();
    conn.setConnectTimeout(30000);
    conn.setReadTimeout(180000);
    conn.setRequestProperty("User-Agent", "GovData/1.0");
    try {
      NetworkOutageWait.connect(conn);
      int status = conn.getResponseCode();
      if (status != HttpURLConnection.HTTP_OK) {
        throw new IOException("HTTP " + status + " from " + url);
      }
      try (InputStream in = CompleteBody.of(conn)) {
        Files.copy(in, target, StandardCopyOption.REPLACE_EXISTING);
      }
    } finally {
      conn.disconnect();
    }
    return target;
  }

  private static void delete(Path dir) throws IOException {
    for (String name : new String[] {
        "data.json", "qies.csv", "iqies.csv", "qies-combined.csv"}) {
      Files.deleteIfExists(dir.resolve(name));
    }
    Files.deleteIfExists(dir);
  }

  /** A downloaded CSV file and its encoding. */
  static final class Source {
    final Path file;
    final Charset charset;

    Source(Path file, Charset charset) {
      this.file = file;
      this.charset = charset;
    }
  }

  /** Reads the records of one CSV file. */
  private static final class Cursor implements AutoCloseable {
    private final BufferedReader reader;
    final CmsPosProviderMapper mapper;

    Cursor(Source source, boolean iqies) throws IOException {
      reader = new BufferedReader(
          new InputStreamReader(Files.newInputStream(source.file), source.charset));
      String header = CsvRecordReader.readRecord(reader);
      if (header == null) {
        reader.close();
        throw new IOException("CMS POS: empty file " + source.file.getFileName());
      }
      List<String> columns = CsvRecordReader.splitFields(header, ',');
      mapper = iqies ? CmsPosProviderMapper.iqies(columns) : CmsPosProviderMapper.qies(columns);
    }

    List<String> read() throws IOException {
      String record = CsvRecordReader.readRecord(reader);
      while (record != null && record.trim().isEmpty()) {
        record = CsvRecordReader.readRecord(reader);
      }
      return record == null ? null : CsvRecordReader.splitFields(record, ',');
    }

    @Override public void close() throws IOException {
      reader.close();
    }
  }

  /**
   * Hospitals, then the iQIES categories, then the terminated non-hospital providers found only
   * in the last combined release. Deletes the downloads once exhausted or closed.
   */
  private static final class UnionIterator
      implements Iterator<Map<String, Object>>, AutoCloseable {
    private static final int HOSPITALS = 0;
    private static final int IQIES = 1;
    private static final int CARRIED = 2;
    private static final int END = 3;

    private final Path dir;
    private final Source[] sources;
    private final Map<String, String> crossReferences;
    private final Set<String> emitted = new HashSet<String>();
    private int phase = HOSPITALS;
    private Cursor cursor;
    private Map<String, Object> pending;

    UnionIterator(Path dir, Source hospitals, Source others, Source combined,
        Map<String, String> crossReferences) throws IOException {
      this.dir = dir;
      this.sources = new Source[] {hospitals, others, combined};
      this.crossReferences = crossReferences;
      this.cursor = new Cursor(hospitals, false);
      advance();
    }

    private void advance() throws IOException {
      pending = null;
      while (phase != END) {
        List<String> values = cursor.read();
        if (values == null) {
          cursor.close();
          cursor = null;
          phase++;
          if (phase == END) {
            delete(dir);
          } else {
            cursor = new Cursor(sources[phase], phase == IQIES);
          }
          continue;
        }
        CmsPosProviderMapper mapper = cursor.mapper;
        String category = mapper.category(values);
        if (phase == IQIES && category == null) {
          continue;
        }
        String number = mapper.providerNumber(values);
        if (phase == CARRIED) {
          if (HOSPITAL.equals(category) || ACTIVE.equals(mapper.terminationCode(values))
              || emitted.contains(number)) {
            continue;
          }
        }
        if (!emitted.add(number)) {
          throw new IOException("CMS POS: provider number " + number + " appears twice");
        }
        pending = mapper.map(values);
        if (phase == IQIES) {
          pending.put("cross_ref_provider_number", crossReferences.get(number));
        }
        return;
      }
    }

    @Override public boolean hasNext() {
      return pending != null;
    }

    @Override public Map<String, Object> next() {
      if (pending == null) {
        throw new NoSuchElementException();
      }
      Map<String, Object> row = pending;
      try {
        advance();
      } catch (IOException e) {
        throw new IllegalStateException("CMS POS: read failed", e);
      }
      return row;
    }

    @Override public void close() throws IOException {
      pending = null;
      phase = END;
      if (cursor != null) {
        cursor.close();
        cursor = null;
      }
      delete(dir);
    }
  }
}
