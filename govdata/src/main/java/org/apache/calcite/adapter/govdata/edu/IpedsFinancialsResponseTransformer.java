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
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
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
 * Streaming transformer for NCES IPEDS Finance survey bulk CSV responses.
 *
 * <p>Downloads the per-form-type ZIP ({@code F{yy1}{yy2}_{form_type}.zip}) from NCES,
 * locates the CSV entry, and streams rows lazily one at a time. Memory usage is
 * O(one CSV row), regardless of ZIP size.
 *
 * <p>Column names are NCES-coded (e.g. F1B01); this class maps them to canonical
 * schema names per form_type (F1A/F2/F3). Injected columns: {@code year},
 * {@code form_type}, {@code is_provisional}.
 */
public class IpedsFinancialsResponseTransformer implements StreamingResponseTransformer {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(IpedsFinancialsResponseTransformer.class);

  static final int CONNECT_TIMEOUT_MS = 30_000;
  static final int READ_TIMEOUT_MS = 600_000;

  private static final int PROVISIONAL_YEAR_THRESHOLD = 2023;

  private static final Map<String, String> F1A_MAP;
  private static final Map<String, String> F2_MAP;
  private static final Map<String, String> F3_MAP;

  static {
    Map<String, String> f1a = new HashMap<String, String>();
    f1a.put("UNITID",   "unitid");
    f1a.put("F1B02",    "rev_tuition_fees_gross");
    f1a.put("F1B01",    "rev_tuition_fees_net");
    f1a.put("F1B06",    "rev_appropriations_fed");
    f1a.put("F1B07",    "rev_appropriations_state");
    f1a.put("F1B04",    "rev_grants_contracts_federal");
    f1a.put("F1B05",    "rev_grants_contracts_state");
    f1a.put("F1B08",    "rev_gifts_grants_contracts");
    f1a.put("F1B09",    "rev_investment_return");
    f1a.put("F1B11",    "rev_auxiliary_enterprises_net");
    f1a.put("F1D01",    "rev_operating");
    f1a.put("F1D02",    "rev_nonoperating");
    f1a.put("F1D04",    "rev_total_current");
    f1a.put("F1E05",    "sch_pell_grant");
    f1a.put("F1E06",    "sch_grants_institutional");
    f1a.put("F1E01",    "sch_total_student_aid");
    f1a.put("F1E02",    "sch_allowances_tuition_fees");
    f1a.put("F1C011",   "exp_instruc_total");
    f1a.put("F1C021",   "exp_research_total");
    f1a.put("F1C031",   "exp_pub_serv_total");
    f1a.put("F1C041",   "exp_acad_supp_total");
    f1a.put("F1C051",   "exp_student_serv_total");
    f1a.put("F1C061",   "exp_inst_supp_total");
    f1a.put("F1C091",   "exp_aux_ent_total");
    f1a.put("F1C101",   "exp_net_grant_aid_total");
    f1a.put("F1C141",   "exp_total_current");
    f1a.put("F1C192",   "exp_total_salaries");
    f1a.put("F1H01",    "endowment_beg");
    f1a.put("F1H02",    "endowment_end");
    f1a.put("F1A01",    "assets");
    f1a.put("F1A02",    "liabilities");
    f1a.put("F1A19",    "net_position_end");
    f1a.put("F1A06",    "longterm_debt");
    F1A_MAP = Collections.unmodifiableMap(f1a);

    Map<String, String> f2 = new HashMap<String, String>();
    f2.put("UNITID",    "unitid");
    f2.put("F2B02",     "rev_tuition_fees_gross");
    f2.put("F2B01",     "rev_tuition_fees_net");
    f2.put("F2B04",     "rev_fed_approps_grants");
    f2.put("F2B05",     "rev_state_local_approps_grants");
    f2.put("F2B03",     "rev_gifts_grants_contracts");
    f2.put("F2B06",     "rev_investment_return");
    f2.put("F2B07",     "rev_auxiliary_enterprises_net");
    f2.put("F2D01",     "rev_operating");
    f2.put("F2D02",     "rev_nonoperating");
    f2.put("F2D04",     "rev_total_current");
    f2.put("F2E05",     "sch_pell_grant");
    f2.put("F2E01",     "sch_total_student_aid");
    f2.put("F2E02",     "sch_allowances_tuition_fees");
    f2.put("F2C01",     "exp_instruc_total");
    f2.put("F2C02",     "exp_research_total");
    f2.put("F2C03",     "exp_pub_serv_total");
    f2.put("F2C04",     "exp_acad_supp_total");
    f2.put("F2C05",     "exp_student_serv_total");
    f2.put("F2C06",     "exp_inst_supp_total");
    f2.put("F2C08",     "exp_aux_ent_total");
    f2.put("F2C09",     "exp_net_grant_aid_total");
    f2.put("F2C19",     "exp_total_current");
    f2.put("F2E132",    "exp_total_salaries");
    f2.put("F2H01",     "endowment_beg");
    f2.put("F2H02",     "endowment_end");
    f2.put("F2A01",     "assets");
    f2.put("F2A02",     "liabilities");
    f2.put("F2A19",     "net_position_end");
    f2.put("F2A06",     "longterm_debt");
    F2_MAP = Collections.unmodifiableMap(f2);

    Map<String, String> f3 = new HashMap<String, String>();
    f3.put("UNITID",    "unitid");
    f3.put("F3B01",     "rev_tuition_fees_net");
    f3.put("F3B04",     "rev_fed_approps_grants");
    f3.put("F3B05",     "rev_state_local_approps_grants");
    f3.put("F3B03",     "rev_gifts_grants_contracts");
    f3.put("F3B06",     "rev_investment_return");
    f3.put("F3B07",     "rev_auxiliary_enterprises_net");
    f3.put("F3D01",     "rev_total_current");
    f3.put("F3E01",     "sch_total_student_aid");
    f3.put("F3C01",     "exp_instruc_total");
    f3.put("F3C02",     "exp_research_total");
    f3.put("F3C03",     "exp_pub_serv_total");
    f3.put("F3C04",     "exp_acad_supp_total");
    f3.put("F3C05",     "exp_student_serv_total");
    f3.put("F3C06",     "exp_inst_supp_total");
    f3.put("F3C08",     "exp_aux_ent_total");
    f3.put("F3C09",     "exp_net_grant_aid_total");
    f3.put("F3C19",     "exp_total_current");
    f3.put("F3E072",    "exp_total_salaries");
    f3.put("F3G01",     "endowment_beg");
    f3.put("F3G02",     "endowment_end");
    f3.put("F3A01",     "assets");
    f3.put("F3A02",     "liabilities");
    f3.put("F3A06",     "longterm_debt");
    F3_MAP = Collections.unmodifiableMap(f3);
  }

  @Override public Iterator<Map<String, Object>> fetchAndTransform(RequestContext context)
      throws IOException {
    String formType = context.getDimensionValues().get("form_type");
    String yearStr = context.getDimensionValues().get("year");
    Map<String, String> columnMap = columnMapFor(formType);
    int isProvisional = isProvisional(yearStr) ? 1 : 0;

    File tempZip = downloadToTemp(context.getUrl());
    ZipEntry entry = findCsvEntry(new ZipFile(tempZip), formType);
    if (entry == null) {
      tempZip.delete();
      LOGGER.warn("IpedsFinancials: no CSV in ZIP for form_type={}, year={}", formType, yearStr);
      return Collections.emptyIterator();
    }

    ZipFile zf = new ZipFile(tempZip);
    BufferedReader reader = new BufferedReader(
        new InputStreamReader(zf.getInputStream(zf.getEntry(entry.getName())),
            StandardCharsets.UTF_8));
    return new LazyFinancialsIterator(reader, zf, tempZip, columnMap,
        formType, yearStr, isProvisional);
  }

  private ZipEntry findCsvEntry(ZipFile zf, String formType) throws IOException {
    String formLower = formType == null ? "" : formType.toLowerCase();
    ZipEntry preferred = null;
    ZipEntry fallback = null;
    Enumeration<? extends ZipEntry> entries = zf.entries();
    while (entries.hasMoreElements()) {
      ZipEntry e = entries.nextElement();
      String name = e.getName().toLowerCase();
      if (name.endsWith(".csv") && name.contains(formLower)) {
        if (name.contains("_rv")) {
          preferred = e;
        } else if (fallback == null) {
          fallback = e;
        }
      }
    }
    zf.close();
    return preferred != null ? preferred : fallback;
  }

  private static Map<String, String> columnMapFor(String formType) {
    if ("F1A".equalsIgnoreCase(formType)) {
      return F1A_MAP;
    } else if ("F2".equalsIgnoreCase(formType)) {
      return F2_MAP;
    } else if ("F3".equalsIgnoreCase(formType)) {
      return F3_MAP;
    }
    LOGGER.warn("IpedsFinancials: unknown form_type '{}', returning empty map", formType);
    return Collections.emptyMap();
  }

  private static boolean isProvisional(String yearStr) {
    try {
      return Integer.parseInt(yearStr) >= PROVISIONAL_YEAR_THRESHOLD;
    } catch (NumberFormatException e) {
      return false;
    }
  }

  static File downloadToTemp(String url) throws IOException {
    File tmp = File.createTempFile("nces-zip-", ".zip");
    tmp.deleteOnExit();
    HttpURLConnection conn =
        (HttpURLConnection) URI.create(url).toURL().openConnection();
    conn.setConnectTimeout(CONNECT_TIMEOUT_MS);
    conn.setReadTimeout(READ_TIMEOUT_MS);
    conn.setRequestProperty("User-Agent", "GovData/1.0");
    int status = conn.getResponseCode();
    if (status != 200) {
      tmp.delete();
      throw new IOException("HTTP " + status + " from " + url);
    }
    try (InputStream in = conn.getInputStream();
         FileOutputStream out = new FileOutputStream(tmp)) {
      byte[] buf = new byte[65536];
      int len;
      while ((len = in.read(buf)) > 0) {
        out.write(buf, 0, len);
      }
    } finally {
      conn.disconnect();
    }
    return tmp;
  }

  static String[] parseCsvLine(String line) {
    return CsvRecordReader.splitFields(line, ',').toArray(new String[0]);
  }

  private static final class LazyFinancialsIterator implements Iterator<Map<String, Object>> {
    private final BufferedReader reader;
    private final ZipFile zipFile;
    private final File tempZip;
    private final Map<String, String> columnMap;
    private final String formType;
    private final String yearStr;
    private final int isProvisional;
    private final String[] header;
    private Map<String, Object> next;
    private boolean closed;

    LazyFinancialsIterator(BufferedReader reader, ZipFile zipFile, File tempZip,
        Map<String, String> columnMap, String formType, String yearStr, int isProvisional)
        throws IOException {
      this.reader = reader;
      this.zipFile = zipFile;
      this.tempZip = tempZip;
      this.columnMap = columnMap;
      this.formType = formType;
      this.yearStr = yearStr;
      this.isProvisional = isProvisional;

      String headerLine = CsvRecordReader.readRecord(reader);
      if (headerLine == null) {
        this.header = new String[0];
        close();
      } else {
        if (!headerLine.isEmpty() && headerLine.charAt(0) == '﻿') {
          headerLine = headerLine.substring(1);
        }
        String[] raw = parseCsvLine(headerLine);
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
        String[] values = parseCsvLine(line);
        Map<String, Object> row = new LinkedHashMap<String, Object>();
        row.put("form_type", formType);
        row.put("is_provisional", isProvisional);
        if (yearStr != null) {
          try { row.put("year", Integer.parseInt(yearStr)); } catch (NumberFormatException e) { /**/ }
        }
        for (int c = 0; c < header.length && c < values.length; c++) {
          String col = header[c];
          if (col.startsWith("X")) continue;
          String canonical = columnMap.get(col);
          if (canonical == null) continue;
          String raw = values[c].trim();
          if (raw.isEmpty()) {
            row.put(canonical, null);
          } else if ("unitid".equals(canonical)) {
            try { row.put(canonical, Integer.parseInt(raw)); } catch (NumberFormatException e) { row.put(canonical, null); }
          } else {
            try { row.put(canonical, Double.parseDouble(raw)); } catch (NumberFormatException e) { row.put(canonical, null); }
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
