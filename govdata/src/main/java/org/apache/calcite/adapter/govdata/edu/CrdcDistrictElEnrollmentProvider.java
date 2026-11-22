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

import org.apache.calcite.adapter.file.etl.CompleteBody;
import org.apache.calcite.adapter.file.etl.CsvRecordReader;
import org.apache.calcite.adapter.file.etl.DataProvider;
import org.apache.calcite.adapter.file.etl.EtlPipelineConfig;
import org.apache.calcite.adapter.file.etl.NetworkOutageWait;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;

/**
 * DataProvider for {@code district_el_enrollment} — English learner (EL) enrollment by school
 * district, summed from the school-level Civil Rights Data Collection (CRDC) enrollment file.
 *
 * <p>The CRDC publishes no LEA-level EL file, so the district figure is the sum of the school
 * rows that share a {@code LEAID}. Each CRDC collection is one zip
 * ({@code https://civilrightsdata.ed.gov/assets/ocr/docs/{yyyy}-{yy}-crdc-data.zip}, 80-830 MB)
 * holding dozens of CSVs, of which only {@code Enrollment.csv} is needed. The provider reads the
 * zip's central directory with HTTP range requests and downloads just that entry (35-70 MB),
 * inflating it when the entry is deflated (2020-21) and reading it as is when stored (2021-22).
 *
 * <p>The {@code year} dimension is the school year's starting calendar year (2021-22 is 2021),
 * the same convention as {@code ccd_districts.year}. The two layouts differ: 2020-21 names the
 * columns {@code LEP} where 2021-22 says {@code EL}, and has no nonbinary ({@code _X}) columns.
 * Columns are resolved by header name; an EL column the layout does not have becomes null. A
 * school's count is dropped when it is negative (the CRDC's non-reported codes: -3, -9, -11,
 * -12). A district's sum is null when none of its schools reported the measure, so a sum
 * covers only reporting schools; {@code schools_reporting_el} says how many that is.
 */
public class CrdcDistrictElEnrollmentProvider implements DataProvider {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(CrdcDistrictElEnrollmentProvider.class);

  /** Race codes in the source's column names, with the output column suffix. */
  private static final String[][] RACES = {
      {"HI", "hispanic"},
      {"AM", "american_indian"},
      {"AS", "asian"},
      {"HP", "pacific_islander"},
      {"BL", "black"},
      {"WH", "white"},
      {"TR", "two_or_more"},
  };

  private static final String[] SEXES = {"M", "F", "X"};

  @Override public Iterator<Map<String, Object>> fetch(EtlPipelineConfig config,
      Map<String, String> variables) throws IOException {
    String year = variables.get("effective_year");
    if (year == null || year.isEmpty()) {
      year = variables.get("year");
    }
    if (year == null || year.trim().isEmpty()) {
      throw new IOException("district_el_enrollment: no year in dimension variables "
          + variables);
    }
    int yearInt = Integer.parseInt(year.trim());
    String url = String.format(Locale.ROOT,
        "https://civilrightsdata.ed.gov/assets/ocr/docs/%d-%02d-crdc-data.zip",
        yearInt, (yearInt + 1) % 100);

    byte[] csv = readEnrollmentCsv(url);
    List<Map<String, Object>> rows = aggregate(csv, yearInt, url);
    LOGGER.info("district_el_enrollment: {} LEA rows for school year {}-{} from {}",
        rows.size(), yearInt, (yearInt + 1) % 100, url);
    return rows.iterator();
  }

  // ---------------------------------------------------------------------------------------------
  // Aggregation
  // ---------------------------------------------------------------------------------------------

  /** Running sum that stays null until a school reports a non-negative value. */
  private static final class Sum {
    private long total;
    private boolean seen;

    void add(Long v) {
      if (v != null) {
        total += v.longValue();
        seen = true;
      }
    }

    Long get() {
      return seen ? Long.valueOf(total) : null;
    }
  }

  private static final class District {
    private final String leaid;
    private final String stateAbbr;
    private final String stateName;
    private final String leaName;
    private int schools;
    private int schoolsReportingEl;
    private final Sum totalEnrollment = new Sum();
    private final Sum el = new Sum();
    private final Sum elMale = new Sum();
    private final Sum elFemale = new Sum();
    private final Sum elNonbinary = new Sum();
    private final Sum elProgram = new Sum();
    private final Sum[] elRace = new Sum[RACES.length];

    District(String leaid, String stateAbbr, String stateName, String leaName) {
      this.leaid = leaid;
      this.stateAbbr = stateAbbr;
      this.stateName = stateName;
      this.leaName = leaName;
      for (int i = 0; i < elRace.length; i++) {
        elRace[i] = new Sum();
      }
    }
  }

  private List<Map<String, Object>> aggregate(byte[] csvBytes, int year, String url)
      throws IOException {
    Map<String, District> districts = new LinkedHashMap<String, District>();
    try (BufferedReader reader = new BufferedReader(new InputStreamReader(
        new ByteArrayInputStream(csvBytes), StandardCharsets.UTF_8))) {
      String line = CsvRecordReader.readRecord(reader);
      if (line == null) {
        throw new IOException("district_el_enrollment: empty Enrollment.csv in " + url);
      }
      List<String> headers = new ArrayList<String>();
      for (String h : CsvRecordReader.splitFields(stripBom(line), ',')) {
        headers.add(h.trim().toUpperCase(Locale.ROOT));
      }
      // 2021-22 says EL, 2020-21 says LEP; the same measure under two names.
      String el = headers.contains("TOT_ELENR_M") ? "EL" : "LEP";
      int iLeaid = requireIndex(headers, "LEAID", url);
      int iState = requireIndex(headers, "LEA_STATE", url);
      int iStateName = requireIndex(headers, "LEA_STATE_NAME", url);
      int iLeaName = requireIndex(headers, "LEA_NAME", url);
      int[] iTotal = sexIndexes(headers, "TOT_ENR_", url, true);
      int[] iEl = sexIndexes(headers, "SCH_ENR_" + el + "_", url, true);
      int[] iProgram = sexIndexes(headers, "TOT_" + el + "PROGENR_", url, true);
      int[][] iRace = new int[RACES.length][];
      for (int r = 0; r < RACES.length; r++) {
        iRace[r] = sexIndexes(headers, "SCH_" + el + "ENR_" + RACES[r][0] + "_", url, true);
      }

      while ((line = CsvRecordReader.readRecord(reader)) != null) {
        if (line.trim().isEmpty()) {
          continue;
        }
        List<String> v = CsvRecordReader.splitFields(line, ',');
        String leaid = v.get(iLeaid).trim();
        if (leaid.isEmpty()) {
          throw new IOException("district_el_enrollment: school row without LEAID in " + url
              + ": " + v);
        }
        District d = districts.get(leaid);
        if (d == null) {
          d = new District(leaid, v.get(iState).trim(), v.get(iStateName).trim(),
              v.get(iLeaName).trim());
          districts.put(leaid, d);
        }
        d.schools++;
        Long schoolEl = sum(v, iEl);
        if (schoolEl != null) {
          d.schoolsReportingEl++;
        }
        d.el.add(schoolEl);
        d.totalEnrollment.add(sum(v, iTotal));
        d.elProgram.add(sum(v, iProgram));
        d.elMale.add(count(v, iEl[0]));
        d.elFemale.add(count(v, iEl[1]));
        d.elNonbinary.add(count(v, iEl[2]));
        for (int r = 0; r < RACES.length; r++) {
          d.elRace[r].add(sum(v, iRace[r]));
        }
      }
    }

    List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>(districts.size());
    for (District d : districts.values()) {
      Map<String, Object> row = new LinkedHashMap<String, Object>();
      row.put("leaid", d.leaid);
      row.put("year", Integer.valueOf(year));
      row.put("state_abbr", d.stateAbbr);
      row.put("state_name", d.stateName);
      row.put("lea_name", d.leaName);
      row.put("schools", Integer.valueOf(d.schools));
      row.put("schools_reporting_el", Integer.valueOf(d.schoolsReportingEl));
      row.put("total_enrollment", d.totalEnrollment.get());
      row.put("el_enrollment", d.el.get());
      row.put("el_male", d.elMale.get());
      row.put("el_female", d.elFemale.get());
      row.put("el_nonbinary", d.elNonbinary.get());
      for (int r = 0; r < RACES.length; r++) {
        row.put("el_" + RACES[r][1], d.elRace[r].get());
      }
      row.put("el_program_enrollment", d.elProgram.get());
      rows.add(row);
    }
    return rows;
  }

  private static String stripBom(String line) {
    return line.startsWith("﻿") ? line.substring(1) : line;
  }

  private static int requireIndex(List<String> headers, String name, String url)
      throws IOException {
    int i = headers.indexOf(name);
    if (i < 0) {
      throw new IOException("district_el_enrollment: column " + name + " not in Enrollment.csv of "
          + url + " (columns: " + headers + ")");
    }
    return i;
  }

  /**
   * Indexes of {@code prefix + M/F/X}; -1 for a column the layout lacks (the 2020-21 file has no
   * X columns). The M and F columns are required.
   */
  private static int[] sexIndexes(List<String> headers, String prefix, String url,
      boolean requireMf) throws IOException {
    int[] idx = new int[SEXES.length];
    for (int s = 0; s < SEXES.length; s++) {
      idx[s] = headers.indexOf(prefix + SEXES[s]);
      if (idx[s] < 0 && requireMf && s < 2) {
        throw new IOException("district_el_enrollment: column " + prefix + SEXES[s]
            + " not in Enrollment.csv of " + url);
      }
    }
    return idx;
  }

  /** A school's count: null for a missing column, a blank, or a negative non-reported code. */
  private static Long count(List<String> values, int index) {
    if (index < 0 || index >= values.size()) {
      return null;
    }
    String s = values.get(index).trim();
    if (s.isEmpty()) {
      return null;
    }
    long v = Long.parseLong(s);
    return v < 0 ? null : Long.valueOf(v);
  }

  /** Sum over the columns that reported for the school; null when none did. */
  private static Long sum(List<String> values, int[] indexes) {
    long total = 0;
    boolean seen = false;
    for (int index : indexes) {
      Long c = count(values, index);
      if (c != null) {
        total += c.longValue();
        seen = true;
      }
    }
    return seen ? Long.valueOf(total) : null;
  }

  // ---------------------------------------------------------------------------------------------
  // Single-entry extraction from the remote zip (HTTP range requests)
  // ---------------------------------------------------------------------------------------------

  private static final int EOCD_SIG = 0x06054b50;
  private static final int CEN_SIG = 0x02014b50;
  private static final int LOC_SIG = 0x04034b50;

  /** Downloads and decompresses the school-level {@code Enrollment.csv} entry of the zip. */
  static byte[] readEnrollmentCsv(String zipUrl) throws IOException {
    ArchiveReader archive = new ArchiveReader(zipUrl);
    // Tail: end-of-central-directory record, which locates the central directory.
    byte[] tail = archive.range("bytes=-65536");
    int eocd = -1;
    for (int i = tail.length - 22; i >= 0; i--) {
      if (le32(tail, i) == EOCD_SIG) {
        eocd = i;
        break;
      }
    }
    if (eocd < 0) {
      throw new IOException("district_el_enrollment: no end-of-central-directory record in "
          + zipUrl);
    }
    long cdSize = le32(tail, eocd + 12) & 0xFFFFFFFFL;
    long cdOffset = le32(tail, eocd + 16) & 0xFFFFFFFFL;
    if (cdSize == 0xFFFFFFFFL || cdOffset == 0xFFFFFFFFL) {
      throw new IOException("district_el_enrollment: zip64 archive not supported: " + zipUrl);
    }
    byte[] cd = archive.range("bytes=" + cdOffset + "-" + (cdOffset + cdSize - 1));

    int pos = 0;
    while (pos + 46 <= cd.length && le32(cd, pos) == CEN_SIG) {
      int method = le16(cd, pos + 10);
      long compSize = le32(cd, pos + 20) & 0xFFFFFFFFL;
      long size = le32(cd, pos + 24) & 0xFFFFFFFFL;
      int nameLen = le16(cd, pos + 28);
      int extraLen = le16(cd, pos + 30);
      int commentLen = le16(cd, pos + 32);
      long localOffset = le32(cd, pos + 42) & 0xFFFFFFFFL;
      String name = new String(cd, pos + 46, nameLen, StandardCharsets.UTF_8);
      pos += 46 + nameLen + extraLen + commentLen;
      if (!isSchoolEnrollment(name)) {
        continue;
      }
      // The local header repeats the name and extra lengths (which may differ from the central
      // directory's), so read it to find where the data starts.
      byte[] local = archive.range("bytes=" + localOffset + "-" + (localOffset + 29));
      if (le32(local, 0) != LOC_SIG) {
        throw new IOException("district_el_enrollment: bad local header for " + name + " in "
            + zipUrl);
      }
      long dataStart = localOffset + 30 + le16(local, 26) + le16(local, 28);
      byte[] data = archive.range("bytes=" + dataStart + "-" + (dataStart + compSize - 1));
      if (method == 0) {
        return data;
      }
      if (method == 8) {
        return inflate(data, size, name);
      }
      throw new IOException("district_el_enrollment: unsupported zip method " + method + " for "
          + name + " in " + zipUrl);
    }
    throw new IOException("district_el_enrollment: no school-level Enrollment.csv in " + zipUrl);
  }

  /** {@code SCH/Enrollment.csv} (2021-22) or {@code CRDC/School/Enrollment.csv} (2020-21). */
  private static boolean isSchoolEnrollment(String name) {
    return name.endsWith("SCH/Enrollment.csv") || name.endsWith("School/Enrollment.csv");
  }

  private static byte[] inflate(byte[] data, long size, String name) throws IOException {
    Inflater inflater = new Inflater(true);
    try {
      inflater.setInput(data);
      ByteArrayOutputStream out = new ByteArrayOutputStream((int) size);
      byte[] buf = new byte[65536];
      while (!inflater.finished()) {
        int n = inflater.inflate(buf);
        if (n == 0 && (inflater.needsInput() || inflater.needsDictionary())) {
          throw new IOException("district_el_enrollment: truncated deflate stream for " + name);
        }
        out.write(buf, 0, n);
      }
      if (out.size() != size) {
        throw new IOException("district_el_enrollment: inflated " + out.size() + " bytes for "
            + name + ", central directory says " + size);
      }
      return out.toByteArray();
    } catch (DataFormatException e) {
      throw new IOException("district_el_enrollment: corrupt deflate stream for " + name, e);
    } finally {
      inflater.end();
    }
  }

  private static int le16(byte[] b, int off) {
    return ByteBuffer.wrap(b, off, 2).order(ByteOrder.LITTLE_ENDIAN).getShort() & 0xFFFF;
  }

  private static int le32(byte[] b, int off) {
    return ByteBuffer.wrap(b, off, 4).order(ByteOrder.LITTLE_ENDIAN).getInt();
  }

  /**
   * Range reads of one archive version. civilrightsdata.ed.gov serves one URL from load-balanced
   * backends that hold different builds of the file (different ETag, length and central directory),
   * so every request after the first carries the first response's load-balancer cookie and its
   * ETag as {@code If-Match}; a request answered by another build fails instead of being read as
   * part of the same zip.
   */
  private static final class ArchiveReader {
    private final String url;
    private String cookie;
    private String etag;

    ArchiveReader(String url) {
      this.url = url;
    }

    byte[] range(String range) throws IOException {
      HttpURLConnection conn = (HttpURLConnection) URI.create(url).toURL().openConnection();
      conn.setConnectTimeout(30000);
      conn.setReadTimeout(180000);
      conn.setRequestProperty("User-Agent", "GovData/1.0");
      conn.setRequestProperty("Range", range);
      if (etag != null) {
        conn.setRequestProperty("If-Match", etag);
        conn.setRequestProperty("Cookie", cookie);
      }
      NetworkOutageWait.connect(conn);
      int status = conn.getResponseCode();
      if (status != HttpURLConnection.HTTP_PARTIAL) {
        throw new IOException("HTTP " + status + " (expected 206 for " + range + ") from " + url);
      }
      if (etag == null) {
        String tag = conn.getHeaderField("ETag");
        if (tag == null) {
          throw new IOException("district_el_enrollment: no ETag on " + url);
        }
        StringBuilder cookies = new StringBuilder();
        List<String> setCookies = conn.getHeaderFields().get("Set-Cookie");
        if (setCookies != null) {
          for (String sc : setCookies) {
            if (cookies.length() > 0) {
              cookies.append("; ");
            }
            cookies.append(sc.substring(0, sc.indexOf(';') < 0 ? sc.length() : sc.indexOf(';')));
          }
        }
        etag = tag;
        cookie = cookies.toString();
      }
      try (InputStream is = CompleteBody.of(conn);
           ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
        byte[] buf = new byte[65536];
        int len;
        while ((len = is.read(buf)) > 0) {
          baos.write(buf, 0, len);
        }
        return baos.toByteArray();
      }
    }
  }
}
