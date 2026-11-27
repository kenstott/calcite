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
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * DataProvider for the {@code district_assessments} school years that the Urban Institute endpoint
 * does not carry: it reads the U.S. Department of Education's own district (LEA) assessment files
 * from ED Data Express.
 *
 * <p>For every other year {@link #fetch} returns null, so the table's HTTP source (Urban) runs.
 * The {@code year} dimension is the school year's starting calendar year, as on the Urban rows
 * (2021 is 2021-22).
 *
 * <p>For 2021-22 ED publishes the district file for reading/language arts (FS178 / DG584) only;
 * there is no district mathematics file, so the math columns are null for that year. The file
 * address is fixed per year: the library page that lists the files answers a browser challenge, so
 * it is not read at run time, while the file itself is a plain download.
 *
 * <p>Rows kept are the all-students rows for grades 3-8 and high school, which takes grade 9 as on
 * the Urban rows. The published value is a text cell; {@link #proficiency} turns it into the
 * table's low / high / midpoint convention and {@code S} / {@code MISSING} into the sentinel codes
 * -3 / -1.
 */
public class DistrictAssessmentsEdDataExpressProvider implements DataProvider {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(DistrictAssessmentsEdDataExpressProvider.class);

  /** School years (starting calendar year) read from ED Data Express: reading file per year. */
  private static final Map<Integer, String> READING_ZIPS = new HashMap<Integer, String>();

  static {
    READING_ZIPS.put(2021, "https://eddataexpress.ed.gov/sites/default/files/data_download/"
        + "EID_12970/SY2122_FS178_DG584_LEA_data_files.zip");
  }

  /** The source's grade label to the table's grade; "All Grades" is not loaded. */
  private static final Map<String, Integer> GRADES = new HashMap<String, Integer>();

  static {
    for (int g = 3; g <= 8; g++) {
      GRADES.put("Grade " + g, g);
    }
    GRADES.put("High School", 9);
  }

  private static final String ALL_STUDENTS = "All Students in LEA";

  private static final Pattern EXACT = Pattern.compile("(\\d{1,3})%");
  private static final Pattern RANGE = Pattern.compile("(\\d{1,3})-(\\d{1,3})%");
  private static final Pattern BOUND = Pattern.compile("(<=|>=|<|>)(\\d{1,3})%");

  private static final String USER_AGENT = "Mozilla/5.0 (X11; Linux x86_64) GovData/1.0";

  /** Parsed file per year: grade to rows. One download serves every grade batch of the year. */
  private static final Map<Integer, Map<Integer, List<Map<String, Object>>>> CACHE =
      new HashMap<Integer, Map<Integer, List<Map<String, Object>>>>();

  @Override public Iterator<Map<String, Object>> fetch(EtlPipelineConfig config,
      Map<String, String> variables) throws IOException {
    int year = intVariable(variables, "year");
    String url = READING_ZIPS.get(year);
    if (url == null) {
      return null;
    }
    int grade = intVariable(variables, "grade");
    List<Map<String, Object>> rows = load(year, url).get(grade);
    if (rows == null) {
      // Grades 10-12 do not exist in the source: high school is reported once, as grade 9.
      return new ArrayList<Map<String, Object>>().iterator();
    }
    List<Map<String, Object>> copy = new ArrayList<Map<String, Object>>(rows.size());
    for (Map<String, Object> row : rows) {
      copy.add(new LinkedHashMap<String, Object>(row));
    }
    return copy.iterator();
  }

  private static int intVariable(Map<String, String> variables, String name) throws IOException {
    String v = variables.get(name);
    if (v == null || v.trim().isEmpty()) {
      throw new IOException("district_assessments: no " + name + " in dimension variables "
          + variables);
    }
    return Integer.parseInt(v.trim());
  }

  private static synchronized Map<Integer, List<Map<String, Object>>> load(int year, String url)
      throws IOException {
    Map<Integer, List<Map<String, Object>>> byGrade = CACHE.get(year);
    if (byGrade != null) {
      return byGrade;
    }
    // The zip holds the full file and the same rows split in two parts; only the full file is read.
    String wanted = url.substring(url.lastIndexOf('/') + 1)
        .replace("_data_files.zip", ".csv");
    try (ZipInputStream zin = new ZipInputStream(new ByteArrayInputStream(download(url)))) {
      ZipEntry entry;
      while ((entry = zin.getNextEntry()) != null) {
        if (wanted.equals(entry.getName())) {
          byGrade = parse(new BufferedReader(
              new InputStreamReader(zin, StandardCharsets.UTF_8)), year);
          break;
        }
      }
    }
    if (byGrade == null) {
      throw new IOException("district_assessments: " + wanted + " not found in " + url);
    }
    if (byGrade.size() != GRADES.size()) {
      throw new IOException("district_assessments: expected grades " + GRADES.values() + " in "
          + url + " but read " + byGrade.keySet());
    }
    LOGGER.info("district_assessments: school year {} read from {}", year, url);
    CACHE.put(year, byGrade);
    return byGrade;
  }

  static Map<Integer, List<Map<String, Object>>> parse(BufferedReader reader, int year)
      throws IOException {
    Map<Integer, List<Map<String, Object>>> byGrade =
        new HashMap<Integer, List<Map<String, Object>>>();
    String schoolYear = year + "-" + (year + 1);
    List<String> headers = null;
    String line;
    while ((line = CsvRecordReader.readRecord(reader)) != null) {
      if (headers == null) {
        headers = new ArrayList<String>();
        for (String h : CsvRecordReader.splitFields(line.replace("﻿", ""), ',')) {
          headers.add(h.trim().toLowerCase(Locale.ROOT));
        }
        continue;
      }
      if (line.trim().isEmpty()) {
        continue;
      }
      List<String> values = CsvRecordReader.splitFields(line, ',');
      if (!ALL_STUDENTS.equals(col(headers, values, "subgroup"))) {
        continue;
      }
      String gradeLabel = require(headers, values, "age/grade");
      if ("All Grades".equals(gradeLabel)) {
        continue;
      }
      Integer grade = GRADES.get(gradeLabel);
      if (grade == null) {
        throw new IOException("district_assessments: unknown grade label " + gradeLabel);
      }
      String rowYear = require(headers, values, "school year");
      String subject = require(headers, values, "academic subject");
      String outcome = require(headers, values, "outcome");
      if (!schoolYear.equals(rowYear) || !"Reading/Language Arts".equals(subject)
          || !"Percent Proficient".equals(outcome)) {
        throw new IOException("district_assessments: unexpected row " + rowYear + " / " + subject
            + " / " + outcome + " in the " + schoolYear + " reading file");
      }
      String leaid = require(headers, values, "nces lea id");
      if (leaid.length() != 7) {
        throw new IOException("district_assessments: NCES LEA ID is not 7 digits: " + leaid);
      }
      Number[] pct = proficiency(require(headers, values, "value"));
      Map<String, Object> row = new LinkedHashMap<String, Object>();
      row.put("leaid", leaid);
      row.put("year", year);
      row.put("grade", grade);
      row.put("lea_name", col(headers, values, "lea"));
      // The first two digits of an NCES LEA ID are the state's FIPS code (59 = BIE).
      row.put("fips", Integer.valueOf(leaid.substring(0, 2)));
      row.put("read_test_num_valid", Integer.valueOf(require(headers, values, "denominator")));
      row.put("read_test_pct_prof_low", pct[0]);
      row.put("read_test_pct_prof_high", pct[1]);
      row.put("read_test_pct_prof_midpt", pct[2]);
      row.put("math_test_num_valid", null);
      row.put("math_test_pct_prof_low", null);
      row.put("math_test_pct_prof_high", null);
      row.put("math_test_pct_prof_midpt", null);
      List<Map<String, Object>> rows = byGrade.get(grade);
      if (rows == null) {
        rows = new ArrayList<Map<String, Object>>();
        byGrade.put(grade, rows);
      }
      rows.add(row);
    }
    return byGrade;
  }

  /**
   * Converts a published value cell to {low, high, midpoint} percent: {@code 35%} is 35/35/35,
   * {@code 30-34%} is 30/34/32, {@code <50%} is 0/49/24.5, {@code <=10%} is 0/10/5, {@code >=80%}
   * is 80/100/90, {@code >95%} is 96/100/98, {@code S} (suppressed) is -3 and {@code MISSING} is
   * -1 in all three. Anything else is an error.
   */
  static Number[] proficiency(String value) throws IOException {
    if ("S".equals(value)) {
      return sentinel(-3);
    }
    if ("MISSING".equals(value)) {
      return sentinel(-1);
    }
    Matcher m = EXACT.matcher(value);
    if (m.matches()) {
      int v = Integer.parseInt(m.group(1));
      return bounds(v, v);
    }
    m = RANGE.matcher(value);
    if (m.matches()) {
      return bounds(Integer.parseInt(m.group(1)), Integer.parseInt(m.group(2)));
    }
    m = BOUND.matcher(value);
    if (m.matches()) {
      String op = m.group(1);
      int v = Integer.parseInt(m.group(2));
      if ("<=".equals(op)) {
        return bounds(0, v);
      }
      if ("<".equals(op)) {
        return bounds(0, v - 1);
      }
      if (">=".equals(op)) {
        return bounds(v, 100);
      }
      return bounds(v + 1, 100);
    }
    throw new IOException("district_assessments: unrecognised proficiency value " + value);
  }

  private static Number[] bounds(int low, int high) throws IOException {
    if (low < 0 || high > 100 || low > high) {
      throw new IOException("district_assessments: proficiency range out of bounds " + low + "-"
          + high);
    }
    return new Number[] {low, high, (low + high) / 2.0d};
  }

  private static Number[] sentinel(int code) {
    return new Number[] {code, code, (double) code};
  }

  private static String require(List<String> h, List<String> v, String name) throws IOException {
    String s = col(h, v, name);
    if (s == null) {
      throw new IOException("district_assessments: row without " + name + ": " + v);
    }
    return s;
  }

  private static String col(List<String> headers, List<String> values, String name) {
    int i = headers.indexOf(name);
    if (i < 0 || i >= values.size()) {
      return null;
    }
    String v = values.get(i).trim();
    return v.isEmpty() ? null : v;
  }

  private static byte[] download(String url) throws IOException {
    HttpURLConnection conn = (HttpURLConnection) URI.create(url).toURL().openConnection();
    conn.setConnectTimeout(30000);
    conn.setReadTimeout(180000);
    conn.setRequestProperty("User-Agent", USER_AGENT);
    NetworkOutageWait.connect(conn);
    int status = conn.getResponseCode();
    if (status != HttpURLConnection.HTTP_OK) {
      throw new IOException("HTTP " + status + " from " + url);
    }
    try (InputStream is = CompleteBody.of(conn)) {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      byte[] buf = new byte[65536];
      int len;
      while ((len = is.read(buf)) > 0) {
        baos.write(buf, 0, len);
      }
      return baos.toByteArray();
    }
  }
}
