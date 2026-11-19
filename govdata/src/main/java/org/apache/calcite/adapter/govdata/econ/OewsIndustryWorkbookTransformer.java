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

import org.apache.calcite.adapter.file.StagingDirectory;
import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.StreamingResponseTransformer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

/**
 * Reads one reference year of the BLS OEWS national industry workbooks
 * ({@code https://www.bls.gov/oes/special-requests/oesm{YY}in4.zip}) and emits the
 * {@code econ.industry_occupation_employment_history} rows for it: national employment for each
 * industry (sector, 3-, 4-, 5- and 6-digit NAICS) by major SOC occupation group.
 *
 * <p>Each zip holds one workbook per NAICS level ({@code natsector}, {@code nat3d},
 * {@code nat4d}, {@code nat5d_6d}; the 4-digit file is split in two for 2012-2013), {@code .xls}
 * for reference years 2012-2013 and {@code .xlsx} from 2014. The {@code *_owner} workbooks, which
 * split an industry by state/local/private ownership, are not read: before 2019 they carry the
 * ownership only as free text, so there is no stable key for them. The workbooks are read with
 * the same POI event readers as {@link OewsStateWorkbookTransformer}.
 *
 * <p>The emitted {@code series} and {@code industry_code} follow the BLS OE flat-file encoding
 * (the one {@code econ.industry_occupation_employment} carries): 6-character NAICS codes pass
 * through, and a sector code is written {@code <first>--<last+1>} (11 becomes {@code 11--12},
 * 31-33 becomes {@code 31--34}, 99 becomes {@code 99-100}). A cell BLS marks {@code **}
 * (suppressed) yields no row. An industry BLS publishes at two NAICS levels appears in both
 * workbooks with identical values and is emitted once; differing values are an error.
 */
public class OewsIndustryWorkbookTransformer implements StreamingResponseTransformer {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(OewsIndustryWorkbookTransformer.class);

  private static final Pattern INDUSTRY_ENTRY =
      Pattern.compile("(?i)(?:^|.*/)(natsector|nat3d|nat4d|nat5d_6d)_m(\\d{4})_dl[^/]*\\.(xlsx|xls)$");

  private static final Pattern MAJOR_OCCUPATION = Pattern.compile("\\d\\d0000");
  private static final Pattern SECTOR = Pattern.compile("(\\d\\d)(?:-(\\d\\d))?");
  private static final String TOTAL_OCCUPATION = "000000";
  private static final String NATIONAL_AREA_TYPE = "1";
  private static final String SUPPRESSED = "**";

  @Override public Iterator<Map<String, Object>> fetchAndTransform(RequestContext context)
      throws IOException {
    String yearText = context.getDimensionValues().get("effective_year");
    if (yearText == null) {
      throw new IllegalStateException("OEWS industry workbook: effective_year dimension is "
          + "required to label the reference year");
    }
    int year = Integer.parseInt(yearText);
    String url = context.getUrl();

    Path zip = StagingDirectory.newTempFile("oews-industry-", ".zip").toPath();
    try {
      OewsStateWorkbookTransformer.download(url, context.getHeaders(), zip);
      IndustryCollector collector = new IndustryCollector(year);
      int files = 0;
      try (ZipFile zf = new ZipFile(zip.toFile())) {
        Enumeration<? extends ZipEntry> entries = zf.entries();
        while (entries.hasMoreElements()) {
          ZipEntry entry = entries.nextElement();
          if (INDUSTRY_ENTRY.matcher(entry.getName()).matches()
              && !entry.getName().toLowerCase(Locale.ROOT).contains("owner")) {
            readEntry(zf, entry, year, collector);
            files++;
          }
        }
      }
      if (files == 0) {
        throw new IOException("OEWS: no natsector/nat3d/nat4d/nat5d_6d workbook in " + url);
      }
      List<Map<String, Object>> rows = collector.finish(url);
      LOGGER.debug("OEWS industry workbooks {}: {} files, {} rows for reference year {}", url,
          files, rows.size(), year);
      return rows.iterator();
    } finally {
      Files.deleteIfExists(zip);
    }
  }

  private static void readEntry(ZipFile zf, ZipEntry entry, int year,
      IndustryCollector collector) throws IOException {
    Matcher m = INDUSTRY_ENTRY.matcher(entry.getName());
    if (!m.matches() || Integer.parseInt(m.group(2)) != year) {
      throw new IOException("OEWS industry workbook " + entry.getName()
          + " is not reference year " + year);
    }
    boolean xlsx = m.group(3).equalsIgnoreCase("xlsx");
    Path workbook =
        StagingDirectory.newTempFile("oews-industry-", xlsx ? ".xlsx" : ".xls").toPath();
    try {
      try (InputStream in = zf.getInputStream(entry)) {
        Files.copy(in, workbook, StandardCopyOption.REPLACE_EXISTING);
      }
      collector.beginFile(entry.getName());
      if (xlsx) {
        OewsStateWorkbookTransformer.readXlsx(workbook.toFile(), collector);
      } else {
        OewsStateWorkbookTransformer.readXls(workbook.toFile(), collector);
      }
    } finally {
      Files.deleteIfExists(workbook);
    }
  }

  /**
   * Maps a workbook NAICS value to the BLS OE flat-file industry code: a 6-character code is
   * unchanged, a sector ({@code 11}, {@code 31-33}) becomes {@code first--(last+1)}.
   */
  static String industryCode(String naics) {
    if (naics.length() == 6) {
      return naics;
    }
    Matcher m = SECTOR.matcher(naics);
    if (!m.matches()) {
      throw new IllegalStateException("OEWS industry: unrecognised NAICS code '" + naics + "'");
    }
    String first = m.group(1);
    int next = Integer.parseInt(m.group(2) != null ? m.group(2) : first) + 1;
    return next < 100 ? first + "--" + next : first + "-" + next;
  }

  /** Header-aware filter that turns matching sheet rows into output rows. */
  static final class IndustryCollector implements OewsRowSink {
    private final int year;
    private final Map<String, Map<String, Object>> rows =
        new LinkedHashMap<String, Map<String, Object>>();
    private String file;
    private boolean headerSeen;
    private int areaType = -1;
    private int naics = -1;
    private int occCode = -1;
    private int totEmp = -1;

    IndustryCollector(int year) {
      this.year = year;
    }

    /** Starts a new workbook: the next row received is its header. */
    void beginFile(String name) {
      this.file = name;
      this.headerSeen = false;
    }

    @Override public void accept(List<String> cells) {
      if (!headerSeen) {
        readHeader(cells);
        return;
      }
      String occ = get(cells, occCode);
      if (occ == null) {
        return;
      }
      String occKey = occ.replace("-", "").trim();
      if (!MAJOR_OCCUPATION.matcher(occKey).matches() || TOTAL_OCCUPATION.equals(occKey)) {
        return;
      }
      if (areaType >= 0 && !NATIONAL_AREA_TYPE.equals(normalizeInt(get(cells, areaType)))) {
        return;
      }
      String naicsRaw = get(cells, naics);
      if (naicsRaw == null) {
        throw new IllegalStateException("OEWS " + year + " " + file + ": empty naics for "
            + "occupation " + occ);
      }
      String industry = industryCode(normalizeInt(naicsRaw));
      String emp = get(cells, totEmp);
      if (emp == null) {
        throw new IllegalStateException("OEWS " + year + " " + file + ": empty tot_emp for "
            + "industry " + industry + " occupation " + occ);
      }
      String trimmed = emp.trim();
      if (SUPPRESSED.equals(trimmed)) {
        return;
      }
      double employment;
      try {
        employment = Double.parseDouble(trimmed.replace(",", ""));
      } catch (NumberFormatException e) {
        throw new IllegalStateException("OEWS " + year + " " + file + ": unrecognised tot_emp '"
            + emp + "' for industry " + industry + " occupation " + occ, e);
      }
      String key = industry + "|" + occKey;
      Map<String, Object> prior = rows.get(key);
      if (prior != null) {
        if (((Double) prior.get("employment")).doubleValue() != employment) {
          throw new IllegalStateException("OEWS " + year + " " + file + ": industry " + industry
              + " occupation " + occ + " has employment " + employment + " but "
              + prior.get("employment") + " in another workbook");
        }
        return;
      }
      Map<String, Object> row = new LinkedHashMap<String, Object>();
      row.put("series", "OEUN0000000" + industry + occKey + "01");
      row.put("year", Integer.valueOf(year));
      row.put("industry_code", industry);
      row.put("occupation_code", occKey);
      row.put("employment", Double.valueOf(employment));
      row.put("footnotes", null);
      rows.put(key, row);
    }

    List<Map<String, Object>> finish(String url) {
      if (rows.isEmpty()) {
        throw new IllegalStateException("OEWS " + year + ": no matching rows in " + url);
      }
      return new ArrayList<Map<String, Object>>(rows.values());
    }

    private void readHeader(List<String> cells) {
      Map<String, Integer> idx = new HashMap<String, Integer>();
      for (int i = 0; i < cells.size(); i++) {
        String h = cells.get(i);
        if (h != null && !h.trim().isEmpty()) {
          idx.put(h.trim().toLowerCase(Locale.ROOT), Integer.valueOf(i));
        }
      }
      naics = required(idx, "naics");
      occCode = required(idx, "occ_code");
      totEmp = required(idx, "tot_emp");
      Integer a = idx.get("area_type");
      areaType = a == null ? -1 : a.intValue();
      headerSeen = true;
    }

    private int required(Map<String, Integer> idx, String name) {
      Integer i = idx.get(name);
      if (i == null) {
        throw new IllegalStateException("OEWS " + year + " " + file + ": header has no '" + name
            + "' column: " + idx.keySet());
      }
      return i.intValue();
    }

    private static String get(List<String> cells, int col) {
      if (col < 0 || col >= cells.size()) {
        return null;
      }
      return cells.get(col);
    }

    private static String normalizeInt(String s) {
      if (s == null) {
        return "";
      }
      String t = s.trim();
      if (t.endsWith(".0")) {
        t = t.substring(0, t.length() - 2);
      }
      return t;
    }
  }
}
