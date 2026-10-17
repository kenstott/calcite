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
package org.apache.calcite.adapter.govdata.geo;

import org.apache.calcite.adapter.file.etl.CachingDataProvider;
import org.apache.calcite.adapter.file.etl.EtlPipelineConfig;
import org.apache.calcite.adapter.file.etl.RawCache;
import org.apache.calcite.adapter.govdata.housing.HmdaLoanLevelAggregateProvider.ResumableHttpStream;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.DataFormatter;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * DataProvider for {@code geo.county_cbsa_delineation}: the Census Bureau's republication of the
 * OMB metropolitan and micropolitan statistical area delineations ("List 1": CBSAs, metropolitan
 * divisions and combined statistical areas with their member counties), one workbook per OMB
 * delineation, all vintages stacked into one table.
 *
 * <p>The workbooks are Excel (.xls for 2013-2020, .xlsx for 2023) and are read through the
 * pipeline's {@link RawCache}; HttpSource cannot carry binary content, hence a provider. A
 * fetch returns every vintage; each workbook is parsed strictly: the header row must carry the
 * twelve expected columns and every data row a 5-digit CBSA code, a state and a county FIPS code,
 * else the fetch fails.
 */
public class CbsaDelineationProvider implements CachingDataProvider {

  private static final Logger LOGGER = LoggerFactory.getLogger(CbsaDelineationProvider.class);

  private static final String BASE =
      "https://www2.census.gov/programs-surveys/metro-micro/geographies/reference-files/";
  private static final int MAX_ATTEMPTS = 3;
  private static final long RESUME_BACKOFF_MS = 5_000L;

  /**
   * Vintage (the year of the delineation file) and the file's published path. 2018 is the
   * September 2018 delineation (OMB Bulletin 18-04), which replaced the April 2018 one
   * (Bulletin 18-03, list1.xls in the same folder).
   */
  static final String[][] FILES = {
      {"2013", BASE + "2013/delineation-files/list1.xls"},
      {"2015", BASE + "2015/delineation-files/list1.xls"},
      {"2017", BASE + "2017/delineation-files/list1.xls"},
      {"2018", BASE + "2018/delineation-files/list1_Sep_2018.xls"},
      {"2020", BASE + "2020/delineation-files/list1_2020.xls"},
      {"2023", BASE + "2023/delineation-files/list1_2023.xlsx"},
  };

  /** Expected header cells, lower case, in column order (the 2013 file says "Metro Division"). */
  private static final String[] HEADERS = {
      "cbsa code", "metro", "csa code", "cbsa title", "metropolitan/micropolitan",
      "metropolitan division title", "csa title", "county/county equivalent", "state name",
      "fips state code", "fips county code", "central/outlying"};

  private static final String[] MONTHS = {"JANUARY", "FEBRUARY", "MARCH", "APRIL", "MAY", "JUNE",
      "JULY", "AUGUST", "SEPTEMBER", "OCTOBER", "NOVEMBER", "DECEMBER"};
  private static final Pattern TITLE_DATE = Pattern.compile("([A-Z]+) (\\d{4})$");
  private static final Pattern DIGITS5 = Pattern.compile("\\d{5}");

  @Override public Iterator<Map<String, Object>> fetch(EtlPipelineConfig config,
      Map<String, String> variables, RawCache rawCache) throws IOException {
    List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
    for (String[] file : FILES) {
      byte[] bytes = readThroughCache(rawCache, file[1]);
      List<Map<String, Object>> vintage = parse(new ByteArrayInputStream(bytes),
          Integer.parseInt(file[0]));
      LOGGER.info("CBSA delineation {}: {} county rows from {}", file[0],
          Integer.valueOf(vintage.size()), file[1]);
      rows.addAll(vintage);
    }
    return rows.iterator();
  }

  private static byte[] readThroughCache(RawCache rawCache, String url) throws IOException {
    IOException last = null;
    for (int attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
      try (InputStream in = rawCache.openStream(url,
          () -> ResumableHttpStream.open("Census CBSA delineation", null, url, RESUME_BACKOFF_MS));
           ByteArrayOutputStream out = new ByteArrayOutputStream()) {
        byte[] buf = new byte[1 << 16];
        int n;
        while ((n = in.read(buf)) != -1) {
          out.write(buf, 0, n);
        }
        return out.toByteArray();
      } catch (IOException e) {
        last = e;
        LOGGER.warn("CBSA delineation: attempt {}/{} failed for {}: {}", Integer.valueOf(attempt),
            Integer.valueOf(MAX_ATTEMPTS), url, e.getMessage());
      }
    }
    throw last;
  }

  /** Parses one List 1 workbook (.xls or .xlsx) into one row per county. */
  static List<Map<String, Object>> parse(InputStream in, int vintage) throws IOException {
    DataFormatter fmt = new DataFormatter();
    try (Workbook wb = WorkbookFactory.create(in)) {
      Sheet sheet = wb.getSheetAt(0);
      String delineationDate = delineationDate(text(fmt, sheet.getRow(1), 0), vintage);
      Row header = sheet.getRow(2);
      for (int i = 0; i < HEADERS.length; i++) {
        String h = text(fmt, header, i).toLowerCase(Locale.ROOT);
        if (!h.startsWith(HEADERS[i])) {
          throw new IOException("CBSA delineation " + vintage + ": header column " + (i + 1)
              + " is '" + h + "', expected '" + HEADERS[i] + "...'");
        }
      }
      List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
      for (int r = 3; r <= sheet.getLastRowNum(); r++) {
        Row row = sheet.getRow(r);
        String cbsa = text(fmt, row, 0);
        if (cbsa.isEmpty()) {
          break; // the blank row before the source notes
        }
        if (!DIGITS5.matcher(cbsa).matches()) {
          throw new IOException("CBSA delineation " + vintage + " row " + (r + 1)
              + ": CBSA code '" + cbsa + "' is not 5 digits");
        }
        String state = digits(text(fmt, row, 9), 2, vintage, r);
        String county = digits(text(fmt, row, 10), 3, vintage, r);
        String type = text(fmt, row, 4);
        String kind;
        if (type.startsWith("Metropolitan")) {
          kind = "metropolitan";
        } else if (type.startsWith("Micropolitan")) {
          kind = "micropolitan";
        } else {
          throw new IOException("CBSA delineation " + vintage + " row " + (r + 1)
              + ": unknown statistical area type '" + type + "'");
        }
        String co = text(fmt, row, 11);
        if (!co.equals("Central") && !co.equals("Outlying")) {
          throw new IOException("CBSA delineation " + vintage + " row " + (r + 1)
              + ": central/outlying is '" + co + "'");
        }
        Map<String, Object> m = new LinkedHashMap<String, Object>();
        m.put("vintage", Integer.valueOf(vintage));
        m.put("delineation_date", delineationDate);
        m.put("county_fips", state + county);
        m.put("state_fips", state);
        m.put("county_name", text(fmt, row, 7));
        m.put("state_name", text(fmt, row, 8));
        m.put("cbsa_code", cbsa);
        m.put("cbsa_title", text(fmt, row, 3));
        m.put("cbsa_type", kind);
        m.put("metro_division_code", emptyToNull(text(fmt, row, 1)));
        m.put("metro_division_title", emptyToNull(text(fmt, row, 5)));
        m.put("csa_code", emptyToNull(text(fmt, row, 2)));
        m.put("csa_title", emptyToNull(text(fmt, row, 6)));
        m.put("central_outlying", co.toLowerCase(Locale.ROOT));
        rows.add(m);
      }
      if (rows.isEmpty()) {
        throw new IOException("CBSA delineation " + vintage + ": no data rows");
      }
      return rows;
    }
  }

  /** "List 1. ..., FEBRUARY 2013" to "2013-02". */
  private static String delineationDate(String title, int vintage) throws IOException {
    Matcher m = TITLE_DATE.matcher(title.trim());
    if (m.find()) {
      for (int i = 0; i < MONTHS.length; i++) {
        if (MONTHS[i].equals(m.group(1))) {
          return String.format(Locale.ROOT, "%s-%02d", m.group(2), Integer.valueOf(i + 1));
        }
      }
    }
    throw new IOException("CBSA delineation " + vintage + ": cannot read the month and year "
        + "from the title '" + title + "'");
  }

  private static String digits(String v, int width, int vintage, int r) throws IOException {
    if (v.isEmpty() || v.length() > width || !v.chars().allMatch(Character::isDigit)) {
      throw new IOException("CBSA delineation " + vintage + " row " + (r + 1)
          + ": FIPS code '" + v + "' is not up to " + width + " digits");
    }
    StringBuilder sb = new StringBuilder(v);
    while (sb.length() < width) {
      sb.insert(0, '0');
    }
    return sb.toString();
  }

  private static String text(DataFormatter fmt, Row row, int col) {
    if (row == null) {
      return "";
    }
    Cell cell = row.getCell(col);
    return cell == null ? "" : fmt.formatCellValue(cell).trim();
  }

  private static String emptyToNull(String v) {
    return v.isEmpty() ? null : v;
  }
}
