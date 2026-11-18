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

import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.govdata.energy.EiaBulkXlsxTransformer;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.openxml4j.util.ZipSecureFile;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.ByteArrayInputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Transforms one NCHS National Vital Statistics Report (NVSR) FTP directory of life-table
 * workbooks into rows for {@code health.life_table_national} and {@code health.life_table_state}.
 *
 * <p>The request URL is the report's directory listing
 * ({@code ftp.cdc.gov/pub/Health_Statistics/NCHS/Publications/NVSR/<report>/}). Every
 * {@code .xlsx} in it is downloaded and classified from its own title cell (A1), never from the
 * file name, because NCHS names files differently in every report ({@code Table02.xlsx},
 * {@code table02.xlsx}, {@code AK2.xlsx}, {@code Alabama-2-Male.xlsx}). A title reads
 * {@code Table 5. Life table for Hispanic males: United States, 2024} or
 * {@code Table AK–2. Life table for males: Alaska, 2022}: population group and sex before the
 * colon, geography and data year after it.
 *
 * <p>Workbooks whose title is not a life table (the state reports' standard-error tables, the
 * methodology tables) are not life tables and are skipped by that test. A life table whose
 * title does not parse, whose age column is not the contiguous ages 0..100, or whose geography is
 * neither "United States" nor a known state fails the whole report rather than loading partial
 * data.
 *
 * <p>Layout is located by content, not position: a data row is a row whose column A is an age
 * interval ({@code 0–1}, {@code 64-65}) or the open-ended last age ({@code 100 and older},
 * {@code 100 and over}); columns B-G are qx, lx, dx, Lx, Tx and ex in every report.
 */
public class NchsLifeTableTransformer extends EiaBulkXlsxTransformer {

  private static final Pattern TITLE =
      Pattern.compile("Life table for (?:the )?(.+?):\\s+(.+?),\\s*(\\d{4})\\s*$");
  private static final Pattern SEX_SUFFIX = Pattern.compile("^(.*?)\\s*\\b(females|males)$");
  private static final Pattern AGE_INTERVAL =
      Pattern.compile("^(\\d+)\\s*[\\u2010-\\u2015-]\\s*(\\d+)$");
  private static final Pattern AGE_OPEN = Pattern.compile("^(\\d+)\\s+and\\s+(?:over|older)$");
  private static final Pattern HREF = Pattern.compile("HREF=\"([^\"]+\\.xlsx)\"",
      Pattern.CASE_INSENSITIVE);
  private static final String NATIONAL = "United States";
  private static final int MAX_AGE = 100;

  private static final Map<String, String> STATE_NAME_TO_FIPS = buildStateFipsMap();

  @Override
  public String transform(String response, RequestContext context) {
    String url = context.getUrl();
    String report = context.getDimensionValues().get("report");
    try {
      // NCHS workbooks are tiny and highly compressed, tripping POI's zip-bomb ratio guard; the
      // source is a trusted federal publication.
      ZipSecureFile.setMinInflateRatio(0.0);
      String listing = new String(downloadBytes(url), StandardCharsets.UTF_8);
      URI base = URI.create(url);
      ArrayNode result = MAPPER.createArrayNode();
      int tables = 0;
      Matcher m = HREF.matcher(listing);
      while (m.find()) {
        if (m.group(1).toLowerCase(java.util.Locale.ROOT).contains("classificationratios")) {
          // NCHS's 2010 report repeats Tables 10-18 as *_Updated_ClassificationRatios.xlsx under the
          // identical title (one with shifted columns); the numbered Table NN.xlsx is the table.
          continue;
        }
        String fileUrl = base.resolve(m.group(1)).toString();
        tables += parseFile(fileUrl, report, result);
      }
      if (tables == 0) {
        throw new IllegalStateException("no life-table workbook in " + url);
      }
      LOGGER.debug("NCHS life tables: report={} tables={} rows={}", report, tables, result.size());
      return result.toString();
    } catch (Exception e) {
      throw new RuntimeException("NCHS life tables: failed to load report " + report
          + " from " + url, e);
    }
  }

  private int parseFile(String fileUrl, String report, ArrayNode out) throws Exception {
    XSSFWorkbook workbook = new XSSFWorkbook(new ByteArrayInputStream(downloadBytes(fileUrl)));
    try {
      Sheet sheet = workbook.getSheetAt(0);
      Row titleRow = sheet.getRow(0);
      String titleCell = titleRow == null ? null : cellString(titleRow.getCell(0));
      if (titleCell == null) {
        return 0;
      }
      String title = titleCell.split("[\\r\\n]")[0].trim();
      if (!title.contains("Life table")) {
        return 0;
      }
      Matcher tm = TITLE.matcher(title);
      if (!tm.find()) {
        throw new IllegalStateException("unparseable life-table title '" + title + "' in " + fileUrl);
      }
      String subject = tm.group(1).trim().replaceAll("\\s+", " ");
      String geography = tm.group(2).trim();
      int dataYear = Integer.parseInt(tm.group(3));

      String sex;
      String group;
      Matcher sm = SEX_SUFFIX.matcher(subject);
      if (sm.find()) {
        sex = sm.group(2).equals("males") ? "male" : "female";
        group = sm.group(1).trim();
      } else {
        sex = "total";
        group = subject;
      }
      if (group.isEmpty() || group.equals("total") || group.equals("total population")) {
        group = "Total population";
      } else if (group.endsWith(" population")) {
        group = group.substring(0, group.length() - " population".length());
      }

      boolean national = NATIONAL.equals(geography);
      String stateFips = null;
      if (!national) {
        stateFips = STATE_NAME_TO_FIPS.get(geography);
        if (stateFips == null) {
          throw new IllegalStateException("unknown geography '" + geography + "' in " + fileUrl);
        }
      }

      List<ObjectNode> rows = new ArrayList<ObjectNode>();
      for (int r = 1; r <= sheet.getLastRowNum(); r++) {
        Row row = sheet.getRow(r);
        if (row == null) {
          continue;
        }
        String label = cellString(row.getCell(0));
        if (label == null) {
          continue;
        }
        label = label.trim();
        Integer age = null;
        boolean open = false;
        Matcher im = AGE_INTERVAL.matcher(label);
        Matcher om = AGE_OPEN.matcher(label);
        if (im.matches()) {
          age = Integer.parseInt(im.group(1));
        } else if (om.matches()) {
          age = Integer.parseInt(om.group(1));
          open = true;
        }
        if (age == null) {
          continue;
        }
        ObjectNode o = MAPPER.createObjectNode();
        o.put("report", report);
        o.put("data_year", dataYear);
        o.put("sex", sex);
        if (national) {
          o.put("population_group", group);
        } else {
          o.put("state_name", geography);
          o.put("state_fips", stateFips);
        }
        o.put("age", age);
        o.put("age_open_ended", open);
        putNum(o, "qx", row.getCell(1));
        putNum(o, "lx", row.getCell(2));
        putNum(o, "dx", row.getCell(3));
        putNum(o, "person_years_lived", row.getCell(4));
        putNum(o, "person_years_remaining", row.getCell(5));
        putNum(o, "life_expectancy_years", row.getCell(6));
        rows.add(o);
      }
      if (rows.size() != MAX_AGE + 1) {
        throw new IllegalStateException(rows.size() + " age rows (expected " + (MAX_AGE + 1)
            + ") in '" + title + "' " + fileUrl);
      }
      for (int i = 0; i <= MAX_AGE; i++) {
        if (rows.get(i).get("age").asInt() != i) {
          throw new IllegalStateException("ages not contiguous 0.." + MAX_AGE + " in '" + title
              + "' " + fileUrl);
        }
      }
      for (ObjectNode o : rows) {
        out.add(o);
      }
      return 1;
    } finally {
      workbook.close();
    }
  }

  private void putNum(ObjectNode o, String field, Cell cell) {
    Double v = cellDouble(cell);
    if (v == null) {
      throw new IllegalStateException("non-numeric " + field + " at age " + o.get("age")
          + " (" + o.get("report") + ", " + o.get("sex") + ")");
    }
    o.put(field, v);
  }

  @Override
  protected String parseWorkbook(XSSFWorkbook workbook, RequestContext context) throws Exception { // NOSONAR - required by base class
    throw new UnsupportedOperationException("transform() is overridden: one request spans a directory");
  }

  private static Map<String, String> buildStateFipsMap() {
    Map<String, String> m = new HashMap<String, String>();
    m.put("Alabama", "01"); m.put("Alaska", "02"); m.put("Arizona", "04");
    m.put("Arkansas", "05"); m.put("California", "06"); m.put("Colorado", "08");
    m.put("Connecticut", "09"); m.put("Delaware", "10"); m.put("District of Columbia", "11");
    m.put("Florida", "12"); m.put("Georgia", "13"); m.put("Hawaii", "15");
    m.put("Idaho", "16"); m.put("Illinois", "17"); m.put("Indiana", "18");
    m.put("Iowa", "19"); m.put("Kansas", "20"); m.put("Kentucky", "21");
    m.put("Louisiana", "22"); m.put("Maine", "23"); m.put("Maryland", "24");
    m.put("Massachusetts", "25"); m.put("Michigan", "26"); m.put("Minnesota", "27");
    m.put("Mississippi", "28"); m.put("Missouri", "29"); m.put("Montana", "30");
    m.put("Nebraska", "31"); m.put("Nevada", "32"); m.put("New Hampshire", "33");
    m.put("New Jersey", "34"); m.put("New Mexico", "35"); m.put("New York", "36");
    m.put("North Carolina", "37"); m.put("North Dakota", "38"); m.put("Ohio", "39");
    m.put("Oklahoma", "40"); m.put("Oregon", "41"); m.put("Pennsylvania", "42");
    m.put("Rhode Island", "44"); m.put("South Carolina", "45"); m.put("South Dakota", "46");
    m.put("Tennessee", "47"); m.put("Texas", "48"); m.put("Utah", "49");
    m.put("Vermont", "50"); m.put("Virginia", "51"); m.put("Washington", "53");
    m.put("West Virginia", "54"); m.put("Wisconsin", "55"); m.put("Wyoming", "56");
    return m;
  }
}
