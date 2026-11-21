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

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Shared reading of the Administration for Children and Families (acf.gov) Office of Family
 * Assistance workbooks: page link discovery, state-name resolution and plain cell values.
 */
final class AcfWorkbooks {

  static final String SITE = "https://acf.gov";
  static final String ENTRY_PAGE = SITE + "/ofa";

  private static final Pattern HREF =
      Pattern.compile("href=\"([^\"]+\\.xlsx?)\"", Pattern.CASE_INSENSITIVE);

  private AcfWorkbooks() {
  }

  /** Every workbook link on an OFA data page, as absolute URLs, in page order and de-duplicated. */
  static List<String> workbookLinks(String html) {
    List<String> out = new ArrayList<String>();
    Matcher m = HREF.matcher(html);
    while (m.find()) {
      String href = m.group(1);
      String url = href.startsWith("/") ? SITE + href : href;
      if (!out.contains(url)) {
        out.add(url);
      }
    }
    return out;
  }

  /** The last path segment of {@code url}, lower-cased. */
  static String fileName(String url) {
    return url.substring(url.lastIndexOf('/') + 1).toLowerCase(java.util.Locale.ROOT);
  }

  static Workbook open(byte[] bytes) throws IOException {
    return WorkbookFactory.create(new ByteArrayInputStream(bytes));
  }

  /** Cell text with every run of whitespace (including line breaks) collapsed to one space. */
  static String text(Cell c) {
    if (c == null) {
      return "";
    }
    switch (c.getCellType()) {
    case STRING:
      return c.getStringCellValue().replaceAll("\\s+", " ").trim();
    case NUMERIC:
      return Double.toString(c.getNumericCellValue());
    case FORMULA:
      switch (c.getCachedFormulaResultType()) {
      case STRING:
        return c.getStringCellValue().replaceAll("\\s+", " ").trim();
      case NUMERIC:
        return Double.toString(c.getNumericCellValue());
      default:
        return "";
      }
    default:
      return "";
    }
  }

  static String text(Row r, int col) {
    return r == null ? "" : text(r.getCell(col));
  }

  /** First cell of the row, the label column. */
  static String label(Row r) {
    return text(r, 0);
  }

  /**
   * A numeric value, or null for a blank cell. A text cell is not a number: it throws so a layout
   * change surfaces instead of becoming a null.
   */
  static Double number(Cell c, String where) {
    if (c == null) {
      return null;
    }
    switch (c.getCellType()) {
    case NUMERIC:
      return c.getNumericCellValue();
    case BLANK:
      return null;
    case FORMULA:
      if (c.getCachedFormulaResultType() == org.apache.poi.ss.usermodel.CellType.NUMERIC) {
        return c.getNumericCellValue();
      }
      break;
    default:
      break;
    }
    throw new IllegalStateException("tanf: expected a number at " + where + " but found '"
        + text(c) + "'");
  }

  /** Row number of the first row in the first {@code limit} rows whose label is {@code label}. */
  static int findRow(Sheet sheet, String label, int limit) {
    for (int i = 0; i <= Math.min(limit, sheet.getLastRowNum()); i++) {
      if (label.equalsIgnoreCase(label(sheet.getRow(i)))) {
        return i;
      }
    }
    return -1;
  }

  /**
   * The postal code for an OFA row label: {@code US} for the national row, otherwise the state,
   * DC, Puerto Rico, Guam or Virgin Islands code. Unknown labels throw.
   */
  static String stateCode(String label) {
    String n = canonical(label);
    if (n.equalsIgnoreCase("U.S. Totals") || n.equalsIgnoreCase("United States")) {
      return "US";
    }
    if (n.equalsIgnoreCase("Guam")) {
      return "GU";
    }
    if (n.equalsIgnoreCase("Virgin Islands")) {
      return "VI";
    }
    String abbr = FhwaApportionmentsTransformer.stateAbbr(n);
    if (abbr == null) {
      throw new IllegalStateException("tanf: unrecognized state row label '" + n + "'");
    }
    return abbr;
  }

  /** A row label with whitespace collapsed and the workbooks' "District of Col." spelled out. */
  private static String canonical(String label) {
    String n = label.replaceAll("\\s+", " ").trim();
    return n.equalsIgnoreCase("District of Col.") ? "District of Columbia" : n;
  }

  /** True when {@code label} names the national row, a state, DC or a territory. */
  static boolean isStateLabel(String label) {
    String n = canonical(label);
    return n.equalsIgnoreCase("U.S. Totals") || n.equalsIgnoreCase("United States")
        || n.equalsIgnoreCase("Guam") || n.equalsIgnoreCase("Virgin Islands")
        || FhwaApportionmentsTransformer.stateAbbr(n) != null;
  }

  /** The state name to store for a code: the national row is "United States". */
  static String stateName(String label, String code) {
    return "US".equals(code) ? "United States" : canonical(label);
  }
}
