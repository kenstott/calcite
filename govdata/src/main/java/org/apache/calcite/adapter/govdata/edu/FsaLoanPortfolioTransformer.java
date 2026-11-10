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

import org.apache.calcite.adapter.file.etl.NetworkOutageWait;
import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.ResponseTransformer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.time.LocalDate;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Parses Federal Student Aid's quarterly portfolio workbooks (legacy .xls) into long-format rows:
 * one row per (portfolio, fiscal quarter, status) with the dollars outstanding and loan-level
 * recipient count.
 *
 * <p>Both workbooks share one layout (verified live 2026-11-09): rows 0-2 title and source, row 4
 * the status group labels (one label every second column starting at column 2), row 5 the
 * "Dollars Outstanding (in billions)" / "Recipients (in millions)" sub-headers, then one data row
 * per fiscal quarter. Column 0 carries the federal fiscal year only on its first row of the year
 * (blank afterwards, so it is carried down) and column 1 carries "Q1".."Q4". Dollars are
 * converted from billions to whole USD and recipients from millions to a count.
 */
public abstract class FsaLoanPortfolioTransformer implements ResponseTransformer {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final Pattern QUARTER = Pattern.compile("^Q([1-4])$");
  private static final int HEADER_ROW = 4;
  private static final int FIRST_STATUS_COL = 2;

  /** Sheet name to portfolio code, in workbook order. */
  protected abstract String[][] sheets();

  @Override
  public String transform(String response, RequestContext context) {
    // The response body is binary corrupted by text decoding — ignore it and re-download.
    String url = context.getUrl();
    try {
      byte[] bytes = download(url);
      ArrayNode result = MAPPER.createArrayNode();
      try (Workbook workbook = WorkbookFactory.create(new ByteArrayInputStream(bytes))) {
        for (String[] sheetAndPortfolio : sheets()) {
          Sheet sheet = workbook.getSheet(sheetAndPortfolio[0]);
          if (sheet == null) {
            throw new IllegalStateException("FSA portfolio workbook " + url + ": sheet '"
                + sheetAndPortfolio[0] + "' not found — layout may have changed");
          }
          parseSheet(sheet, sheetAndPortfolio[1], result);
        }
      }
      if (result.size() == 0) {
        throw new IllegalStateException(
            "FSA portfolio workbook " + url + ": parsed zero rows — layout may have changed");
      }
      return result.toString();
    } catch (Exception e) {
      throw new RuntimeException("FSA portfolio workbook: failed to parse from " + url, e);
    }
  }

  private void parseSheet(Sheet sheet, String portfolio, ArrayNode result) {
    Row header = sheet.getRow(HEADER_ROW);
    if (header == null) {
      throw new IllegalStateException("FSA portfolio sheet " + sheet.getSheetName()
          + ": missing status header row");
    }
    int lastCol = header.getLastCellNum();
    Integer fiscalYear = null;
    for (int r = HEADER_ROW + 2; r <= sheet.getLastRowNum(); r++) {
      Row row = sheet.getRow(r);
      if (row == null) {
        continue;
      }
      Matcher q = QUARTER.matcher(text(row.getCell(1)));
      if (!q.matches()) {
        // Footnote / blank row after the data block.
        continue;
      }
      Double year = number(row.getCell(0));
      if (year != null) {
        fiscalYear = year.intValue();
      }
      if (fiscalYear == null) {
        throw new IllegalStateException("FSA portfolio sheet " + sheet.getSheetName()
            + " row " + (r + 1) + ": quarter without a fiscal year");
      }
      int quarter = Integer.parseInt(q.group(1));
      for (int c = FIRST_STATUS_COL; c + 1 < lastCol; c += 2) {
        String label = text(header.getCell(c));
        if (label.isEmpty()) {
          continue;
        }
        Double dollars = number(row.getCell(c));
        Double recipients = number(row.getCell(c + 1));
        if (dollars == null && recipients == null) {
          continue;
        }
        ObjectNode out = MAPPER.createObjectNode();
        out.put("portfolio", portfolio);
        out.put("fiscal_year", fiscalYear);
        out.put("fiscal_quarter", quarter);
        out.put("period_end_date", periodEnd(fiscalYear, quarter).toString());
        out.put("status", statusCode(label));
        out.put("status_label", label.replaceAll("\\*+$", "").trim());
        if (dollars == null) {
          out.putNull("dollars_outstanding");
        } else {
          out.put("dollars_outstanding", Math.round(dollars * 1.0e9));
        }
        if (recipients == null) {
          out.putNull("recipients");
        } else {
          out.put("recipients", Math.round(recipients * 1.0e6));
        }
        result.add(out);
      }
    }
  }

  /** Federal fiscal Q1 ends 12/31 of the prior calendar year; Q2 3/31, Q3 6/30, Q4 9/30. */
  static LocalDate periodEnd(int fiscalYear, int quarter) {
    switch (quarter) {
    case 1:
      return LocalDate.of(fiscalYear - 1, 12, 31);
    case 2:
      return LocalDate.of(fiscalYear, 3, 31);
    case 3:
      return LocalDate.of(fiscalYear, 6, 30);
    default:
      return LocalDate.of(fiscalYear, 9, 30);
    }
  }

  static String statusCode(String label) {
    String s = label.replaceAll("\\*+$", "").trim().toLowerCase(Locale.ROOT);
    if (s.startsWith("loans transferring to the debt management")) {
      return "transferring_to_dmcs";
    }
    return s.replaceAll("[^a-z0-9]+", "_").replaceAll("^_+|_+$", "");
  }

  private static String text(Cell cell) {
    if (cell == null || cell.getCellType() != CellType.STRING) {
      return "";
    }
    return cell.getStringCellValue().trim();
  }

  private static Double number(Cell cell) {
    if (cell == null || cell.getCellType() != CellType.NUMERIC) {
      return null;
    }
    return cell.getNumericCellValue();
  }

  private static byte[] download(String url) throws IOException {
    HttpURLConnection conn = (HttpURLConnection) URI.create(url).toURL().openConnection();
    conn.setConnectTimeout(30000);
    conn.setReadTimeout(120000);
    conn.setRequestProperty("User-Agent",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
            + "Chrome/128.0.0.0 Safari/537.36");
    NetworkOutageWait.connect(conn);
    int status = conn.getResponseCode();
    if (status != 200) {
      throw new IOException("HTTP " + status + " from " + url);
    }
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (InputStream is = conn.getInputStream()) {
      byte[] buf = new byte[65536];
      int len;
      while ((len = is.read(buf)) > 0) {
        baos.write(buf, 0, len);
      }
    }
    return baos.toByteArray();
  }

  /** Loan status by portfolio (Direct, FFEL, ED-held FFEL, federally managed). */
  public static class LoanStatus extends FsaLoanPortfolioTransformer {
    @Override protected String[][] sheets() {
      return new String[][] {
          {"Direct Loan", "direct_loan"},
          {"FFEL", "ffel"},
          {"ED-Held FFEL", "ed_held_ffel"},
          {"Federally Managed", "federally_managed"}};
    }
  }

  /** Repayment-portfolio delinquency buckets by portfolio. */
  public static class Delinquency extends FsaLoanPortfolioTransformer {
    @Override protected String[][] sheets() {
      return new String[][] {
          {"DLPortfoliobyDelinquencyStatus", "direct_loan"},
          {"ED-HeldFFELbyDelinquencyStatus", "ed_held_ffel"},
          {"FedManagedPortbyDelinquencyStat", "federally_managed"}};
    }
  }
}
