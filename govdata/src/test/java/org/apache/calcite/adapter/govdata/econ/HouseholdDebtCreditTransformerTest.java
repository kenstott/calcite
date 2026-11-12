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

import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Tag("unit")
class HouseholdDebtCreditTransformerTest {

  private static void row(Sheet sheet, int r, Object... values) {
    Row row = sheet.createRow(r);
    for (int c = 0; c < values.length; c++) {
      if (values[c] instanceof Number) {
        row.createCell(c).setCellValue(((Number) values[c]).doubleValue());
      } else if (values[c] != null) {
        row.createCell(c).setCellValue(values[c].toString());
      }
    }
  }

  private static File workbook() throws Exception {
    XSSFWorkbook wb = new XSSFWorkbook();
    Sheet s14 = wb.createSheet("Page 14 Data");
    row(s14, 0, "New Seriously Delinquent Balances by Loan Type");
    row(s14, 3, "Return to Table of Contents");
    row(s14, 4, null, "AUTO", "CC", "MORTGAGE", "HELOC", "STUDENT LOAN", "OTHER", "ALL");
    // student loan blank in the first quarter, as in the real workbook
    row(s14, 5, "03:Q1", 2.18, 8.27, 1.44, 0.81, null, 4.83, 2.54);
    row(s14, 6, "26:Q3", 3.02, 6.87, 1.6, 1.17, 7.7, 5.13, 2.64);
    row(s14, 7, "Footnote: something");
    Sheet s3 = wb.createSheet("Page 3 Data");
    row(s3, 3, null, "Mortgage", "HE Revolving", "Auto Loan", "Credit Card", "Student Loan",
        "Other", "Total");
    row(s3, 4, "26:Q3", 13.374, 0.481, 1.734, 1.285, 1.6557, 0.575, 19.1047);
    // a cell displayed with one decimal must still be read at full precision
    org.apache.poi.ss.usermodel.CellStyle oneDecimal = wb.createCellStyle();
    oneDecimal.setDataFormat(wb.createDataFormat().getFormat("0.0"));
    s3.getRow(4).getCell(4).setCellStyle(oneDecimal);
    File f = Files.createTempFile("hhdc-test-", ".xlsx").toFile();
    f.deleteOnExit();
    try (OutputStream out = new FileOutputStream(f)) {
      wb.write(out);
    }
    wb.close();
    return f;
  }

  private static HouseholdDebtCreditTransformer.SheetSpec spec(String sheet) {
    for (HouseholdDebtCreditTransformer.SheetSpec s : HouseholdDebtCreditTransformer.SHEETS) {
      if (s.sheet.equals(sheet)) {
        return s;
      }
    }
    throw new IllegalArgumentException(sheet);
  }

  private static Map<String, Object> find(HouseholdDebtCreditTransformer.SheetCollector c,
      String loanType) {
    for (Map<String, Object> r : c.rows) {
      if (loanType.equals(r.get("loan_type"))) {
        return r;
      }
    }
    return null;
  }

  @Test void emitsOnlyRequestedYearAndMapsLoanTypes() throws Exception {
    HouseholdDebtCreditTransformer.SheetCollector c =
        new HouseholdDebtCreditTransformer.SheetCollector(spec("Page 14 Data"), 2026, "2026q3");
    HouseholdDebtCreditTransformer.readSheet(workbook(), "Page 14 Data", c);
    c.finish();
    assertEquals(7, c.rows.size());
    Map<String, Object> cc = find(c, "credit_card");
    assertEquals(6.87, cc.get("value"));
    assertEquals("flow_into_serious_delinquency_90plus", cc.get("metric"));
    assertEquals(2026, cc.get("year"));
    assertEquals(3, cc.get("quarter"));
    assertEquals("2026Q3", cc.get("period"));
    assertEquals("percent", cc.get("unit"));
    assertEquals(2.64, find(c, "total").get("value"));
    assertNull(cc.get("delinquency_status"));
  }

  @Test void blankCellYieldsNoRow() throws Exception {
    HouseholdDebtCreditTransformer.SheetCollector c =
        new HouseholdDebtCreditTransformer.SheetCollector(spec("Page 14 Data"), 2003, "2026q3");
    HouseholdDebtCreditTransformer.readSheet(workbook(), "Page 14 Data", c);
    c.finish();
    assertEquals(6, c.rows.size());
    assertNull(find(c, "student_loan"));
  }

  @Test void balanceSheetUsesTrillionsUnit() throws Exception {
    HouseholdDebtCreditTransformer.SheetCollector c =
        new HouseholdDebtCreditTransformer.SheetCollector(spec("Page 3 Data"), 2026, "2026q3");
    HouseholdDebtCreditTransformer.readSheet(workbook(), "Page 3 Data", c);
    c.finish();
    assertEquals(1.285, find(c, "credit_card").get("value"));
    assertEquals(1.6557, find(c, "student_loan").get("value"));
    assertEquals("trillions_usd", find(c, "credit_card").get("unit"));
    assertEquals(19.1047, find(c, "total").get("value"));
    assertEquals(7, c.rows.size());
  }

  @Test void missingSheetFails() throws Exception {
    HouseholdDebtCreditTransformer.SheetCollector c =
        new HouseholdDebtCreditTransformer.SheetCollector(spec("Page 12 Data"), 2026, "2026q3");
    assertThrows(java.io.IOException.class,
        () -> HouseholdDebtCreditTransformer.readSheet(workbook(), "Page 12 Data", c));
  }

  @Test void unrecognisedHeaderFails() throws Exception {
    XSSFWorkbook wb = new XSSFWorkbook();
    Sheet s = wb.createSheet("Page 14 Data");
    row(s, 0, null, "AUTO", "CRYPTO");
    row(s, 1, "26:Q3", 1, 2);
    File f = Files.createTempFile("hhdc-bad-", ".xlsx").toFile();
    f.deleteOnExit();
    try (OutputStream out = new FileOutputStream(f)) {
      wb.write(out);
    }
    wb.close();
    HouseholdDebtCreditTransformer.SheetCollector c =
        new HouseholdDebtCreditTransformer.SheetCollector(spec("Page 14 Data"), 2026, "2026q3");
    java.io.IOException e = assertThrows(java.io.IOException.class,
        () -> HouseholdDebtCreditTransformer.readSheet(f, "Page 14 Data", c));
    assertEquals(IllegalStateException.class, e.getCause().getClass());
  }
}
