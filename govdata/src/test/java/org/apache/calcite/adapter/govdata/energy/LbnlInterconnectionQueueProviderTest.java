/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.adapter.govdata.energy;

import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.CreationHelper;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Validates {@link LbnlInterconnectionQueueProvider} against a workbook laid out like LBNL's real
 * Queued Up file: a "RETURN TO CONTENTS" banner above the header row on the queue sheet, FIPS
 * stored as a number (leading zero lost), and the as-of year only on the summary sheet's title.
 */
class LbnlInterconnectionQueueProviderTest {

  private static final String[] HEADER = {
      "q_id", "q_status", "q_date", "prop_date", "on_date", "wd_date", "ia_date",
      "IA_phase_raw", "IA_phase_clean", "county", "state", "fips_code", "poi_name", "region",
      "project_name", "utility", "entity", "developer", "cluster", "service", "project_type",
      "type_1", "type_2", "type_3", "type_clean", "mw_1", "mw_2", "mw_3", "q_year", "prop_year"};

  private static File writeWorkbook(boolean withAsOf) throws IOException {
    XSSFWorkbook wb = new XSSFWorkbook();
    try {
      Sheet summary = wb.createSheet("02. Data Sample by Region");
      summary.createRow(0).createCell(0).setCellValue("RETURN TO CONTENTS");
      summary.createRow(1).createCell(0).setCellValue(withAsOf
          ? "Summary of data sample by region and status (as of the end of 2025)"
          : "Summary of data sample by region and status");

      Sheet queue = wb.createSheet("03. Complete Queue Data");
      queue.createRow(0).createCell(0).setCellValue("RETURN TO CONTENTS");
      Row header = queue.createRow(1);
      for (int c = 0; c < HEADER.length; c++) {
        header.createCell(c).setCellValue(HEADER[c]);
      }
      CreationHelper helper = wb.getCreationHelper();
      CellStyle dateStyle = wb.createCellStyle();
      dateStyle.setDataFormat(helper.createDataFormat().getFormat("yyyy-mm-dd"));

      // Arizona: FIPS 4005 is stored without its leading zero; hybrid Solar+Battery with dates.
      Row az = queue.createRow(2);
      az.createCell(0).setCellValue("Q100");
      az.createCell(1).setCellValue("active");
      az.createCell(2).setCellValue(java.sql.Date.valueOf("2019-02-15"));
      az.getCell(2).setCellStyle(dateStyle);
      az.createCell(7).setCellValue("Feasibility Study");
      az.createCell(9).setCellValue("Coconino");
      az.createCell(10).setCellValue("AZ");
      az.createCell(11).setCellValue(4005);
      az.createCell(16).setCellValue("APS");
      az.createCell(21).setCellValue("Solar");
      az.createCell(22).setCellValue("Battery");
      az.createCell(25).setCellValue(100.5);
      az.createCell(26).setCellValue(50);
      az.createCell(28).setCellValue(2019);

      // Two-county request: both codes concatenated, first listed county wins; numeric q_id.
      Row ny = queue.createRow(3);
      ny.createCell(0).setCellValue(214);
      ny.createCell(1).setCellValue("operational");
      ny.createCell(10).setCellValue("NY");
      ny.createCell(11).setCellValue(3603336019L);
      ny.createCell(16).setCellValue("NYISO");

      // No county at source and a blank row after it: blank FIPS stays null, blank row skipped.
      Row none = queue.createRow(4);
      none.createCell(0).setCellValue("not assigned");
      none.createCell(1).setCellValue("withdrawn");
      none.createCell(10).setCellValue("MX");
      queue.createRow(5);

      File f = File.createTempFile("lbnl-test-", ".xlsx");
      f.deleteOnExit();
      try (FileOutputStream out = new FileOutputStream(f)) {
        wb.write(out);
      }
      return f;
    } finally {
      wb.close();
    }
  }

  @Test @Tag("unit") void readsQueueRowsBelowTheBanner() throws Exception {
    List<Map<String, Object>> rows =
        new LbnlInterconnectionQueueProvider().parseWorkbook(writeWorkbook(true), "test");
    assertEquals(3, rows.size());

    Map<String, Object> az = rows.get(0);
    assertEquals(3, az.get("source_row"));
    assertEquals("Q100", az.get("q_id"));
    assertEquals("04005", az.get("county_fips"));
    assertEquals("AZ", az.get("state"));
    assertEquals("APS", az.get("entity"));
    assertEquals("Feasibility Study", az.get("ia_phase_raw"));
    assertEquals(LocalDate.of(2019, 2, 15).toString(), az.get("q_date"));
    assertNull(az.get("prop_date"));
    assertEquals(100.5, az.get("mw_1"));
    assertEquals(50.0, az.get("mw_2"));
    assertNull(az.get("mw_3"));
    assertEquals(2019, az.get("q_year"));
    assertEquals(2025, az.get("as_of_year"));
  }

  @Test @Tag("unit") void takesFirstOfConcatenatedCountyCodes() throws Exception {
    List<Map<String, Object>> rows =
        new LbnlInterconnectionQueueProvider().parseWorkbook(writeWorkbook(true), "test");
    assertEquals("214", rows.get(1).get("q_id"));
    assertEquals("36033", rows.get(1).get("county_fips"));
    assertNull(rows.get(2).get("county_fips"));
    assertEquals("not assigned", rows.get(2).get("q_id"));
  }

  @Test @Tag("unit") void padsAndSplitsFips() {
    assertEquals("04005", LbnlInterconnectionQueueProvider.firstCountyFips("4005", 1));
    assertEquals("36033", LbnlInterconnectionQueueProvider.firstCountyFips("3603336019", 1));
    assertEquals("06031", LbnlInterconnectionQueueProvider.firstCountyFips("603106019", 1));
    assertEquals("48453", LbnlInterconnectionQueueProvider.firstCountyFips("48453", 1));
    assertNull(LbnlInterconnectionQueueProvider.firstCountyFips(null, 1));
  }

  @Test @Tag("unit") void failsWhenAsOfYearIsMissing() throws Exception {
    File f = writeWorkbook(false);
    assertThrows(IOException.class,
        () -> new LbnlInterconnectionQueueProvider().parseWorkbook(f, "test"));
  }
}
