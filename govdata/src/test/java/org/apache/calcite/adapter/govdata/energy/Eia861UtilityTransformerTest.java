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

import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Validates {@link Eia861UtilityTransformer#parseOperationalData}: the real EIA-861
 * Operational_Data_&lt;year&gt;.xlsx workbook (confirmed live for 2013-2023) carries its data on
 * a sheet literally named "States" -- never a sheet named "Operational_Data" -- so the lookup
 * must fall back to that name the same way the sales-sheet lookup already does.
 */
class Eia861UtilityTransformerTest {

  @Test @Tag("unit") void findsOperationalDataOnStatesSheet() throws Exception {
    XSSFWorkbook wb = new XSSFWorkbook();
    try {
      // Real archives name this sheet "States" (with a sibling "Territories" sheet), not
      // "Operational_Data" -- this is the exact shape that left summer_peak_demand_mw,
      // winter_peak_demand_mw, and net_generation_mwh entirely null.
      Sheet sheet = wb.createSheet("States");
      // Real archives spread the composite header across 3 rows (category / unit / field
      // name); the parser always reads rows 0-2 as the header block, so rows 0-1 must exist
      // even though this fixture puts the full label directly on row 2.
      sheet.createRow(0);
      sheet.createRow(1);
      Row header = sheet.createRow(2);
      header.createCell(0).setCellValue("Data Year");
      header.createCell(1).setCellValue("Utility Number");
      header.createCell(2).setCellValue("Utility Name");
      header.createCell(3).setCellValue("State");
      header.createCell(4).setCellValue("Ownership Type");
      header.createCell(5).setCellValue("NERC Region");
      header.createCell(6).setCellValue("Summer Peak Demand");
      header.createCell(7).setCellValue("Winter Peak Demand");
      header.createCell(8).setCellValue("Net Generation");

      Row data = sheet.createRow(3);
      data.createCell(0).setCellValue(2022);
      data.createCell(1).setCellValue("12341");
      data.createCell(2).setCellValue("Test Utility");
      data.createCell(3).setCellValue("CA");
      data.createCell(4).setCellValue("Municipal");
      data.createCell(5).setCellValue("WECC");
      data.createCell(6).setCellValue(100.5);
      data.createCell(7).setCellValue(80.2);
      data.createCell(8).setCellValue(5000.0);

      Map<String, Map<String, Double>> result =
          new Eia861UtilityTransformer().parseOperationalData(wb, 2022);

      assertTrue(result.containsKey("12341"), "expected utility 12341 in operational lookup");
      Map<String, Double> row = result.get("12341");
      assertEquals(100.5, row.get("summer_peak_demand_mw"));
      assertEquals(80.2, row.get("winter_peak_demand_mw"));
      assertEquals(5000.0, row.get("net_generation_mwh"));
    } finally {
      wb.close();
    }
  }
}
