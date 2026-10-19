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
package org.apache.calcite.adapter.file.feature;

import org.apache.calcite.adapter.file.converters.MultiTableExcelToJsonConverter;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.util.CellRangeAddress;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * A sheet whose data rows are short text alone -- ids, codes, ISO timestamps -- has one header
 * row: a row after a full header row is data, not another header. A sparse group header row
 * above its detail header row is still read as one.
 */
@Tag("unit")
public class ShortTextDataRowTest {

  @TempDir
  public File tempDir;

  private static void row(Sheet sheet, int index, String... values) {
    Row row = sheet.createRow(index);
    for (int i = 0; i < values.length; i++) {
      if (values[i] != null) {
        row.createCell(i).setCellValue(values[i]);
      }
    }
  }

  private JsonNode convert(Workbook workbook) throws Exception {
    File file = new File(tempDir, "book.xlsx");
    try (FileOutputStream fos = new FileOutputStream(file)) {
      workbook.write(fos);
    }
    workbook.close();
    MultiTableExcelToJsonConverter.convertFileToJson(
        file, tempDir, true, "SMART_CASING", "SMART_CASING", tempDir);
    File[] json = tempDir.listFiles((dir, name) -> name.endsWith(".json") && !name.startsWith("."));
    assertEquals(1, json.length, "one table");
    return new ObjectMapper().readTree(json[0]);
  }

  private static List<String> names(JsonNode row) {
    List<String> names = new ArrayList<>();
    row.fieldNames().forEachRemaining(names::add);
    return names;
  }

  @Test void shortTextRowsAfterTheHeaderAreData() throws Exception {
    Workbook workbook = new XSSFWorkbook();
    Sheet sheet = workbook.createSheet("deal_team");
    row(sheet, 0, "deal_id", "employee_id", "over_wall_at");
    row(sheet, 1, "D-01", "E0001", "2026-09-01T09:00:00");
    row(sheet, 2, "D-01", "E0002", "2026-09-01T09:30:00");
    row(sheet, 3, "D-02", "E0003", "2026-09-02T10:00:00");
    JsonNode rows = convert(workbook);
    assertEquals(3, rows.size(), "every data row is read");
    assertEquals(List.of("deal_id", "employee_id", "over_wall_at"), names(rows.get(0)));
    assertEquals("D-01", rows.get(0).get("deal_id").asText());
    assertEquals("E0003", rows.get(2).get("employee_id").asText());
  }

  @Test void aSparseGroupHeaderStillGroupsItsDetailHeader() throws Exception {
    Workbook workbook = new XSSFWorkbook();
    Sheet sheet = workbook.createSheet("revenue");
    row(sheet, 0, null, "q1", null, "q2", null);
    sheet.addMergedRegion(new CellRangeAddress(0, 0, 1, 2));
    sheet.addMergedRegion(new CellRangeAddress(0, 0, 3, 4));
    row(sheet, 1, "region", "sales", "costs", "sales", "costs");
    Row data = sheet.createRow(2);
    data.createCell(0).setCellValue("north");
    for (int i = 1; i <= 4; i++) {
      data.createCell(i).setCellValue(1000.0 * i + 1);
    }
    JsonNode rows = convert(workbook);
    assertEquals(1, rows.size());
    assertEquals(List.of("region", "q1_sales", "q1_costs", "q2_sales", "q2_costs"), names(rows.get(0)));
  }
}
