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
package org.apache.calcite.adapter.file;

import org.apache.calcite.adapter.file.converters.MultiTableExcelToJsonConverter;

import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test single table with identifier to verify no _T1 suffix.
 */
@Tag("unit")
public class SingleTableTest {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(SingleTableTest.class);
  @TempDir
  Path tempDir;

  @Test public void testSingleTableWithIdentifier() throws Exception {
    File excelFile = new File(tempDir.toFile(), "single_table.xlsx");

    try (Workbook workbook = new XSSFWorkbook()) {
      Sheet sheet = workbook.createSheet("Data");

      // Table with identifier
      Row identifierRow = sheet.createRow(0);
      identifierRow.createCell(0).setCellValue("Products");

      // Explicit empty row so converter doesn't hit null-row gap limit
      sheet.createRow(1);

      // Headers at row 2
      Row headerRow = sheet.createRow(2);
      headerRow.createCell(0).setCellValue("ProductName");
      headerRow.createCell(1).setCellValue("Category");
      headerRow.createCell(2).setCellValue("Price");

      Row data1 = sheet.createRow(3);
      data1.createCell(0).setCellValue("Widget");
      data1.createCell(1).setCellValue("Electronics");
      data1.createCell(2).setCellValue(19.99);

      Row data2 = sheet.createRow(4);
      data2.createCell(0).setCellValue("Gadget");
      data2.createCell(1).setCellValue("Tools");
      data2.createCell(2).setCellValue(29.99);

      try (FileOutputStream out = new FileOutputStream(excelFile)) {
        workbook.write(out);
      }
    }

    MultiTableExcelToJsonConverter.convertFileToJson(
        excelFile, tempDir.toFile(), true,
        "SMART_CASING", "SMART_CASING", tempDir.toFile());

    // Filter to data JSON files (exclude metadata like .conversions.json, model.json)
    File[] jsonFiles =
        tempDir.toFile().listFiles((dir, name) ->
            name.endsWith(".json") && !name.startsWith(".") && !name.equals("model.json"));
    assertNotNull(jsonFiles, "JSON files array should not be null");
    assertEquals(1, jsonFiles.length,
        "Should produce exactly 1 data JSON file");
    assertFalse(jsonFiles[0].getName().contains("_T1"),
        "Single table should not have _T1 suffix, got: "
            + jsonFiles[0].getName());
    assertTrue(jsonFiles[0].getName().endsWith(".json"),
        "Output file should have .json extension");
    LOGGER.debug("Produced JSON file: {}", jsonFiles[0].getName());
  }
}
