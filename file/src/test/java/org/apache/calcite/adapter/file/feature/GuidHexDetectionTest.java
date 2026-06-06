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

import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileOutputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Tag("unit")
public class GuidHexDetectionTest {

  @TempDir
  public File tempDir;

  @Test void testGuidHexDetection() throws Exception {
    // Create Excel file with headers followed by GUID/hex data
    File file = new File(tempDir, "guid_test.xlsx");
    try (Workbook workbook = new XSSFWorkbook();
         FileOutputStream fos = new FileOutputStream(file)) {

      Sheet sheet = workbook.createSheet("Sheet1");

      // Header row (should be detected as headers)
      Row header = sheet.createRow(0);
      header.createCell(0).setCellValue("id");
      header.createCell(1).setCellValue("Name");
      header.createCell(2).setCellValue("Token");

      // Data row with GUID (should NOT be detected as headers)
      Row data1 = sheet.createRow(1);
      data1.createCell(0).setCellValue("550e8400-e29b-41d4-a716-446655440000"); // GUID
      data1.createCell(1).setCellValue("John Smith"); // Regular name
      data1.createCell(2).setCellValue("0x1234ABCD"); // Hex string

      // Another data row
      Row data2 = sheet.createRow(2);
      data2.createCell(0).setCellValue("f47ac10b-58cc-4372-a567-0e02b2c3d479"); // Another GUID
      data2.createCell(1).setCellValue("Jane Doe");
      data2.createCell(2).setCellValue("#FF00FF"); // Color hex

      workbook.write(fos);
    }

    System.out.println("Testing GUID/hex detection...");
    MultiTableExcelToJsonConverter.convertFileToJson(file, tempDir, true, "SMART_CASING", "SMART_CASING", tempDir);

    // Should create exactly 1 JSON file (not treat GUID rows as separate tables)
    // Exclude metadata files that start with dots
    File[] jsonFiles = tempDir.listFiles((dir, name) -> name.endsWith(".json") && !name.startsWith("."));
    System.out.println("JSON files created: " + jsonFiles.length);
    for (File f : jsonFiles) {
      System.out.println("  " + f.getName());
    }

    assertEquals(1, jsonFiles.length, "Should create exactly 1 JSON file");
    System.out.println("✓ GUID/hex detection working correctly!");
  }
}
