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
package org.apache.calcite.adapter.file.etl;

import org.apache.calcite.adapter.file.storage.LocalFileStorageProvider;
import org.apache.calcite.adapter.file.storage.StorageProvider;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link FileSource}.
 */
@Tag("integration")
class FileSourceTest {

  @TempDir
  Path tempDir;

  private StorageProvider storageProvider;

  @BeforeEach
  void setUp() {
    storageProvider = new LocalFileStorageProvider();
  }

  @Test void testReadExcelFile() throws IOException {
    Path xlsxFile =
        createTestExcel("test.xlsx", "Sheet1", new String[]{"id", "name", "year"},
        new Object[][]{
            {1L, "Alice", 2020L},
            {2L, "Bob", 2021L},
            {3L, "Charlie", 2022L}
        });

    FileSourceConfig config = FileSourceConfig.builder()
        .path(xlsxFile.toString())
        .format("xlsx")
        .build();

    FileSource source = new FileSource(config, storageProvider);
    assertEquals("file", source.getType());

    Iterator<Map<String, Object>> iter =
        source.fetch(new HashMap<String, String>());

    List<Map<String, Object>> rows = toList(iter);
    assertEquals(3, rows.size());

    Map<String, Object> firstRow = rows.get(0);
    assertEquals(1L, firstRow.get("id"));
    assertEquals("Alice", firstRow.get("name"));
    assertEquals(2020L, firstRow.get("year"));
  }

  @Test void testReadExcelWithSheetName() throws IOException {
    Path xlsxFile = createTestExcelMultiSheet("multi.xlsx");

    FileSourceConfig config = FileSourceConfig.builder()
        .path(xlsxFile.toString())
        .format("xlsx")
        .sheet("Data")
        .build();

    FileSource source = new FileSource(config, storageProvider);
    Iterator<Map<String, Object>> iter =
        source.fetch(new HashMap<String, String>());

    List<Map<String, Object>> rows = toList(iter);
    assertEquals(2, rows.size());
  }

  @Test void testReadExcelMissingSheet() throws IOException {
    Path xlsxFile =
        createTestExcel("test.xlsx", "Sheet1", new String[]{"col1"}, new Object[][]{{1L}});

    FileSourceConfig config = FileSourceConfig.builder()
        .path(xlsxFile.toString())
        .format("xlsx")
        .sheet("NonExistentSheet")
        .build();

    FileSource source = new FileSource(config, storageProvider);
    assertThrows(IOException.class,
        () -> source.fetch(new HashMap<String, String>()));
  }

  @Test void testReadCsvFile() throws IOException {
    Path csvFile = tempDir.resolve("test.csv");
    Files.write(csvFile, "id,name,year\n1,Alice,2020\n2,Bob,2021\n".getBytes());

    FileSourceConfig config = FileSourceConfig.builder()
        .path(csvFile.toString())
        .format("csv")
        .build();

    FileSource source = new FileSource(config, storageProvider);
    Iterator<Map<String, Object>> iter =
        source.fetch(new HashMap<String, String>());

    List<Map<String, Object>> rows = toList(iter);
    assertEquals(2, rows.size());
  }

  @Test void testReadTsvFile() throws IOException {
    Path tsvFile = tempDir.resolve("test.tsv");
    Files.write(tsvFile, "id\tname\tyear\n1\tAlice\t2020\n2\tBob\t2021\n".getBytes());

    FileSourceConfig config = FileSourceConfig.builder()
        .path(tsvFile.toString())
        .format("tsv")
        .build();

    FileSource source = new FileSource(config, storageProvider);
    Iterator<Map<String, Object>> iter =
        source.fetch(new HashMap<String, String>());

    List<Map<String, Object>> rows = toList(iter);
    assertEquals(2, rows.size());
  }

  @Test void testReadJsonFile() throws IOException {
    Path jsonFile = tempDir.resolve("test.json");
    Files.write(jsonFile,
        "[{\"id\":1,\"name\":\"Alice\"},{\"id\":2,\"name\":\"Bob\"}]".getBytes());

    FileSourceConfig config = FileSourceConfig.builder()
        .path(jsonFile.toString())
        .format("json")
        .build();

    FileSource source = new FileSource(config, storageProvider);
    Iterator<Map<String, Object>> iter =
        source.fetch(new HashMap<String, String>());

    List<Map<String, Object>> rows = toList(iter);
    assertEquals(2, rows.size());
  }

  @Test void testAutoDetectFormatCsv() throws IOException {
    Path csvFile = tempDir.resolve("auto.csv");
    Files.write(csvFile, "col1,col2\na,b\n".getBytes());

    FileSourceConfig config = FileSourceConfig.builder()
        .path(csvFile.toString())
        .build();

    FileSource source = new FileSource(config, storageProvider);
    Iterator<Map<String, Object>> iter =
        source.fetch(new HashMap<String, String>());

    List<Map<String, Object>> rows = toList(iter);
    assertEquals(1, rows.size());
  }

  @Test void testAutoDetectFormatJson() throws IOException {
    Path jsonFile = tempDir.resolve("auto.json");
    Files.write(jsonFile, "[{\"x\":1}]".getBytes());

    FileSourceConfig config = FileSourceConfig.builder()
        .path(jsonFile.toString())
        .build();

    FileSource source = new FileSource(config, storageProvider);
    Iterator<Map<String, Object>> iter =
        source.fetch(new HashMap<String, String>());

    List<Map<String, Object>> rows = toList(iter);
    assertEquals(1, rows.size());
  }

  @Test void testAutoDetectFormatXlsx() throws IOException {
    Path xlsxFile =
        createTestExcel("auto.xlsx", "Sheet1", new String[]{"col1"}, new Object[][]{{1L}});

    FileSourceConfig config = FileSourceConfig.builder()
        .path(xlsxFile.toString())
        .build();

    FileSource source = new FileSource(config, storageProvider);
    Iterator<Map<String, Object>> iter =
        source.fetch(new HashMap<String, String>());

    List<Map<String, Object>> rows = toList(iter);
    assertEquals(1, rows.size());
  }

  @Test void testVariableSubstitutionInPath() throws IOException {
    Path csvFile = tempDir.resolve("data_2024.csv");
    Files.write(csvFile, "val\n42\n".getBytes());

    FileSourceConfig config = FileSourceConfig.builder()
        .path(tempDir.toString() + "/data_${year}.csv")
        .build();

    FileSource source = new FileSource(config, storageProvider);

    Map<String, String> variables = new HashMap<String, String>();
    variables.put("year", "2024");

    Iterator<Map<String, Object>> iter = source.fetch(variables);
    List<Map<String, Object>> rows = toList(iter);
    assertEquals(1, rows.size());
  }

  @Test void testUnsupportedFormat() {
    FileSourceConfig config = FileSourceConfig.builder()
        .path("/some/file.xyz")
        .format("xyz")
        .build();

    FileSource source = new FileSource(config, storageProvider);
    assertThrows(IOException.class,
        () -> source.fetch(new HashMap<String, String>()));
  }

  @Test void testReadExcelWithBooleanAndFormulaValues() throws IOException {
    Path xlsxFile = tempDir.resolve("types.xlsx");

    Workbook workbook = new XSSFWorkbook();
    Sheet sheet = workbook.createSheet("Sheet1");
    Row header = sheet.createRow(0);
    header.createCell(0).setCellValue("text");
    header.createCell(1).setCellValue("num");
    header.createCell(2).setCellValue("bool");

    Row dataRow = sheet.createRow(1);
    dataRow.createCell(0).setCellValue("hello");
    dataRow.createCell(1).setCellValue(42.5);
    dataRow.createCell(2).setCellValue(true);

    Row dataRow2 = sheet.createRow(2);
    // Empty cell (null)
    dataRow2.createCell(1).setCellValue(100.0);

    try (FileOutputStream fos = new FileOutputStream(xlsxFile.toFile())) {
      workbook.write(fos);
    }
    workbook.close();

    FileSourceConfig config = FileSourceConfig.builder()
        .path(xlsxFile.toString())
        .format("xlsx")
        .build();

    FileSource source = new FileSource(config, storageProvider);
    Iterator<Map<String, Object>> iter =
        source.fetch(new HashMap<String, String>());

    List<Map<String, Object>> rows = toList(iter);
    assertEquals(2, rows.size());

    Map<String, Object> row1 = rows.get(0);
    assertEquals("hello", row1.get("text"));
    assertEquals(42.5, row1.get("num"));
    assertEquals(true, row1.get("bool"));
  }

  @Test void testReadExcelEmptySheet() throws IOException {
    Path xlsxFile = tempDir.resolve("empty.xlsx");
    Workbook workbook = new XSSFWorkbook();
    workbook.createSheet("Empty");

    try (FileOutputStream fos = new FileOutputStream(xlsxFile.toFile())) {
      workbook.write(fos);
    }
    workbook.close();

    FileSourceConfig config = FileSourceConfig.builder()
        .path(xlsxFile.toString())
        .format("xlsx")
        .build();

    FileSource source = new FileSource(config, storageProvider);
    Iterator<Map<String, Object>> iter =
        source.fetch(new HashMap<String, String>());

    List<Map<String, Object>> rows = toList(iter);
    assertTrue(rows.isEmpty());
  }

  private Path createTestExcel(String fileName, String sheetName,
      String[] headers, Object[][] data) throws IOException {
    Path xlsxFile = tempDir.resolve(fileName);

    Workbook workbook = new XSSFWorkbook();
    Sheet sheet = workbook.createSheet(sheetName);

    Row headerRow = sheet.createRow(0);
    for (int i = 0; i < headers.length; i++) {
      headerRow.createCell(i).setCellValue(headers[i]);
    }

    for (int r = 0; r < data.length; r++) {
      Row row = sheet.createRow(r + 1);
      for (int c = 0; c < data[r].length; c++) {
        Cell cell = row.createCell(c);
        Object val = data[r][c];
        if (val instanceof Long) {
          cell.setCellValue(((Long) val).doubleValue());
        } else if (val instanceof Double) {
          cell.setCellValue((Double) val);
        } else if (val instanceof String) {
          cell.setCellValue((String) val);
        } else if (val instanceof Boolean) {
          cell.setCellValue((Boolean) val);
        }
      }
    }

    try (FileOutputStream fos = new FileOutputStream(xlsxFile.toFile())) {
      workbook.write(fos);
    }
    workbook.close();

    return xlsxFile;
  }

  private Path createTestExcelMultiSheet(String fileName) throws IOException {
    Path xlsxFile = tempDir.resolve(fileName);
    Workbook workbook = new XSSFWorkbook();

    // Sheet 1: Summary
    Sheet summary = workbook.createSheet("Summary");
    Row sHeader = summary.createRow(0);
    sHeader.createCell(0).setCellValue("total");
    Row sData = summary.createRow(1);
    sData.createCell(0).setCellValue(100);

    // Sheet 2: Data
    Sheet data = workbook.createSheet("Data");
    Row dHeader = data.createRow(0);
    dHeader.createCell(0).setCellValue("id");
    dHeader.createCell(1).setCellValue("value");
    Row dRow1 = data.createRow(1);
    dRow1.createCell(0).setCellValue(1);
    dRow1.createCell(1).setCellValue("alpha");
    Row dRow2 = data.createRow(2);
    dRow2.createCell(0).setCellValue(2);
    dRow2.createCell(1).setCellValue("beta");

    try (FileOutputStream fos = new FileOutputStream(xlsxFile.toFile())) {
      workbook.write(fos);
    }
    workbook.close();

    return xlsxFile;
  }

  private List<Map<String, Object>> toList(Iterator<Map<String, Object>> iter) {
    List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
    while (iter.hasNext()) {
      list.add(iter.next());
    }
    return list;
  }
}
