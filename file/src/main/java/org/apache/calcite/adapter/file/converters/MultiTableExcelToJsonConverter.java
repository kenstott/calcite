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
package org.apache.calcite.adapter.file.converters;
// storage-provider-guard:ignore-file - audited: all filesystem operations here target genuinely-local paths (temp / local cache / spill / local config), not object-store URIs.

import org.apache.calcite.adapter.file.cache.SourceFileLockManager;
import org.apache.calcite.adapter.file.util.LocalPaths;
import org.apache.calcite.adapter.file.util.SmartCasing;
import org.apache.calcite.util.trace.CalciteLogger;

import org.apache.poi.ss.usermodel.FormulaEvaluator;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Excel to JSON converter that finds the tables of each sheet ({@link ExcelTableFinder}) and
 * writes each as a JSON file, named {@code <file>__<sheet>}, after its title when it has one.
 * The workbook's own tables (Excel tables and named ranges) and the ranges an operator gives
 * are read as declared; the rest of each sheet is searched for blocks of cells, as the
 * operator's {@link ExcelReadHints} say.
 */
public final class MultiTableExcelToJsonConverter {
  private static final CalciteLogger LOGGER =
      new CalciteLogger(LoggerFactory.getLogger(MultiTableExcelToJsonConverter.class));

  private MultiTableExcelToJsonConverter() {
    // Prevent instantiation
  }

  /**
   * Converts an Excel file to JSON with multi-table detection and custom casing.
   * Outputs files to the specified output directory.
   */
  public static void convertFileToJson(File inputFile, File outputDir, boolean detectMultipleTables,
      String tableNameCasing, String columnNameCasing, File baseDirectory)
      throws IOException {
    convertFileToJson(inputFile, outputDir, detectMultipleTables, tableNameCasing, columnNameCasing, baseDirectory, null);
  }

  /**
   * Converts an Excel file to JSON with multi-table detection and custom casing.
   * Outputs files to the specified output directory, preserving directory structure if relativePath is provided.
   */
  public static void convertFileToJson(File inputFile, File outputDir, boolean detectMultipleTables,
      String tableNameCasing, String columnNameCasing, File baseDirectory, String relativePath)
      throws IOException {
    convertFileToJson(inputFile, outputDir, detectMultipleTables, tableNameCasing,
        columnNameCasing, baseDirectory, relativePath, ExcelReadHints.AUTO);
  }

  /**
   * Converts an Excel file to JSON with multi-table detection and custom casing, finding its tables
   * as the operator's {@code hints} say (by the reader's own rules where they say nothing).
   */
  public static void convertFileToJson(File inputFile, File outputDir, boolean detectMultipleTables,
      String tableNameCasing, String columnNameCasing, File baseDirectory, String relativePath,
      ExcelReadHints hints)
      throws IOException {
    if (!detectMultipleTables) {
      // Fall back to standard conversion with relativePath
      ExcelToJsonConverter.convertFileToJson(inputFile, outputDir, tableNameCasing, columnNameCasing, baseDirectory, relativePath);
      return;
    }

    LOGGER.debug("Converting file with multi-table detection: " + inputFile.getName());

    // Acquire read lock on source file
    SourceFileLockManager.LockHandle lockHandle = null;
    try {
      lockHandle = SourceFileLockManager.acquireReadLock(inputFile);
      LOGGER.debug("Acquired read lock on Excel file: " + inputFile.getPath());
    } catch (IOException e) {
      LOGGER.warn("Could not acquire lock on file: "
          + inputFile.getPath()
          + " - proceeding without lock");
      // Continue without lock
    }

    try (FileInputStream file = new FileInputStream(inputFile)) {
      Workbook workbook = WorkbookFactory.create(file);
      ObjectMapper mapper = new ObjectMapper();
      // Date-formatted cells are written as java.util.Date via putPOJO below; without this,
      // Jackson's default WRITE_DATES_AS_TIMESTAMPS serializes them as raw epoch millis,
      // which downstream type inference then reads back as a meaningless BIGINT column.
      mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
      FormulaEvaluator evaluator = workbook.getCreationHelper().createFormulaEvaluator();
      String fileName = inputFile.getName();
      String rawBaseName = fileName.substring(0, fileName.lastIndexOf('.'));

      // If relativePath is provided, include directory structure in the base name
      String dirPrefix =
          relativePath == null ? null : LocalPaths.directoryPrefix(relativePath);
      if (dirPrefix != null) {
        rawBaseName = dirPrefix + "_" + rawBaseName;
      }

      String baseName = SmartCasing.applyCasing(rawBaseName, tableNameCasing);

      Map<String, ExcelRange> ranges = hints.ranges(baseName);
      for (int i = 0; i < workbook.getNumberOfSheets(); i++) {
        Sheet sheet = workbook.getSheetAt(i);
        String sheetTable =
            baseName + "__" + SmartCasing.applyCasing(sheet.getSheetName(), tableNameCasing);
        ExcelTableFinder finder =
            new ExcelTableFinder(sheet, evaluator, mapper, tableNameCasing, columnNameCasing,
                hints, fileName, sheetTable);
        List<ExcelTableFinder.Found> tables = finder.find(ranges);
        if (tables.isEmpty()) {
          LOGGER.debug("No tables found in sheet: " + sheet.getSheetName());
          continue;
        }
        LOGGER.debug("Found " + tables.size() + " tables in sheet: " + sheet.getSheetName());

        // Each table's name: the one the operator or the workbook gives it, else the sheet's,
        // after its title when it has one; tables that would share a name are told apart
        // _t1, _t2, ... in the order they stand in the sheet.
        List<String> planned = new ArrayList<>();
        Map<String, Integer> counts = new HashMap<>();
        for (ExcelTableFinder.Found table : tables) {
          String name = table.name != null ? table.name : finder.nameFor(table.title);
          planned.add(name);
          counts.merge(name, 1, Integer::sum);
        }
        int conflictIndex = 1;
        for (int t = 0; t < tables.size(); t++) {
          String name = planned.get(t);
          String jsonFileName;
          if (counts.get(name) > 1) {
            jsonFileName = name + "_t" + conflictIndex + ".json";
            conflictIndex++;
          } else {
            jsonFileName = name + ".json";
          }
          LOGGER.trace("Writing JSON file: " + jsonFileName);
          File jsonFile = new File(outputDir, jsonFileName);
          try (OutputStreamWriter fileWriter =
              new OutputStreamWriter(new FileOutputStream(jsonFile), StandardCharsets.UTF_8)) {
            mapper.writerWithDefaultPrettyPrinter().writeValue(fileWriter, tables.get(t).rows);
          }

          // Record the conversion for refresh tracking
          ConversionRecorder.recordExcelConversion(inputFile, jsonFile, baseDirectory, hints);
        }
      }
      workbook.close();
    } finally {
      // Release the lock
      if (lockHandle != null) {
        lockHandle.close();
        LOGGER.debug("Released read lock on Excel file");
      }
    }
  }
}
