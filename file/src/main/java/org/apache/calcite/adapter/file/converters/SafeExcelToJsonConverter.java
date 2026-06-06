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

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * Safe wrapper around ExcelToJsonConverter that prevents conflicts.
 */
public final class SafeExcelToJsonConverter {

  // Track which Excel files have been converted in this session
  private static final Set<String> CONVERTED_FILES = new HashSet<>();

  private SafeExcelToJsonConverter() {
    // Prevent instantiation
  }

  /**
   * Converts Excel file to JSON only if not already converted or if Excel file is newer.
   * This prevents repeated conversions and checks for file freshness.
   *
   * @param excelFile Excel file to convert
   * @param outputDir Directory where JSON files should be written
   * @param detectMultipleTables If true, uses enhanced converter to detect multiple tables
   * @param tableNameCasing The casing strategy for table names
   * @param columnNameCasing The casing strategy for column names
   */
  public static void convertIfNeeded(File excelFile, File outputDir, boolean detectMultipleTables,
      String tableNameCasing, String columnNameCasing, File baseDirectory)
      throws IOException {
    String canonicalPath = excelFile.getCanonicalPath();
    long excelLastModified = excelFile.lastModified();

    // Check if we need to convert
    boolean needsConversion = false;

    // Get expected JSON file names
    String fileName = excelFile.getName();
    String baseName = fileName.substring(0, fileName.lastIndexOf('.'));

    // Check if any converted JSON files exist in the output directory
    File[] existingJsonFiles = outputDir.listFiles((dir, name) ->
        name.startsWith(baseName + "__") && name.endsWith(".json"));

    if (existingJsonFiles == null || existingJsonFiles.length == 0) {
      // No JSON files exist, need to convert
      needsConversion = true;
    } else {
      // Check if Excel file is newer than existing JSON files
      for (File jsonFile : existingJsonFiles) {
        if (excelLastModified > jsonFile.lastModified()) {
          needsConversion = true;
          break;
        }
      }
    }

    // Only convert if needed and not already converted in this session
    if (needsConversion && !CONVERTED_FILES.contains(canonicalPath)) {
      if (detectMultipleTables) {
        MultiTableExcelToJsonConverter.convertFileToJson(excelFile, outputDir, true, tableNameCasing, columnNameCasing, baseDirectory);
      } else {
        ExcelToJsonConverter.convertFileToJson(excelFile, outputDir, tableNameCasing, columnNameCasing, baseDirectory);
      }
      CONVERTED_FILES.add(canonicalPath);
    }
  }

  /**
   * Clears the conversion cache. Useful for testing or when files need to be re-converted.
   */
  public static void clearCache() {
    CONVERTED_FILES.clear();
  }
}
