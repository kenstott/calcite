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

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test showing what tables are extracted from multi-table Excel files.
 */
@Tag("unit")
public class MultiTableExtractorTest {
  @TempDir
  Path tempDir;

  @Test public void testExtractTablesFromComplexExcel() throws Exception {
    // Copy the lots_of_tables.xlsx to temp directory
    File targetFile = new File(tempDir.toFile(), "lots_of_tables.xlsx");
    try (InputStream in = getClass().getResourceAsStream("/lots_of_tables.xlsx")) {
      if (in == null) {
        System.out.println("Test file /lots_of_tables.xlsx not found, skipping test");
        return;
      }
      Files.copy(in, targetFile.toPath());
    }

    // Convert with multi-table detection
    System.out.println("Converting " + targetFile.getName() + " with multi-table detection...");
    MultiTableExcelToJsonConverter.convertFileToJson(targetFile, tempDir.toFile(), true, "SMART_CASING", "SMART_CASING", tempDir.toFile());

    // List all JSON files created
    File[] jsonFiles = tempDir.toFile().listFiles((dir, name) -> name.endsWith(".json"));

    System.out.println("\nTables extracted from lots_of_tables.xlsx:");
    if (jsonFiles != null) {
      for (File jsonFile : jsonFiles) {
        System.out.println("  - " + jsonFile.getName());

        // Show first few lines of content
        String content = Files.readString(jsonFile.toPath());
        String[] lines = content.split("\n");
        System.out.println("    Content preview:");
        for (int i = 0; i < Math.min(3, lines.length); i++) {
          System.out.println("      " + lines[i]);
        }
      }

      System.out.println("\nTotal tables found: " + jsonFiles.length);
      assertTrue(jsonFiles.length > 0, "Should extract at least one table");
    } else {
      System.out.println("No JSON files created");
    }
  }
}
