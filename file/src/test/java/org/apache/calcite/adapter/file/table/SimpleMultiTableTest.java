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
package org.apache.calcite.adapter.file;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Simple test to show what tables are extracted from Excel files.
 */
@Tag("unit")
public class SimpleMultiTableTest extends BaseFileTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(SimpleMultiTableTest.class);

  private Path tempDir;

  @BeforeEach
  void setUp() throws Exception {
    tempDir = Files.createTempDirectory("simple-multi-table-test");
  }

  @AfterEach
  void tearDown() {
    if (tempDir != null) {
      deleteRecursive(tempDir.toFile());
    }
  }

  @Test public void testShowExtractedTables() throws Exception {
    // Copy test file
    File targetFile = new File(tempDir.toFile(), "lots_of_tables.xlsx");
    try (InputStream in = getClass().getResourceAsStream("/lots_of_tables.xlsx")) {
      if (in == null) {
        LOGGER.debug("Test file /lots_of_tables.xlsx not found, using dummy");
        targetFile = new File(tempDir.toFile(), "dummy_multi_table.xlsx");
        Files.writeString(targetFile.toPath(), "dummy");
        return;
      }
      Files.copy(in, targetFile.toPath());
    }

    // Use FileSchema to see what tables it creates
    try {
      FileSchema schema =
          new FileSchema(null, "TEST", tempDir.toFile(), tempDir.toFile(), null, null,
              getEngineConfig(), false, null, null, null, null,
              "SMART_CASING", "SMART_CASING", null, null, null, null, true);

      for (String tableName : schema.getTableMap().keySet()) {
        if (tableName.startsWith("lots_of_tables") || tableName.startsWith("dummy")) {
          LOGGER.debug("Extracted table: {}", tableName);
        }
      }
    } catch (Exception e) {
      LOGGER.debug("Error during table extraction: {}", e.getMessage());
    }
  }

  private static void deleteRecursive(File file) {
    if (file.isDirectory()) {
      File[] children = file.listFiles();
      if (children != null) {
        for (File child : children) {
          deleteRecursive(child);
        }
      }
    }
    file.delete();
  }
}
