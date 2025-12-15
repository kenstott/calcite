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
package org.apache.calcite.adapter.file.iceberg;

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.types.Types;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Tests for IcebergTableWriter.
 */
@Tag("unit")
public class IcebergTableWriterTest {

  @TempDir
  Path tempDir;

  private Map<String, Object> catalogConfig;
  private Table table;

  @BeforeEach
  void setUp() {
    String warehousePath = tempDir.resolve("warehouse").toString();
    catalogConfig = new HashMap<>();
    catalogConfig.put("catalogType", "hadoop");
    catalogConfig.put("warehousePath", warehousePath);

    // Create a test table
    Schema schema = new Schema(
        Types.NestedField.optional(1, "id", Types.IntegerType.get()),
        Types.NestedField.optional(2, "data", Types.StringType.get()),
        Types.NestedField.optional(3, "year", Types.IntegerType.get())
    );

    PartitionSpec spec = PartitionSpec.builderFor(schema)
        .identity("year")
        .build();

    table = IcebergCatalogManager.createTable(catalogConfig, "test_table", schema, spec);
  }

  @AfterEach
  void tearDown() {
    IcebergCatalogManager.clearCache();
  }

  @Test
  void testWriterCreation() {
    IcebergTableWriter writer = new IcebergTableWriter(table);
    assertNotNull(writer);
    assertNotNull(writer.getTable());
  }

  @Test
  void testMaintenanceDoesNotThrow() {
    IcebergTableWriter writer = new IcebergTableWriter(table);
    // Should not throw even on empty table
    writer.runMaintenance(7, 1);
  }

  @Test
  void testCommitFromStagingEmptyDirectory() throws Exception {
    IcebergTableWriter writer = new IcebergTableWriter(table);

    // Create an empty staging directory
    Path stagingPath = tempDir.resolve("staging");
    Files.createDirectories(stagingPath);

    // Should not throw, just log warning
    writer.commitFromStaging(stagingPath, null);
  }
}
