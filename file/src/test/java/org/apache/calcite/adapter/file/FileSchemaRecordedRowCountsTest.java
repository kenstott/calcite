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

import org.apache.calcite.adapter.file.execution.ExecutionEngineConfig;
import org.apache.calcite.adapter.file.metadata.ConversionMetadata;
import org.apache.calcite.schema.SchemaPlus;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * {@link FileSchema#getRecordedRowCounts} is what a catalog listing reports as each relation's
 * cardinality. It must answer from what is already recorded and never resolve a table: the
 * declarations here name tables that exist nowhere, so any attempt to load one would fail.
 */
@Tag("unit")
class FileSchemaRecordedRowCountsTest {

  @TempDir Path tempDir;

  private static Map<String, Object> table(String name, Long coverageRowCount) {
    Map<String, Object> def = new LinkedHashMap<>();
    def.put("name", name);
    if (coverageRowCount != null) {
      Map<String, Object> coverage = new LinkedHashMap<>();
      coverage.put("rowCount", coverageRowCount);
      def.put("observedCoverage", coverage);
    }
    return def;
  }

  private FileSchema schema(List<Map<String, Object>> tables) {
    SchemaPlus parent = mock(SchemaPlus.class);
    when(parent.getName()).thenReturn("root");
    return new FileSchema(parent, "recorded_row_counts", tempDir.toFile(), tables,
        new ExecutionEngineConfig());
  }

  @Test void declaredCoverageCountIsReported() {
    List<Map<String, Object>> tables = new ArrayList<>();
    tables.add(table("covered", Long.valueOf(639682L)));

    assertEquals(Long.valueOf(639682L), schema(tables).getRecordedRowCounts().get("covered"));
  }

  @Test void countReadFromIcebergMetadataWinsOverDeclaredCoverage() {
    List<Map<String, Object>> tables = new ArrayList<>();
    tables.add(table("tracked", Long.valueOf(10L)));
    FileSchema schema = schema(tables);
    ConversionMetadata tracker = schema.getConversionMetadata();
    assertNotNull(tracker);
    tracker.updateMaterializationInfo("tracked", "s3://bucket/recorded/tracked", "ICEBERG_PARQUET",
        Long.valueOf(42L));

    assertEquals(Long.valueOf(42L), schema.getRecordedRowCounts().get("tracked"));
  }

  @Test void viewIsZeroAndTableWithNothingRecordedIsAbsent() {
    List<Map<String, Object>> tables = new ArrayList<>();
    Map<String, Object> view = table("a_view", null);
    view.put("type", "view");
    view.put("sql", "select 1");
    tables.add(view);
    tables.add(table("unrecorded", null));
    Map<String, Long> counts = schema(tables).getRecordedRowCounts();

    assertEquals(Long.valueOf(0L), counts.get("a_view"));
    assertFalse(counts.containsKey("unrecorded"));
  }
}
