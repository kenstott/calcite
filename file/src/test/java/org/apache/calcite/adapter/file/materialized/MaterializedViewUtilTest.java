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
package org.apache.calcite.adapter.file.materialized;

import org.apache.calcite.adapter.file.execution.ExecutionEngineConfig;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link MaterializedViewUtil}.
 */
@Tag("unit")
public class MaterializedViewUtilTest {

  @Test public void testGetFileExtensionParquet() {
    assertEquals("parquet", MaterializedViewUtil.getFileExtension("PARQUET"));
  }

  @Test public void testGetFileExtensionParquetLowerCase() {
    assertEquals("parquet", MaterializedViewUtil.getFileExtension("parquet"));
  }

  @Test public void testGetFileExtensionArrow() {
    assertEquals("arrow", MaterializedViewUtil.getFileExtension("ARROW"));
  }

  @Test public void testGetFileExtensionArrowLowerCase() {
    assertEquals("arrow", MaterializedViewUtil.getFileExtension("arrow"));
  }

  @Test public void testGetFileExtensionVectorized() {
    assertEquals("csv", MaterializedViewUtil.getFileExtension("VECTORIZED"));
  }

  @Test public void testGetFileExtensionLinq4j() {
    assertEquals("csv", MaterializedViewUtil.getFileExtension("LINQ4J"));
  }

  @Test public void testGetFileExtensionLinq4jLowerCase() {
    assertEquals("csv", MaterializedViewUtil.getFileExtension("linq4j"));
  }

  @Test public void testGetFileExtensionNull() {
    assertEquals("csv", MaterializedViewUtil.getFileExtension(null));
  }

  @Test public void testGetFileExtensionUnknown() {
    assertEquals("csv", MaterializedViewUtil.getFileExtension("UNKNOWN"));
  }

  @Test public void testGetFileExtensionDuckdb() {
    // DuckDB is not explicitly handled, should fall through to default (csv)
    assertEquals("csv", MaterializedViewUtil.getFileExtension("DUCKDB"));
  }

  @Test public void testGetFileExtensionMixedCase() {
    assertEquals("parquet", MaterializedViewUtil.getFileExtension("Parquet"));
    assertEquals("arrow", MaterializedViewUtil.getFileExtension("Arrow"));
  }

  @Test public void testGetMaterializedViewFilename() {
    ExecutionEngineConfig config = new ExecutionEngineConfig();
    String filename = MaterializedViewUtil.getMaterializedViewFilename("my_view", config);
    // Default engine is DUCKDB; extension determined by getFileExtension
    assertTrue(filename.startsWith("my_view."));
    assertTrue(
        filename.endsWith(
            "." + MaterializedViewUtil.getFileExtension(
        config.getEngineType().name())));
  }

  @Test public void testGetMaterializedViewFilenameContainsTableName() {
    ExecutionEngineConfig config = new ExecutionEngineConfig();
    String filename = MaterializedViewUtil.getMaterializedViewFilename("sales_summary", config);
    assertTrue(filename.startsWith("sales_summary."));
  }

  @Test public void testIsMaterializedViewFileParquet() {
    assertTrue(MaterializedViewUtil.isMaterializedViewFile("view.parquet", "PARQUET"));
    assertFalse(MaterializedViewUtil.isMaterializedViewFile("view.csv", "PARQUET"));
  }

  @Test public void testIsMaterializedViewFileArrow() {
    assertTrue(MaterializedViewUtil.isMaterializedViewFile("view.arrow", "ARROW"));
    assertFalse(MaterializedViewUtil.isMaterializedViewFile("view.parquet", "ARROW"));
  }

  @Test public void testIsMaterializedViewFileCsv() {
    assertTrue(MaterializedViewUtil.isMaterializedViewFile("view.csv", "LINQ4J"));
    assertTrue(MaterializedViewUtil.isMaterializedViewFile("view.csv", "VECTORIZED"));
    assertFalse(MaterializedViewUtil.isMaterializedViewFile("view.parquet", "LINQ4J"));
  }

  @Test public void testIsMaterializedViewFileNullEngine() {
    // Null engine type defaults to csv
    assertTrue(MaterializedViewUtil.isMaterializedViewFile("view.csv", null));
    assertFalse(MaterializedViewUtil.isMaterializedViewFile("view.parquet", null));
  }

  @Test public void testIsMaterializedViewFileWithPath() {
    assertTrue(
        MaterializedViewUtil.isMaterializedViewFile(
        "/path/to/view.parquet", "PARQUET"));
    assertFalse(
        MaterializedViewUtil.isMaterializedViewFile(
        "/path/to/view.csv", "PARQUET"));
  }

  @Test public void testIsMaterializedViewFileEmptyFilename() {
    assertFalse(MaterializedViewUtil.isMaterializedViewFile("", "PARQUET"));
  }

  @Test public void testIsMaterializedViewFileNoExtension() {
    assertFalse(MaterializedViewUtil.isMaterializedViewFile("viewfile", "PARQUET"));
  }
}
