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
import org.apache.calcite.adapter.file.materialized.MaterializedViewUtil;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Test for MaterializedViewUtil.
 */
@Tag("unit")
public class MaterializedViewUtilTest {

  @Test public void testEngineSpecificExtensions() {
    // Test default CSV extension
    assertThat(MaterializedViewUtil.getFileExtension("LINQ4J"), is("csv"));
    assertThat(MaterializedViewUtil.getFileExtension("VECTORIZED"), is("csv"));
    assertThat(MaterializedViewUtil.getFileExtension(null), is("csv"));

    // Test Parquet extension
    assertThat(MaterializedViewUtil.getFileExtension("PARQUET"), is("parquet"));
    assertThat(MaterializedViewUtil.getFileExtension("parquet"), is("parquet"));

    // Test Arrow extension
    assertThat(MaterializedViewUtil.getFileExtension("ARROW"), is("arrow"));
    assertThat(MaterializedViewUtil.getFileExtension("arrow"), is("arrow"));
  }

  @Test public void testMaterializedViewFilename() {
    ExecutionEngineConfig csvConfig = new ExecutionEngineConfig("LINQ4J", 1000);
    assertThat(MaterializedViewUtil.getMaterializedViewFilename("sales_summary", csvConfig),
        is("sales_summary.csv"));

    ExecutionEngineConfig parquetConfig = new ExecutionEngineConfig("PARQUET", 10000);
    assertThat(MaterializedViewUtil.getMaterializedViewFilename("sales_summary", parquetConfig),
        is("sales_summary.parquet"));

    ExecutionEngineConfig arrowConfig = new ExecutionEngineConfig("ARROW", 5000);
    assertThat(MaterializedViewUtil.getMaterializedViewFilename("product_stats", arrowConfig),
        is("product_stats.arrow"));
  }

  @Test public void testIsMaterializedViewFile() {
    // CSV files for LINQ4J and VECTORIZED
    assertThat(MaterializedViewUtil.isMaterializedViewFile("sales.csv", "LINQ4J"), is(true));
    assertThat(MaterializedViewUtil.isMaterializedViewFile("sales.csv", "VECTORIZED"), is(true));
    assertThat(MaterializedViewUtil.isMaterializedViewFile("sales.parquet", "LINQ4J"), is(false));

    // Parquet files for PARQUET engine
    assertThat(MaterializedViewUtil.isMaterializedViewFile("sales.parquet", "PARQUET"), is(true));
    assertThat(MaterializedViewUtil.isMaterializedViewFile("sales.csv", "PARQUET"), is(false));

    // Arrow files for ARROW engine
    assertThat(MaterializedViewUtil.isMaterializedViewFile("sales.arrow", "ARROW"), is(true));
    assertThat(MaterializedViewUtil.isMaterializedViewFile("sales.csv", "ARROW"), is(false));
  }
}
