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

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A period whose fetch throws is a failed period, not a skipped one. Found live on
 * {@code fiscal.exempt_org_990}: two of three periods threw (a row-count invariant and a missing
 * index column), yet the run ended "1 successful, 0 failed, 2 skipped", the schema reported
 * "1 ok, 0 failed" and force-reprocess.sh exited 0. Skipping stays reserved for the periods the
 * tracker or a skipOn status legitimately excludes.
 */
@Tag("unit")
class EtlPipelineFailedPeriodTest {

  @TempDir
  File tempDir;

  private static EtlPipelineConfig config(File warehouseDir) {
    List<ColumnConfig> columns = Arrays.asList(
        ColumnConfig.builder().name("id").type("INTEGER").build(),
        ColumnConfig.builder().name("year").type("INTEGER").build());
    Map<String, DimensionConfig> dims = new LinkedHashMap<String, DimensionConfig>();
    dims.put("year", DimensionConfig.builder()
        .name("year")
        .type(DimensionType.YEAR_RANGE)
        .start(2023)
        .end(2025)
        .build());
    return EtlPipelineConfig.builder()
        .name("failed_period_test")
        .source(HttpSourceConfig.builder().url("https://example.invalid/api").build())
        .dimensions(dims)
        .materialize(MaterializeConfig.builder()
            .enabled(true)
            .format(MaterializeConfig.Format.ICEBERG)
            .name("failed_period_test")
            .targetTableId("failed_period_test")
            .output(MaterializeOutputConfig.builder().build())
            .columns(columns)
            .partition(MaterializePartitionConfig.builder()
                .columns(Arrays.asList("year"))
                .build())
            .iceberg(MaterializeConfig.IcebergConfig.builder()
                .catalogType(MaterializeConfig.IcebergConfig.CatalogType.HADOOP)
                .warehousePath(warehouseDir.getAbsolutePath())
                .namespace("default")
                .overwritePartitions(true)
                .build())
            .build())
        .build();
  }

  /** One row per period, except 2024 which throws {@code failure}. */
  private static DataProvider secondPeriodThrows(final RuntimeException failure) {
    return new DataProvider() {
      @Override public Iterator<Map<String, Object>> fetch(
          EtlPipelineConfig config, Map<String, String> variables) {
        int year = Integer.parseInt(variables.get("year"));
        if (year == 2024) {
          throw failure;
        }
        Map<String, Object> row = new HashMap<String, Object>();
        row.put("id", year);
        row.put("year", year);
        List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
        rows.add(row);
        return rows.iterator();
      }
    };
  }

  private EtlResult run(RuntimeException failure) throws IOException {
    File warehouseDir = new File(tempDir, "warehouse");
    warehouseDir.mkdirs();
    StorageProvider sp = new LocalFileStorageProvider();
    EtlPipelineCommitFailureTrackerTest.RecordingTracker tracker =
        new EtlPipelineCommitFailureTrackerTest.RecordingTracker();
    return new EtlPipeline(config(warehouseDir), sp, warehouseDir.getAbsolutePath(), null,
        tracker, secondPeriodThrows(failure), null).execute();
  }

  @Test void periodThatThrowsIsFailedNotSkipped() throws IOException {
    EtlResult result = run(new IllegalStateException(
        "batch 2024_TEOS_XML_05A yielded 81770 rows but the index lists 163540 filings"));

    assertEquals(1, result.getFailedBatches(), "the throwing period must count as failed");
    assertEquals(0, result.getSkippedBatches(), "a throw is not a legitimate skip");
    assertEquals(2, result.getSuccessfulBatches(), "the other periods still complete");
    assertEquals(2, result.getTotalRows(), "completed periods stay committed");
    assertTrue(result.isFailed(), "a table with a failed period is a failed table");
    assertTrue(result.isPartiallyFailed(), "...that still has committed data");
    assertFalse(result.isCompleteSuccess());
    assertTrue(result.getFailureMessage().contains("year=2024"),
        "the failure must name the failed period: " + result.getFailureMessage());
    assertTrue(result.getFailureMessage().contains("yielded 81770 rows"),
        result.getFailureMessage());
  }

  @Test void failedPeriodMakesSchemaResultFailedAndIsRecordedForTheExitCode()
      throws IOException {
    EtlResult result = run(new java.io.UncheckedIOException("Expected column 'XML_BATCH_ID' not found",
        new IOException("header")));

    SchemaResult schema = SchemaResult.builder().schemaName("fiscal")
        .addTableResult("failed_period_test", result).build();
    assertEquals(1, schema.getFailedTables(), "the schema must not report the table ok");
    assertEquals(0, schema.getSuccessfulTables());
    assertTrue(schema.hasErrors());

    EtlFailureLedger.enable();
    EtlFailureLedger.record("fiscal", "failed_period_test", result);
    List<String> failures = EtlFailureLedger.drain();
    assertEquals(1, failures.size(), "EtlRunner exits non-zero when the ledger is non-empty");
    assertTrue(failures.get(0).contains("year=2024"), failures.get(0));
    assertTrue(EtlFailureLedger.drain().isEmpty(), "drain clears the ledger");
  }

  /** The legitimate skip is unchanged: an HTTP 404 is still skipped and does not fail the table. */
  @Test void notFoundPeriodStaysSkipped() throws IOException {
    EtlResult result = run(new RuntimeException("HTTP 404 from https://example.invalid/api"));

    assertEquals(0, result.getFailedBatches());
    assertEquals(1, result.getSkippedBatches());
    assertFalse(result.isFailed(), "a 404-skipped period must not fail the table");
  }
}
