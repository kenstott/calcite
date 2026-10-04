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

import org.apache.calcite.adapter.file.partition.IncrementalTracker;
import org.apache.calcite.adapter.file.storage.LocalFileStorageProvider;
import org.apache.calcite.adapter.file.storage.StorageProvider;


import org.apache.calcite.adapter.file.partition.IncrementalTracker;
import org.apache.calcite.adapter.file.storage.LocalFileStorageProvider;
import org.apache.calcite.adapter.file.storage.StorageProvider;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A fetch unit the source answers with a {@code skipOn} status has no rows to contribute and is
 * not a failure, so it must not stop a replace-partitions run from committing — in the parallel
 * dispatch paths as well as the sequential one. A unit that fails for any other reason still
 * blocks the commit.
 */
@Tag("unit")
class ParallelSkipOnCommitTest {

  @TempDir
  File tempDir;

  /** Tracker that treats every combination as unprocessed and records nothing. */
  static final class InertTracker implements IncrementalTracker {
    @Override public boolean isProcessed(String an, String st, Map<String, String> kv) {
      return false;
    }

    @Override public boolean isProcessedWithTtl(String an, String st,
        Map<String, String> kv, long ttl) {
      return false;
    }

    @Override public void markProcessed(String an, String st,
        Map<String, String> kv, String tp) { }

    @Override public Set<Map<String, String>> getProcessedKeyValues(String an) {
      return Collections.emptySet();
    }

    @Override public void invalidate(String an, Map<String, String> kv) { }

    @Override public void invalidateAll(String an) { }

    @Override public Set<Integer> filterUnprocessed(String an, String st,
        List<Map<String, String>> combos) {
      Set<Integer> all = new HashSet<Integer>();
      for (int i = 0; i < combos.size(); i++) {
        all.add(i);
      }
      return all;
    }

    @Override public boolean isTableComplete(String p, String sig) { return false; }

    @Override public void markTableComplete(String p, String sig) { }

    @Override public void invalidateTableCompletion(String p) { }

    @Override public void clearAllCompletions() { }
  }

  /** Year-partitioned table fed by three state units, so the partition key omits the unit. */
  private static EtlPipelineConfig config(File warehouseDir) {
    Map<String, DimensionConfig> dims = new LinkedHashMap<String, DimensionConfig>();
    dims.put("state", DimensionConfig.builder().name("state").type(DimensionType.LIST)
        .values(Arrays.asList("AK", "AL", "AZ")).build());
    dims.put("year", DimensionConfig.builder().name("year").type(DimensionType.YEAR_RANGE)
        .start(2024).end(2024).build());
    List<ColumnConfig> columns = Arrays.asList(
        ColumnConfig.builder().name("id").type("INTEGER").build(),
        ColumnConfig.builder().name("year").type("INTEGER").build());
    return EtlPipelineConfig.builder()
        .name("parallel_skip_on_test")
        .source(HttpSourceConfig.builder().url("https://example.invalid/{state}")
            .parallel(2).build())
        .dimensions(dims)
        .materialize(MaterializeConfig.builder()
            .enabled(true)
            .format(MaterializeConfig.Format.ICEBERG)
            .name("parallel_skip_on_test")
            .targetTableId("parallel_skip_on_test")
            .partition(MaterializePartitionConfig.builder()
                .columns(Arrays.asList("year")).build())
            .output(MaterializeOutputConfig.builder().build())
            .columns(columns)
            .iceberg(MaterializeConfig.IcebergConfig.builder()
                .catalogType(MaterializeConfig.IcebergConfig.CatalogType.HADOOP)
                .warehousePath(warehouseDir.getAbsolutePath())
                .namespace("default")
                .overwritePartitions(true)
                .build())
            .build())
        .build();
  }

  /** Answers AK with the given exception and every other state with one row. */
  private static DataProvider alaskaThrows(final IOException alaska) {
    return new DataProvider() {
      @Override public Iterator<Map<String, Object>> fetch(
          EtlPipelineConfig config, Map<String, String> variables) throws IOException {
        if ("AK".equals(variables.get("state"))) {
          throw alaska;
        }
        Map<String, Object> row = new HashMap<String, Object>();
        row.put("id", variables.get("state").hashCode());
        row.put("year", 2024);
        List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
        rows.add(row);
        return rows.iterator();
      }
    };
  }

  private EtlResult run(IOException alaska) throws IOException {
    File warehouseDir = new File(tempDir, "warehouse");
    warehouseDir.mkdirs();
    StorageProvider sp = new LocalFileStorageProvider();
    return new EtlPipeline(config(warehouseDir), sp, warehouseDir.getAbsolutePath(), null,
        new InertTracker(), alaskaThrows(alaska), null).execute();
  }

  @Test void skippedUnitDoesNotBlockCommit() throws IOException {
    EtlResult result =
        run(new SkippedBatchException("HTTP 400 (skipped): https://example.invalid/AK"));
    assertFalse(result.isFailed(),
        "a skipOn unit has no rows to lose and must not stop the commit: " + result.getErrors());
  }

  @Test void failedUnitStillBlocksCommit() throws IOException {
    EtlResult result = run(new IOException("quickstats.nass.usda.gov"));
    assertTrue(result.isFailed(),
        "a unit that failed to fetch would drop its committed rows and must stop the commit");
  }
}
