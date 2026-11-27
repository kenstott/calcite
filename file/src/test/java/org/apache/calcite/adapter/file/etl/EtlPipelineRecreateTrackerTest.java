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

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Table;
import org.apache.iceberg.hadoop.HadoopTables;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * A run that drops and recreates a table must refill it: the tracker's combo marks, period markers
 * and freshness tokens describe the discarded table, so honouring them leaves every unit whose
 * source is unchanged permanently missing.
 */
@Tag("unit")
class EtlPipelineRecreateTrackerTest {

  @TempDir
  File tempDir;

  private static final String NAME = "recreate_test";

  /** In-memory tracker that persists combo marks, period markers and freshness tokens. */
  static final class MemoryTracker implements IncrementalTracker {
    final Set<Map<String, String>> processed = ConcurrentHashMap.newKeySet();
    final Set<String> periods = ConcurrentHashMap.newKeySet();
    final Map<String, String> tokens = new ConcurrentHashMap<String, String>();

    @Override public boolean isProcessed(String an, String st, Map<String, String> kv) {
      return processed.contains(kv);
    }

    @Override public boolean isProcessedWithTtl(String an, String st,
        Map<String, String> kv, long ttl) {
      return processed.contains(kv);
    }

    @Override public void markProcessed(String an, String st,
        Map<String, String> kv, String tp) {
      processed.add(new LinkedHashMap<String, String>(kv));
    }

    @Override public Set<Map<String, String>> getProcessedKeyValues(String an) {
      return new HashSet<Map<String, String>>(processed);
    }

    @Override public void invalidate(String an, Map<String, String> kv) {
      processed.remove(kv);
    }

    @Override public void invalidateAll(String an) {
      processed.clear();
    }

    @Override public void invalidatePipeline(String p) {
      processed.clear();
      periods.clear();
      tokens.clear();
    }

    @Override public Set<Integer> filterUnprocessed(String an, String st,
        List<Map<String, String>> combos) {
      Set<Integer> out = new HashSet<Integer>();
      for (int i = 0; i < combos.size(); i++) {
        if (!processed.contains(combos.get(i))) {
          out.add(i);
        }
      }
      return out;
    }

    @Override public boolean isPeriodComplete(String p, Map<String, String> v) {
      return periods.contains(IncrementalTracker.periodCompletionKey(p, v));
    }

    @Override public void markPeriodComplete(String p, Map<String, String> v) {
      periods.add(IncrementalTracker.periodCompletionKey(p, v));
    }

    @Override public String getFreshnessToken(String p) {
      return tokens.get(p);
    }

    @Override public void putFreshnessToken(String p, String token) {
      tokens.put(p, token);
    }

    @Override public boolean isTableComplete(String p, String sig) {
      return false;
    }

    @Override public void markTableComplete(String p, String sig) { }

    @Override public void invalidateTableCompletion(String p) { }

    @Override public void clearAllCompletions() { }
  }

  private static EtlPipelineConfig config(File warehouseDir, int startYear, int endYear,
      List<String> partitionColumns) {
    Map<String, DimensionConfig> dims = new LinkedHashMap<String, DimensionConfig>();
    dims.put("year", DimensionConfig.builder()
        .name("year")
        .type(DimensionType.YEAR_RANGE)
        .start(startYear)
        .end(endYear)
        .build());
    List<ColumnConfig> columns = Arrays.asList(
        ColumnConfig.builder().name("id").type("INTEGER").build(),
        ColumnConfig.builder().name("year").type("INTEGER").build());
    Map<String, Object> freshness = new HashMap<String, Object>();
    freshness.put("type", "hash");
    return EtlPipelineConfig.builder()
        .name(NAME)
        .source(HttpSourceConfig.builder().url("https://example.invalid/api/{year}").build())
        .dimensions(dims)
        .freshness(FreshnessConfig.fromMap(freshness))
        .lookbackPeriods(6)
        .materialize(MaterializeConfig.builder()
            .enabled(true)
            .format(MaterializeConfig.Format.ICEBERG)
            .name(NAME)
            .targetTableId(NAME)
            .output(MaterializeOutputConfig.builder().build())
            .columns(columns)
            .partition(MaterializePartitionConfig.builder().columns(partitionColumns).build())
            .iceberg(MaterializeConfig.IcebergConfig.builder()
                .catalogType(MaterializeConfig.IcebergConfig.CatalogType.HADOOP)
                .warehousePath(warehouseDir.getAbsolutePath())
                .namespace("default")
                .overwritePartitions(true)
                .build())
            .build())
        .build();
  }

  /** One row per year, identical on every run, so an unchanged-source gate would skip it. */
  private static DataProvider rowPerYear() {
    return new DataProvider() {
      @Override public Iterator<Map<String, Object>> fetch(
          EtlPipelineConfig config, Map<String, String> variables) {
        Map<String, Object> row = new HashMap<String, Object>();
        row.put("id", 1);
        row.put("year", Integer.valueOf(variables.get("year")));
        List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
        rows.add(row);
        return rows.iterator();
      }
    };
  }

  private static long totalRecords(File warehouseDir) {
    Table table = new HadoopTables(new Configuration())
        .load(new File(warehouseDir, NAME).getAbsolutePath());
    return Long.parseLong(table.currentSnapshot().summary().get("total-records"));
  }

  @Test void recreatedTableIsRefilledDespiteUnchangedSource() throws IOException {
    File warehouseDir = new File(tempDir, "warehouse");
    warehouseDir.mkdirs();
    MemoryTracker tracker = new MemoryTracker();

    new EtlPipeline(config(warehouseDir, 2021, 2022, Arrays.asList("year")),
        new LocalFileStorageProvider(), warehouseDir.getAbsolutePath(), null, tracker,
        rowPerYear(), null).execute();
    assertEquals(2, totalRecords(warehouseDir));

    // A new partition column forces a drop-and-recreate; 2021 and 2022 are unchanged upstream.
    EtlResult result =
        new EtlPipeline(config(warehouseDir, 2021, 2023, Arrays.asList("year", "id")),
            new LocalFileStorageProvider(), warehouseDir.getAbsolutePath(), null, tracker,
            rowPerYear(), null).execute();

    assertEquals(3, result.getTotalRows());
    assertEquals(3, totalRecords(warehouseDir));
  }
}
