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
package org.apache.calcite.adapter.govdata.ref;

import org.apache.calcite.adapter.file.etl.EtlPipelineConfig;
import org.apache.calcite.adapter.file.etl.MaterializeConfig;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The calendar provider emits the whole date spine on every run, so the write has to replace the
 * table. An appending write stays at one row per day only while a high-water mark held outside the
 * table is present and readable.
 */
@Tag("unit")
public class CalendarWriteModeTest {

  @SuppressWarnings("unchecked")
  private static Map<String, Object> calendarTable() throws IOException {
    LoaderOptions options = new LoaderOptions();
    options.setMaxAliasesForCollections(1000);
    options.setCodePointLimit(10 * 1024 * 1024);
    try (InputStream in =
        CalendarWriteModeTest.class.getResourceAsStream("/ref/ref-schema.yaml")) {
      assertNotNull(in, "ref-schema.yaml must be on the classpath");
      Map<String, Object> schema = new Yaml(options).load(in);
      for (Map<String, Object> table
          : (List<Map<String, Object>>) schema.get("partitionedTables")) {
        if ("calendar".equals(table.get("name"))) {
          return table;
        }
      }
    }
    throw new AssertionError("ref-schema.yaml declares no calendar table");
  }

  @Test void everyRunReplacesTheSpine() throws IOException {
    Map<String, Object> table = calendarTable();
    assertEquals("snapshot", table.get("dataset_type"));
    MaterializeConfig materialize = EtlPipelineConfig.materializeFromTableMap(table);
    assertNotNull(materialize);
    assertTrue(materialize.getIceberg().isReplacingPartitionsThisRun(),
        "calendar must replace its partition, not append to it");
  }
}
