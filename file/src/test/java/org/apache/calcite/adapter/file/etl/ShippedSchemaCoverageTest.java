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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Asserts what data years the shipped EIA configs actually reach.
 *
 * <p>These tables expressed a roughly two-month publication lag as a two-<em>year</em> lag, which
 * silently capped them about two years short of what the source publishes. Nothing failed: the ETL
 * succeeded, the partitions were well-formed, and the most recent data simply was not requested.
 * A defect with no symptom needs a test that states the expected reach, or it returns quietly the
 * next time someone rounds a lag up to whole years.
 *
 * <p>Runs against the real schema files at a pinned date, so it exercises the shipped configuration
 * rather than a fixture, and does so deterministically.
 */
@Tag("unit")
@Execution(ExecutionMode.SAME_THREAD)
@ResourceLock(PipelineClockTest.CLOCK_LOCK)
public class ShippedSchemaCoverageTest {

  /** Tables converted from a year-granular lag to a month-granular one. */
  private static final List<String> MONTH_LAG_TABLES = Arrays.asList(
      "eia_electricity_generation", "eia_electricity_prices", "eia_fossil_fuel_production",
      "eia_refinery_operations", "eia_crude_oil_imports");

  @AfterEach void unpin() {
    PipelineClock.clearOverride();
  }

  private static File energySchema() {
    File dir = new File(System.getProperty("user.dir")).getAbsoluteFile();
    for (int up = 0; dir != null && up < 6; up++, dir = dir.getParentFile()) {
      for (String candidate : new String[] {
          "govdata/src/main/resources/energy/energy-schema.yaml",
          "src/main/resources/energy/energy-schema.yaml"}) {
        File f = new File(dir, candidate);
        if (f.isFile()) {
          return f;
        }
      }
    }
    return null;
  }

  @SuppressWarnings("unchecked")
  private static Map<String, Object> tableConfig(String name) throws Exception {
    File file = energySchema();
    assertNotNull(file, "energy schema not found");
    LoaderOptions options = new LoaderOptions();
    options.setMaxAliasesForCollections(10000);
    Map<String, Object> schema;
    try (InputStream in = new FileInputStream(file)) {
      schema = (Map<String, Object>) new Yaml(options).load(in);
    }
    for (Object entry : (List<Object>) schema.get("partitionedTables")) {
      Map<String, Object> table = (Map<String, Object>) entry;
      if (name.equals(table.get("name"))) {
        return table;
      }
    }
    return null;
  }

  /** The data years the table's year dimension resolves to, at whatever date is pinned. */
  @SuppressWarnings("unchecked")
  private static List<String> dataYears(String tableName) throws Exception {
    Map<String, Object> table = tableConfig(tableName);
    assertNotNull(table, tableName + " not found in the energy schema");
    Map<String, Object> dims = (Map<String, Object>) table.get("dimensions");
    Map<String, Object> yearSpec = (Map<String, Object>) dims.get("year");

    Map<String, DimensionConfig> only = new LinkedHashMap<String, DimensionConfig>();
    only.put("year", DimensionConfig.fromMap("year", yearSpec));

    List<String> out = new ArrayList<String>();
    for (Map<String, String> combo : new DimensionIterator().expand(only)) {
      out.add(combo.containsKey("effective_year")
          ? combo.get("effective_year") : combo.get("year"));
    }
    return out;
  }

  /**
   * The regression this exists for: at a mid-year date these tables must reach the current and
   * prior data year. A year-granular lag reaches neither, which is the ~2 years that were missing.
   */
  @Test void monthLagTablesReachTheCurrentAndPriorDataYear() throws Exception {
    PipelineClock.setOverrideForTest(LocalDate.of(2026, 8, 15));
    for (String table : MONTH_LAG_TABLES) {
      List<String> years = dataYears(table);
      assertTrue(years.contains("2026"),
          table + " must reach 2026 data; got " + years.subList(0, Math.min(4, years.size())));
      assertTrue(years.contains("2025"),
          table + " must reach 2025 data; got " + years.subList(0, Math.min(4, years.size())));
    }
  }

  /**
   * In January the same lag lands in the prior year, so the current year is correctly absent
   * rather than being requested before anything is published.
   */
  @Test void inJanuaryTheLagLandsInThePriorYear() throws Exception {
    PipelineClock.setOverrideForTest(LocalDate.of(2026, 1, 15));
    for (String table : MONTH_LAG_TABLES) {
      List<String> years = dataYears(table);
      assertTrue(years.contains("2025"), table + " must reach 2025 in January");
      assertFalse(years.contains("2026"),
          table + " must not request 2026 in January — nothing is published for it yet");
    }
  }

  /**
   * Guards the shape rather than the value: these must declare a month-granular lag and must not
   * also declare a year-granular one, which the config parser refuses as ambiguous.
   */
  @SuppressWarnings("unchecked")
  @Test void monthLagTablesDeclareExactlyOneLag() throws Exception {
    for (String table : MONTH_LAG_TABLES) {
      Map<String, Object> config = tableConfig(table);
      Map<String, Object> year =
          (Map<String, Object>) ((Map<String, Object>) config.get("dimensions")).get("year");
      assertTrue(year.containsKey("dataMonthLag"),
          table + " must declare dataMonthLag");
      assertFalse(year.containsKey("dataLag"),
          table + " must not declare both lags — the parser refuses it as ambiguous");
    }
  }
}
