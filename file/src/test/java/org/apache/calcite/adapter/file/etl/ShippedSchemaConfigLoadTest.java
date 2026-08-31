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

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Loads every table in every shipped schema through the real config parser.
 *
 * <p>A YAML file can parse cleanly and still be rejected by the parser it feeds — a superseded key,
 * a value out of range, or two mutually exclusive settings declared together all survive
 * {@code snakeyaml} and fail only when a pipeline is actually built. Checking the YAML alone
 * therefore proves less than it appears to, and the gap is exactly where a bulk config migration
 * goes wrong: every file still parses, and the schema stops loading.
 *
 * <p>This walks all {@code *-schema.yaml} resources shipped by the govdata adapter and constructs
 * an {@link EtlPipelineConfig} per table, failing with the offending schema, table and reason
 * rather than a single opaque error. It lives here, beside the parser it exercises, so it is not
 * blocked by unrelated compilation problems in the schema module's own test sources.
 */
@Tag("unit")
public class ShippedSchemaConfigLoadTest {

  /**
   * Locates the shipped schema files on disk. They are read as files rather than classpath
   * resources so the test reports which source file is at fault, which is what a fix needs.
   */
  private static List<File> schemaFiles() {
    // Tests do not run from a fixed working directory, so walk up from wherever this JVM started
    // until the module's resources are found rather than assuming a relative path.
    File dir = new File(System.getProperty("user.dir")).getAbsoluteFile();
    for (int up = 0; dir != null && up < 6; up++, dir = dir.getParentFile()) {
      for (String candidate : new String[] {
          "govdata/src/main/resources", "src/main/resources"}) {
        File root = new File(dir, candidate);
        if (root.isDirectory()) {
          List<File> found = new ArrayList<File>();
          collect(root, found);
          if (!found.isEmpty()) {
            return found;
          }
        }
      }
    }
    return new ArrayList<File>();
  }

  /**
   * A loader that tolerates the schemas' heavy use of anchors. They share year-range and iceberg
   * blocks across many tables by alias, which exceeds snakeyaml's conservative default.
   */
  private static Yaml loader() {
    LoaderOptions options = new LoaderOptions();
    options.setMaxAliasesForCollections(10000);
    return new Yaml(options);
  }

  private static void collect(File dir, List<File> out) {
    File[] entries = dir.listFiles();
    if (entries == null) {
      return;
    }
    for (File f : entries) {
      if (f.isDirectory()) {
        collect(f, out);
      } else if (f.getName().endsWith("-schema.yaml")) {
        out.add(f);
      }
    }
  }

  /**
   * Supplies the calendar context the schema factory normally sets before any schema is read.
   * Several schemas resolve a year through it, so without these a standalone load is not
   * representative of production.
   */
  private static void seedCalendarContext() {
    java.time.LocalDate today = java.time.LocalDate.now();
    setIfAbsent("GOVDATA_CURRENT_YEAR", String.valueOf(today.getYear()));
    setIfAbsent("GOVDATA_CURRENT_MONTH", String.format("%02d", today.getMonthValue()));
    setIfAbsent("GOVDATA_CURRENT_QUARTER", String.valueOf((today.getMonthValue() - 1) / 3 + 1));
  }

  private static void setIfAbsent(String key, String value) {
    if (System.getProperty(key) == null) {
      System.setProperty(key, value);
    }
  }

  @SuppressWarnings("unchecked")
  @Test void everyTableInEverySchemaBuildsAPipelineConfig() throws Exception {
    seedCalendarContext();
    List<File> files = schemaFiles();
    assertTrue(files.size() >= 20,
        "expected the shipped schemas to be found, got " + files.size());

    List<String> failures = new ArrayList<String>();
    int tables = 0;

    for (File file : files) {
      Map<String, Object> schema;
      try (InputStream in = new FileInputStream(file)) {
        schema = (Map<String, Object>) loader().load(in);
      }
      if (schema == null) {
        continue;
      }
      Object tablesObj = schema.get("partitionedTables");
      if (!(tablesObj instanceof List)) {
        continue;
      }
      for (Object entry : (List<Object>) tablesObj) {
        if (!(entry instanceof Map)) {
          continue;
        }
        Map<String, Object> table = (Map<String, Object>) entry;
        Object name = table.get("name");
        // Only tables that actually declare a fetch are pipelines; the rest are view-like
        // definitions the ETL never builds a config for.
        if (!(table.get("source") instanceof Map)) {
          continue;
        }
        tables++;
        try {
          EtlPipelineConfig.fromMap(new LinkedHashMap<String, Object>(table));
        } catch (IllegalArgumentException e) {
          // A fixed-width layout lives in the schema module's resources, which are not on this
          // module's test classpath. That is a module boundary, not a config defect.
          if (e.getMessage() != null && e.getMessage().contains("columnsResource not found")) {
            continue;
          }
          failures.add(file.getName() + " / " + name + ": " + e.getMessage());
        } catch (RuntimeException e) {
          failures.add(file.getName() + " / " + name + ": "
              + e.getClass().getSimpleName() + ": " + e.getMessage());
        }
      }
    }

    assertTrue(tables > 100, "expected to exercise the full table set, got " + tables);
    if (!failures.isEmpty()) {
      fail("These tables do not build a pipeline config (" + failures.size() + " of " + tables
          + "):\n  " + String.join("\n  ", failures));
    }
  }

  /**
   * Guards the lookback's own validity across the shipped schemas: the value must be positive, and
   * it must only appear on tables that are period-tracked, since a table with no canonical period
   * slot has nothing for it to count and the setting would sit there looking effective.
   */
  @SuppressWarnings("unchecked")
  @Test void everyDeclaredLookbackIsUsable() throws Exception {
    List<String> slots = Arrays.asList("year", "quarter", "month", "week", "day", "day_of_week");
    List<String> problems = new ArrayList<String>();

    for (File file : schemaFiles()) {
      Map<String, Object> schema;
      try (InputStream in = new FileInputStream(file)) {
        schema = (Map<String, Object>) loader().load(in);
      }
      if (schema == null || !(schema.get("partitionedTables") instanceof List)) {
        continue;
      }
      for (Object entry : (List<Object>) schema.get("partitionedTables")) {
        if (!(entry instanceof Map)) {
          continue;
        }
        Map<String, Object> table = (Map<String, Object>) entry;
        Object lookback = table.get("lookbackPeriods");
        if (lookback == null) {
          continue;
        }
        String name = file.getName() + " / " + table.get("name");
        if (!(lookback instanceof Number) || ((Number) lookback).intValue() < 1) {
          problems.add(name + ": lookbackPeriods must be a positive integer, got " + lookback);
          continue;
        }
        Object dims = table.get("dimensions");
        boolean periodTracked = false;
        if (dims instanceof Map) {
          for (String slot : slots) {
            if (((Map<String, Object>) dims).containsKey(slot)) {
              periodTracked = true;
              break;
            }
          }
        }
        if (!periodTracked) {
          problems.add(name + ": declares lookbackPeriods but has no canonical period dimension, "
              + "so there are no periods for it to count");
        }
      }
    }

    if (!problems.isEmpty()) {
      fail("Unusable lookback declarations:\n  " + String.join("\n  ", problems));
    }
  }
}
