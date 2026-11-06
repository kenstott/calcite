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
package org.apache.calcite.adapter.govdata.etl;

import org.apache.calcite.adapter.file.etl.ColumnConfig;
import org.apache.calcite.adapter.file.etl.EtlPipelineConfig;
import org.apache.calcite.adapter.file.etl.MaterializeConfig;
import org.apache.calcite.adapter.file.etl.MaterializePartitionConfig;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;

import java.io.InputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Each govdata table writes its partition columns as the types it declares them, which are the
 * types the DuckDB engine serves.
 *
 * <p>Readers serve a partition column as its {@code partitions.columnDefinitions} type; the
 * writer types it from the materialize block, else from a data column of the same name. When
 * the three disagreed, 211 tables wrote their INTEGER year as text (fiscal.usaspending_by_county
 * among them). A table's materialize block now takes the declared partition types; this holds
 * every schema to them, and every data column of a partition's name to its type.
 */
@Tag("unit")
public class DeclaredPartitionTypesTest {

  private static final Set<String> INTEGER_TYPES =
      Set.of("TINYINT", "SMALLINT", "INT", "INTEGER", "BIGINT", "LONG");

  /** The type a declared type name means here: integer names are one type, others themselves. */
  private static String kind(String type) {
    String upper = type.toUpperCase(Locale.ROOT);
    if (INTEGER_TYPES.contains(upper)) {
      return "integer";
    }
    return "STRING".equals(upper) ? "VARCHAR" : upper;
  }

  private static List<Path> schemaFiles() throws Exception {
    URL fiscal = DeclaredPartitionTypesTest.class.getResource("/fiscal/fiscal-schema.yaml");
    Path resources = Paths.get(fiscal.toURI()).getParent().getParent();
    try (Stream<Path> dirs = Files.list(resources)) {
      List<Path> files = new ArrayList<>();
      for (Path dir : dirs.filter(Files::isDirectory).collect(Collectors.toList())) {
        try (Stream<Path> f = Files.list(dir)) {
          f.filter(p -> p.getFileName().toString().endsWith("-schema.yaml")).forEach(files::add);
        }
      }
      return files;
    }
  }

  @SuppressWarnings("unchecked")
  private static Map<String, Object> load(Path file) throws Exception {
    try (InputStream in = Files.newInputStream(file)) {
      // SnakeYAML expands the schemas' merge keys and anchors, which Jackson does not.
      LoaderOptions options = new LoaderOptions();
      options.setMaxAliasesForCollections(100000);
      return new Yaml(new SafeConstructor(options)).load(in);
    }
  }

  @SuppressWarnings("unchecked")
  @Test void everyTableWritesItsPartitionColumnsAsItDeclaresThem() throws Exception {
    List<Path> files = schemaFiles();
    assertEquals(29, files.size(), "every govdata schema yaml is checked: " + files);
    List<String> disagreements = new ArrayList<>();
    int checked = 0;
    for (Path file : files) {
      Map<String, Object> schema = load(file);
      Object tables = schema.get("partitionedTables");
      if (!(tables instanceof List)) {
        continue;
      }
      for (Object t : (List<Object>) tables) {
        Map<String, Object> table = (Map<String, Object>) t;
        String where = file.getFileName() + " " + table.get("name");
        Map<String, Object> partitions = (Map<String, Object>) table.get("partitions");
        if (partitions == null || !(partitions.get("columnDefinitions") instanceof List)) {
          continue;
        }
        MaterializeConfig materialize;
        try {
          materialize = EtlPipelineConfig.materializeFromTableMap(table);
        } catch (IllegalArgumentException e) {
          disagreements.add(where + ": " + e.getMessage());
          continue;
        }
        if (materialize == null || materialize.getPartition() == null) {
          continue;
        }
        List<MaterializePartitionConfig.ColumnDefinition> written =
            materialize.getPartition().getColumnDefinitions();
        for (Object d : (List<Object>) partitions.get("columnDefinitions")) {
          Map<String, Object> declared = (Map<String, Object>) d;
          String name = (String) declared.get("name");
          String type = (String) declared.get("type");
          if (name == null || type == null) {
            continue;
          }
          checked++;
          String writtenType = null;
          for (MaterializePartitionConfig.ColumnDefinition w : written) {
            if (w.getName().equalsIgnoreCase(name)) {
              writtenType = w.getType();
            }
          }
          if (writtenType == null || !kind(writtenType).equals(kind(type))) {
            disagreements.add(where + "." + name + ": declared " + type + ", written "
                + writtenType);
          }
          List<ColumnConfig> columns = materialize.getColumns();
          if (columns == null) {
            continue;
          }
          for (ColumnConfig column : columns) {
            if (column.getName() != null && column.getName().equalsIgnoreCase(name)
                && column.getType() != null && !kind(column.getType()).equals(kind(type))) {
              disagreements.add(where + "." + name + ": declared partition " + type
                  + ", data column " + column.getType());
            }
          }
        }
      }
    }
    assertTrue(checked > 500, "partition columns checked: " + checked);
    assertEquals(List.of(), disagreements);
  }
}
