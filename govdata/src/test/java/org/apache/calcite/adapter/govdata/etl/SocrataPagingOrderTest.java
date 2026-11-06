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
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Every Socrata source a govdata table pages by offset names a {@code $order}.
 *
 * <p>Socrata returns pages in a stable order only when the request orders them. Without one,
 * paging health.cdc_county_injury_mortality by {@code $offset} returned 1,346 of its 131,934
 * rows twice and left out 1,346 others, so the published table held a second Van Wert County,
 * Ohio row for 2023 overdose deaths; with {@code $order=:id} every row came back once.
 */
@Tag("unit")
public class SocrataPagingOrderTest {

  private static List<Path> schemaFiles() throws Exception {
    URL fiscal = SocrataPagingOrderTest.class.getResource("/fiscal/fiscal-schema.yaml");
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
      LoaderOptions options = new LoaderOptions();
      options.setMaxAliasesForCollections(100000);
      return new Yaml(new SafeConstructor(options)).load(in);
    }
  }

  @SuppressWarnings("unchecked")
  private static Map<String, Object> map(Object o) {
    return o instanceof Map ? (Map<String, Object>) o : null;
  }

  @SuppressWarnings("unchecked")
  @Test void everyOffsetPagedSocrataSourceIsOrdered() throws Exception {
    List<String> unordered = new ArrayList<>();
    int paged = 0;
    for (Path file : schemaFiles()) {
      Object tables = load(file).get("partitionedTables");
      if (!(tables instanceof List)) {
        continue;
      }
      for (Object t : (List<Object>) tables) {
        Map<String, Object> table = map(t);
        Map<String, Object> source = table == null ? null : map(table.get("source"));
        Object url = source == null ? null : source.get("url");
        Map<String, Object> response = source == null ? null : map(source.get("response"));
        Map<String, Object> pagination = response == null ? null : map(response.get("pagination"));
        if (!(url instanceof String) || !((String) url).contains("/resource/")
            || pagination == null
            || !"OFFSET".equals(String.valueOf(pagination.get("type")).toUpperCase(Locale.ROOT))) {
          continue;
        }
        paged++;
        Map<String, Object> parameters = map(source.get("parameters"));
        boolean ordered = ((String) url).contains("$order=")
            || parameters != null && parameters.get("$order") != null;
        if (!ordered) {
          unordered.add(file.getFileName() + " " + table.get("name"));
        }
      }
    }
    assertTrue(paged >= 14, "offset-paged Socrata sources checked: " + paged);
    assertEquals(List.of(), unordered, "offset-paged Socrata sources with no $order");
  }
}
