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
package org.apache.calcite.adapter.govdata;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Raw-cache keys built from dimensions must not recur: a cached response is immutable, so a key
 * that comes round again replays the old response. The bare calendar month recurs every year
 * (and a run may pin it), so a dimension may use it only beside a current-year dimension; a
 * month-grain cache buster otherwise takes {@code GOVDATA_CURRENT_MONTH_TOKEN} (yyyy-MM).
 */
@Tag("unit")
public class CacheBusterDimensionTest {

  private static final String BARE_MONTH = "${GOVDATA_CURRENT_MONTH";
  private static final String MONTH_TOKEN = "${GOVDATA_CURRENT_MONTH_TOKEN";
  private static final String CURRENT_YEAR = "${GOVDATA_CURRENT_YEAR";

  private static List<Path> schemaYamls() throws Exception {
    Path root = Paths.get(
        CacheBusterDimensionTest.class.getResource("/econ/econ-schema.yaml").toURI())
        .getParent().getParent();
    try (Stream<Path> s = Files.walk(root)) {
      return s.filter(p -> p.toString().endsWith("-schema.yaml")).collect(Collectors.toList());
    }
  }

  /** Every table map (has name and dimensions) anywhere in the document. */
  @SuppressWarnings("unchecked")
  private static void tables(Object node, List<Map<String, Object>> out) {
    if (node instanceof Map) {
      Map<String, Object> m = (Map<String, Object>) node;
      if (m.get("name") instanceof String && m.get("dimensions") instanceof Map) {
        out.add(m);
      }
      for (Object v : m.values()) {
        tables(v, out);
      }
    } else if (node instanceof List) {
      for (Object v : (List<Object>) node) {
        tables(v, out);
      }
    }
  }

  private static List<Map<String, Object>> allTables() throws Exception {
    LoaderOptions opts = new LoaderOptions();
    opts.setMaxAliasesForCollections(1000);
    List<Map<String, Object>> out = new ArrayList<>();
    for (Path y : schemaYamls()) {
      try (InputStream in = Files.newInputStream(y)) {
        tables(new Yaml(opts).load(in), out);
      }
    }
    assertTrue(out.size() > 100, "expected the schema tables, found " + out.size());
    return out;
  }

  @Test void bareCurrentMonthNeedsACurrentYearDimension() throws Exception {
    List<String> recurring = new ArrayList<>();
    for (Map<String, Object> table : allTables()) {
      String dimensions = String.valueOf(table.get("dimensions"));
      boolean bare = dimensions.replace(MONTH_TOKEN, "").contains(BARE_MONTH);
      if (bare && !dimensions.contains(CURRENT_YEAR)) {
        recurring.add((String) table.get("name"));
      }
    }
    assertEquals(new ArrayList<String>(), recurring,
        "tables whose cache key repeats every year");
  }

  @SuppressWarnings("unchecked")
  @Test void rawCacheKeyVarsNameDimensionsOfTheTable() throws Exception {
    List<String> unknown = new ArrayList<>();
    int checked = 0;
    for (Map<String, Object> table : allTables()) {
      Object source = table.get("source");
      if (!(source instanceof Map)) {
        continue;
      }
      Object rawCache = ((Map<String, Object>) source).get("rawCache");
      if (!(rawCache instanceof Map)) {
        continue;
      }
      Object keyVars = ((Map<String, Object>) rawCache).get("keyVars");
      if (!(keyVars instanceof List)) {
        continue;
      }
      checked++;
      Map<String, Object> dimensions = (Map<String, Object>) table.get("dimensions");
      for (Object key : (List<Object>) keyVars) {
        // effective_year is derived from a year dimension rather than declared
        if (!"effective_year".equals(key) && !dimensions.containsKey(String.valueOf(key))) {
          unknown.add(table.get("name") + "." + key);
        }
      }
    }
    assertTrue(checked >= 3, "expected tables with rawCache.keyVars, found " + checked);
    assertEquals(new ArrayList<String>(), unknown, "keyVars without a matching dimension");
  }
}
