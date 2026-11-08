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
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The column-comment code-list convention: a comment may end with
 * {@code Codes: A = meaning; B = meaning; ...}, one entry per code, so SQL users read the codes
 * as the column comment and a text index can split them into one entry each. Every such section
 * must parse cleanly, and econ.fred_indicators must list exactly the series it ingests.
 */
@Tag("unit")
public class ColumnCodesConventionTest {

  private static final String MARKER = "Codes:";

  private static Object load(InputStream in) {
    LoaderOptions opts = new LoaderOptions();
    opts.setMaxAliasesForCollections(1000);
    return new Yaml(opts).load(in);
  }

  /** Every column map (has name + comment) anywhere in the document, with its table name. */
  @SuppressWarnings("unchecked")
  private static void columns(Object node, String table, List<String[]> out) {
    if (node instanceof Map) {
      Map<String, Object> m = (Map<String, Object>) node;
      Object cols = m.get("columns");
      String t = m.get("name") instanceof String && cols instanceof List
          ? (String) m.get("name") : table;
      if (cols instanceof List) {
        for (Object c : (List<Object>) cols) {
          if (c instanceof Map && ((Map<String, Object>) c).get("comment") != null) {
            Map<String, Object> cm = (Map<String, Object>) c;
            out.add(new String[]{t, String.valueOf(cm.get("name")),
                String.valueOf(cm.get("comment"))});
          }
        }
      }
      for (Object v : m.values()) {
        columns(v, t, out);
      }
    } else if (node instanceof List) {
      for (Object v : (List<Object>) node) {
        columns(v, table, out);
      }
    }
  }

  /** The {@code [code, meaning]} entries of a comment's Codes section, failing on a bad one. */
  private static List<String[]> parse(String where, String comment) {
    String list = comment.substring(comment.indexOf(MARKER) + MARKER.length()).trim();
    if (list.endsWith(".")) {
      list = list.substring(0, list.length() - 1);
    }
    List<String[]> out = new ArrayList<>();
    for (String entry : list.split(";")) {
      int eq = entry.indexOf(" = ");
      assertTrue(eq > 0, where + ": entry without ' = ': '" + entry.trim() + "'");
      String code = entry.substring(0, eq).trim();
      String meaning = entry.substring(eq + 3).trim();
      assertTrue(!code.isEmpty() && !code.contains(" "), where + ": bad code '" + code + "'");
      assertTrue(meaning.length() > 3, where + ": thin meaning for " + code);
      out.add(new String[]{code, meaning});
    }
    return out;
  }

  @Test void everyCodesSectionParses() throws Exception {
    // The schema YAMLs' directory on the test classpath (build/resources/main), found from one
    // known resource, so the test does not depend on the working directory.
    Path root = Paths.get(getClass().getResource("/econ/econ-schema.yaml").toURI())
        .getParent().getParent();
    List<Path> yamls;
    try (Stream<Path> s = Files.walk(root)) {
      yamls = s.filter(p -> p.toString().endsWith("-schema.yaml")).collect(Collectors.toList());
    }
    assertTrue(yamls.size() > 10, "expected the schema YAMLs under " + root.toAbsolutePath());
    int sections = 0;
    for (Path y : yamls) {
      List<String[]> cols = new ArrayList<>();
      try (InputStream in = Files.newInputStream(y)) {
        columns(load(in), null, cols);
      }
      for (String[] c : cols) {
        if (c[2].contains(MARKER)) {
          sections++;
          assertTrue(parse(y.getFileName() + " " + c[0] + "." + c[1], c[2]).size() > 0);
        }
      }
    }
    assertTrue(sections > 0, "no column uses the Codes: convention");
  }

  @SuppressWarnings("unchecked")
  @Test void fredIndicatorsListsExactlyTheSeriesItIngests() throws Exception {
    Object doc;
    try (InputStream in = getClass().getResourceAsStream("/econ/econ-schema.yaml")) {
      assertNotNull(in);
      doc = load(in);
    }
    Map<String, Object> table = find(doc);
    assertNotNull(table, "fred_indicators not found");
    List<Object> series = (List<Object>) ((Map<String, Object>) table.get("dimensions"))
        .get("series");
    String comment = null;
    for (Object c : (List<Object>) table.get("columns")) {
      if (c instanceof Map && "series".equals(((Map<String, Object>) c).get("name"))) {
        comment = String.valueOf(((Map<String, Object>) c).get("comment"));
      }
    }
    assertNotNull(comment);
    assertTrue(comment.contains(MARKER), "fred_indicators.series has no Codes: section");
    TreeSet<String> listed = new TreeSet<>();
    for (String[] e : parse("fred_indicators.series", comment)) {
      listed.add(e[0]);
    }
    TreeSet<String> ingested = new TreeSet<>();
    for (Object s : series) {
      ingested.add(String.valueOf(s));
    }
    assertEquals(ingested, listed,
        "fred_indicators.series Codes: must list exactly the series in fred_series_list");
  }

  @SuppressWarnings("unchecked")
  private static Map<String, Object> find(Object node) {
    if (node instanceof Map) {
      Map<String, Object> m = (Map<String, Object>) node;
      if ("fred_indicators".equals(m.get("name")) && m.get("columns") instanceof List) {
        return m;
      }
      for (Object v : m.values()) {
        Map<String, Object> r = find(v);
        if (r != null) {
          return r;
        }
      }
    } else if (node instanceof List) {
      for (Object v : (List<Object>) node) {
        Map<String, Object> r = find(v);
        if (r != null) {
          return r;
        }
      }
    }
    return null;
  }
}
